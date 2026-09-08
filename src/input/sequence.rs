use super::{
    action::{
        Action, CSUnary, CStream, CallId, Message, MessageConnection, SStream, Stream,
        StreamAction, StreamType,
    },
    error::{ActionError, ActionSequenceError},
    poor::PAction,
};
use prost_reflect::{DescriptorPool, DynamicMessage, MethodDescriptor, ReflectMessage};
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};

fn get_stream<'a, R>(actions: &'a R, interval: &StreamInterval) -> Option<&'a Stream>
where
    R: Deref<Target = Vec<Action>>,
{
    match &actions[interval.start] {
        Action::Stream(StreamAction::Start(stream)) => Some(stream),
        _ => None,
    }
}

#[derive(Debug)]
pub(super) struct StreamMessageFactory<R> {
    interval: StreamInterval,
    actions: R,
}

impl<R> StreamMessageFactory<R>
where
    R: DerefMut<Target = Vec<Action>>,
{
    pub(super) fn message(
        mut self,
        payload: DynamicMessage,
        index: usize,
    ) -> Result<(), ActionError> {
        let stream =
            get_stream(&self.actions, &self.interval).ok_or(ActionError::InvalidStreamInterval)?;

        if !matches!(stream.ty, StreamType::Client(_)) {
            return Err(ActionError::StreamIsNotClient);
        }
        if stream.ty.method().input() != payload.descriptor() {
            return Err(ActionError::MessageCallDescriptorMismatch);
        }
        if self.interval.start >= index || self.interval.end < index {
            return Err(ActionError::InvalidMessageIdx);
        }
        let stream_id = stream.id;

        self.actions.insert(
            index,
            Message {
                con: MessageConnection::Stream(stream_id),
                payload,
            }
            .into(),
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct StreamInterval {
    start: usize,
    end: usize,
}

#[derive(Debug)]
pub(super) struct StreamsView<R> {
    intervals: HashMap<CallId, StreamInterval>,
    actions: R,
}

impl<R> StreamsView<R>
where
    R: DerefMut<Target = Vec<Action>>,
{
    pub(super) fn into_message_factory(self, id: &CallId) -> Option<StreamMessageFactory<R>> {
        Some(StreamMessageFactory {
            interval: *self.intervals.get(id)?,
            actions: self.actions,
        })
    }
}

impl<R> StreamsView<R>
where
    R: Deref<Target = Vec<Action>>,
{
    pub(super) fn new(actions: R) -> Result<Self, ActionSequenceError> {
        let mut stream_indices: HashMap<CallId, (usize, Option<usize>)> = HashMap::new();

        for (index, action) in actions.iter().enumerate() {
            match action {
                Action::Stream(StreamAction::Start(stream)) => {
                    if stream_indices.insert(stream.id, (index, None)).is_some() {
                        return Err(ActionSequenceError {
                            kind: ActionError::DuplicateStreamStart,
                            index,
                        });
                    }
                }
                Action::Stream(StreamAction::End(id)) => {
                    let Some((_, end)) = stream_indices.get_mut(id) else {
                        return Err(ActionSequenceError {
                            kind: ActionError::StreamEndWithoutStart,
                            index,
                        });
                    };

                    if end.replace(index).is_some() {
                        return Err(ActionSequenceError {
                            kind: ActionError::DuplicateStreamEnd,
                            index,
                        });
                    }
                }
                _ => {}
            }
        }

        if let Some((_, (start, _))) = stream_indices
            .iter()
            .filter(|(_, (_, end))| end.is_none())
            .min_by_key(|(_, (start, _))| *start)
        {
            return Err(ActionSequenceError {
                kind: ActionError::MissingStreamEnd,
                index: *start,
            });
        }

        let intervals = stream_indices
            .into_iter()
            .map(|(id, (start, end))| {
                (
                    id,
                    StreamInterval {
                        start,
                        end: end.expect("missing stream ends were checked above"),
                    },
                )
            })
            .collect();

        Ok(Self { actions, intervals })
    }

    pub(super) fn intervals(&self) -> &HashMap<CallId, StreamInterval> {
        &self.intervals
    }

    pub(super) fn streams(&self) -> HashMap<CallId, &Stream> {
        self.intervals
            .iter()
            .filter_map(|(id, interval)| {
                get_stream(&self.actions, interval).map(|stream| (*id, stream))
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub(super) struct ActionSequence {
    pub(super) actions: Vec<Action>,
    pub(super) last_call_id: CallId,
}

impl ActionSequence {
    pub(super) fn from_poor(
        poor: &[PAction],
        proto: &DescriptorPool,
    ) -> Result<Self, ActionSequenceError> {
        let actions = poor
            .iter()
            .enumerate()
            .map(|(index, action)| {
                Action::from_poor(action, proto).map_err(|kind| ActionSequenceError { kind, index })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let stream_view = StreamsView::new(&actions)?;
        let last_call_id = *stream_view.intervals().keys().max().unwrap_or(&CallId(0));

        Ok(Self {
            actions,
            last_call_id,
        })
    }

    pub(super) fn gen_poor(&self) -> Result<Vec<PAction>, ActionSequenceError> {
        let stream_list = self.stream_list()?;
        let streams = stream_list.streams();

        self.actions
            .iter()
            .enumerate()
            .map(|(index, action)| {
                action
                    .to_poor(&streams)
                    .map_err(|kind| ActionSequenceError { kind, index })
            })
            .collect()
    }

    pub(super) fn len(&self) -> usize {
        self.actions.len()
    }

    pub(super) fn get(&self) -> &Vec<Action> {
        &self.actions
    }

    pub(super) fn get_mut(&mut self) -> &mut Vec<Action> {
        &mut self.actions
    }

    fn allocate_call_id(&mut self) -> Option<CallId> {
        let next_call_id = self.last_call_id.0.checked_add(1)?;
        self.last_call_id = CallId(next_call_id);
        Some(self.last_call_id)
    }

    fn add_stream(
        &mut self,
        start_action: Action,
        id: CallId,
        start: usize,
        end: usize,
    ) -> Result<(), ActionError> {
        if start >= self.len() || end >= self.len() || start >= end {
            return Err(ActionError::InvalidStreamInterval);
        }

        let actions = self.get_mut();
        actions.reserve(2);
        actions.insert(end, id.into());
        actions.insert(start, start_action);
        Ok(())
    }

    pub(super) fn add_server_stream(
        &mut self,
        md: MethodDescriptor,
        payload: DynamicMessage,
        start: usize,
        end: usize,
    ) -> Result<(), ActionError> {
        let ty = StreamType::Server(SStream::new(md, payload)?);
        let id = self.allocate_call_id().ok_or(ActionError::NoCallIdsLeft)?;
        self.add_stream(Stream { ty, id }.into(), id, start, end)
    }

    pub(super) fn add_client_stream(
        &mut self,
        md: MethodDescriptor,
        start: usize,
        end: usize,
    ) -> Result<(), ActionError> {
        let ty = StreamType::Client(CStream::new(md)?);
        let id = self.allocate_call_id().ok_or(ActionError::NoCallIdsLeft)?;
        self.add_stream(Stream { ty, id }.into(), id, start, end)
    }

    pub(super) fn add_unary(
        &mut self,
        md: CSUnary,
        payload: DynamicMessage,
        index: usize,
    ) -> Result<(), ActionError> {
        if md.0.input() != payload.descriptor() {
            return Err(ActionError::MessageCallDescriptorMismatch);
        }
        if index >= self.len() {
            return Err(ActionError::InvalidMessageIdx);
        }

        self.get_mut().insert(
            index,
            Message {
                con: MessageConnection::Unary(md),
                payload,
            }
            .into(),
        );
        Ok(())
    }

    fn stream_list(&self) -> Result<StreamsView<&Vec<Action>>, ActionSequenceError> {
        StreamsView::new(self.get())
    }

    pub(super) fn stream_list_mut(
        &mut self,
    ) -> Result<StreamsView<&mut Vec<Action>>, ActionSequenceError> {
        StreamsView::new(self.get_mut())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_end_without_start_is_an_error() {
        let actions = vec![Action::Stream(StreamAction::End(CallId(1)))];

        let error = StreamsView::new(&actions).unwrap_err();
        assert!(matches!(error.kind, ActionError::StreamEndWithoutStart));
        assert_eq!(error.index, 0);
    }
}
