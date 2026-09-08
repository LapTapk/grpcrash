mod poor;

use libafl::inputs::Input;
use once_cell::sync::OnceCell;
use poor::*;
use prost_reflect::{DescriptorPool, DynamicMessage, MethodDescriptor, ReflectMessage};
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    ops::{Deref, DerefMut},
};

fn get_method_by_full_name(
    full_name: &str,
    pool: &DescriptorPool,
) -> Option<prost_reflect::MethodDescriptor> {
    let (service_name, method_name) = full_name.rsplit_once('.')?;

    let service = pool.get_service_by_name(service_name)?;

    service
        .methods()
        .find(|method| method.name() == method_name)
}

// TODO: get rid of this struct
#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct CallId(usize);

impl Into<Action> for CallId {
    fn into(self) -> Action {
        Action::Stream(StreamAction::End(self))
    }
}

impl From<usize> for CallId {
    fn from(value: usize) -> Self {
        CallId(value)
    }
}

impl Into<usize> for CallId {
    fn into(self) -> usize {
        self.0
    }
}

impl CallId {
    fn from_poor(ps_id: &PCallId) -> Self {
        Self(ps_id.0)
    }
}

#[derive(Debug, Clone)]
struct CSUnary(MethodDescriptor);

impl CSUnary {
    fn new(md: MethodDescriptor) -> Result<Self, ActionError> {
        if md.is_client_streaming() || md.is_server_streaming() {
            return Err(ActionError::InvalidMethod);
        }

        Ok(Self(md))
    }
    fn from_poor(pcsu: &PCSUnary, proto: &DescriptorPool) -> Result<Self, ActionError> {
        let name = &pcsu.0;
        Self::new(get_method_by_full_name(name, proto).ok_or(ActionError::ProtoMismatch)?)
    }
}

#[derive(Debug, Clone)]
struct CStream(MethodDescriptor);

impl CStream {
    fn new(md: MethodDescriptor) -> Result<Self, ActionError> {
        if !md.is_client_streaming() {
            return Err(ActionError::InvalidMethod);
        }
        Ok(Self(md))
    }

    fn from_poor(pcs: &PCStream, proto: &DescriptorPool) -> Result<Self, ActionError> {
        let name = &pcs.0;
        Self::new(get_method_by_full_name(name, proto).ok_or(ActionError::ProtoMismatch)?)
    }
}

#[derive(Debug, Clone)]
struct SStream {
    md: MethodDescriptor,
    payload: DynamicMessage,
}

impl SStream {
    fn new(md: MethodDescriptor, payload: DynamicMessage) -> Result<Self, ActionError> {
        if !md.is_server_streaming() {
            return Err(ActionError::InvalidMethod);
        }

        if md.input() != payload.descriptor() {
            return Err(ActionError::MessageCallDescriptorMismatch);
        }

        Ok(Self { md, payload })
    }

    fn from_poor(pss: &PSStream, proto: &DescriptorPool) -> Result<Self, ActionError> {
        let name = &pss.md;
        let md = get_method_by_full_name(name, proto).ok_or(ActionError::ProtoMismatch)?;
        let payload = DynamicMessage::decode(md.input(), pss.payload.as_slice())
            .map_err(|_| ActionError::ProtoMismatch)?;

        Self::new(md, payload)
    }
}

#[derive(Debug, Clone)]
enum StreamType {
    Client(CStream),
    Server(SStream),
}

impl StreamType {
    fn from_poor(pst: &PStreamType, proto: &DescriptorPool) -> Result<Self, ActionError> {
        Ok(match pst {
            PStreamType::Client(pcs) => Self::Client(CStream::from_poor(pcs, proto)?),
            PStreamType::Server(pss) => Self::Server(SStream::from_poor(pss, proto)?),
        })
    }
}

impl StreamType {
    fn method(&self) -> &MethodDescriptor {
        match self {
            Self::Client(s) => &s.0,
            Self::Server(s) => &s.md,
        }
    }
}

#[derive(Debug, Clone)]
struct Stream {
    ty: StreamType,
    id: CallId,
}

impl Stream {
    fn from_poor(ps: &PStream, proto: &DescriptorPool) -> Result<Self, ActionError> {
        let ty = StreamType::from_poor(&ps.ty, proto)?;
        let id = CallId::from_poor(&ps.id);
        Ok(Self { ty, id })
    }
}

impl Into<Action> for Stream {
    fn into(self) -> Action {
        Action::Stream(StreamAction::Start(self))
    }
}

#[derive(Debug, Clone)]
enum StreamAction {
    Start(Stream),
    End(CallId),
}

impl StreamAction {
    fn from_poor(psa: &PStreamAction, proto: &DescriptorPool) -> Result<Self, ActionError> {
        Ok(match psa {
            PStreamAction::StreamStart(ps) => Self::Start(Stream::from_poor(ps, proto)?),
            PStreamAction::StreamEnd(ps_id) => Self::End(CallId::from_poor(ps_id)),
        })
    }
}

impl Into<Action> for StreamAction {
    fn into(self) -> Action {
        Action::Stream(self)
    }
}

#[derive(Debug, Clone)]
enum MessageConnection {
    Stream(CallId),
    Unary(CSUnary),
}

#[derive(Debug, Clone)]
struct Message {
    con: MessageConnection,
    payload: DynamicMessage,
}

impl Into<Action> for Message {
    fn into(self) -> Action {
        Action::Message(self)
    }
}

impl Message {
    fn from_poor(pm: &PMessage, proto: &DescriptorPool) -> Result<Self, ActionError> {
        let (con, md) = match &pm.con {
            PMessageConnection::Stream(ps_id, pcs) => (
                MessageConnection::Stream(CallId::from_poor(ps_id)),
                CStream::from_poor(pcs, proto)?.0,
            ),
            PMessageConnection::Unary(pcsu) => (
                MessageConnection::Unary(CSUnary::from_poor(pcsu, proto)?),
                CSUnary::from_poor(pcsu, proto)?.0,
            ),
        };
        let payload = DynamicMessage::decode(md.input(), pm.payload.as_slice())
            .map_err(|_| ActionError::ProtoMismatch)?;

        Ok(Self { con, payload })
    }
}

#[derive(Debug, Clone)]
enum Action {
    Stream(StreamAction),
    Message(Message),
    Delay(u64),
}

impl Action {
    fn from_poor(pa: &PAction, proto: &DescriptorPool) -> Result<Self, ActionError> {
        Ok(match pa {
            PAction::Stream(ps) => StreamAction::from_poor(ps, proto)?.into(),
            PAction::Message(pm) => Message::from_poor(pm, proto)?.into(),
            PAction::Delay(del) => Self::Delay(*del),
        })
    }
}

fn get_stream<'a, R>(actions: &'a R, si: &'a StreamInterval) -> Option<&'a Stream>
where
    R: Deref<Target = Vec<Action>>,
{
    match &actions[si.s] {
        Action::Stream(StreamAction::Start(s)) => Some(s),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, Hash, serde::Serialize, serde::Deserialize)]
enum ActionError {
    InvalidMessageIdx,
    InvalidStreamId,
    InvalidStreamInterval,
    StreamIsNotClient,
    MessageCallDescriptorMismatch,
    NoCallIdsLeft,
    InvalidMethod,
    ProtoMismatch,
    DuplicateStreamEnd,
}

#[derive(Debug, Clone, Copy, Hash, serde::Serialize, serde::Deserialize)]
struct ActionSequenceError {
    kind: ActionError,
    index: usize,
}

impl std::fmt::Display for ActionSequenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} at action index {}", self.kind, self.index)
    }
}

impl std::error::Error for ActionSequenceError {}

impl std::fmt::Display for ActionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ActionError {}

#[derive(Debug)]
struct StreamMessageFactory<R> {
    si: StreamInterval,
    actions: R,
}

impl<'a, R> StreamMessageFactory<R>
where
    R: DerefMut<Target = Vec<Action>>,
{
    fn message(mut self, payload: DynamicMessage, i: usize) -> Result<(), ActionError> {
        let stream =
            get_stream(&self.actions, &self.si).ok_or(ActionError::InvalidStreamInterval)?;

        if !matches!(stream.ty, StreamType::Client(_)) {
            return Err(ActionError::StreamIsNotClient);
        }
        if stream.ty.method().input() != payload.descriptor() {
            return Err(ActionError::MessageCallDescriptorMismatch);
        }
        if self.si.s >= i || self.si.e < i {
            return Err(ActionError::InvalidMessageIdx);
        }

        let message = Message {
            con: MessageConnection::Stream(stream.id),
            payload,
        };
        self.actions.insert(i, message.into());
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct StreamInterval {
    s: usize,
    e: usize,
}

#[derive(Debug)]
struct StreamsView<R> {
    intervals: HashMap<CallId, StreamInterval>,
    actions: R,
}

impl<R> StreamsView<R>
where
    R: DerefMut<Target = Vec<Action>>,
{
    fn into_message_factory(self, id: &CallId) -> Option<StreamMessageFactory<R>> {
        Some(StreamMessageFactory {
            si: *self.intervals.get(id)?,
            actions: self.actions,
        })
    }
}

impl<R> StreamsView<R>
where
    R: Deref<Target = Vec<Action>>,
{
    fn new(actions: R) -> Result<Self, ActionSequenceError> {
        let mut streams_idxs: HashMap<CallId, (usize, Option<usize>)> = HashMap::new();

        for (i, a) in actions.iter().enumerate() {
            match a {
                Action::Stream(StreamAction::Start(s)) => {
                    streams_idxs.insert(s.id, (i, None));
                }
                Action::Stream(StreamAction::End(s_id)) => {
                    let old_end = streams_idxs
                        .get_mut(s_id)
                        .expect("Actions has ending for unstarted stream")
                        .1
                        .replace(i);
                    if !old_end.is_none() {
                        return Err(ActionSequenceError {
                            kind: ActionError::DuplicateStreamEnd,
                            index: i,
                        });
                    }
                }
                _ => {}
            }
        }

        let intervals = streams_idxs
            .iter()
            .map(|(id, (s, e))| {
                (
                    *id,
                    StreamInterval {
                        s: *s,
                        e: e.expect(&format!("Stream {:?} does not have an end action", id)),
                    },
                )
            })
            .collect();

        Ok(Self { actions, intervals })
    }

    fn intervals(&self) -> &HashMap<CallId, StreamInterval> {
        &self.intervals
    }

    fn streams(&self) -> HashMap<CallId, &Stream> {
        self.intervals
            .iter()
            .map(|(id, si)| {
                (
                    *id,
                    get_stream(&self.actions, si).expect("invalid stream interval"),
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
struct ActionSequence {
    actions: Vec<Action>,
    last_call_id: CallId,
}

impl ActionSequence {
    fn from_poor(poor: &[PAction], proto: &DescriptorPool) -> Result<Self, ActionSequenceError> {
        let actions: Vec<Action> = poor
            .iter()
            .enumerate()
            .map(|(i, p)| {
                Action::from_poor(p, proto).map_err(|e| ActionSequenceError { kind: e, index: i })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let last_call_id = *StreamsView::new(&actions)?
            .intervals()
            .keys()
            .max()
            .unwrap_or(&CallId(0));

        Ok(ActionSequence {
            actions,
            last_call_id,
        })
    }

    fn gen_poor(&self) -> Result<Vec<PAction>, ActionSequenceError> {
        self.actions
            .iter()
            .map(|a| Ok(PAction::from_rich(a, &self.stream_list()?.streams())))
            .collect::<Result<Vec<_>, _>>()
    }

    fn len(&self) -> usize {
        self.actions.len()
    }

    fn get(&self) -> &Vec<Action> {
        &self.actions
    }

    fn get_mut(&mut self) -> &mut Vec<Action> {
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

        let end_action: Action = CallId::from(id).into();

        let actions = self.get_mut();
        actions.reserve(2);
        actions.insert(end, end_action);
        actions.insert(start, start_action);
        Ok(())
    }

    fn add_server_stream(
        &mut self,
        md: MethodDescriptor,
        payload: DynamicMessage,
        start: usize,
        end: usize,
    ) -> Result<(), ActionError> {
        let ty = StreamType::Server(SStream::new(md, payload)?);
        let id = self.allocate_call_id().ok_or(ActionError::NoCallIdsLeft)?;
        let stream = Stream { ty, id };
        let start_action: Action = stream.into();
        self.add_stream(start_action, id, start, end)
    }

    fn add_client_stream(
        &mut self,
        md: MethodDescriptor,
        start: usize,
        end: usize,
    ) -> Result<(), ActionError> {
        let ty = StreamType::Client(CStream::new(md)?).into();
        let id = self.allocate_call_id().ok_or(ActionError::NoCallIdsLeft)?;
        let stream = Stream { ty, id };
        let start_action: Action = stream.into();
        self.add_stream(start_action, id, start, end)
    }

    fn add_unary(
        &mut self,
        md: CSUnary,
        payload: DynamicMessage,
        i: usize,
    ) -> Result<(), ActionError> {
        if md.0.input() != payload.descriptor() {
            return Err(ActionError::MessageCallDescriptorMismatch);
        }
        if i >= self.len() {
            return Err(ActionError::InvalidMessageIdx);
        }

        let message = Message {
            con: MessageConnection::Unary(md),
            payload,
        };
        self.get_mut().insert(i, message.into());
        Ok(())
    }

    fn stream_list(&self) -> Result<StreamsView<&Vec<Action>>, ActionSequenceError> {
        StreamsView::new(self.get())
    }

    fn stream_list_mut(&mut self) -> Result<StreamsView<&mut Vec<Action>>, ActionSequenceError> {
        StreamsView::new(self.get_mut())
    }
}

#[derive(serde::Deserialize, Debug, Clone)]
struct GrpcInput {
    #[serde(skip)]
    rich: OnceCell<ActionSequence>,

    poor: Vec<PAction>,
}

impl Hash for GrpcInput {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.get_poor()
            .expect("Trying to compute hash of invalid GrpcInput")
            .hash(state);
    }
}

impl serde::Serialize for GrpcInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error as _;

        let mut state = serializer.serialize_struct("GrpcInput", 1)?;

        let poor = self
            .get_poor()
            .map_err(|e| S::Error::custom(e.to_string()))?;

        serde::ser::SerializeStruct::serialize_field(&mut state, "poor", &poor)?;

        serde::ser::SerializeStruct::end(state)
    }
}

impl GrpcInput {
    fn rich(&self, proto: &DescriptorPool) -> Result<&ActionSequence, ActionSequenceError> {
        self.rich
            .get_or_try_init(|| ActionSequence::from_poor(self.poor.as_slice(), proto))
    }

    fn rich_mut(
        &mut self,
        proto: &DescriptorPool,
    ) -> Result<&mut ActionSequence, ActionSequenceError> {
        let _ = self.rich(proto)?;
        Ok(self.rich.get_mut().unwrap())
    }

    fn get_poor(&self) -> Result<Vec<PAction>, ActionSequenceError> {
        if let Some(rich) = self.rich.get() {
            return rich.gen_poor();
        };

        Ok(self.poor.clone())
    }
}

impl Input for GrpcInput {}
