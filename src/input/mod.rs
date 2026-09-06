mod dump;

use prost_reflect::{DynamicMessage, MethodDescriptor, ReflectMessage};

#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StreamId(usize);

impl Into<Action> for StreamId {
    fn into(self) -> Action {
        Action::Stream(StreamAction::StreamEnd(self))
    }
}

impl From<usize> for StreamId {
    fn from(value: usize) -> Self {
        StreamId(value)
    }
}

impl Into<usize> for StreamId {
    fn into(self) -> usize {
        self.0
    }
}

struct CSUnary(MethodDescriptor);

impl CSUnary {
    fn new(md: MethodDescriptor) -> Self {
        debug_assert!(
            !(md.is_client_streaming() || md.is_server_streaming()),
            "ClientStream can only contain method with client streaming"
        );
        Self(md)
    }
}

struct CStream(MethodDescriptor);

impl CStream {
    fn new(md: MethodDescriptor) -> Self {
        debug_assert!(
            md.is_client_streaming(),
            "ClientStream can only contain method with client streaming"
        );
        Self(md)
    }
}

struct SStream {
    md: MethodDescriptor,
    payload: DynamicMessage,
}

impl SStream {
    fn new(md: MethodDescriptor, payload: DynamicMessage) -> Self {
        debug_assert!(
            md.is_server_streaming(),
            "ServerStream can only contain method with server streaming"
        );
        debug_assert_eq!(md.input(), payload.descriptor());

        Self { md, payload }
    }
}

enum Stream {
    Client(CStream),
    Server(SStream),
}

impl Stream {
    fn method(&self) -> &MethodDescriptor {
        match self {
            Self::Client(s) => &s.0,
            Self::Server(s) => &s.md,
        }
    }
}

impl Into<Action> for Stream {
    fn into(self) -> Action {
        Action::Stream(StreamAction::StreamStart(self))
    }
}

enum StreamAction {
    StreamStart(Stream),
    StreamEnd(StreamId),
}

struct StreamMessage {
    stream_id: StreamId,
}

enum MessageConnection {
    Stream(StreamId),
    Unary(CSUnary),
}

struct Message {
    con: MessageConnection,
    payload: DynamicMessage,
}

impl Into<Action> for Message {
    fn into(self) -> Action {
        Action::Message(self)
    }
}

enum Action {
    Stream(StreamAction),
    Message(Message),
    Delay(u64),
}

#[derive(Clone, Debug, Copy)]
struct StreamInterval {
    s: usize,
    e: usize,
}

impl StreamInterval {
    fn stream<'a>(&self, actions: &'a [Action]) -> &'a Stream {
        match &actions[self.s] {
            Action::Stream(StreamAction::StreamStart(stream)) => stream,
            _ => unreachable!(),
        }
    }
}

struct StreamList<'a> {
    streams: Vec<StreamInterval>,
    actions: &'a mut Vec<Action>,
}

impl<'a> StreamList<'a> {
    fn new(actions: &'a mut Vec<Action>) -> Self {
        let mut stream_starts: Vec<usize> = Vec::new();
        let mut stream_ends: Vec<Option<usize>> = Vec::new();

        for (i, a) in actions.iter().enumerate() {
            match a {
                Action::Stream(StreamAction::StreamStart(_)) => {
                    stream_starts.push(i);
                }
                Action::Stream(StreamAction::StreamEnd(s_id)) => {
                    stream_ends.resize_with(stream_starts.len(), || None);
                    let s_id_usize: usize = (*s_id).into();
                    let old_end = stream_ends[s_id_usize].replace(i);
                    debug_assert_eq!(old_end, None);
                }
                _ => {}
            }
        }

        let streams: Vec<StreamInterval> = stream_starts
            .into_iter()
            .zip(stream_ends)
            .map(|(s, e)| StreamInterval {
                s,
                e: e.expect("Not all streams have an end. GrpcInput is broken"),
            })
            .collect();

        Self { actions, streams }
    }

    fn stream(&self, s_id: StreamId) -> &Stream {
        let s_id_idx: usize = s_id.into();
        let si = self.streams[s_id_idx];
        si.stream(self.actions)
    }

    fn client_streams(&self) -> Vec<StreamId> {
        self.streams
            .iter()
            .enumerate()
            .filter(|(_, si)| matches!(si.stream(self.actions), Stream::Client(_)))
            .map(|(s_id, _)| s_id.into())
            .collect()
    }

    fn message(self, s_id: StreamId, payload: DynamicMessage, i: usize) {
        let stream = &self.stream(s_id);

        debug_assert!(matches!(stream, Stream::Client(_)));
        debug_assert_eq!(stream.method().input(), payload.descriptor());

        let s_id_idx: usize = s_id.into();
        let si = self.streams[s_id_idx];

        debug_assert!(si.s >= i);
        debug_assert!(si.e < i);

        let message = Message {
            con: MessageConnection::Stream(s_id),
            payload,
        };
        self.actions.insert(i, message.into());
    }
}

struct GrpcInput(Vec<Action>);

impl GrpcInput {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn actions(&self) -> &Vec<Action> {
        &self.0
    }

    fn actions_mut(&mut self) -> &mut Vec<Action> {
        &mut self.0
    }

    fn add_stream(&mut self, start_action: Action, start: usize, end: usize) {
        debug_assert!(start < self.len(), "start {} >= len {}", start, self.len());
        debug_assert!(end < self.len(), "end {} >= len {}", end, self.len());
        debug_assert!(start < end, "start {} >= end {}", start, end);

        let mut cnt = 0;
        for (i, a) in self.actions_mut().iter_mut().enumerate() {
            if i <= start && matches!(a, Action::Stream(StreamAction::StreamStart(_))) {
                cnt += 1
            }

            if i > start
                && let Action::Stream(StreamAction::StreamEnd(s_id)) = a
            {
                let s_id_usize: usize = (*s_id).into();
                if s_id_usize >= cnt {
                    *s_id = StreamId::from(s_id_usize + 1)
                }
            }
        }

        let end_action: Action = StreamId::from(cnt).into();

        let actions = self.actions_mut();
        actions.reserve(2);
        actions.insert(end, end_action);
        actions.insert(start, start_action);
    }

    fn add_server_stream(
        &mut self,
        md: MethodDescriptor,
        payload: DynamicMessage,
        start: usize,
        end: usize,
    ) {
        let start_action: Action = Stream::Server(SStream::new(md, payload)).into();
        self.add_stream(start_action, start, end);
    }

    fn add_client_stream(&mut self, md: MethodDescriptor, start: usize, end: usize) {
        let start_action: Action = Stream::Client(CStream::new(md)).into();
        self.add_stream(start_action, start, end);
    }

    fn add_unary(&mut self, md: CSUnary, payload: DynamicMessage, i: usize) {
        debug_assert_eq!(md.0.input(), payload.descriptor());
        debug_assert!(i < self.len(), "i {} < len {}", i, self.len());

        let message = Message {
            con: MessageConnection::Unary(md),
            payload,
        };
        self.actions_mut().insert(i, message.into());
    }

    fn stream_list(&mut self) -> StreamList {
        StreamList::new(self.actions_mut())
    }
}

//impl Input for GrpcInput {}
