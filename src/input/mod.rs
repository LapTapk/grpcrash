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
    fn new(md: MethodDescriptor) -> Self {
        debug_assert!(
            !(md.is_client_streaming() || md.is_server_streaming()),
            "ClientStream can only contain method with client streaming"
        );
        Self(md)
    }
    fn from_poor(pcsu: &PCSUnary, proto: &DescriptorPool) -> Self {
        let name = &pcsu.0;
        Self(get_method_by_full_name(name, proto).expect(&format!("No method named {}", name)))
    }
}

#[derive(Debug, Clone)]
struct CStream(MethodDescriptor);

impl CStream {
    fn new(md: MethodDescriptor) -> Self {
        debug_assert!(
            md.is_client_streaming(),
            "ClientStream can only contain method with client streaming"
        );
        Self(md)
    }

    fn from_poor(pcs: &PCStream, proto: &DescriptorPool) -> Self {
        let name = &pcs.0;
        Self(get_method_by_full_name(name, proto).expect(&format!("No method named {}", name)))
    }
}

#[derive(Debug, Clone)]
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

    fn from_poor(pss: &PSStream, proto: &DescriptorPool) -> Self {
        let name = &pss.md;
        let md = get_method_by_full_name(name, proto).expect(&format!("No method named {}", name));
        let payload =
            DynamicMessage::decode(md.input(), pss.payload.as_slice()).expect("Invalid PSStream");

        Self { md, payload }
    }
}

#[derive(Debug, Clone)]
enum StreamType {
    Client(CStream),
    Server(SStream),
}

impl StreamType {
    fn from_poor(pst: &PStreamType, proto: &DescriptorPool) -> Self {
        match pst {
            PStreamType::Client(pcs) => Self::Client(CStream::from_poor(pcs, proto)),
            PStreamType::Server(pss) => Self::Server(SStream::from_poor(pss, proto)),
        }
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
    fn from_poor(ps: &PStream, proto: &DescriptorPool) -> Self {
        let ty = StreamType::from_poor(&ps.ty, proto);
        let id = CallId::from_poor(&ps.id);
        Self { ty, id }
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
    fn from_poor(psa: &PStreamAction, proto: &DescriptorPool) -> Self {
        match psa {
            PStreamAction::StreamStart(ps) => Self::Start(Stream::from_poor(ps, proto)),
            PStreamAction::StreamEnd(ps_id) => Self::End(CallId::from_poor(ps_id)),
        }
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
    fn from_poor(pm: &PMessage, proto: &DescriptorPool) -> Self {
        let (con, md) = match &pm.con {
            PMessageConnection::Stream(ps_id, pcs) => (
                MessageConnection::Stream(CallId::from_poor(ps_id)),
                CStream::from_poor(pcs, proto).0,
            ),
            PMessageConnection::Unary(pcsu) => (
                MessageConnection::Unary(CSUnary::from_poor(pcsu, proto)),
                CSUnary::from_poor(pcsu, proto).0,
            ),
        };
        let payload =
            DynamicMessage::decode(md.input(), pm.payload.as_slice()).expect("Incorrect PMessage");

        Self { con, payload }
    }
}

#[derive(Debug, Clone)]
enum Action {
    Stream(StreamAction),
    Message(Message),
    Delay(u64),
}

impl Action {
    fn from_poor(pa: &PAction, proto: &DescriptorPool) -> Self {
        match pa {
            PAction::Stream(ps) => StreamAction::from_poor(ps, proto).into(),
            PAction::Message(pm) => Message::from_poor(pm, proto).into(),
            PAction::Delay(del) => Self::Delay(*del),
        }
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

#[derive(Debug)]
struct StreamMessageFactory<R> {
    si: StreamInterval,
    actions: R,
}

impl<'a, R> StreamMessageFactory<R>
where
    R: DerefMut<Target = Vec<Action>>,
{
    fn message(mut self, payload: DynamicMessage, i: usize) -> Option<()> {
        let stream = get_stream(&self.actions, &self.si)?;
        debug_assert!(matches!(stream.ty, StreamType::Client(_)));
        debug_assert_eq!(stream.ty.method().input(), payload.descriptor());
        debug_assert!(self.si.s < i);
        debug_assert!(self.si.e >= i);

        let message = Message {
            con: MessageConnection::Stream(stream.id),
            payload,
        };
        self.actions.insert(i, message.into());
        Some(())
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
    fn new(actions: R) -> Self {
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
                    debug_assert_eq!(old_end, None);
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

        Self { actions, intervals }
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
struct Actions {
    actions: Vec<Action>,
    last_call_id: CallId,
}

impl Actions {
    fn from_poor(poor: &[PAction], proto: &DescriptorPool) -> Self {
        let actions = poor.iter().map(|p| Action::from_poor(p, proto)).collect();
        let last_call_id = *StreamsView::new(&actions)
            .intervals()
            .keys()
            .max()
            .unwrap_or(&CallId(0));

        Actions {
            actions,
            last_call_id,
        }
    }

    fn gen_poor(&self) -> Vec<PAction> {
        self.actions
            .iter()
            .map(|a| PAction::from_rich(a, self))
            .collect()
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
    ) -> Option<()> {
        debug_assert!(start < self.len(), "start {} >= len {}", start, self.len());
        debug_assert!(end < self.len(), "end {} >= len {}", end, self.len());
        debug_assert!(start < end, "start {} >= end {}", start, end);

        let end_action: Action = CallId::from(id).into();

        let actions = self.get_mut();
        actions.reserve(2);
        actions.insert(end, end_action);
        actions.insert(start, start_action);
        Some(())
    }

    fn add_server_stream(
        &mut self,
        md: MethodDescriptor,
        payload: DynamicMessage,
        start: usize,
        end: usize,
    ) -> Option<()> {
        let ty = StreamType::Server(SStream::new(md, payload));
        let id = self.allocate_call_id()?;
        let stream = Stream { ty, id };
        let start_action: Action = stream.into();
        self.add_stream(start_action, id, start, end)
    }

    fn add_client_stream(&mut self, md: MethodDescriptor, start: usize, end: usize) -> Option<()> {
        let ty = StreamType::Client(CStream::new(md)).into();
        let id = self.allocate_call_id()?;
        let stream = Stream { ty, id };
        let start_action: Action = stream.into();
        self.add_stream(start_action, id, start, end)
    }

    fn add_unary(&mut self, md: CSUnary, payload: DynamicMessage, i: usize) {
        debug_assert_eq!(md.0.input(), payload.descriptor());
        debug_assert!(i < self.len(), "i {} < len {}", i, self.len());

        let message = Message {
            con: MessageConnection::Unary(md),
            payload,
        };
        self.get_mut().insert(i, message.into());
    }

    fn stream_list(&self) -> StreamsView<&Vec<Action>> {
        StreamsView::new(self.get())
    }

    fn stream_list_mut(&mut self) -> StreamsView<&mut Vec<Action>> {
        StreamsView::new(self.get_mut())
    }
}

#[derive(serde::Deserialize, Debug, Clone)]
struct GrpcInput {
    #[serde(skip)]
    rich: OnceCell<Actions>,

    poor: Vec<PAction>,
}

impl Hash for GrpcInput {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.get_poor().hash(state);
    }
}

impl serde::Serialize for GrpcInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("GrpcInput", 1)?;

        serde::ser::SerializeStruct::serialize_field(&mut state, "poor", &self.get_poor())?;

        serde::ser::SerializeStruct::end(state)
    }
}

impl GrpcInput {
    fn rich(&self, proto: &DescriptorPool) -> &Actions {
        self.rich
            .get_or_init(|| Actions::from_poor(self.poor.as_slice(), proto))
    }

    fn rich_mut(&mut self, proto: &DescriptorPool) -> &mut Actions {
        let _ = self.rich(proto);
        self.rich.get_mut().unwrap()
    }

    fn get_poor(&self) -> Vec<PAction> {
        if let Some(rich) = self.rich.get() {
            return rich.gen_poor();
        };

        self.poor.clone()
    }
}

impl Input for GrpcInput {}
