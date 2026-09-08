use super::*;
use prost_reflect::DynamicMessage;

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub struct PCallId(pub usize);

impl PCallId {
    fn from_rich(s_id: &CallId) -> Self {
        Self(s_id.0)
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub struct PCSUnary(pub String);

impl PCSUnary {
    fn from_rich(csu: &CSUnary) -> Self {
        Self(csu.0.full_name().into())
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub struct PCStream(pub String);

impl PCStream {
    fn from_rich(cs: &CStream) -> Self {
        Self(cs.0.full_name().into())
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub struct PSStream {
    pub md: String,
    pub payload: Vec<u8>,
}

impl PSStream {
    fn from_rich(ss: &SStream) -> Self {
        let md = String::from(ss.md.full_name());
        let payload = <DynamicMessage as prost::Message>::encode_to_vec(&ss.payload);

        Self { md, payload }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub enum PStreamType {
    Client(PCStream),
    Server(PSStream),
}

impl PStreamType {
    fn from_rich(s: &StreamType) -> Self {
        match s {
            StreamType::Client(cs) => Self::Client(PCStream::from_rich(cs)),
            StreamType::Server(ss) => Self::Server(PSStream::from_rich(ss)),
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub struct PStream {
    pub ty: PStreamType,
    pub id: PCallId,
}

impl PStream {
    fn from_rich(s: &Stream) -> Self {
        let ty = PStreamType::from_rich(&s.ty);
        let id = PCallId::from_rich(&s.id);
        Self { ty, id }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub enum PStreamAction {
    StreamStart(PStream),
    StreamEnd(PCallId),
}

impl PStreamAction {
    fn from_rich(sa: &StreamAction) -> Self {
        match sa {
            StreamAction::Start(s) => Self::StreamStart(PStream::from_rich(s)),
            StreamAction::End(s_id) => Self::StreamEnd(PCallId::from_rich(s_id)),
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub enum PMessageConnection {
    Stream(PCallId, PCStream),
    Unary(PCSUnary),
}

impl PMessageConnection {
    fn from_rich(mc: &MessageConnection, streams: &HashMap<CallId, &Stream>) -> Self {
        match mc {
            MessageConnection::Stream(s_id) => Self::Stream(
                PCallId::from_rich(s_id),
                PCStream::from_rich(&CStream(
                    streams.get(s_id).expect("invalid id").ty.method().clone(),
                )),
            ),
            MessageConnection::Unary(csu) => Self::Unary(PCSUnary::from_rich(csu)),
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub struct PMessage {
    pub con: PMessageConnection,
    pub payload: Vec<u8>,
}

impl PMessage {
    fn from_rich(m: &Message, streams: &HashMap<CallId, &Stream>) -> Self {
        let con = PMessageConnection::from_rich(&m.con, streams);
        let payload = <DynamicMessage as prost::Message>::encode_to_vec(&m.payload);

        Self { con, payload }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub enum PAction {
    Stream(PStreamAction),
    Message(PMessage),
    Delay(u64),
}

impl PAction {
    pub fn from_rich(a: &Action, streams: &HashMap<CallId, &Stream>) -> Self {
        match a {
            Action::Stream(sa) => Self::Stream(PStreamAction::from_rich(sa)),
            Action::Message(m) => Self::Message(PMessage::from_rich(m, streams)),
            Action::Delay(d) => Self::Delay(*d),
        }
    }
}
