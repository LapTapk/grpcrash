use super::*;
use prost_reflect::DynamicMessage;

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub struct PStreamId(pub usize);

impl PStreamId {
    fn from_rich(s_id: &StreamId) -> Self {
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
pub enum PStream {
    Client(PCStream),
    Server(PSStream),
}

impl PStream {
    fn from_rich(s: &Stream) -> Self {
        match s {
            Stream::Client(cs) => Self::Client(PCStream::from_rich(cs)),
            Stream::Server(ss) => Self::Server(PSStream::from_rich(ss)),
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub enum PStreamAction {
    StreamStart(PStream),
    StreamEnd(PStreamId),
}

impl PStreamAction {
    fn from_rich(sa: &StreamAction) -> Self {
        match sa {
            StreamAction::StreamStart(s) => Self::StreamStart(PStream::from_rich(s)),
            StreamAction::StreamEnd(s_id) => Self::StreamEnd(PStreamId::from_rich(s_id)),
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub enum PMessageConnection {
    Stream(PStreamId, PCStream),
    Unary(PCSUnary),
}

impl PMessageConnection {
    fn from_rich(mc: &MessageConnection, actions: &Actions) -> Self {
        match mc {
            MessageConnection::Stream(s_id) => Self::Stream(
                PStreamId::from_rich(s_id),
                PCStream::from_rich(&CStream(
                    actions.stream_list().stream(*s_id).method().clone(),
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
    fn from_rich(m: &Message, actions: &Actions) -> Self {
        let con = PMessageConnection::from_rich(&m.con, actions);
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
    pub fn from_rich(a: &Action, actions: &Actions) -> Self {
        match a {
            Action::Stream(sa) => Self::Stream(PStreamAction::from_rich(sa)),
            Action::Message(m) => Self::Message(PMessage::from_rich(m, actions)),
            Action::Delay(d) => Self::Delay(*d),
        }
    }
}
