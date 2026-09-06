use prost_reflect::{DynamicMessage, MethodDescriptor};

#[derive(serde::Deserialize)]
pub struct PStreamId(pub usize);

#[derive(serde::Deserialize)]
pub struct PCSUnary(pub String);

#[derive(serde::Deserialize)]
pub struct PCStream(pub String);

#[derive(serde::Deserialize)]
pub struct PSStream {
    pub md: String,
    pub payload: Vec<u8>,
}

#[derive(serde::Deserialize)]
pub enum PStream {
    Client(PCStream),
    Server(PSStream),
}

#[derive(serde::Deserialize)]
pub enum PStreamAction {
    StreamStart(PStream),
    StreamEnd(PStreamId),
}

#[derive(serde::Deserialize)]
pub struct PStreamMessage {
    pub stream_id: PStreamId,
}

#[derive(serde::Deserialize)]
pub enum PMessageConnection {
    Stream(PStreamId, PCStream),
    Unary(PCSUnary),
}

#[derive(serde::Deserialize)]
pub struct PMessage {
    pub con: PMessageConnection,
    pub payload: Vec<u8>,
}

#[derive(serde::Deserialize)]
pub enum PAction {
    Stream(PStreamAction),
    Message(PMessage),
    Delay(u64),
}
