#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub(super) struct PCallId(pub(super) usize);

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub(super) struct PCSUnary(pub(super) String);

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub(super) struct PCStream(pub(super) String);

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub(super) struct PSStream {
    pub(super) md: String,
    pub(super) payload: Vec<u8>,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub(super) enum PStreamType {
    Client(PCStream),
    Server(PSStream),
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub(super) struct PStream {
    pub(super) ty: PStreamType,
    pub(super) id: PCallId,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub(super) enum PStreamAction {
    StreamStart(PStream),
    StreamEnd(PCallId),
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub(super) enum PMessageConnection {
    Stream(PCallId, PCStream),
    Unary(PCSUnary),
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub(super) struct PMessage {
    pub(super) con: PMessageConnection,
    pub(super) payload: Vec<u8>,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug, Hash)]
pub(super) enum PAction {
    Stream(PStreamAction),
    Message(PMessage),
    Delay(u64),
}
