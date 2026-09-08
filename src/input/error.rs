#[derive(Debug, Clone, Copy, Hash, serde::Serialize, serde::Deserialize)]
pub(super) enum ActionError {
    InvalidMessageIdx,
    InvalidStreamId,
    InvalidStreamInterval,
    StreamIsNotClient,
    MessageCallDescriptorMismatch,
    NoCallIdsLeft,
    InvalidMethod,
    ProtoMismatch,
    DuplicateStreamStart,
    DuplicateStreamEnd,
    StreamEndWithoutStart,
    MissingStreamEnd,
}

#[derive(Debug, Clone, Copy, Hash, serde::Serialize, serde::Deserialize)]
pub(super) struct ActionSequenceError {
    pub(super) kind: ActionError,
    pub(super) index: usize,
}

impl std::fmt::Display for ActionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ActionError {}

impl std::fmt::Display for ActionSequenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} at action index {}", self.kind, self.index)
    }
}

impl std::error::Error for ActionSequenceError {}
