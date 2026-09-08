use super::{
    error::ActionError,
    poor::{
        PAction, PCSUnary, PCStream, PCallId, PMessage, PMessageConnection, PSStream, PStream,
        PStreamAction, PStreamType,
    },
};
use prost_reflect::{DescriptorPool, DynamicMessage, MethodDescriptor, ReflectMessage};
use std::collections::HashMap;

fn get_method_by_full_name(full_name: &str, pool: &DescriptorPool) -> Option<MethodDescriptor> {
    let (service_name, method_name) = full_name.rsplit_once('.')?;
    let service = pool.get_service_by_name(service_name)?;

    service
        .methods()
        .find(|method| method.name() == method_name)
}

// TODO: get rid of this struct
#[derive(Copy, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct CallId(pub(super) usize);

impl From<CallId> for Action {
    fn from(id: CallId) -> Self {
        Action::Stream(StreamAction::End(id))
    }
}

impl From<usize> for CallId {
    fn from(value: usize) -> Self {
        CallId(value)
    }
}

impl From<CallId> for usize {
    fn from(value: CallId) -> Self {
        value.0
    }
}

impl CallId {
    fn from_poor(id: &PCallId) -> Self {
        Self(id.0)
    }

    fn to_poor(self) -> PCallId {
        PCallId(self.0)
    }
}

#[derive(Debug, Clone)]
pub(super) struct CSUnary(pub(super) MethodDescriptor);

impl CSUnary {
    pub(super) fn new(md: MethodDescriptor) -> Result<Self, ActionError> {
        if md.is_client_streaming() || md.is_server_streaming() {
            return Err(ActionError::InvalidMethod);
        }

        Ok(Self(md))
    }

    fn from_poor(unary: &PCSUnary, proto: &DescriptorPool) -> Result<Self, ActionError> {
        Self::new(get_method_by_full_name(&unary.0, proto).ok_or(ActionError::ProtoMismatch)?)
    }

    fn to_poor(&self) -> PCSUnary {
        PCSUnary(self.0.full_name().into())
    }
}

#[derive(Debug, Clone)]
pub(super) struct CStream(pub(super) MethodDescriptor);

impl CStream {
    pub(super) fn new(md: MethodDescriptor) -> Result<Self, ActionError> {
        if !md.is_client_streaming() {
            return Err(ActionError::InvalidMethod);
        }

        Ok(Self(md))
    }

    fn from_poor(stream: &PCStream, proto: &DescriptorPool) -> Result<Self, ActionError> {
        Self::new(get_method_by_full_name(&stream.0, proto).ok_or(ActionError::ProtoMismatch)?)
    }

    fn to_poor(&self) -> PCStream {
        PCStream(self.0.full_name().into())
    }
}

#[derive(Debug, Clone)]
pub(super) struct SStream {
    md: MethodDescriptor,
    payload: DynamicMessage,
}

impl SStream {
    pub(super) fn new(md: MethodDescriptor, payload: DynamicMessage) -> Result<Self, ActionError> {
        if !md.is_server_streaming() {
            return Err(ActionError::InvalidMethod);
        }

        if md.input() != payload.descriptor() {
            return Err(ActionError::MessageCallDescriptorMismatch);
        }

        Ok(Self { md, payload })
    }

    fn from_poor(stream: &PSStream, proto: &DescriptorPool) -> Result<Self, ActionError> {
        let md = get_method_by_full_name(&stream.md, proto).ok_or(ActionError::ProtoMismatch)?;
        let payload = DynamicMessage::decode(md.input(), stream.payload.as_slice())
            .map_err(|_| ActionError::ProtoMismatch)?;

        Self::new(md, payload)
    }

    fn to_poor(&self) -> PSStream {
        PSStream {
            md: self.md.full_name().into(),
            payload: prost::Message::encode_to_vec(&self.payload),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) enum StreamType {
    Client(CStream),
    Server(SStream),
}

impl StreamType {
    fn from_poor(stream: &PStreamType, proto: &DescriptorPool) -> Result<Self, ActionError> {
        Ok(match stream {
            PStreamType::Client(stream) => Self::Client(CStream::from_poor(stream, proto)?),
            PStreamType::Server(stream) => Self::Server(SStream::from_poor(stream, proto)?),
        })
    }

    fn to_poor(&self) -> PStreamType {
        match self {
            Self::Client(stream) => PStreamType::Client(stream.to_poor()),
            Self::Server(stream) => PStreamType::Server(stream.to_poor()),
        }
    }

    pub(super) fn method(&self) -> &MethodDescriptor {
        match self {
            Self::Client(stream) => &stream.0,
            Self::Server(stream) => &stream.md,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Stream {
    pub(super) ty: StreamType,
    pub(super) id: CallId,
}

impl Stream {
    fn from_poor(stream: &PStream, proto: &DescriptorPool) -> Result<Self, ActionError> {
        Ok(Self {
            ty: StreamType::from_poor(&stream.ty, proto)?,
            id: CallId::from_poor(&stream.id),
        })
    }

    fn to_poor(&self) -> PStream {
        PStream {
            ty: self.ty.to_poor(),
            id: self.id.to_poor(),
        }
    }
}

impl From<Stream> for Action {
    fn from(stream: Stream) -> Self {
        Action::Stream(StreamAction::Start(stream))
    }
}

#[derive(Debug, Clone)]
pub(super) enum StreamAction {
    Start(Stream),
    End(CallId),
}

impl StreamAction {
    fn from_poor(action: &PStreamAction, proto: &DescriptorPool) -> Result<Self, ActionError> {
        Ok(match action {
            PStreamAction::StreamStart(stream) => Self::Start(Stream::from_poor(stream, proto)?),
            PStreamAction::StreamEnd(id) => Self::End(CallId::from_poor(id)),
        })
    }

    fn to_poor(&self) -> PStreamAction {
        match self {
            Self::Start(stream) => PStreamAction::StreamStart(stream.to_poor()),
            Self::End(id) => PStreamAction::StreamEnd(id.to_poor()),
        }
    }
}

impl From<StreamAction> for Action {
    fn from(action: StreamAction) -> Self {
        Action::Stream(action)
    }
}

#[derive(Debug, Clone)]
pub(super) enum MessageConnection {
    Stream(CallId),
    Unary(CSUnary),
}

impl MessageConnection {
    fn to_poor(
        &self,
        streams: &HashMap<CallId, &Stream>,
    ) -> Result<PMessageConnection, ActionError> {
        match self {
            Self::Stream(id) => {
                let stream = streams.get(id).ok_or(ActionError::InvalidStreamId)?;
                Ok(PMessageConnection::Stream(
                    id.to_poor(),
                    PCStream(stream.ty.method().full_name().into()),
                ))
            }
            Self::Unary(unary) => Ok(PMessageConnection::Unary(unary.to_poor())),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Message {
    pub(super) con: MessageConnection,
    pub(super) payload: DynamicMessage,
}

impl Message {
    fn from_poor(message: &PMessage, proto: &DescriptorPool) -> Result<Self, ActionError> {
        let (con, md) = match &message.con {
            PMessageConnection::Stream(id, stream) => {
                let stream = CStream::from_poor(stream, proto)?;
                (MessageConnection::Stream(CallId::from_poor(id)), stream.0)
            }
            PMessageConnection::Unary(unary) => {
                let unary = CSUnary::from_poor(unary, proto)?;
                let md = unary.0.clone();
                (MessageConnection::Unary(unary), md)
            }
        };
        let payload = DynamicMessage::decode(md.input(), message.payload.as_slice())
            .map_err(|_| ActionError::ProtoMismatch)?;

        Ok(Self { con, payload })
    }

    fn to_poor(&self, streams: &HashMap<CallId, &Stream>) -> Result<PMessage, ActionError> {
        Ok(PMessage {
            con: self.con.to_poor(streams)?,
            payload: prost::Message::encode_to_vec(&self.payload),
        })
    }
}

impl From<Message> for Action {
    fn from(message: Message) -> Self {
        Action::Message(message)
    }
}

#[derive(Debug, Clone)]
pub(super) enum Action {
    Stream(StreamAction),
    Message(Message),
    Delay(u64),
}

impl Action {
    pub(super) fn from_poor(action: &PAction, proto: &DescriptorPool) -> Result<Self, ActionError> {
        Ok(match action {
            PAction::Stream(action) => StreamAction::from_poor(action, proto)?.into(),
            PAction::Message(message) => Message::from_poor(message, proto)?.into(),
            PAction::Delay(delay) => Self::Delay(*delay),
        })
    }

    pub(super) fn to_poor(
        &self,
        streams: &HashMap<CallId, &Stream>,
    ) -> Result<PAction, ActionError> {
        Ok(match self {
            Self::Stream(action) => PAction::Stream(action.to_poor()),
            Self::Message(message) => PAction::Message(message.to_poor(streams)?),
            Self::Delay(delay) => PAction::Delay(*delay),
        })
    }
}
