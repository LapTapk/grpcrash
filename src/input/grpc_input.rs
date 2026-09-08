use super::{error::ActionSequenceError, poor::PAction, sequence::ActionSequence};
use libafl::inputs::Input;
use once_cell::sync::OnceCell;
use prost_reflect::DescriptorPool;
use std::hash::{Hash, Hasher};

#[derive(serde::Deserialize, Debug, Clone)]
pub(super) struct GrpcInput {
    #[serde(skip)]
    rich: OnceCell<ActionSequence>,

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
        use serde::ser::{Error as _, SerializeStruct};

        let poor = self
            .get_poor()
            .map_err(|error| S::Error::custom(error.to_string()))?;

        let mut state = serializer.serialize_struct("GrpcInput", 1)?;
        state.serialize_field("poor", &poor)?;
        state.end()
    }
}

impl GrpcInput {
    pub(super) fn rich(
        &self,
        proto: &DescriptorPool,
    ) -> Result<&ActionSequence, ActionSequenceError> {
        self.rich
            .get_or_try_init(|| ActionSequence::from_poor(&self.poor, proto))
    }

    pub(super) fn rich_mut(
        &mut self,
        proto: &DescriptorPool,
    ) -> Result<&mut ActionSequence, ActionSequenceError> {
        self.rich(proto)?;
        Ok(self
            .rich
            .get_mut()
            .expect("rich input was initialized immediately above"))
    }

    fn get_poor(&self) -> Result<Vec<PAction>, ActionSequenceError> {
        if let Some(rich) = self.rich.get() {
            return rich.gen_poor();
        }

        Ok(self.poor.clone())
    }
}

impl Input for GrpcInput {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::action::{Action, CallId, StreamAction};

    #[test]
    fn serialization_uses_the_poor_schema_directly() {
        let input = GrpcInput {
            rich: OnceCell::new(),
            poor: vec![PAction::Delay(7)],
        };

        let yaml = serde_yaml::to_string(&input).unwrap();
        assert!(!yaml.contains("Ok:"));

        let decoded: GrpcInput = serde_yaml::from_str(&yaml).unwrap();
        assert!(matches!(decoded.poor.as_slice(), [PAction::Delay(7)]));
    }

    #[test]
    fn invalid_rich_input_returns_a_serialization_error() {
        let rich = OnceCell::new();
        assert!(
            rich.set(ActionSequence {
                actions: vec![Action::Stream(StreamAction::End(CallId(1)))],
                last_call_id: CallId(1),
            })
            .is_ok()
        );
        let input = GrpcInput {
            rich,
            poor: Vec::new(),
        };

        assert!(serde_yaml::to_string(&input).is_err());
    }
}
