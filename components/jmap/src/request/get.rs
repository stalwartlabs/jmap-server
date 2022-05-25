use crate::{
    error::method::MethodError,
    id::{jmap::JMAPId, state::JMAPState},
    jmap_store::get::GetObject,
    protocol::json_pointer::{JSONPointer, JSONPointerEval},
};

use super::{MaybeResultReference, ResultReference};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct GetRequest<O: GetObject> {
    #[serde(rename = "accountId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<JMAPId>,

    #[serde(alias = "#ids")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ids: Option<MaybeResultReference<Vec<JMAPId>>>,

    #[serde(alias = "#properties")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<MaybeResultReference<Vec<O::Property>>>,

    #[serde(flatten)]
    pub arguments: O::GetArguments,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct GetResponse<O: GetObject> {
    #[serde(rename = "accountId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<JMAPId>,

    pub state: JMAPState,

    pub list: Vec<O>,

    #[serde(rename = "notFound")]
    pub not_found: Vec<JMAPId>,
}

impl<O: GetObject> GetRequest<O> {
    pub fn eval_result_references(
        &mut self,
        mut fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>,
    ) -> crate::Result<()> {
        if let Some(items) = self.ids.as_mut() {
            if let MaybeResultReference::Reference(rr) = items {
                if let Some(ids) = fnc(rr) {
                    *items = MaybeResultReference::Value(ids.into_iter().map(Into::into).collect());
                } else {
                    return Err(MethodError::InvalidResultReference(
                        "Failed to evaluate #ids result reference.".to_string(),
                    ));
                }
            }
        }

        if let Some(items) = self.properties.as_mut() {
            if let MaybeResultReference::Reference(rr) = items {
                if let Some(property_ids) = fnc(rr) {
                    *items = MaybeResultReference::Value(
                        property_ids
                            .into_iter()
                            .map(|property_id| O::Property::from(property_id as u8))
                            .collect(),
                    );
                } else {
                    return Err(MethodError::InvalidResultReference(
                        "Failed to evaluate #properties result reference.".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}

impl<O: GetObject> JSONPointerEval for GetResponse<O> {
    fn eval_json_pointer(&self, ptr: &JSONPointer) -> Option<Vec<u64>> {
        match ptr {
            JSONPointer::Path(path) if path.len() == 3 => {
                match (path.get(0)?, path.get(1)?, path.get(2)?) {
                    (
                        JSONPointer::String(root),
                        JSONPointer::Wildcard,
                        JSONPointer::String(property),
                    ) if root == "list" => {
                        let property = O::Property::try_from(property).ok()?;

                        Some(
                            self.list
                                .iter()
                                .filter_map(|item| item.get_as_id(&property))
                                .flat_map(|v| v.into_iter().map(Into::into))
                                .collect::<Vec<u64>>(),
                        )
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
}
