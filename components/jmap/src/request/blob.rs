use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use store::core::acl::ACLToken;

use crate::{
    error::set::SetError,
    types::{blob::JMAPBlob, jmap::JMAPId},
};

#[derive(Debug, Clone, Deserialize)]
pub struct CopyBlobRequest {
    #[serde(skip)]
    pub acl: Option<Arc<ACLToken>>,

    #[serde(rename = "fromAccountId")]
    pub from_account_id: JMAPId,

    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "blobIds")]
    pub blob_ids: Vec<JMAPBlob>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CopyBlobResponse {
    #[serde(rename = "fromAccountId")]
    pub from_account_id: JMAPId,

    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "copied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub copied: Option<HashMap<JMAPBlob, JMAPBlob>>,

    #[serde(rename = "notCopied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_copied: Option<HashMap<JMAPBlob, SetError<()>>>,
}
