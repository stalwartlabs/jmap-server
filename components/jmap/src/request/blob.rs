/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use store::core::{acl::ACLToken, vec_map::VecMap};

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
    pub copied: Option<VecMap<JMAPBlob, JMAPBlob>>,

    #[serde(rename = "notCopied")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_copied: Option<VecMap<JMAPBlob, SetError<()>>>,
}
