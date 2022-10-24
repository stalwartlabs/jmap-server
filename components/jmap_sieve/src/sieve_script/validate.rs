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

use jmap::{
    error::set::{SetError, SetErrorType},
    request::ACLEnforce,
    types::{blob::JMAPBlob, jmap::JMAPId},
    SUPERUSER_ID,
};
use serde::{Deserialize, Serialize};
use store::{core::acl::ACLToken, JMAPStore, Store};

use super::schema::Property;

#[derive(Debug, Deserialize)]
pub struct SieveScriptValidateRequest {
    #[serde(skip)]
    pub acl: Option<Arc<ACLToken>>,
    #[serde(rename = "accountId")]
    pub account_id: JMAPId,
    #[serde(rename = "blobId")]
    pub blob_id: JMAPBlob,
}

#[derive(Debug, Serialize)]
pub struct SieveScriptValidateResponse {
    #[serde(rename = "accountId")]
    pub account_id: JMAPId,
    pub error: Option<SetError<Property>>,
}

pub trait JMAPMailSieveScriptValidate<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn sieve_script_validate(
        &self,
        request: SieveScriptValidateRequest,
    ) -> jmap::Result<SieveScriptValidateResponse>;
}

impl<T> JMAPMailSieveScriptValidate<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn sieve_script_validate(
        &self,
        request: SieveScriptValidateRequest,
    ) -> jmap::Result<SieveScriptValidateResponse> {
        let acl = request.acl.unwrap();
        let mut response = SieveScriptValidateResponse {
            account_id: request.account_id,
            error: None,
        };

        if let Some(script) = self.blob_get(&request.blob_id.id)? {
            if self.blob_account_has_access(&request.blob_id.id, &acl.member_of)?
                || acl.is_member(SUPERUSER_ID)
            {
                if let Err(err) = self.sieve_compiler.compile(&script) {
                    response.error = SetError::new(SetErrorType::InvalidScript)
                        .with_description(err.to_string())
                        .into();
                }
            } else {
                response.error = SetError::forbidden()
                    .with_property(Property::BlobId)
                    .with_description("You do not have enough permissions to access this blob.")
                    .into();
            }
        } else {
            response.error = SetError::new(SetErrorType::BlobNotFound).into();
        }

        Ok(response)
    }
}
