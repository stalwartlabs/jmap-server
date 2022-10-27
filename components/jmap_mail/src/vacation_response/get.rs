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

use jmap::jmap_store::changes::JMAPChanges;
use jmap::jmap_store::get::GetObject;
use jmap::orm::serialize::JMAPOrm;
use jmap::request::get::{GetRequest, GetResponse};
use jmap::request::MaybeResultReference;
use jmap::types::jmap::JMAPId;
use jmap_sieve::sieve_script::schema::SieveScript;
use mail_parser::decoders::base64::decode_base64;
use store::blob::BlobId;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::core::JMAPIdPrefix;
use store::read::comparator::Comparator;
use store::read::filter::{ComparisonOperator, Filter, Query};
use store::read::FilterMapper;
use store::{bincode, AccountId, JMAPStore};
use store::{DocumentId, Store};

use super::schema::{Property, VacationResponse, Value};

impl GetObject for VacationResponse {
    type GetArguments = ();

    fn default_properties() -> Vec<Self::Property> {
        vec![
            Property::Id,
            Property::IsEnabled,
            Property::FromDate,
            Property::ToDate,
            Property::Subject,
            Property::TextBody,
            Property::HtmlBody,
        ]
    }

    fn get_as_id(&self, property: &Self::Property) -> Option<Vec<JMAPId>> {
        match self.properties.get(property)? {
            Value::Id { value } => Some(vec![*value]),
            _ => None,
        }
    }
}

pub trait JMAPGetVacationResponse<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn vacation_response_get(
        &self,
        request: GetRequest<VacationResponse>,
    ) -> jmap::Result<GetResponse<VacationResponse>>;

    fn get_vacation_sieve_script_id(
        &self,
        account_id: AccountId,
    ) -> store::Result<Option<DocumentId>>;

    fn deserialize_vacation_sieve_script(
        &self,
        blob_id: &BlobId,
    ) -> store::Result<Option<VacationResponse>>;
}

impl<T> JMAPGetVacationResponse<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn vacation_response_get(
        &self,
        request: GetRequest<VacationResponse>,
    ) -> jmap::Result<GetResponse<VacationResponse>> {
        let account_id = request.account_id.get_document_id();
        let properties = request
            .properties
            .and_then(|p| p.unwrap_value())
            .unwrap_or_else(VacationResponse::default_properties);

        let mut response = GetResponse {
            account_id: request.account_id.into(),
            state: self.get_state(account_id, Collection::SieveScript)?,
            list: Vec::with_capacity(1),
            not_found: Vec::new(),
        };

        let do_get = if let Some(MaybeResultReference::Value(ids)) = request.ids {
            let mut do_get = false;
            for id in ids {
                if id.is_singleton() {
                    do_get = true;
                } else {
                    response.not_found.push(id);
                }
            }
            do_get
        } else {
            true
        };

        if do_get {
            let mut vacation_response = VacationResponse::default();

            if let Some(document_id) = self.get_vacation_sieve_script_id(account_id)? {
                let script = self
                    .get_orm::<SieveScript>(account_id, document_id)?
                    .ok_or_else(|| {
                        StoreError::NotFound(format!(
                            "SieveScript ORM data for {}:{} not found.",
                            account_id, document_id
                        ))
                    })?;

                // Deserialize VacationResponse object, stored in base64 as a comment.
                if let Some(jmap_sieve::sieve_script::schema::Value::BlobId { value }) =
                    script.get(&jmap_sieve::sieve_script::schema::Property::BlobId)
                {
                    if let Some(vacation_response_) =
                        self.deserialize_vacation_sieve_script(&value.id)?
                    {
                        vacation_response = vacation_response_;
                    }
                }
            }

            if !vacation_response.properties.is_empty() {
                let mut result = VacationResponse::default();

                for property in properties {
                    result.properties.append(
                        property,
                        if let Property::Id = property {
                            Value::Id {
                                value: JMAPId::singleton(),
                            }
                        } else if let Some(value) = vacation_response.properties.remove(&property) {
                            value
                        } else {
                            Value::Null
                        },
                    );
                }

                response.list.push(result);
            } else {
                response.not_found.push(JMAPId::singleton());
            }
        }

        Ok(response)
    }

    fn get_vacation_sieve_script_id(
        &self,
        account_id: AccountId,
    ) -> store::Result<Option<DocumentId>> {
        self.query_store::<FilterMapper>(
            account_id,
            Collection::SieveScript,
            Filter::new_condition(
                jmap_sieve::sieve_script::schema::Property::Name.into(),
                ComparisonOperator::Equal,
                Query::Keyword("vacation".to_string()),
            ),
            Comparator::None,
        )
        .map(|mut it| it.next().map(|id| id.get_document_id()))
    }

    fn deserialize_vacation_sieve_script(
        &self,
        blob_id: &BlobId,
    ) -> store::Result<Option<VacationResponse>> {
        if let Some(blob) = self.blob_get(blob_id)? {
            let mut start_pos = 0;
            let mut end_pos = 0;
            for (pos, &ch) in blob.iter().enumerate() {
                if ch == b'*' {
                    if start_pos == 0 {
                        start_pos = pos;
                    } else {
                        end_pos = pos;
                        break;
                    }
                }
            }
            if start_pos > 0 && end_pos > start_pos {
                if let Some(properties) =
                    decode_base64(blob.get(start_pos + 1..end_pos).unwrap_or(&b""[..]))
                        .and_then(|bytes| bincode::deserialize(&bytes).ok())
                {
                    return Ok(Some(VacationResponse { properties }));
                }
            }
        }

        Ok(None)
    }
}
