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

use jmap::jmap_store::get::{default_mapper, GetHelper, GetObject, SharedDocsFnc};
use jmap::orm::serialize::JMAPOrm;
use jmap::orm::TinyORM;
use jmap::request::get::{GetRequest, GetResponse};
use jmap::types::jmap::JMAPId;

use store::ahash::AHashSet;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::core::vec_map::VecMap;
use store::read::comparator::Comparator;
use store::read::filter::{ComparisonOperator, Filter, Query};
use store::read::FilterMapper;
use store::sieve::Sieve;
use store::tracing::error;
use store::{AccountId, Store};
use store::{DocumentId, JMAPStore};

use crate::SeenIdHash;

use super::schema::{Property, SieveScript, Value};

pub struct ActiveScript {
    pub document_id: DocumentId,
    pub orm: TinyORM<SieveScript>,
    pub script: Sieve,
    pub seen_ids: AHashSet<SeenIdHash>,
    pub has_changes: bool,
}

impl GetObject for SieveScript {
    type GetArguments = ();

    fn default_properties() -> Vec<Self::Property> {
        vec![Property::Id, Property::Name, Property::BlobId]
    }

    fn get_as_id(&self, property: &Self::Property) -> Option<Vec<JMAPId>> {
        match self.properties.get(property)? {
            Value::Id { value } => Some(vec![*value]),
            _ => None,
        }
    }
}

pub trait JMAPGetSieveScript<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn sieve_script_get(
        &self,
        request: GetRequest<SieveScript>,
    ) -> jmap::Result<GetResponse<SieveScript>>;

    fn sieve_script_get_active(&self, account_id: AccountId)
        -> store::Result<Option<ActiveScript>>;

    fn sieve_script_get_by_name(
        &self,
        account_id: AccountId,
        name: String,
    ) -> store::Result<Option<Sieve>>;
}

impl<T> JMAPGetSieveScript<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn sieve_script_get(
        &self,
        request: GetRequest<SieveScript>,
    ) -> jmap::Result<GetResponse<SieveScript>> {
        let mut helper =
            GetHelper::new(self, request, default_mapper.into(), None::<SharedDocsFnc>)?;
        let account_id = helper.account_id;

        // Add Id Property
        if !helper.properties.contains(&Property::Id) {
            helper.properties.push(Property::Id);
        }

        helper.get(|id, properties| {
            let document_id = id.get_document_id();
            let mut fields = self
                .get_orm::<SieveScript>(account_id, document_id)?
                .ok_or_else(|| StoreError::NotFound("SieveScript data not found".to_string()))?;
            let mut sieve_script = VecMap::with_capacity(properties.len());

            for property in properties {
                sieve_script.append(
                    *property,
                    if let Property::Id = property {
                        Value::Id { value: id }
                    } else if let Some(value) = fields.remove(property) {
                        value
                    } else {
                        Value::Null
                    },
                );
            }
            Ok(Some(SieveScript {
                properties: sieve_script,
            }))
        })
    }

    fn sieve_script_get_active(
        &self,
        account_id: AccountId,
    ) -> store::Result<Option<ActiveScript>> {
        if let Some(document_id) = self
            .query_store::<FilterMapper>(
                account_id,
                Collection::SieveScript,
                Filter::new_condition(
                    Property::IsActive.into(),
                    ComparisonOperator::Equal,
                    Query::Keyword("1".to_string()),
                ),
                Comparator::None,
            )?
            .into_bitmap()
            .min()
        {
            // Fetch ORM
            let mut orm = self
                .get_orm::<SieveScript>(account_id, document_id)?
                .ok_or_else(|| {
                    StoreError::NotFound(format!(
                        "SieveScript ORM data for {}:{} not found.",
                        account_id, document_id
                    ))
                })?;

            // Get seenIds
            let (seen_ids, has_changes) =
                if let Some(Value::SeenIds { value }) = orm.get_mut(&Property::CompiledScript) {
                    (
                        std::mem::take(&mut value.ids),
                        if value.has_changes {
                            value.has_changes = false;
                            true
                        } else {
                            false
                        },
                    )
                } else {
                    (AHashSet::new(), false)
                };

            // Get compiled script
            if let Some(script) = orm.get_mut(&Property::CompiledScript).and_then(|f| {
                if let Value::CompiledScript { value } = f {
                    value.script.take()
                } else {
                    None
                }
            }) {
                return Ok(Some(ActiveScript {
                    document_id,
                    orm,
                    script,
                    seen_ids,
                    has_changes,
                }));
            } else if let Some(Value::BlobId { value }) = orm.get(&Property::BlobId) {
                if let Some(blob) = self.blob_get(&value.id)? {
                    match self.sieve_compiler.compile(&blob) {
                        Ok(script) => {
                            return Ok(Some(ActiveScript {
                                document_id,
                                orm,
                                script,
                                seen_ids,
                                has_changes: true,
                            }))
                        }
                        Err(err) => {
                            error!(
                                "Failed to compile SieveScript {}/{}: {}",
                                account_id, document_id, err
                            );
                        }
                    }
                } else {
                    error!(
                        "Blob {} found for SieveScript {}/{} ",
                        value, account_id, document_id
                    );
                }
            } else {
                error!(
                    "No blobId entry found for SieveScript {}/{}",
                    account_id, document_id
                );
            }
        }

        Ok(None)
    }

    fn sieve_script_get_by_name(
        &self,
        account_id: AccountId,
        name: String,
    ) -> store::Result<Option<Sieve>> {
        if let Some(document_id) = self
            .query_store::<FilterMapper>(
                account_id,
                Collection::SieveScript,
                Filter::new_condition(
                    Property::Name.into(),
                    ComparisonOperator::Equal,
                    Query::Keyword(name),
                ),
                Comparator::None,
            )?
            .into_bitmap()
            .min()
        {
            // Fetch ORM
            let mut orm = self
                .get_orm::<SieveScript>(account_id, document_id)?
                .ok_or_else(|| {
                    StoreError::NotFound(format!(
                        "SieveScript ORM data for {}:{} not found.",
                        account_id, document_id
                    ))
                })?;

            // Get compiled script
            if let Some(script) = orm.remove(&Property::CompiledScript).and_then(|f| {
                if let Value::CompiledScript { value } = f {
                    value.script
                } else {
                    None
                }
            }) {
                return Ok(Some(script));
            } else if let Some(Value::BlobId { value }) = orm.get(&Property::BlobId) {
                if let Some(blob) = self.blob_get(&value.id)? {
                    match self.sieve_compiler.compile(&blob) {
                        Ok(script) => return Ok(Some(script)),
                        Err(err) => {
                            error!(
                                "Failed to compile SieveScript {}/{}: {}",
                                account_id, document_id, err
                            );
                        }
                    }
                } else {
                    error!(
                        "Blob {} found for SieveScript {}/{} ",
                        value, account_id, document_id
                    );
                }
            }
        }

        Ok(None)
    }
}
