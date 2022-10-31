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

use super::schema::{CompiledScript, Property, SieveScript, Value};
use jmap::error::set::{SetError, SetErrorType};
use jmap::jmap_store::set::SetHelper;
use jmap::jmap_store::Object;
use jmap::orm::{serialize::JMAPOrm, TinyORM};
use jmap::request::set::SetResponse;
use jmap::request::{ACLEnforce, MaybeIdReference, ResultReference};
use jmap::types::jmap::JMAPId;
use jmap::SUPERUSER_ID;
use jmap::{jmap_store::set::SetObject, request::set::SetRequest};
use store::core::collection::Collection;
use store::core::document::Document;
use store::core::error::StoreError;
use store::rand::distributions::Alphanumeric;
use store::rand::{thread_rng, Rng};
use store::read::comparator::Comparator;
use store::read::filter::{ComparisonOperator, Filter, Query};
use store::read::FilterMapper;
use store::sieve::compiler::ErrorType;
use store::sieve::Compiler;
use store::write::batch::WriteBatch;
use store::write::options::{IndexOptions, Options};
use store::{AccountId, DocumentId, JMAPStore, Store};

#[derive(Debug, Clone, Default)]
pub struct SetArguments {
    pub on_success_activate_script: ActivateScript,
}

#[derive(Debug, Clone)]
pub enum ActivateScript {
    Activate(MaybeIdReference),
    Deactivate,
    None,
}

impl SetObject for SieveScript {
    type SetArguments = SetArguments;

    type NextCall = ();

    fn eval_id_references(&mut self, _fnc: impl FnMut(&str) -> Option<JMAPId>) {}

    fn eval_result_references(&mut self, _fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>) {}

    fn set_property(&mut self, property: Self::Property, value: Self::Value) {
        self.properties.set(property, value);
    }
}

pub trait JMAPSetSieveScript<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn sieve_script_set(
        &self,
        request: SetRequest<SieveScript>,
    ) -> jmap::Result<SetResponse<SieveScript>>;

    fn sieve_script_delete(
        &self,
        account_id: AccountId,
        document: &mut Document,
    ) -> store::Result<()>;

    fn sieve_script_activate_id(
        &self,
        changes: &mut WriteBatch,
        document_id: Option<DocumentId>,
    ) -> store::Result<(bool, Vec<DocumentId>)>;
}

impl<T> JMAPSetSieveScript<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn sieve_script_set(
        &self,
        request: SetRequest<SieveScript>,
    ) -> jmap::Result<SetResponse<SieveScript>> {
        let mut helper = SetHelper::new(self, request)?;

        helper.create(|_create_id, item, helper, document| {
            if (helper.document_ids.len() as usize) >= helper.store.config.sieve_max_scripts {
                return Err(SetError::new(SetErrorType::OverQuota).with_description(
                    "Maximum number of stored scripts exceeded, please delete some and try again",
                ));
            }

            let mut fields =
                TinyORM::<SieveScript>::new().sieve_script_set(helper, item, document, None)?;

            // Add name if missing
            if !matches!(fields.get(&Property::Name), Some(Value::Text { value }) if !value.is_empty()) {
                fields.set(Property::Name, Value::Text { value: thread_rng()
                    .sample_iter(Alphanumeric)
                    .take(15)
                    .map(char::from)
                    .collect::<String>() });
            }

            // Set script as inactive
            fields.set(Property::IsActive, Value::Bool { value: false });

            // Validate fields
            fields.insert_validate(document)?;

            Ok(SieveScript::new(document.document_id.into()))
        })?;

        helper.update(|id, item, helper, document| {
            let current_fields = self
                .get_orm::<SieveScript>(helper.account_id, id.get_document_id())?
                .ok_or_else(|| SetError::new(SetErrorType::NotFound))?;
            let fields = TinyORM::track_changes(&current_fields).sieve_script_set(
                helper,
                item,
                document,
                Some(&current_fields),
            )?;

            // Merge changes
            current_fields.merge_validate(document, fields)?;
            Ok(None)
        })?;

        helper.destroy(|_id, helper, document| {
            // Fetch ORM
            let sieve_script = self
                .get_orm::<SieveScript>(helper.account_id, document.document_id)?
                .ok_or_else(|| {
                    StoreError::NotFound(format!(
                        "SieveScript ORM data for {}:{} not found.",
                        helper.account_id, document.document_id
                    ))
                })?;

            // Fail if the script is currently active
            if matches!(
                sieve_script.get(&Property::IsActive),
                Some(Value::Bool { value: true })
            ) {
                return Err(SetError::new(SetErrorType::ScriptIsActive)
                    .with_description("Deactivate Sieve script before deletion."));
            }

            // Unlink blob
            if let Some(Value::BlobId { value }) = sieve_script.get(&Property::BlobId) {
                document.blob(value.id.clone(), IndexOptions::new().clear());
            }

            // Delete ORM
            sieve_script.delete(document);
            Ok(())
        })?;

        if helper.response.not_created.is_empty()
            && helper.response.not_updated.is_empty()
            && helper.response.not_destroyed.is_empty()
        {
            match &helper.request.arguments.on_success_activate_script {
                ActivateScript::Activate(maybe_reference) => {
                    let activate_id = match maybe_reference {
                        MaybeIdReference::Value(id) => (*id).into(),
                        MaybeIdReference::Reference(create_id) => {
                            helper.map_id_reference(create_id)
                        }
                    };

                    match activate_id {
                        Some(activate_id)
                            if helper.document_ids.contains(activate_id.get_document_id()) =>
                        {
                            helper.commit_changes()?;

                            let (did_activate, deactivated_ids) = self.sieve_script_activate_id(
                                &mut helper.changes,
                                activate_id.get_document_id().into(),
                            )?;

                            if did_activate {
                                if let ActivateScript::Activate(MaybeIdReference::Reference(
                                    create_id,
                                )) = std::mem::take(
                                    &mut helper.request.arguments.on_success_activate_script,
                                ) {
                                    helper.set_created_property(
                                        &create_id,
                                        Property::IsActive,
                                        Value::Bool { value: true },
                                    );
                                } else {
                                    helper.set_updated_property(
                                        activate_id,
                                        Property::IsActive,
                                        Value::Bool { value: true },
                                    );
                                }
                            }

                            for id in deactivated_ids {
                                helper.set_updated_property(
                                    id.into(),
                                    Property::IsActive,
                                    Value::Bool { value: false },
                                );
                            }
                        }
                        _ => (),
                    }
                }
                ActivateScript::Deactivate => {
                    helper.commit_changes()?;

                    let (_, deactivated_ids) =
                        self.sieve_script_activate_id(&mut helper.changes, None)?;

                    for id in deactivated_ids {
                        helper.set_updated_property(
                            id.into(),
                            Property::IsActive,
                            Value::Bool { value: false },
                        );
                    }
                }
                ActivateScript::None => (),
            }
        }

        helper.into_response()
    }

    fn sieve_script_delete(
        &self,
        account_id: AccountId,
        document: &mut Document,
    ) -> store::Result<()> {
        let document_id = document.document_id;

        // Fetch ORM
        let sieve_script = self
            .get_orm::<SieveScript>(account_id, document_id)?
            .ok_or_else(|| {
                StoreError::NotFound(format!(
                    "SieveScript ORM data for {}:{} not found.",
                    account_id, document_id
                ))
            })?;

        // Unlink blob
        if let Some(Value::BlobId { value }) = sieve_script.get(&Property::BlobId) {
            document.blob(value.id.clone(), IndexOptions::new().clear());
        }

        // Delete ORM
        sieve_script.delete(document);

        Ok(())
    }

    fn sieve_script_activate_id(
        &self,
        changes: &mut WriteBatch,
        mut document_id: Option<DocumentId>,
    ) -> store::Result<(bool, Vec<DocumentId>)> {
        let mut deactivated_ids = Vec::new();
        for document_id_ in self
            .query_store::<FilterMapper>(
                changes.account_id,
                Collection::SieveScript,
                Filter::new_condition(
                    Property::IsActive.into(),
                    ComparisonOperator::Equal,
                    Query::Index("1".to_string()),
                ),
                Comparator::None,
            )?
            .into_bitmap()
        {
            if document_id != Some(document_id_) {
                // Fetch ORM
                let script = self
                    .get_orm::<SieveScript>(changes.account_id, document_id_)?
                    .ok_or_else(|| {
                        StoreError::NotFound(format!(
                            "SieveScript ORM data for {}:{} not found.",
                            changes.account_id, document_id_
                        ))
                    })?;
                let mut updated_script = TinyORM::track_changes(&script);
                updated_script.set(Property::IsActive, Value::Bool { value: false });
                updated_script.remove(&Property::SeenIds);

                let mut document = Document::new(Collection::SieveScript, document_id_);
                script.merge(&mut document, updated_script)?;

                changes.update_document(document);
                changes.log_update(Collection::SieveScript, document_id_);

                deactivated_ids.push(document_id_);
            } else {
                document_id = None;
            }
        }

        // Set script as active
        if let Some(document_id) = document_id {
            // Fetch ORM
            let script = self
                .get_orm::<SieveScript>(changes.account_id, document_id)?
                .ok_or_else(|| {
                    StoreError::NotFound(format!(
                        "SieveScript ORM data for {}:{} not found.",
                        changes.account_id, document_id
                    ))
                })?;
            let mut updated_script = TinyORM::track_changes(&script);
            updated_script.set(Property::IsActive, Value::Bool { value: true });

            let mut document = Document::new(Collection::SieveScript, document_id);
            script.merge(&mut document, updated_script)?;

            changes.update_document(document);
            changes.log_update(Collection::SieveScript, document_id);

            Ok((true, deactivated_ids))
        } else {
            Ok((false, deactivated_ids))
        }
    }
}

trait SieveScriptSet<T>: Sized
where
    T: for<'x> Store<'x> + 'static,
{
    fn sieve_script_set(
        self,
        helper: &mut SetHelper<SieveScript, T>,
        sieve_script: SieveScript,
        document: &mut Document,
        fields: Option<&TinyORM<SieveScript>>,
    ) -> jmap::error::set::Result<Self, Property>;
}

impl<T> SieveScriptSet<T> for TinyORM<SieveScript>
where
    T: for<'x> Store<'x> + 'static,
{
    fn sieve_script_set(
        mut self,
        helper: &mut SetHelper<SieveScript, T>,
        sieve_script: SieveScript,
        document: &mut Document,
        fields: Option<&TinyORM<SieveScript>>,
    ) -> jmap::error::set::Result<Self, Property> {
        if matches!(fields.as_ref().and_then(|f| f.get(&Property::Name)), Some(Value::Text { value }) if value.eq_ignore_ascii_case("vacation"))
        {
            return Err(SetError::forbidden().with_description(
                "The 'vacation' script cannot be modified, use VacationResponse/set instead.",
            ));
        }

        for (property, value) in sieve_script.properties {
            let value = match (property, value) {
                (Property::Name, Value::Text { value }) => {
                    if value.len() > helper.store.config.sieve_max_script_name {
                        return Err(SetError::invalid_properties()
                        .with_property(property)
                        .with_description("Script name is too long."));
                    } else if value.eq_ignore_ascii_case("vacation") {
                        return Err(SetError::forbidden()
                        .with_property(property)
                        .with_description("The 'vacation' name is reserved, please use a different name."));
                    } else if fields
                        .as_ref()
                        .and_then(|fields| fields.get(&Property::Name))
                        .map_or(true, |p| matches!(p, Value::Text { value: prev_value } if prev_value != &value))
                    {
                        if let Some(id) = helper
                            .store
                            .query_store::<FilterMapper>(
                                helper.account_id,
                                Collection::SieveScript,
                                Filter::new_condition(
                                    Property::Name.into(),
                                    ComparisonOperator::Equal,
                                    Query::Index(value.clone()),
                                ),
                                Comparator::None,
                            )?
                            .next()
                        {
                            return Err(SetError::already_exists()
                                .with_existing_id(id.into())
                                .with_description(format!(
                                    "A sieve script with name '{}' already exists.",
                                    value
                                )));
                        }
                    }

                    Value::Text { value }
                }
                (Property::BlobId, value @ Value::BlobId { .. }) => value,
                (Property::Name, Value::Null) => {
                    continue;
                }
                (property, _) => {
                    return Err(SetError::invalid_properties()
                        .with_property(property)
                        .with_description("Field could not be set."));
                }
            };
            self.set(property, value);
        }

        // Compile and link Sieve blob
        if let Some(Value::BlobId { value }) = self.get(&Property::BlobId) {
            // Unlink previous blob
            let mut add_blob = true;
            if let Some(Value::BlobId { value: prev_value }) = fields
                .as_ref()
                .and_then(|fields| fields.get(&Property::Name))
            {
                if value.id != prev_value.id {
                    document.blob(prev_value.id.clone(), IndexOptions::new().clear());
                } else {
                    add_blob = false;
                }
            }

            if add_blob {
                let script = helper
                    .store
                    .blob_get(&value.id)?
                    .ok_or_else(|| SetError::new(SetErrorType::BlobNotFound))?;

                if !helper
                    .store
                    .blob_account_has_access(&value.id, &helper.acl.member_of)?
                    && !helper.acl.is_member(SUPERUSER_ID)
                {
                    return Err(SetError::forbidden()
                        .with_property(Property::BlobId)
                        .with_description(
                            "You do not have enough permissions to access this blob.",
                        ));
                }

                // Link blob
                document.blob(value.id.clone(), IndexOptions::new());

                // Compile script
                self.set(
                    Property::CompiledScript,
                    Value::CompiledScript {
                        value: CompiledScript {
                            version: Compiler::VERSION,
                            script: helper
                                .store
                                .sieve_compiler
                                .compile(&script)
                                .map_err(|err| {
                                    SetError::new(
                                        if let ErrorType::ScriptTooLong = &err.error_type() {
                                            SetErrorType::TooLarge
                                        } else {
                                            SetErrorType::InvalidScript
                                        },
                                    )
                                    .with_description(err.to_string())
                                })?
                                .into(),
                        },
                    },
                );
            }
        } else if fields.is_none() {
            return Err(SetError::invalid_properties()
                .with_property(Property::BlobId)
                .with_description("Missing blobId."));
        };

        Ok(self)
    }
}

impl Default for ActivateScript {
    fn default() -> Self {
        ActivateScript::None
    }
}
