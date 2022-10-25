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

use std::borrow::Cow;

use crate::vacation_response::schema::VacationResponse;
use jmap::error::method::MethodError;
use jmap::error::set::{SetError, SetErrorType};
use jmap::jmap_store::changes::JMAPChanges;
use jmap::jmap_store::Object;
use jmap::orm::serialize::JMAPOrm;
use jmap::orm::TinyORM;
use jmap::request::set::SetResponse;
use jmap::request::ResultReference;
use jmap::types::jmap::JMAPId;
use jmap::types::state::JMAPState;
use jmap::{jmap_store::set::SetObject, request::set::SetRequest};
use jmap_sieve::sieve_script::schema::{CompiledScript, SieveScript};
use jmap_sieve::sieve_script::set::JMAPSetSieveScript;
use mail_builder::encoders::base64::base64_encode_mime;
use mail_builder::MessageBuilder;
use mail_parser::decoders::html::html_to_text;
use store::ahash::AHashMap;
use store::blob::BlobId;
use store::core::collection::Collection;
use store::core::document::Document;
use store::core::error::StoreError;
use store::core::vec_map::VecMap;
use store::sieve::Compiler;
use store::tracing::error;
use store::write::batch::WriteBatch;
use store::write::options::{IndexOptions, Options};
use store::{bincode, JMAPStore, Store};

use super::get::JMAPGetVacationResponse;
use super::schema::{Property, Value};

impl SetObject for VacationResponse {
    type SetArguments = ();

    type NextCall = ();

    fn eval_id_references(&mut self, _fnc: impl FnMut(&str) -> Option<JMAPId>) {}
    fn eval_result_references(&mut self, _fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>) {}
    fn set_property(&mut self, property: Self::Property, value: Self::Value) {
        self.properties.set(property, value);
    }
}

pub trait JMAPSetVacationResponse<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn vacation_response_set(
        &self,
        request: SetRequest<VacationResponse>,
    ) -> jmap::Result<SetResponse<VacationResponse>>;
}

impl<T> JMAPSetVacationResponse<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn vacation_response_set(
        &self,
        request: SetRequest<VacationResponse>,
    ) -> jmap::Result<SetResponse<VacationResponse>> {
        let account_id = request.account_id.get_document_id();
        let _lock = self.lock_collection(account_id, Collection::SieveScript);

        let old_state = self.get_state(account_id, Collection::SieveScript)?;
        if let Some(if_in_state) = request.if_in_state {
            if old_state != if_in_state {
                return Err(MethodError::StateMismatch);
            }
        }
        let will_destroy = request
            .destroy
            .and_then(|d| d.unwrap_value())
            .unwrap_or_default();
        let mut response = SetResponse {
            account_id: request.account_id.into(),
            new_state: old_state.clone().into(),
            old_state: old_state.into(),
            created: AHashMap::new(),
            not_created: VecMap::new(),
            updated: VecMap::new(),
            not_updated: VecMap::new(),
            destroyed: Vec::new(),
            not_destroyed: VecMap::new(),
            next_call: None,
            change_id: None,
            state_changes: None,
        };
        let mut changes = WriteBatch::new(account_id);

        let (create_id, updates) = match (request.create, request.update) {
            (Some(create), _) if !create.is_empty() => {
                let mut updates = None;
                let mut create_id = None;
                for (create_id_, updates_) in create {
                    if will_destroy.contains(&JMAPId::singleton()) {
                        response.not_created.append(
                            create_id_,
                            SetError::new(SetErrorType::WillDestroy)
                                .with_description("ID will be destroyed."),
                        );
                    } else if updates.is_none() {
                        updates = updates_.into();
                        create_id = create_id_.into();
                    } else {
                        response.not_created.append(
                            create_id_,
                            SetError::new(SetErrorType::InvalidProperties)
                                .with_description("Multiple create requests for singleton."),
                        );
                    }
                }
                (create_id, updates)
            }
            (_, Some(update)) if !update.is_empty() => {
                let mut updates = None;
                for (id, updates_) in update {
                    if id.is_singleton() {
                        if will_destroy.contains(&id) {
                            response.not_updated.append(
                                id,
                                SetError::new(SetErrorType::WillDestroy)
                                    .with_description("ID will be destroyed."),
                            );
                        } else if updates.is_none() {
                            updates = updates_.into();
                        } else {
                            response.not_updated.append(
                                id,
                                SetError::new(SetErrorType::InvalidProperties)
                                    .with_description("Multiple update requests for singleton."),
                            );
                        }
                    } else {
                        response.not_updated.append(
                            id,
                            SetError::new(SetErrorType::NotFound).with_description("ID not found."),
                        );
                    }
                }
                (None, updates)
            }
            _ => (None, None),
        };

        // Process changes
        if let Some(updates) = updates {
            let mut vacation_response = VacationResponse::default();
            let mut was_active = false;

            if let Some(document_id) = self.get_vacation_sieve_script_id(account_id)? {
                // Fetch ORM
                let mut document = Document::new(Collection::SieveScript, document_id);
                let script = self
                    .get_orm::<SieveScript>(account_id, document_id)?
                    .ok_or_else(|| {
                        StoreError::NotFound(format!(
                            "SieveScript ORM data for {}:{} not found.",
                            account_id, document_id
                        ))
                    })?;

                was_active = matches!(
                    script.get(&jmap_sieve::sieve_script::schema::Property::IsActive),
                    Some(jmap_sieve::sieve_script::schema::Value::Bool { value: true })
                );

                // Deserialize VacationResponse object, stored in base64 as a comment.
                if let Some(jmap_sieve::sieve_script::schema::Value::BlobId { value }) =
                    script.get(&jmap_sieve::sieve_script::schema::Property::BlobId)
                {
                    if let Some(vacation_response_) =
                        self.deserialize_vacation_sieve_script(&value.id)?
                    {
                        vacation_response = vacation_response_;
                    }

                    // Delete current blob
                    document.blob(value.id.clone(), IndexOptions::new().clear());
                }

                // Delete current Sieve script
                script.delete(&mut document);
                changes.delete_document(document);
                changes.log_delete(Collection::SieveScript, document_id);
            }

            for (property, value) in updates.properties {
                match (property, value) {
                    (Property::Subject, Value::Text { value }) if value.len() < 512 => {
                        vacation_response
                            .properties
                            .set(property, Value::Text { value });
                    }
                    (Property::HtmlBody | Property::TextBody, Value::Text { value })
                        if value.len() < 2048 =>
                    {
                        vacation_response
                            .properties
                            .set(property, Value::Text { value });
                    }
                    (Property::ToDate | Property::FromDate, value @ Value::DateTime { .. })
                    | (Property::IsEnabled, value @ Value::Bool { .. }) => {
                        vacation_response.properties.set(property, value);
                    }
                    (Property::IsEnabled, Value::Null) => {
                        vacation_response
                            .properties
                            .set(property, Value::Bool { value: false });
                    }
                    (
                        Property::Subject
                        | Property::HtmlBody
                        | Property::TextBody
                        | Property::ToDate
                        | Property::FromDate,
                        Value::Null,
                    ) => {
                        vacation_response.properties.remove(&property);
                    }
                    (property, _) => {
                        let error = SetError::invalid_properties()
                            .with_property(property)
                            .with_description("Field could not be set.");

                        if let Some(create_id) = create_id {
                            response.not_created.set(create_id, error);
                        } else {
                            response.not_updated.set(JMAPId::singleton(), error);
                        }

                        return Ok(response);
                    }
                }
            }

            // Generate Sieve script only if there is more than one property or
            // if the script is enabled
            let is_active = matches!(
                vacation_response.properties.get(&Property::IsEnabled),
                Some(Value::Bool { value: true })
            );
            if vacation_response.properties.len() > 1 || is_active {
                // Build Sieve script
                let mut script = Vec::with_capacity(1024);
                script.extend_from_slice(b"/*");
                base64_encode_mime(
                    &bincode::serialize(&vacation_response).unwrap_or_default(),
                    &mut script,
                    false,
                )
                .ok();
                script.extend_from_slice(b"*/\r\n\r\n");
                script.extend_from_slice(
                    b"require [\"vacation\", \"relational\", \"date\"];\r\n\r\n",
                );
                let mut num_blocks = 0;

                // Add start date
                if let Some(Value::DateTime { value }) =
                    vacation_response.properties.get(&Property::FromDate)
                {
                    script.extend_from_slice(b"if currentdate :value \"ge\" \"iso8601\" \"");
                    script.extend_from_slice(value.to_string().as_bytes());
                    script.extend_from_slice(b"\" {\r\n");
                    num_blocks += 1;
                }

                // Add end date
                if let Some(Value::DateTime { value }) =
                    vacation_response.properties.get(&Property::ToDate)
                {
                    script.extend_from_slice(b"if currentdate :value \"le\" \"iso8601\" \"");
                    script.extend_from_slice(value.to_string().as_bytes());
                    script.extend_from_slice(b"\" {\r\n");
                    num_blocks += 1;
                }

                script.extend_from_slice(b"vacation :mime ");
                if let Some(Value::Text { value }) =
                    vacation_response.properties.get(&Property::Subject)
                {
                    script.extend_from_slice(b":subject \"");
                    for &ch in value.as_bytes().iter() {
                        match ch {
                            b'\\' | b'\"' => {
                                script.push(b'\\');
                            }
                            b'\r' | b'\n' => {
                                continue;
                            }
                            _ => (),
                        }
                        script.push(ch);
                    }
                    script.extend_from_slice(b"\" ");
                }

                let mut text_body = if let Some(Value::Text { value }) =
                    vacation_response.properties.get(&Property::TextBody)
                {
                    Cow::from(value.as_str()).into()
                } else {
                    None
                };
                let html_body = if let Some(Value::Text { value }) =
                    vacation_response.properties.get(&Property::HtmlBody)
                {
                    Cow::from(value.as_str()).into()
                } else {
                    None
                };
                match (&html_body, &text_body) {
                    (Some(html_body), None) => {
                        text_body = Cow::from(html_to_text(html_body.as_ref())).into();
                    }
                    (None, None) => {
                        text_body = Cow::from("I am away.").into();
                    }
                    _ => (),
                }

                let mut builder = MessageBuilder::new();
                let mut body_len = 0;
                if let Some(html_body) = html_body {
                    body_len = html_body.len();
                    builder = builder.html_body(html_body);
                }
                if let Some(text_body) = text_body {
                    body_len += text_body.len();
                    builder = builder.html_body(text_body);
                }
                let mut message_body = Vec::with_capacity(body_len + 128);
                builder.write_body(&mut message_body).ok();

                script.push(b'\"');
                for ch in message_body {
                    if [b'\\', b'\"'].contains(&ch) {
                        script.push(b'\\');
                    }
                    script.push(ch);
                }
                script.extend_from_slice(b"\";\r\n");

                // Close blocks
                for _ in 0..num_blocks {
                    script.extend_from_slice(b"}\r\n");
                }

                // Compile script
                let compiled_script = match self.sieve_compiler.compile(&script) {
                    Ok(compiled_script) => compiled_script,
                    Err(err) => {
                        error!("Vacation Sieve Script failed to compile: {}", err);
                        let error =
                            SetError::new(SetErrorType::Forbidden).with_description(concat!(
                                "VacationScript compilation failed, ",
                                "please contact the system administrator."
                            ));

                        if let Some(create_id) = create_id {
                            response.not_created.set(create_id, error);
                        } else {
                            response.not_updated.set(JMAPId::singleton(), error);
                        }

                        return Ok(response);
                    }
                };

                // Deactivate other scripts
                if is_active && !was_active {
                    self.sieve_script_activate_id(&mut changes, None)?;
                }

                // Store blob
                let blob_id = BlobId::new_external(&script);
                self.blob_store(&blob_id, script)?;

                // Create ORM object
                let mut fields = TinyORM::<SieveScript>::new();
                fields.set(
                    jmap_sieve::sieve_script::schema::Property::BlobId,
                    jmap_sieve::sieve_script::schema::Value::BlobId {
                        value: blob_id.clone().into(),
                    },
                );
                fields.set(
                    jmap_sieve::sieve_script::schema::Property::Name,
                    jmap_sieve::sieve_script::schema::Value::Text {
                        value: "vacation".to_string(),
                    },
                );
                fields.set(
                    jmap_sieve::sieve_script::schema::Property::CompiledScript,
                    jmap_sieve::sieve_script::schema::Value::CompiledScript {
                        value: CompiledScript {
                            version: Compiler::VERSION,
                            script: compiled_script.into(),
                        },
                    },
                );
                fields.set(
                    jmap_sieve::sieve_script::schema::Property::IsActive,
                    jmap_sieve::sieve_script::schema::Value::Bool { value: is_active },
                );

                // Create document
                let document_id = self.assign_document_id(account_id, Collection::SieveScript)?;
                let mut document = Document::new(Collection::SieveScript, document_id);
                document.blob(blob_id, IndexOptions::new());
                fields.insert(&mut document)?;
                changes.insert_document(document);
                changes.log_insert(Collection::SieveScript, document_id);
            }

            // Set response
            if let Some(create_id) = create_id {
                response
                    .created
                    .insert(create_id, VacationResponse::new(JMAPId::singleton()));
            } else {
                response.updated.set(JMAPId::singleton(), None);
            }
        }

        // Delete vacation response
        for destroy_id in will_destroy {
            if destroy_id.is_singleton() && response.destroyed.is_empty() {
                if let Some(document_id) = self.get_vacation_sieve_script_id(account_id)? {
                    let mut document = Document::new(Collection::SieveScript, document_id);
                    self.sieve_script_delete(account_id, &mut document)?;
                    response.destroyed.push(destroy_id);
                    changes.delete_document(document);
                    changes.log_delete(Collection::SieveScript, document_id);
                    continue;
                }
            }

            response.not_destroyed.append(
                destroy_id,
                SetError::new(SetErrorType::NotFound).with_description("ID not found."),
            );
        }

        if !changes.is_empty() {
            if let Some(changes) = self.write(changes)? {
                response.new_state = JMAPState::from(changes.change_id).into();
                response.change_id = changes.change_id.into();
            }
        }

        Ok(response)
    }
}
