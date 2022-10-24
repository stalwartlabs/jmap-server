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

use crate::identity::schema::Identity;
use jmap::error::set::{SetError, SetErrorType};
use jmap::jmap_store::set::SetHelper;
use jmap::jmap_store::Object;
use jmap::orm::{serialize::JMAPOrm, TinyORM};
use jmap::request::set::SetResponse;
use jmap::request::ResultReference;
use jmap::types::jmap::JMAPId;
use jmap::{jmap_store::set::SetObject, request::set::SetRequest};
use jmap::{principal, sanitize_email, SUPERUSER_ID};
use store::core::collection::Collection;
use store::core::document::Document;
use store::core::error::StoreError;
use store::core::JMAPIdPrefix;
use store::read::comparator::Comparator;
use store::read::filter::{Filter, Query};
use store::read::FilterMapper;
use store::{AccountId, JMAPStore, Store};

use super::schema::{Property, Value};

impl SetObject for Identity {
    type SetArguments = ();

    type NextCall = ();

    fn eval_id_references(&mut self, _fnc: impl FnMut(&str) -> Option<JMAPId>) {}
    fn eval_result_references(&mut self, _fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>) {}
    fn set_property(&mut self, property: Self::Property, value: Self::Value) {
        self.properties.set(property, value);
    }
}

pub trait JMAPSetIdentity<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn identity_set(&self, request: SetRequest<Identity>) -> jmap::Result<SetResponse<Identity>>;

    fn identity_delete(&self, account_id: AccountId, document: &mut Document) -> store::Result<()>;
}

impl<T> JMAPSetIdentity<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn identity_set(&self, request: SetRequest<Identity>) -> jmap::Result<SetResponse<Identity>> {
        let mut helper = SetHelper::new(self, request)?;

        helper.create(|_create_id, item, helper, document| {
            let mut fields = TinyORM::<Identity>::new();

            for (property, value) in item.properties {
                fields.set(
                    property,
                    match (property, value) {
                        (
                            Property::Name | Property::TextSignature | Property::HtmlSignature,
                            value @ Value::Text { .. },
                        ) => value,

                        (Property::Email, Value::Text { value }) => {
                            let value = sanitize_email(&value).ok_or_else(|| {
                                SetError::invalid_properties()
                                    .with_property(Property::Email)
                                    .with_description("Invalid e-mail address.")
                            })?;
                            if !helper
                                .store
                                .query_store::<FilterMapper>(
                                    SUPERUSER_ID,
                                    Collection::Principal,
                                    Filter::or(vec![
                                        Filter::eq(
                                            principal::schema::Property::Email.into(),
                                            Query::Index(value.clone()),
                                        ),
                                        Filter::eq(
                                            principal::schema::Property::Aliases.into(),
                                            Query::Index(value.clone()),
                                        ),
                                    ]),
                                    Comparator::None,
                                )?
                                .into_iter()
                                .any(|id| id.get_document_id() == helper.account_id)
                            {
                                return Err(SetError::invalid_properties()
                                    .with_property(Property::Email)
                                    .with_description(
                                        "E-mail address not configured for this account."
                                            .to_string(),
                                    ));
                            }
                            Value::Text { value }
                        }
                        (Property::ReplyTo | Property::Bcc, value @ Value::Addresses { .. }) => {
                            value
                        }
                        (
                            Property::Name
                            | Property::TextSignature
                            | Property::HtmlSignature
                            | Property::ReplyTo
                            | Property::Bcc,
                            Value::Null,
                        ) => Value::Null,
                        (property, _) => {
                            return Err(SetError::invalid_properties()
                                .with_property(property)
                                .with_description("Field could not be set."));
                        }
                    },
                );
            }

            // Validate fields
            fields.insert_validate(document)?;

            Ok(Identity::new(document.document_id.into()))
        })?;

        helper.update(|id, item, helper, document| {
            let current_fields = self
                .get_orm::<Identity>(helper.account_id, id.get_document_id())?
                .ok_or_else(|| SetError::new(SetErrorType::NotFound))?;
            let mut fields = TinyORM::track_changes(&current_fields);

            for (property, value) in item.properties {
                fields.set(
                    property,
                    match (property, value) {
                        (
                            Property::Name | Property::TextSignature | Property::HtmlSignature,
                            value @ Value::Text { .. },
                        ) => value,

                        (Property::ReplyTo | Property::Bcc, value @ Value::Addresses { .. }) => {
                            value
                        }
                        (
                            Property::Name
                            | Property::TextSignature
                            | Property::HtmlSignature
                            | Property::ReplyTo
                            | Property::Bcc,
                            Value::Null,
                        ) => Value::Null,
                        (property, _) => {
                            return Err(SetError::invalid_properties()
                                .with_property(property)
                                .with_description("Field could not be set."));
                        }
                    },
                );
            }

            // Merge changes
            current_fields.merge_validate(document, fields)?;
            Ok(None)
        })?;

        helper.destroy(|_id, helper, document| {
            if let Some(orm) = self.get_orm::<Identity>(helper.account_id, document.document_id)? {
                orm.delete(document);
            }
            Ok(())
        })?;

        helper.into_response()
    }

    fn identity_delete(&self, account_id: AccountId, document: &mut Document) -> store::Result<()> {
        // Delete ORM
        self.get_orm::<Identity>(account_id, document.document_id)?
            .ok_or_else(|| {
                StoreError::NotFound(format!(
                    "Failed to fetch Identity ORM for {}:{}.",
                    account_id, document.document_id
                ))
            })?
            .delete(document);

        Ok(())
    }
}
