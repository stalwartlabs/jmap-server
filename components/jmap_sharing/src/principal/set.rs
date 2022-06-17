use std::collections::HashSet;

use jmap::error::set::{SetError, SetErrorType};
use jmap::jmap_store::set::SetHelper;
use jmap::jmap_store::Object;
use jmap::orm::{serialize::JMAPOrm, TinyORM};
use jmap::request::set::SetResponse;
use jmap::request::ResultReference;
use jmap::types::jmap::JMAPId;
use jmap::{jmap_store::set::SetObject, request::set::SetRequest};
use jmap::{sanitize_domain, sanitize_email};
use jmap_mail::mailbox::schema::Mailbox;
use jmap_mail::mailbox::CreateMailbox;
use store::core::collection::Collection;
use store::core::document::Document;
use store::parking_lot::MutexGuard;
use store::read::comparator::Comparator;
use store::read::filter::{Filter, Query};
use store::read::FilterMapper;
use store::{DocumentId, JMAPStore, Store};

use super::schema::{Principal, Property, Type, Value};

impl SetObject for Principal {
    type SetArguments = ();

    type NextCall = ();

    fn eval_id_references(&mut self, _fnc: impl FnMut(&str) -> Option<JMAPId>) {}
    fn eval_result_references(&mut self, _fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>) {}
}

pub trait JMAPSetPrincipal<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn principal_set(&self, request: SetRequest<Principal>)
        -> jmap::Result<SetResponse<Principal>>;
}

impl<T> JMAPSetPrincipal<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn principal_set(
        &self,
        request: SetRequest<Principal>,
    ) -> jmap::Result<SetResponse<Principal>> {
        let mut helper = SetHelper::new(self, request)?;

        helper.create(|_create_id, item, helper, document| {
            // Set values
            TinyORM::<Principal>::new()
                .principal_set(helper, item, None, document.document_id)?
                .insert_validate(document)?;

            Ok((
                Principal::new(document.document_id.into()),
                None::<MutexGuard<'_, ()>>,
            ))
        })?;

        helper.update(|id, item, helper, document| {
            let document_id = id.get_document_id();
            let current_fields = self
                .get_orm::<Principal>(helper.account_id, document_id)?
                .ok_or_else(|| SetError::new_err(SetErrorType::NotFound))?;
            let fields = TinyORM::track_changes(&current_fields).principal_set(
                helper,
                item,
                Some(&current_fields),
                document.document_id,
            )?;

            // Merge changes
            current_fields.merge_validate(document, fields)?;

            Ok(None)
        })?;

        helper.destroy(|_id, helper, document| {
            //TODO delete members
            //TODO delete account messages
            if let Some(orm) = self.get_orm::<Principal>(helper.account_id, document.document_id)? {
                orm.delete(document);
            }
            Ok(())
        })?;

        helper.into_response()
    }
}

trait PrincipalSet<T>: Sized
where
    T: for<'x> Store<'x> + 'static,
{
    fn principal_set(
        self,
        helper: &mut SetHelper<Principal, T>,
        principal: Principal,
        fields: Option<&TinyORM<Principal>>,
        document_id: DocumentId,
    ) -> jmap::error::set::Result<Self, Property>;
}

impl<T> PrincipalSet<T> for TinyORM<Principal>
where
    T: for<'x> Store<'x> + 'static,
{
    fn principal_set(
        mut self,
        helper: &mut SetHelper<Principal, T>,
        mut principal: Principal,
        current_fields: Option<&TinyORM<Principal>>,
        document_id: DocumentId,
    ) -> jmap::error::set::Result<Self, Property> {
        // Obtain type
        let ptype = match if let Some(current_fields) = current_fields {
            current_fields.get(&Property::Type).cloned()
        } else {
            principal.properties.remove(&Property::Type).map(|v| {
                self.set(Property::Type, v.clone());
                v
            })
        } {
            Some(Value::Type { value }) => value,
            _ => {
                return Err(SetError::invalid_property(
                    Property::Type,
                    "Missing property.".to_string(),
                ));
            }
        };
        let mut validate_emails = Vec::new();

        // Process changes
        for (property, value) in principal.properties {
            let value = match (property, value) {
                (Property::Name, Value::Text { mut value }) if !value.is_empty() => {
                    if ptype == Type::Domain {
                        if let Some(domain) = sanitize_domain(&value) {
                            // Validate that the domain does not exist already
                            if current_fields.map_or(true, |v| match v.get(&Property::Name) {
                                Some(Value::Text { value }) => value != &domain,
                                _ => true,
                            }) && !helper
                                .store
                                .query_store::<FilterMapper>(
                                    helper.account_id,
                                    Collection::Principal,
                                    Filter::and(vec![
                                        Filter::eq(
                                            Property::Name.into(),
                                            Query::Index(domain.clone()),
                                        ),
                                        Filter::eq(
                                            Property::Type.into(),
                                            Query::Keyword("d".to_string()),
                                        ),
                                    ]),
                                    Comparator::None,
                                )?
                                .is_empty()
                            {
                                return Err(SetError::invalid_property(
                                    property,
                                    format!("A domain with name '{}' already exists.", domain),
                                ));
                            }

                            value = domain;
                        } else {
                            return Err(SetError::invalid_property(
                                property,
                                "Invalid domain name.".to_string(),
                            ));
                        }
                    }
                    Value::Text { value }
                }
                (Property::Description, value @ (Value::Text { .. } | Value::Null)) => value,
                (Property::Email, Value::Text { value }) if ptype != Type::Domain => {
                    if let Some(email) = sanitize_email(&value) {
                        if current_fields.map_or(true, |v| match v.get(&Property::Email) {
                            Some(Value::Text { value }) => value != &email,
                            _ => true,
                        }) {
                            validate_emails.push(email.clone());
                        }
                        Value::Text { value: email }
                    } else {
                        return Err(SetError::invalid_property(
                            property,
                            "Invalid e-mail address.".to_string(),
                        ));
                    }
                }
                (Property::Timezone, value @ (Value::Text { .. } | Value::Null))
                    if ![Type::Domain, Type::List].contains(&ptype) =>
                {
                    value
                }
                (Property::Capabilities, value @ (Value::TextList { .. } | Value::Null))
                    if ![Type::Domain, Type::List].contains(&ptype) =>
                {
                    value
                }
                (Property::Aliases, Value::TextList { value }) if ptype != Type::Domain => {
                    let mut aliases = Vec::with_capacity(value.len());
                    for email in value {
                        if let Some(email) = sanitize_email(&email) {
                            if current_fields.map_or(true, |v| match v.get(&Property::Email) {
                                Some(Value::TextList { value }) => !value.contains(&email),
                                _ => true,
                            }) {
                                validate_emails.push(email.clone());
                            }
                            aliases.push(email);
                        } else {
                            return Err(SetError::invalid_property(
                                property,
                                "One or more invalid e-mail addresses.".to_string(),
                            ));
                        }
                    }
                    Value::TextList { value: aliases }
                }
                (Property::Secret, Value::Text { value })
                    if !value.is_empty() && [Type::Individual, Type::Domain].contains(&ptype) =>
                {
                    Value::Text {
                        value: value.to_string(), /*Scrypt
                                                  .hash_password(value.as_bytes(), &SaltString::generate(&mut OsRng))
                                                  .map_err(|_| {
                                                      SetError::invalid_property(
                                                          property,
                                                          "Failed to hash password.".to_string(),
                                                      )
                                                  })?
                                                  .to_string()*/
                    }
                }
                (Property::Secret, Value::Text { value })
                    if !value.is_empty() && ptype == Type::Domain =>
                {
                    Value::Text { value }
                }
                (Property::ACL, Value::ACL(value)) => {
                    for id in value.acl.keys() {
                        if !helper.document_ids.contains(id.get_document_id()) {
                            return Err(SetError::invalid_property(
                                property,
                                format!("Principal {} does not exist.", id),
                            ));
                        }
                    }

                    self.acl_update(value);
                    continue;
                }
                //TODO DKIM on mailsubmissions
                (Property::DKIM, value @ Value::DKIM { .. }) if ptype == Type::Domain => value,
                (Property::Quota, value @ (Value::Number { .. } | Value::Null)) => value,
                (Property::Picture, value @ (Value::Blob { .. } | Value::Null)) => value,
                (Property::Members, Value::Members { value })
                    if ![Type::Individual, Type::Domain].contains(&ptype) =>
                {
                    let mut new_members = Vec::with_capacity(value.len());
                    for id in &value {
                        if helper.document_ids.contains(id.get_document_id()) {
                            new_members.push(id.get_document_id());
                        } else {
                            return Err(SetError::invalid_property(
                                property,
                                format!("Principal {} does not exist.", id),
                            ));
                        }
                    }

                    Value::Members { value }
                }
                (
                    Property::Email
                    | Property::Secret
                    | Property::DKIM
                    | Property::Aliases
                    | Property::Members,
                    Value::Null,
                ) => Value::Null,
                (Property::Type, _) => {
                    continue;
                }
                (_, _) => {
                    return Err(SetError::invalid_property(
                        property,
                        "Unexpected value.".to_string(),
                    ));
                }
            };

            self.set(property, value);
        }

        // Validate e-mail addresses
        if !validate_emails.is_empty() {
            // Check that the domains exist
            for domain in validate_emails
                .iter()
                .filter_map(|e| e.split_once('@')?.1.into())
                .collect::<HashSet<_>>()
            {
                if helper
                    .store
                    .query_store::<FilterMapper>(
                        helper.account_id,
                        Collection::Principal,
                        Filter::and(vec![
                            Filter::eq(Property::Name.into(), Query::Index(domain.to_string())),
                            Filter::eq(Property::Type.into(), Query::Keyword("d".to_string())),
                        ]),
                        Comparator::None,
                    )?
                    .is_empty()
                {
                    return Err(SetError::invalid_property(
                        Property::Email,
                        format!("Domain '{}' does not exist on this server.", domain),
                    ));
                }
            }

            // Check if the e-mail address is already in use
            if !helper
                .store
                .query_store::<FilterMapper>(
                    helper.account_id,
                    Collection::Principal,
                    Filter::or(
                        validate_emails
                            .into_iter()
                            .map(|email| {
                                Filter::or(vec![
                                    Filter::eq(Property::Email.into(), Query::Index(email.clone())),
                                    Filter::eq(Property::Aliases.into(), Query::Index(email)),
                                ])
                            })
                            .collect(),
                    ),
                    Comparator::None,
                )?
                .is_empty()
            {
                return Err(SetError::invalid_property(
                    Property::Email,
                    "One of the entered email addresses is linked to another principal."
                        .to_string(),
                ));
            }
        }

        // Validate required fields
        if [Type::Individual, Type::List].contains(&ptype)
            && self
                .get(&Property::Email)
                .or_else(|| current_fields.and_then(|f| f.get(&Property::Email)))
                .map_or(true, |v| !matches!(v, Value::Text { .. }))
        {
            return Err(SetError::invalid_property(
                Property::Email,
                "Missing 'email' property.".to_string(),
            ));
        }

        if self
            .get(&Property::Name)
            .or_else(|| current_fields.and_then(|f| f.get(&Property::Name)))
            .map_or(true, |v| !matches!(v, Value::Text { .. }))
        {
            return Err(SetError::invalid_property(
                Property::Email,
                "Missing 'name' property.".to_string(),
            ));
        }

        if ptype == Type::Individual {
            // Make sure the account has a password
            if self
                .get(&Property::Secret)
                .or_else(|| current_fields.and_then(|f| f.get(&Property::Secret)))
                .map_or(true, |v| !matches!(v, Value::Text { .. }))
            {
                return Err(SetError::invalid_property(
                    Property::Email,
                    "Missing 'secret' property.".to_string(),
                ));
            }

            // Create default mailboxes in new accounts
            if current_fields.is_none() {
                for (name, role) in [("Inbox", "inbox"), ("Deleted Items", "trash")] {
                    let mut document = Document::new(
                        Collection::Mailbox,
                        helper
                            .store
                            .assign_document_id(document_id, Collection::Mailbox)?,
                    );
                    TinyORM::<Mailbox>::new_mailbox(name, role).insert(&mut document)?;
                    helper.changes.insert_document(document);
                }
            }
        }

        Ok(self)
    }
}
