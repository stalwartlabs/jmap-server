use jmap::error::set::{SetError, SetErrorType};
use jmap::jmap_store::set::SetHelper;
use jmap::jmap_store::Object;
use jmap::orm::acl::ACLUpdate;
use jmap::orm::{serialize::JMAPOrm, TinyORM};
use jmap::principal::schema::{Patch, Principal, Property, Type, Value, ACCOUNTS_TO_DELETE};
use jmap::principal::store::JMAPPrincipals;
use jmap::request::set::SetRequest;
use jmap::request::set::SetResponse;
use jmap::types::jmap::JMAPId;
use jmap::{sanitize_domain, sanitize_email, INGEST_ID, SUPERUSER_ID};
use jmap_mail::mailbox::schema::Mailbox;
use jmap_mail::mailbox::CreateMailbox;
use store::ahash::AHashSet;
use store::core::collection::Collection;
use store::core::document::Document;
use store::core::error::StoreError;
use store::core::tag::Tag;
use store::read::comparator::Comparator;
use store::read::filter::{self, Filter, Query};
use store::read::FilterMapper;
use store::write::batch::WriteBatch;
use store::write::options::IndexOptions;
use store::{DocumentId, JMAPStore, Store};

pub trait JMAPSetPrincipal<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn principal_set(&self, request: SetRequest<Principal>)
        -> jmap::Result<SetResponse<Principal>>;

    fn principal_delete(
        &self,
        batch: &mut WriteBatch,
        document: &mut Document,
    ) -> store::Result<()>;

    fn principal_purge(&self) -> store::Result<()>;
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

        let tagged_for_deletion_ids = self.get_tag(
            SUPERUSER_ID,
            Collection::Principal,
            ACCOUNTS_TO_DELETE,
            Tag::Static(ACCOUNTS_TO_DELETE),
        )?;

        helper.create(|_create_id, item, helper, document| {
            // Make sure the assigned principal Id is not scheduled for deletion
            if let Some(tagged_for_deletion_ids) = &tagged_for_deletion_ids {
                while tagged_for_deletion_ids.contains(document.document_id) {
                    document.document_id = helper
                        .store
                        .assign_document_id(SUPERUSER_ID, Collection::Principal)?;
                }
            }

            // Set values
            TinyORM::<Principal>::new()
                .principal_set(helper, item, None, document.document_id)?
                .insert_validate(document)?;

            Ok(Principal::new(document.document_id.into()))
        })?;

        helper.update(|id, item, helper, document| {
            let document_id = id.get_document_id();
            let current_fields = self
                .get_orm::<Principal>(SUPERUSER_ID, document_id)?
                .ok_or_else(|| SetError::new_err(SetErrorType::NotFound))?;
            let fields = TinyORM::track_changes(&current_fields).principal_set(
                helper,
                item,
                Some(&current_fields),
                document.document_id,
            )?;

            // Invalidate cache on mailing lists or email address changes
            match (
                fields.get(&Property::Email),
                current_fields.get(&Property::Email),
            ) {
                (
                    Some(Value::Text { value: new_email }),
                    Some(Value::Text { value: old_email }),
                ) if new_email != old_email => {
                    helper.store.recipients.invalidate(old_email);
                }
                _ => (),
            }
            if let (Some(Value::Members { .. }), Some(Value::Text { value: email })) = (
                fields.get(&Property::Members),
                current_fields.get(&Property::Email),
            ) {
                helper.store.recipients.invalidate(email);
            }

            // Merge changes
            current_fields.merge_validate(document, fields)?;

            Ok(None)
        })?;

        helper.destroy(|id, helper, document| {
            if [SUPERUSER_ID, INGEST_ID].contains(&document.document_id) {
                return Err(SetError::forbidden("Cannot delete system accounts."));
            }

            if let Some(fields) = self.get_orm::<Principal>(SUPERUSER_ID, document.document_id)? {
                // Remove member from all principals
                for document_id in self
                    .query_store::<FilterMapper>(
                        SUPERUSER_ID,
                        Collection::Principal,
                        filter::Filter::eq(
                            Property::Members.into(),
                            Query::Integer(document.document_id),
                        ),
                        Comparator::None,
                    )?
                    .into_bitmap()
                {
                    if let Some(fields) = self.get_orm::<Principal>(SUPERUSER_ID, document_id)? {
                        if let Some(members) =
                            fields.get(&Property::Members).and_then(|p| match p {
                                Value::Members { value } if value.contains(&id) => Some(value),
                                _ => None,
                            })
                        {
                            let mut new_fields = TinyORM::track_changes(&fields);
                            new_fields.set(
                                Property::Members,
                                if members.len() > 1 {
                                    Value::Members {
                                        value: members
                                            .iter()
                                            .filter(|m| *m != &id)
                                            .cloned()
                                            .collect::<Vec<_>>(),
                                    }
                                } else {
                                    Value::Null
                                },
                            );
                            let mut document = Document::new(Collection::Principal, document_id);
                            fields.merge(&mut document, new_fields)?;
                            helper.changes.update_document(document);
                            helper
                                .changes
                                .log_update(Collection::Principal, JMAPId::from(document_id));
                        }
                    }
                }

                // Tag account for deletion
                let mut tag_deletion = Document::new(Collection::Principal, document.document_id);
                tag_deletion.tag(
                    ACCOUNTS_TO_DELETE,
                    Tag::Static(ACCOUNTS_TO_DELETE),
                    IndexOptions::new(),
                );
                helper.changes.update_document(tag_deletion);

                if let Some(Value::Text { value }) = fields.get(&Property::Email) {
                    helper.store.recipients.invalidate(value);
                }
                helper.store.acl_tokens.invalidate(&document.document_id);
                fields.delete(document);
            }
            Ok(())
        })?;

        helper.into_response()
    }

    fn principal_delete(
        &self,
        batch: &mut WriteBatch,
        document: &mut Document,
    ) -> store::Result<()> {
        // Delete ORM
        self.get_orm::<Principal>(SUPERUSER_ID, document.document_id)?
            .ok_or_else(|| {
                StoreError::NotFound(format!(
                    "Failed to fetch Principal ORM for {}:{}.",
                    SUPERUSER_ID, document.document_id
                ))
            })?
            .delete(document);

        // Tag account for deletion
        let mut tag_deletion = Document::new(Collection::Principal, document.document_id);
        tag_deletion.tag(
            ACCOUNTS_TO_DELETE,
            Tag::Static(ACCOUNTS_TO_DELETE),
            IndexOptions::new(),
        );
        batch.update_document(tag_deletion);

        Ok(())
    }

    fn principal_purge(&self) -> store::Result<()> {
        if let Some(accounts_to_delete) = self.get_tag(
            SUPERUSER_ID,
            Collection::Principal,
            ACCOUNTS_TO_DELETE,
            Tag::Static(ACCOUNTS_TO_DELETE),
        )? {
            self.delete_accounts(&accounts_to_delete)?;
            self.untag(
                SUPERUSER_ID,
                Collection::Principal,
                ACCOUNTS_TO_DELETE,
                Tag::Static(ACCOUNTS_TO_DELETE),
                accounts_to_delete.iter(),
            )?;
        }

        Ok(())
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
                                    SUPERUSER_ID,
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

                (Property::Aliases, Value::Patch(Patch::Aliases(value)))
                    if ptype != Type::Domain =>
                {
                    let mut aliases = match current_fields {
                        Some(v) => match v.get(&Property::Aliases) {
                            Some(Value::TextList { value }) => value.to_vec(),
                            _ => vec![],
                        },
                        None => vec![],
                    };

                    for (email, do_set) in value {
                        if do_set {
                            if let Some(email) = sanitize_email(&email) {
                                if !aliases.contains(&email) {
                                    validate_emails.push(email.clone());
                                    aliases.push(email);
                                }
                            } else {
                                return Err(SetError::invalid_property(
                                    property,
                                    "One or more invalid e-mail addresses.".to_string(),
                                ));
                            }
                        } else {
                            aliases.retain(|v| v != &email);
                        }
                    }

                    if !aliases.is_empty() {
                        Value::TextList { value: aliases }
                    } else {
                        Value::Null
                    }
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

                (Property::ACL, Value::Patch(Patch::ACL(value))) => {
                    for acl_update in &value {
                        match acl_update {
                            ACLUpdate::Replace { acls } => {
                                self.acl_clear();
                                for (account_id, acls) in acls {
                                    self.acl_update(
                                        helper.store.principal_to_id(account_id)?,
                                        acls,
                                    );
                                }
                            }
                            ACLUpdate::Update { account_id, acls } => {
                                self.acl_update(helper.store.principal_to_id(account_id)?, acls);
                            }
                            ACLUpdate::Set {
                                account_id,
                                acl,
                                is_set,
                            } => {
                                self.acl_set(
                                    helper.store.principal_to_id(account_id)?,
                                    *acl,
                                    *is_set,
                                );
                            }
                        }
                    }
                    self.acl_finish();
                    continue;
                }

                (Property::DKIM, value @ Value::DKIM { .. }) if ptype == Type::Domain => value,

                (Property::Quota, value @ (Value::Number { .. } | Value::Null)) => value,

                (Property::Picture, value @ (Value::Blob { .. } | Value::Null)) => value,

                (Property::Members, Value::Members { value }) if ptype == Type::Group => {
                    let current_members = current_fields.as_ref().and_then(|f| {
                        f.get(&Property::Members).and_then(|current_members| {
                            if let Value::Members {
                                value: current_members,
                            } = current_members
                            {
                                for id in current_members {
                                    if !value.contains(id) {
                                        helper.store.acl_tokens.invalidate(&id.get_document_id());
                                    }
                                }
                                Some(current_members)
                            } else {
                                None
                            }
                        })
                    });
                    for id in &value {
                        let account_id = id.get_document_id();

                        if account_id == document_id {
                            return Err(SetError::invalid_property(
                                property,
                                "Cannot add a principal as its member.".to_string(),
                            ));
                        } else if helper.document_ids.contains(account_id) {
                            if current_members.as_ref().map_or(true, |l| !l.contains(id)) {
                                helper.store.acl_tokens.invalidate(&account_id);
                            }
                        } else {
                            return Err(SetError::invalid_property(
                                property,
                                format!("Principal '{}' does not exist.", id),
                            ));
                        }
                    }

                    Value::Members { value }
                }

                (Property::Members, Value::Patch(Patch::Members(value)))
                    if ptype == Type::Group =>
                {
                    let mut members = match current_fields {
                        Some(v) => match v.get(&Property::Members) {
                            Some(Value::Members { value }) => value.to_vec(),
                            _ => vec![],
                        },
                        None => vec![],
                    };

                    for (id, do_set) in value {
                        let account_id = id.get_document_id();

                        if do_set {
                            if !members.contains(&id) {
                                if account_id == document_id {
                                    return Err(SetError::invalid_property(
                                        property,
                                        "Cannot add a principal as its member.".to_string(),
                                    ));
                                } else if helper.document_ids.contains(account_id) {
                                    members.push(id);
                                    helper.store.acl_tokens.invalidate(&account_id);
                                } else {
                                    return Err(SetError::invalid_property(
                                        property,
                                        format!("Principal '{}' does not exist.", id),
                                    ));
                                }
                            }
                        } else if let Some(pos) = members.iter().position(|id_| id_ == &id) {
                            helper.store.acl_tokens.invalidate(&account_id);
                            members.swap_remove(pos);
                        }
                    }

                    Value::Members { value: members }
                }

                (Property::Members, Value::Members { value }) if ptype == Type::List => {
                    let individuals = helper
                        .store
                        .query_store::<FilterMapper>(
                            SUPERUSER_ID,
                            Collection::Principal,
                            Filter::eq(Property::Type.into(), Query::Keyword("i".to_string())),
                            Comparator::None,
                        )?
                        .into_bitmap();

                    for id in &value {
                        if !individuals.contains(id.get_document_id()) {
                            return Err(SetError::invalid_property(
                                property,
                                format!("Principal '{}' is not an individual.", id),
                            ));
                        } else if id.get_document_id() == document_id {
                            return Err(SetError::invalid_property(
                                property,
                                "Cannot add a principal as its member.".to_string(),
                            ));
                        }
                    }

                    Value::Members { value }
                }

                (Property::Members, Value::Patch(Patch::Members(value))) if ptype == Type::List => {
                    let mut members = match current_fields {
                        Some(v) => match v.get(&Property::Members) {
                            Some(Value::Members { value }) => value.to_vec(),
                            _ => vec![],
                        },
                        None => vec![],
                    };

                    let individuals = helper
                        .store
                        .query_store::<FilterMapper>(
                            SUPERUSER_ID,
                            Collection::Principal,
                            Filter::eq(Property::Type.into(), Query::Keyword("i".to_string())),
                            Comparator::None,
                        )?
                        .into_bitmap();

                    for (id, do_set) in value {
                        if do_set {
                            if !members.contains(&id) {
                                if !individuals.contains(id.get_document_id()) {
                                    return Err(SetError::invalid_property(
                                        property,
                                        format!("Principal '{}' is not an individual.", id),
                                    ));
                                } else if id.get_document_id() == document_id {
                                    return Err(SetError::invalid_property(
                                        property,
                                        "Cannot add a principal as its member.".to_string(),
                                    ));
                                }
                                members.push(id);
                            }
                        } else {
                            members.retain(|m| m != &id);
                        }
                    }

                    Value::Members { value: members }
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
                .collect::<AHashSet<_>>()
            {
                if helper
                    .store
                    .query_store::<FilterMapper>(
                        SUPERUSER_ID,
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
                    SUPERUSER_ID,
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
        if [Type::Individual, Type::List, Type::Group].contains(&ptype)
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
        }

        // Create default mailboxes in new accounts
        if current_fields.is_none() && [Type::Individual, Type::Group].contains(&ptype) {
            let mut batch = WriteBatch::new(document_id);
            for (name, role) in [("Inbox", "inbox"), ("Deleted Items", "trash")] {
                let mut document = Document::new(
                    Collection::Mailbox,
                    helper
                        .store
                        .assign_document_id(document_id, Collection::Mailbox)?,
                );
                TinyORM::<Mailbox>::new_mailbox(name, role).insert(&mut document)?;
                batch.log_insert(Collection::Mailbox, document.document_id);
                batch.insert_document(document);
            }
            helper.changes.add_linked_batch(batch);
        }

        Ok(self)
    }
}
