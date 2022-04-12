use chrono::DateTime;
use jmap::blob::JMAPBlobStore;
use jmap::changes::JMAPChanges;
use jmap::id::{BlobId, JMAPIdSerialize};
use jmap::json::JSONValue;

use jmap::{json::JSONPointer, JMAPError, JMAPSet, JMAPSetErrorType};
use mail_builder::headers::address::Address;
use mail_builder::headers::content_type::ContentType;
use mail_builder::headers::date::Date;
use mail_builder::headers::message_id::MessageId;
use mail_builder::headers::raw::Raw;
use mail_builder::headers::text::Text;
use mail_builder::headers::url::URL;
use mail_builder::mime::{BodyPart, MimePart};
use mail_builder::MessageBuilder;
use std::collections::{BTreeMap, HashMap, HashSet};
use store::batch::{Document, WriteBatch};
use store::field::{DefaultOptions, Options};
use store::roaring::RoaringBitmap;
use store::{AccountId, Collection, JMAPId, JMAPIdPrefix, JMAPStore, Store, Tag};

use crate::import::JMAPMailImport;
use crate::parse::get_message_blob;
use crate::query::MailboxId;
use crate::{
    HeaderName, JMAPMailHeaderForm, JMAPMailHeaderProperty, JMAPMailProperties, MessageField,
};

pub struct MessageItem {
    pub blob: Vec<u8>,
    pub mailbox_ids: Vec<MailboxId>,
    pub keywords: Vec<Tag>,
    pub received_at: Option<i64>,
}

pub trait JMAPMailSet {
    fn mail_set(&self, request: JMAPSet<()>) -> jmap::Result<JSONValue>;
    fn build_message(
        &self,
        account: AccountId,
        fields: JSONValue,
        existing_mailboxes: &RoaringBitmap,
    ) -> Result<MessageItem, JSONValue>;
    fn import_body_structure<'x: 'y, 'y>(
        &'y self,
        account: AccountId,
        part: &'x JSONValue,
        body_values: Option<&'x HashMap<String, JSONValue>>,
    ) -> Result<MimePart, JSONValue>;
    fn import_body_part<'x: 'y, 'y>(
        &'y self,
        account: AccountId,
        part: &'x JSONValue,
        body_values: Option<&'x HashMap<String, JSONValue>>,
        implicit_type: Option<&'x str>,
        strict_implicit_type: bool,
    ) -> Result<(MimePart, Option<&'x Vec<JSONValue>>), JSONValue>;
    fn import_body_parts<'x: 'y, 'y>(
        &'y self,
        account: AccountId,
        parts: &'x JSONValue,
        body_values: Option<&'x HashMap<String, JSONValue>>,
        implicit_type: Option<&'x str>,
        strict_implicit_type: bool,
    ) -> Result<Vec<MimePart>, JSONValue>;
}

impl<T> JMAPMailSet for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_set(&self, request: JMAPSet<()>) -> jmap::Result<JSONValue> {
        let old_state = self.get_state(request.account_id, Collection::Mail)?;
        if let Some(if_in_state) = request.if_in_state {
            if old_state != if_in_state {
                return Err(JMAPError::StateMismatch);
            }
        }

        let total_changes = request.create.to_object().map_or(0, |c| c.len())
            + request.update.to_object().map_or(0, |c| c.len())
            + request.destroy.to_array().map_or(0, |c| c.len());
        if total_changes > self.config.set_max_changes {
            return Err(JMAPError::RequestTooLarge);
        }

        let mut changes = WriteBatch::new(request.account_id, self.config.is_in_cluster);
        let document_ids = self
            .get_document_ids(request.account_id, Collection::Mail)?
            .unwrap_or_else(RoaringBitmap::new);
        let mut mailbox_ids = None;
        let mut response = HashMap::new();

        if let JSONValue::Object(create) = request.create {
            let mut created = HashMap::with_capacity(create.len());
            let mut not_created = HashMap::with_capacity(create.len());

            for (create_id, message_fields) in create {
                let mailbox_ids = if let Some(mailbox_ids) = &mailbox_ids {
                    mailbox_ids
                } else {
                    mailbox_ids = self
                        .get_document_ids(request.account_id, Collection::Mailbox)?
                        .unwrap_or_default()
                        .into();
                    mailbox_ids.as_ref().unwrap()
                };

                match self.build_message(request.account_id, message_fields, mailbox_ids) {
                    Ok(import_item) => {
                        created.insert(
                            create_id,
                            self.mail_import_blob(
                                request.account_id,
                                import_item.blob,
                                import_item.mailbox_ids,
                                import_item.keywords,
                                import_item.received_at,
                            )?,
                        );
                    }
                    Err(err) => {
                        not_created.insert(create_id, err);
                    }
                }
            }

            response.insert(
                "created".to_string(),
                if !created.is_empty() {
                    created.into()
                } else {
                    JSONValue::Null
                },
            );
            response.insert(
                "notCreated".to_string(),
                if !not_created.is_empty() {
                    not_created.into()
                } else {
                    JSONValue::Null
                },
            );
        } else {
            response.insert("created".to_string(), JSONValue::Null);
            response.insert("notCreated".to_string(), JSONValue::Null);
        }

        if let JSONValue::Object(update) = request.update {
            let mut updated = HashMap::with_capacity(update.len());
            let mut not_updated = HashMap::with_capacity(update.len());

            'main: for (jmap_id_str, properties) in update {
                let (jmap_id, properties) = if let (Some(jmap_id), Some(properties)) = (
                    JMAPId::from_jmap_string(&jmap_id_str),
                    properties.unwrap_object(),
                ) {
                    (jmap_id, properties)
                } else {
                    not_updated.insert(
                        jmap_id_str,
                        JSONValue::new_error(
                            JMAPSetErrorType::InvalidProperties,
                            "Failed to parse request.",
                        ),
                    );
                    continue 'main;
                };
                let document_id = jmap_id.get_document_id();
                if !document_ids.contains(document_id) {
                    not_updated.insert(
                        jmap_id_str,
                        JSONValue::new_error(JMAPSetErrorType::NotFound, "ID not found."),
                    );
                    continue;
                } else if let JSONValue::Array(destroy_ids) = &request.destroy {
                    if destroy_ids
                        .iter()
                        .any(|x| x.to_string().map(|v| v == jmap_id_str).unwrap_or(false))
                    {
                        not_updated.insert(
                            jmap_id_str,
                            JSONValue::new_error(
                                JMAPSetErrorType::WillDestroy,
                                "ID will be destroyed.",
                            ),
                        );
                        continue;
                    }
                }
                let mut document = Document::new(Collection::Mail, document_id);

                let mut keyword_op_list = HashMap::new();
                let mut keyword_op_clear_all = false;
                let mut mailbox_op_list = HashMap::new();
                let mut mailbox_op_clear_all = false;

                for (field, value) in properties {
                    match JSONPointer::parse(&field).unwrap_or(JSONPointer::Root) {
                        JSONPointer::String(field) => {
                            match JMAPMailProperties::parse(&field)
                                .unwrap_or(JMAPMailProperties::Id)
                            {
                                JMAPMailProperties::Keywords => {
                                    if let JSONValue::Object(value) = value {
                                        // Add keywords to the list
                                        for (keyword, value) in value {
                                            if let JSONValue::Bool(true) = value {
                                                keyword_op_list.insert(Tag::Text(keyword), true);
                                            }
                                        }
                                        keyword_op_clear_all = true;
                                    } else {
                                        not_updated.insert(
                                            jmap_id_str,
                                            JSONValue::new_invalid_property(
                                                "keywords",
                                                "Expected an object.",
                                            ),
                                        );
                                        continue 'main;
                                    }
                                }
                                JMAPMailProperties::MailboxIds => {
                                    // Unwrap JSON object
                                    if let JSONValue::Object(value) = value {
                                        // Add mailbox ids to the list
                                        for (mailbox_id, value) in value {
                                            match (
                                                JMAPId::from_jmap_string(mailbox_id.as_ref()),
                                                value,
                                            ) {
                                                (Some(mailbox_id), JSONValue::Bool(true)) => {
                                                    mailbox_op_list.insert(
                                                        Tag::Id(mailbox_id.get_document_id()),
                                                        true,
                                                    );
                                                }
                                                (None, _) => {
                                                    not_updated.insert(
                                                        jmap_id_str,
                                                        JSONValue::new_invalid_property(
                                                            format!("mailboxIds/{}", mailbox_id),
                                                            "Failed to parse mailbox id.",
                                                        ),
                                                    );
                                                    continue 'main;
                                                }
                                                _ => (),
                                            }
                                        }
                                        mailbox_op_clear_all = true;
                                    } else {
                                        // mailboxIds is not a JSON object
                                        not_updated.insert(
                                            jmap_id_str,
                                            JSONValue::new_invalid_property(
                                                "mailboxIds",
                                                "Expected an object.",
                                            ),
                                        );
                                        continue 'main;
                                    }
                                }
                                _ => {
                                    not_updated.insert(
                                        jmap_id_str,
                                        JSONValue::new_invalid_property(
                                            field,
                                            "Unsupported property.",
                                        ),
                                    );
                                    continue 'main;
                                }
                            }
                        }

                        JSONPointer::Path(mut path) if path.len() == 2 => {
                            if let (JSONPointer::String(property), JSONPointer::String(field)) =
                                (path.pop().unwrap(), path.pop().unwrap())
                            {
                                let is_mailbox = match JMAPMailProperties::parse(&field)
                                    .unwrap_or(JMAPMailProperties::Id)
                                {
                                    JMAPMailProperties::MailboxIds => true,
                                    JMAPMailProperties::Keywords => false,
                                    _ => {
                                        not_updated.insert(
                                            jmap_id_str,
                                            JSONValue::new_invalid_property(
                                                format!("{}/{}", field, property),
                                                "Unsupported property.",
                                            ),
                                        );
                                        continue 'main;
                                    }
                                };
                                match value {
                                    JSONValue::Null | JSONValue::Bool(false) => {
                                        if is_mailbox {
                                            if let Some(mailbox_id) =
                                                JMAPId::from_jmap_string(property.as_ref())
                                            {
                                                mailbox_op_list.insert(
                                                    Tag::Id(mailbox_id.get_document_id()),
                                                    false,
                                                );
                                            }
                                        } else {
                                            keyword_op_list.insert(Tag::Text(property), false);
                                        }
                                    }
                                    JSONValue::Bool(true) => {
                                        if is_mailbox {
                                            if let Some(mailbox_id) =
                                                JMAPId::from_jmap_string(property.as_ref())
                                            {
                                                mailbox_op_list.insert(
                                                    Tag::Id(mailbox_id.get_document_id()),
                                                    true,
                                                );
                                            }
                                        } else {
                                            keyword_op_list.insert(Tag::Text(property), true);
                                        }
                                    }
                                    _ => {
                                        not_updated.insert(
                                            jmap_id_str,
                                            JSONValue::new_invalid_property(
                                                format!("{}/{}", field, property),
                                                "Expected a boolean or null value.",
                                            ),
                                        );
                                        continue 'main;
                                    }
                                }
                            } else {
                                not_updated.insert(
                                    jmap_id_str,
                                    JSONValue::new_invalid_property(field, "Unsupported property."),
                                );
                                continue 'main;
                            }
                        }
                        _ => {
                            not_updated.insert(
                                jmap_id_str,
                                JSONValue::new_invalid_property(
                                    field.to_string(),
                                    "Unsupported property.",
                                ),
                            );
                            continue 'main;
                        }
                    }
                }

                let mut changed_mailboxes = HashSet::with_capacity(mailbox_op_list.len());
                if !mailbox_op_list.is_empty() || mailbox_op_clear_all {
                    // Obtain mailboxes
                    let mailbox_ids = if let Some(mailbox_ids) = &mailbox_ids {
                        mailbox_ids
                    } else {
                        mailbox_ids = self
                            .get_document_ids(request.account_id, Collection::Mailbox)?
                            .unwrap_or_default()
                            .into();
                        mailbox_ids.as_ref().unwrap()
                    };

                    // Deserialize mailbox list
                    let current_mailboxes = self
                        .get_document_tags(
                            request.account_id,
                            Collection::Mail,
                            document_id,
                            MessageField::Mailbox.into(),
                        )?
                        .map(|current_mailboxes| current_mailboxes.items)
                        .unwrap_or_default();

                    let mut has_mailboxes = false;

                    for mailbox in &current_mailboxes {
                        if mailbox_op_clear_all {
                            // Untag mailbox unless it is in the list of mailboxes to tag
                            if !mailbox_op_list.get(mailbox).unwrap_or(&false) {
                                document.tag(
                                    MessageField::Mailbox,
                                    mailbox.clone(),
                                    DefaultOptions::new().clear(),
                                );
                                changed_mailboxes.insert(mailbox.unwrap_id().unwrap_or_default());
                            }
                        } else if !mailbox_op_list.get(mailbox).unwrap_or(&true) {
                            // Untag mailbox if is marked for untagging
                            document.tag(
                                MessageField::Mailbox,
                                mailbox.clone(),
                                DefaultOptions::new().clear(),
                            );
                            changed_mailboxes.insert(mailbox.unwrap_id().unwrap_or_default());
                        } else {
                            // Keep mailbox in the list
                            has_mailboxes = true;
                        }
                    }

                    for (mailbox, do_create) in mailbox_op_list {
                        if do_create {
                            let mailbox_id = mailbox.unwrap_id().unwrap_or_default();
                            // Make sure the mailbox exists
                            if mailbox_ids.contains(mailbox_id) {
                                // Tag mailbox if it is not already tagged
                                if !current_mailboxes.contains(&mailbox) {
                                    document.tag(
                                        MessageField::Mailbox,
                                        mailbox,
                                        DefaultOptions::new(),
                                    );
                                    changed_mailboxes.insert(mailbox_id);
                                }
                                has_mailboxes = true;
                            } else {
                                not_updated.insert(
                                    jmap_id_str,
                                    JSONValue::new_invalid_property(
                                        format!("mailboxIds/{}", mailbox_id),
                                        "Mailbox does not exist.",
                                    ),
                                );
                                continue 'main;
                            }
                        }
                    }

                    // Messages have to be in at least one mailbox
                    if !has_mailboxes {
                        not_updated.insert(
                            jmap_id_str,
                            JSONValue::new_invalid_property(
                                "mailboxIds",
                                "Message must belong to at least one mailbox.",
                            ),
                        );
                        continue 'main;
                    }
                }

                if !keyword_op_list.is_empty() || keyword_op_clear_all {
                    // Deserialize current keywords
                    let current_keywords = self
                        .get_document_tags(
                            request.account_id,
                            Collection::Mail,
                            document_id,
                            MessageField::Keyword.into(),
                        )?
                        .map(|tags| tags.items)
                        .unwrap_or_default();

                    let mut unread_changed = false;
                    for keyword in &current_keywords {
                        if keyword_op_clear_all {
                            // Untag keyword unless it is in the list of keywords to tag
                            if !keyword_op_list.get(keyword).unwrap_or(&false) {
                                document.tag(
                                    MessageField::Keyword,
                                    keyword.clone(),
                                    DefaultOptions::new().clear(),
                                );
                                if !unread_changed
                                    && matches!(keyword, Tag::Text(text) if text == "$seen" )
                                {
                                    //TODO use id
                                    unread_changed = true;
                                }
                            }
                        } else if !keyword_op_list.get(keyword).unwrap_or(&true) {
                            // Untag keyword if is marked for untagging
                            document.tag(
                                MessageField::Keyword,
                                keyword.clone(),
                                DefaultOptions::new().clear(),
                            );
                            if !unread_changed
                                && matches!(keyword, Tag::Text(text) if text == "$seen" )
                            {
                                //TODO use id
                                unread_changed = true;
                            }
                        }
                    }

                    for (keyword, do_create) in keyword_op_list {
                        if do_create {
                            // Tag keyword if it is not already tagged
                            if !current_keywords.contains(&keyword) {
                                document.tag(
                                    MessageField::Keyword,
                                    keyword.clone(),
                                    DefaultOptions::new(),
                                );
                                if !unread_changed
                                    && matches!(&keyword, Tag::Text(text) if text == "$seen" )
                                {
                                    //TODO use id
                                    unread_changed = true;
                                }
                            }
                        }
                    }

                    // Mark mailboxes as changed if the message is tagged/untagged with $seen
                    if unread_changed {
                        if let Some(current_mailboxes) = self.get_document_tags(
                            request.account_id,
                            Collection::Mail,
                            document_id,
                            MessageField::Mailbox.into(),
                        )? {
                            for mailbox in current_mailboxes.items {
                                changed_mailboxes.insert(mailbox.unwrap_id().unwrap_or_default());
                            }
                        }
                    }
                }

                // Log mailbox changes
                if !changed_mailboxes.is_empty() {
                    for changed_mailbox_id in changed_mailboxes {
                        changes.log_child_update(Collection::Mailbox, changed_mailbox_id);
                    }
                }

                if !document.is_empty() {
                    changes.update_document(document);
                    changes.log_update(Collection::Mail, jmap_id);
                    updated.insert(jmap_id_str, JSONValue::Null);
                } else {
                    not_updated.insert(
                        jmap_id_str,
                        JSONValue::new_error(
                            JMAPSetErrorType::InvalidPatch,
                            "No changes found in request.",
                        ),
                    );
                }
            }

            response.insert(
                "updated".to_string(),
                if !updated.is_empty() {
                    updated.into()
                } else {
                    JSONValue::Null
                },
            );
            response.insert(
                "notUpdated".to_string(),
                if !not_updated.is_empty() {
                    not_updated.into()
                } else {
                    JSONValue::Null
                },
            );
        } else {
            response.insert("updated".to_string(), JSONValue::Null);
            response.insert("notUpdated".to_string(), JSONValue::Null);
        }

        if let JSONValue::Array(destroy_ids) = request.destroy {
            let mut destroyed = Vec::with_capacity(destroy_ids.len());
            let mut not_destroyed = HashMap::with_capacity(destroy_ids.len());

            for destroy_id in destroy_ids {
                if let Some(jmap_id) = destroy_id.to_jmap_id() {
                    let document_id = jmap_id.get_document_id();
                    if document_ids.contains(document_id) {
                        changes.delete_document(Collection::Mail, document_id);
                        changes.log_delete(Collection::Mail, jmap_id);
                        destroyed.push(destroy_id);
                        continue;
                    }
                }
                if let JSONValue::String(destroy_id) = destroy_id {
                    not_destroyed.insert(
                        destroy_id,
                        JSONValue::new_error(JMAPSetErrorType::NotFound, "ID not found."),
                    );
                }
            }

            response.insert(
                "destroyed".to_string(),
                if !destroyed.is_empty() {
                    destroyed.into()
                } else {
                    JSONValue::Null
                },
            );
            response.insert(
                "notDestroyed".to_string(),
                if !not_destroyed.is_empty() {
                    not_destroyed.into()
                } else {
                    JSONValue::Null
                },
            );
        } else {
            response.insert("destroyed".to_string(), JSONValue::Null);
            response.insert("notDestroyed".to_string(), JSONValue::Null);
        }

        response.insert(
            "newState".to_string(),
            if !changes.is_empty() {
                self.write(changes)?;
                self.get_state(request.account_id, Collection::Mail)?
            } else {
                old_state.clone()
            }
            .into(),
        );
        response.insert("oldState".to_string(), old_state.into());

        Ok(response.into())
    }

    #[allow(clippy::blocks_in_if_conditions)]
    fn build_message(
        &self,
        account: AccountId,
        fields: JSONValue,
        existing_mailboxes: &RoaringBitmap,
    ) -> Result<MessageItem, JSONValue> {
        let fields = if let JSONValue::Object(fields) = fields {
            fields
        } else {
            return Err(JSONValue::new_error(
                JMAPSetErrorType::InvalidProperties,
                "Failed to parse request.",
            ));
        };

        let body_values = fields.get("bodyValues").and_then(|v| v.to_object());

        let mut builder = MessageBuilder::new();
        let mut mailbox_ids: Vec<MailboxId> = Vec::new();
        let mut keywords: Vec<Tag> = Vec::new();
        let mut received_at: Option<i64> = None;

        for (property, value) in &fields {
            match JMAPMailProperties::parse(property).ok_or_else(|| {
                JSONValue::new_error(
                    JMAPSetErrorType::InvalidProperties,
                    format!("Failed to parse {}", property),
                )
            })? {
                JMAPMailProperties::MailboxIds => {
                    for (mailbox, value) in value.to_object().ok_or_else(|| {
                        JSONValue::new_error(
                            JMAPSetErrorType::InvalidProperties,
                            "Expected object containing mailboxIds",
                        )
                    })? {
                        let mailbox_id = JMAPId::from_jmap_string(mailbox)
                            .ok_or_else(|| {
                                JSONValue::new_error(
                                    JMAPSetErrorType::InvalidProperties,
                                    format!("Failed to parse mailboxId: {}", mailbox),
                                )
                            })?
                            .get_document_id();

                        if value.to_bool().ok_or_else(|| {
                            JSONValue::new_error(
                                JMAPSetErrorType::InvalidProperties,
                                "Expected boolean value in mailboxIds",
                            )
                        })? {
                            if !existing_mailboxes.contains(mailbox_id) {
                                return Err(JSONValue::new_error(
                                    JMAPSetErrorType::InvalidProperties,
                                    format!("mailboxId {} does not exist.", mailbox),
                                ));
                            }
                            mailbox_ids.push(mailbox_id);
                        }
                    }
                }
                JMAPMailProperties::Keywords => {
                    for (keyword, value) in value.to_object().ok_or_else(|| {
                        JSONValue::new_error(
                            JMAPSetErrorType::InvalidProperties,
                            "Expected object containing keywords",
                        )
                    })? {
                        if value.to_bool().ok_or_else(|| {
                            JSONValue::new_error(
                                JMAPSetErrorType::InvalidProperties,
                                "Expected boolean value in keywords",
                            )
                        })? {
                            keywords.push(Tag::Text(keyword.to_string()));
                        }
                    }
                }
                JMAPMailProperties::ReceivedAt => {
                    received_at = import_json_date(value)?.into();
                }
                JMAPMailProperties::MessageId => builder.header(
                    "Message-ID",
                    MessageId::from(import_json_string_list(value)?),
                ),
                JMAPMailProperties::InReplyTo => builder.header(
                    "In-Reply-To",
                    MessageId::from(import_json_string_list(value)?),
                ),
                JMAPMailProperties::References => builder.header(
                    "References",
                    MessageId::from(import_json_string_list(value)?),
                ),
                JMAPMailProperties::Sender => {
                    builder.header("Sender", Address::List(import_json_addresses(value)?))
                }
                JMAPMailProperties::From => {
                    builder.header("From", Address::List(import_json_addresses(value)?))
                }
                JMAPMailProperties::To => {
                    builder.header("To", Address::List(import_json_addresses(value)?))
                }
                JMAPMailProperties::Cc => {
                    builder.header("Cc", Address::List(import_json_addresses(value)?))
                }
                JMAPMailProperties::Bcc => {
                    builder.header("Bcc", Address::List(import_json_addresses(value)?))
                }
                JMAPMailProperties::ReplyTo => {
                    builder.header("Reply-To", Address::List(import_json_addresses(value)?))
                }
                JMAPMailProperties::Subject => {
                    builder.header("Subject", Text::new(import_json_string(value)?));
                }
                JMAPMailProperties::SentAt => {
                    builder.header("Date", Date::new(import_json_date(value)?))
                }
                JMAPMailProperties::TextBody => {
                    builder.text_body = self
                        .import_body_parts(account, value, body_values, "text/plain".into(), true)?
                        .pop()
                        .ok_or_else(|| {
                            JSONValue::new_error(
                                JMAPSetErrorType::InvalidProperties,
                                "No text body part found".to_string(),
                            )
                        })?
                        .into();
                }
                JMAPMailProperties::HtmlBody => {
                    builder.html_body = self
                        .import_body_parts(account, value, body_values, "text/html".into(), true)?
                        .pop()
                        .ok_or_else(|| {
                            JSONValue::new_error(
                                JMAPSetErrorType::InvalidProperties,
                                "No html body part found".to_string(),
                            )
                        })?
                        .into();
                }
                JMAPMailProperties::Attachments => {
                    builder.attachments = self
                        .import_body_parts(account, value, body_values, None, false)?
                        .into();
                }
                JMAPMailProperties::BodyStructure => {
                    builder.body = self
                        .import_body_structure(account, value, body_values)?
                        .into();
                }
                JMAPMailProperties::Header(JMAPMailHeaderProperty { form, header, all }) => {
                    if !all {
                        import_header(&mut builder, header, form, value)?;
                    } else {
                        for value in value.to_array().ok_or_else(|| {
                            JSONValue::new_error(
                                JMAPSetErrorType::InvalidProperties,
                                "Expected an array.".to_string(),
                            )
                        })? {
                            import_header(&mut builder, header.clone(), form.clone(), value)?;
                        }
                    }
                }

                JMAPMailProperties::Id
                | JMAPMailProperties::Size
                | JMAPMailProperties::BlobId
                | JMAPMailProperties::Preview
                | JMAPMailProperties::ThreadId
                | JMAPMailProperties::BodyValues
                | JMAPMailProperties::HasAttachment => (),
            }
        }

        if mailbox_ids.is_empty() {
            return Err(JSONValue::new_error(
                JMAPSetErrorType::InvalidProperties,
                "Message has to belong to at least one mailbox.",
            ));
        }

        if builder.headers.is_empty()
            && builder.body.is_none()
            && builder.html_body.is_none()
            && builder.text_body.is_none()
            && builder.attachments.is_none()
        {
            return Err(JSONValue::new_error(
                JMAPSetErrorType::InvalidProperties,
                "Message has to have at least one header or body part.",
            ));
        }

        // TODO: write parsed message directly to store, avoid parsing it again.
        let mut blob = Vec::with_capacity(1024);
        builder.write_to(&mut blob).map_err(|_| {
            JSONValue::new_error(JMAPSetErrorType::InvalidProperties, "Internal error")
        })?;

        Ok(MessageItem {
            blob,
            mailbox_ids,
            keywords,
            received_at,
        })
    }

    fn import_body_structure<'x: 'y, 'y>(
        &'y self,
        account: AccountId,
        part: &'x JSONValue,
        body_values: Option<&'x HashMap<String, JSONValue>>,
    ) -> Result<MimePart, JSONValue> {
        let (mut mime_part, sub_parts) =
            self.import_body_part(account, part, body_values, None, false)?;

        if let Some(sub_parts) = sub_parts {
            let mut stack = Vec::new();
            let mut it = sub_parts.iter();

            loop {
                while let Some(part) = it.next() {
                    let (sub_mime_part, sub_parts) =
                        self.import_body_part(account, part, body_values, None, false)?;
                    if let Some(sub_parts) = sub_parts {
                        stack.push((mime_part, it));
                        mime_part = sub_mime_part;
                        it = sub_parts.iter();
                    } else {
                        mime_part.add_part(sub_mime_part);
                    }
                }
                if let Some((mut prev_mime_part, prev_it)) = stack.pop() {
                    prev_mime_part.add_part(mime_part);
                    mime_part = prev_mime_part;
                    it = prev_it;
                } else {
                    break;
                }
            }
        }

        Ok(mime_part)
    }

    fn import_body_part<'x: 'y, 'y>(
        &'y self,
        account: AccountId,
        part: &'x JSONValue,
        body_values: Option<&'x HashMap<String, JSONValue>>,
        implicit_type: Option<&'x str>,
        strict_implicit_type: bool,
    ) -> Result<(MimePart, Option<&'x Vec<JSONValue>>), JSONValue> {
        let part = part.to_object().ok_or_else(|| {
            JSONValue::new_error(
                JMAPSetErrorType::InvalidProperties,
                "Expected an object in body part list.".to_string(),
            )
        })?;

        let content_type = part
            .get("type")
            .and_then(|v| v.to_string())
            .unwrap_or_else(|| implicit_type.unwrap_or("text/plain"));

        if strict_implicit_type && implicit_type.unwrap() != content_type {
            return Err(JSONValue::new_error(
                JMAPSetErrorType::InvalidProperties,
                format!(
                    "Expected exactly body part of type \"{}\"",
                    implicit_type.unwrap()
                ),
            ));
        }

        let is_multipart = content_type.starts_with("multipart/");
        let mut mime_part = MimePart {
            headers: BTreeMap::new(),
            contents: if is_multipart {
                BodyPart::Multipart(vec![])
            } else if let Some(part_id) = part.get("partId").and_then(|v| v.to_string()) {
                BodyPart::Text( body_values
                    .ok_or_else(|| {
                        JSONValue::new_error(
                            JMAPSetErrorType::InvalidProperties,
                            "Missing \"bodyValues\" object containing partId.".to_string(),
                        )
                    })?
                    .get(part_id)
                    .ok_or_else(|| {
                        JSONValue::new_error(
                            JMAPSetErrorType::InvalidProperties,
                            format!("Missing body value for partId \"{}\"", part_id),
                        )
                    })?
                    .to_object()
                    .ok_or_else(|| {
                        JSONValue::new_error(
                            JMAPSetErrorType::InvalidProperties,
                            format!("Expected a bodyValues object defining partId \"{}\"", part_id),
                        )
                    })?
                    .get("value")
                    .ok_or_else(|| {
                        JSONValue::new_error(
                            JMAPSetErrorType::InvalidProperties,
                            format!("Missing \"value\" field in bodyValues object defining partId \"{}\"", part_id),
                        )
                    })?
                    .to_string()
                    .ok_or_else(|| {
                        JSONValue::new_error(
                            JMAPSetErrorType::InvalidProperties,
                            format!("Expected a string \"value\" field in bodyValues object defining partId \"{}\"", part_id),
                        )
                    })?.into())
            } else if let Some(blob_id) = part.get("blobId").and_then(|v| v.to_string()) {
                BodyPart::Binary(
                    self.download_blob(
                        account,
                        &BlobId::from_jmap_string(blob_id).ok_or_else(|| {
                            JSONValue::new_error(
                                JMAPSetErrorType::BlobNotFound,
                                "Failed to parse blobId",
                            )
                        })?,
                        get_message_blob,
                    )
                    .map_err(|_| {
                        JSONValue::new_error(
                            JMAPSetErrorType::BlobNotFound,
                            "Failed to fetch blob.",
                        )
                    })?
                    .ok_or_else(|| {
                        JSONValue::new_error(
                            JMAPSetErrorType::BlobNotFound,
                            "blobId does not exist on this server.",
                        )
                    })?
                    .into(),
                )
            } else {
                return Err(JSONValue::new_error(
                    JMAPSetErrorType::InvalidProperties,
                    "Expected a \"partId\" or \"blobId\" field in body part.".to_string(),
                ));
            },
        };

        let mut content_type = ContentType::new(content_type);
        if !is_multipart {
            if content_type.c_type.starts_with("text/") {
                if matches!(mime_part.contents, BodyPart::Text(_)) {
                    content_type
                        .attributes
                        .insert("charset".into(), "utf-8".into());
                } else if let Some(charset) = part.get("charset") {
                    content_type.attributes.insert(
                        "charset".into(),
                        charset
                            .to_string()
                            .ok_or_else(|| {
                                JSONValue::new_error(
                                    JMAPSetErrorType::InvalidProperties,
                                    "Expected a string value for \"charset\" field.".to_string(),
                                )
                            })?
                            .into(),
                    );
                };
            }

            match (
                part.get("disposition").and_then(|v| v.to_string()),
                part.get("name").and_then(|v| v.to_string()),
            ) {
                (Some(disposition), Some(filename)) => {
                    mime_part.headers.insert(
                        "Content-Disposition".into(),
                        ContentType::new(disposition)
                            .attribute("filename", filename)
                            .into(),
                    );
                }
                (Some(disposition), None) => {
                    mime_part.headers.insert(
                        "Content-Disposition".into(),
                        ContentType::new(disposition).into(),
                    );
                }
                (None, Some(filename)) => {
                    content_type
                        .attributes
                        .insert("name".into(), filename.into());
                }
                (None, None) => (),
            };

            if let Some(languages) = part.get("language").and_then(|v| v.to_array()) {
                mime_part.headers.insert(
                    "Content-Language".into(),
                    Text::new(
                        languages
                            .iter()
                            .filter_map(|v| v.to_string())
                            .collect::<Vec<&str>>()
                            .join(","),
                    )
                    .into(),
                );
            }

            if let Some(cid) = part.get("cid").and_then(|v| v.to_string()) {
                mime_part
                    .headers
                    .insert("Content-ID".into(), MessageId::new(cid).into());
            }

            if let Some(location) = part.get("location").and_then(|v| v.to_string()) {
                mime_part
                    .headers
                    .insert("Content-Location".into(), Text::new(location).into());
            }
        }

        mime_part
            .headers
            .insert("Content-Type".into(), content_type.into());

        for (property, value) in part {
            if property.starts_with("header:") {
                match property.split(':').nth(1) {
                    Some(header_name) if !header_name.is_empty() => {
                        mime_part.headers.insert(
                            header_name.into(),
                            Raw::new(value.to_string().ok_or_else(|| {
                                JSONValue::new_error(
                                    JMAPSetErrorType::InvalidProperties,
                                    format!("Expected a string value for \"{}\" field.", property),
                                )
                            })?)
                            .into(),
                        );
                    }
                    _ => (),
                }
            }
        }

        if let Some(headers) = part.get("headers").and_then(|v| v.to_array()) {
            for header in headers {
                let header = header.to_object().ok_or_else(|| {
                    JSONValue::new_error(
                        JMAPSetErrorType::InvalidProperties,
                        "Expected an object for \"headers\" field.".to_string(),
                    )
                })?;
                mime_part.headers.insert(
                    header
                        .get("name")
                        .and_then(|v| v.to_string())
                        .ok_or_else(|| {
                            JSONValue::new_error(
                                JMAPSetErrorType::InvalidProperties,
                                "Expected a string value for \"name\" field in \"headers\" field."
                                    .to_string(),
                            )
                        })?
                        .into(),
                    Raw::new(
                        header
                            .get("value")
                            .and_then(|v| v.to_string())
                            .ok_or_else(|| {
                                JSONValue::new_error(
                                JMAPSetErrorType::InvalidProperties,
                                "Expected a string value for \"value\" field in \"headers\" field."
                                    .to_string(),
                            )
                            })?,
                    )
                    .into(),
                );
            }
        }
        Ok((
            mime_part,
            if is_multipart {
                part.get("subParts").and_then(|v| v.to_array())
            } else {
                None
            },
        ))
    }

    fn import_body_parts<'x: 'y, 'y>(
        &'y self,
        account: AccountId,
        parts: &'x JSONValue,
        body_values: Option<&'x HashMap<String, JSONValue>>,
        implicit_type: Option<&'x str>,
        strict_implicit_type: bool,
    ) -> Result<Vec<MimePart>, JSONValue> {
        let parts = parts.to_array().ok_or_else(|| {
            JSONValue::new_error(
                JMAPSetErrorType::InvalidProperties,
                "Expected an array containing body part.".to_string(),
            )
        })?;

        let mut result = Vec::with_capacity(parts.len());
        for part in parts {
            result.push(
                self.import_body_part(
                    account,
                    part,
                    body_values,
                    implicit_type,
                    strict_implicit_type,
                )?
                .0,
            );
        }

        Ok(result)
    }
}

fn import_header<'y>(
    builder: &mut MessageBuilder<'y>,
    header: HeaderName,
    form: JMAPMailHeaderForm,
    value: &'y JSONValue,
) -> Result<(), JSONValue> {
    match form {
        JMAPMailHeaderForm::Raw => {
            builder.header(header.unwrap(), Raw::new(import_json_string(value)?))
        }
        JMAPMailHeaderForm::Text => {
            builder.header(header.unwrap(), Text::new(import_json_string(value)?))
        }
        JMAPMailHeaderForm::Addresses => builder.header(
            header.unwrap(),
            Address::List(import_json_addresses(value)?),
        ),
        JMAPMailHeaderForm::GroupedAddresses => builder.header(
            header.unwrap(),
            Address::List(import_json_grouped_addresses(value)?),
        ),
        JMAPMailHeaderForm::MessageIds => builder.header(
            header.unwrap(),
            MessageId::from(import_json_string_list(value)?),
        ),
        JMAPMailHeaderForm::Date => {
            builder.header(header.unwrap(), Date::new(import_json_date(value)?))
        }
        JMAPMailHeaderForm::URLs => {
            builder.header(header.unwrap(), URL::from(import_json_string_list(value)?))
        }
    }
    Ok(())
}

fn import_json_string(value: &JSONValue) -> Result<&str, JSONValue> {
    value.to_string().ok_or_else(|| {
        JSONValue::new_error(
            JMAPSetErrorType::InvalidProperties,
            "Expected a String property.".to_string(),
        )
    })
}

fn import_json_date(value: &JSONValue) -> Result<i64, JSONValue> {
    Ok(
        DateTime::parse_from_rfc3339(value.to_string().ok_or_else(|| {
            JSONValue::new_error(
                JMAPSetErrorType::InvalidProperties,
                "Expected a Date property.".to_string(),
            )
        })?)
        .map_err(|_| {
            JSONValue::new_error(
                JMAPSetErrorType::InvalidProperties,
                "Expected a valid Date property.".to_string(),
            )
        })?
        .timestamp(),
    )
}

fn import_json_string_list(value: &JSONValue) -> Result<Vec<&str>, JSONValue> {
    let value = value.to_array().ok_or_else(|| {
        JSONValue::new_error(
            JMAPSetErrorType::InvalidProperties,
            "Expected an array with String.".to_string(),
        )
    })?;

    let mut list = Vec::with_capacity(value.len());
    for v in value {
        list.push(v.to_string().ok_or_else(|| {
            JSONValue::new_error(
                JMAPSetErrorType::InvalidProperties,
                "Expected an array with String.".to_string(),
            )
        })?);
    }

    Ok(list)
}

fn import_json_addresses(value: &JSONValue) -> Result<Vec<Address>, JSONValue> {
    let value = value.to_array().ok_or_else(|| {
        JSONValue::new_error(
            JMAPSetErrorType::InvalidProperties,
            "Expected an array with EmailAddress objects.".to_string(),
        )
    })?;

    let mut result = Vec::with_capacity(value.len());
    for addr in value {
        let addr = addr.to_object().ok_or_else(|| {
            JSONValue::new_error(
                JMAPSetErrorType::InvalidProperties,
                "Expected an array containing EmailAddress objects.".to_string(),
            )
        })?;
        result.push(Address::new_address(
            addr.get("name").and_then(|n| n.to_string()),
            addr.get("email")
                .and_then(|n| n.to_string())
                .ok_or_else(|| {
                    JSONValue::new_error(
                        JMAPSetErrorType::InvalidProperties,
                        "Missing 'email' field in EmailAddress object.".to_string(),
                    )
                })?,
        ));
    }

    Ok(result)
}

fn import_json_grouped_addresses(value: &JSONValue) -> Result<Vec<Address>, JSONValue> {
    let value = value.to_array().ok_or_else(|| {
        JSONValue::new_error(
            JMAPSetErrorType::InvalidProperties,
            "Expected an array with EmailAddressGroup objects.".to_string(),
        )
    })?;

    let mut result = Vec::with_capacity(value.len());
    for addr in value {
        let addr = addr.to_object().ok_or_else(|| {
            JSONValue::new_error(
                JMAPSetErrorType::InvalidProperties,
                "Expected an array containing EmailAddressGroup objects.".to_string(),
            )
        })?;
        result.push(Address::new_group(
            addr.get("name").and_then(|n| n.to_string()),
            import_json_addresses(addr.get("addresses").ok_or_else(|| {
                JSONValue::new_error(
                    JMAPSetErrorType::InvalidProperties,
                    "Missing 'addresses' field in EmailAddressGroup object.".to_string(),
                )
            })?)?,
        ));
    }

    Ok(result)
}
