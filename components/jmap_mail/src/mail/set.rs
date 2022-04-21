use jmap::error::method::MethodError;
use jmap::error::set::SetErrorType;
use jmap::id::blob::BlobId;
use jmap::id::JMAPIdSerialize;
use jmap::jmap_store::blob::JMAPBlobStore;
use jmap::jmap_store::set::{SetObject, SetObjectData, SetObjectHelper};
use jmap::protocol::json::JSONValue;
use jmap::request::set::SetRequest;
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
use store::batch::Document;
use store::chrono::DateTime;
use store::field::{DefaultOptions, Options};
use store::roaring::RoaringBitmap;
use store::{Collection, DocumentId, JMAPId, JMAPIdPrefix, JMAPStore, Store, StoreError, Tag};

use crate::mail::import::JMAPMailImport;
use crate::mail::parse::get_message_blob;
use crate::mail::{
    HeaderName, Keyword, MailHeaderForm, MailHeaderProperty, MailProperties, MessageField,
};

#[allow(clippy::large_enum_variant)]
pub enum SetMail {
    Create {
        mailbox_ids: HashSet<DocumentId>,
        keywords: HashSet<Tag>,
        received_at: Option<i64>,
        builder: MessageBuilder,
        body_values: Option<HashMap<String, JSONValue>>,
    },
    Update {
        document: Document,
        keyword_op_list: HashMap<Tag, bool>,
        keyword_op_clear_all: bool,
        mailbox_op_list: HashMap<Tag, bool>,
        mailbox_op_clear_all: bool,
    },
}

pub struct SetMailHelper {
    pub mailbox_ids: RoaringBitmap,
}

impl<T> SetObjectData<T> for SetMailHelper
where
    T: for<'x> Store<'x> + 'static,
{
    fn init(store: &JMAPStore<T>, request: &SetRequest) -> jmap::Result<Self> {
        Ok(SetMailHelper {
            mailbox_ids: store
                .get_document_ids(request.account_id, Collection::Mailbox)?
                .unwrap_or_default(),
        })
    }

    fn collection() -> Collection {
        Collection::Mail
    }
}

impl<'y, T> SetObject<'y, T> for SetMail
where
    T: for<'x> Store<'x> + 'static,
{
    type Property = MailProperties;
    type Helper = SetMailHelper;

    fn create(
        _helper: &mut SetObjectHelper<T, SetMailHelper>,
        fields: &mut HashMap<String, JSONValue>,
    ) -> Result<Self, JSONValue> {
        Ok(Self::Create {
            mailbox_ids: HashSet::new(),
            keywords: HashSet::new(),
            received_at: None,
            builder: MessageBuilder::new(),
            body_values: fields.remove("bodyValues").and_then(|v| v.unwrap_object()),
        })
    }

    fn update(
        _helper: &mut SetObjectHelper<T, SetMailHelper>,
        _fields: &mut HashMap<String, JSONValue>,
        jmap_id: JMAPId,
    ) -> Result<Self, JSONValue> {
        Ok(SetMail::Update {
            document: Document::new(Collection::Mail, jmap_id.get_document_id()),
            keyword_op_list: HashMap::new(),
            keyword_op_clear_all: false,
            mailbox_op_list: HashMap::new(),
            mailbox_op_clear_all: false,
        })
    }

    fn set_field(
        &mut self,
        helper: &mut SetObjectHelper<T, SetMailHelper>,
        field: Self::Property,
        value: JSONValue,
    ) -> Result<(), JSONValue> {
        match self {
            SetMail::Create {
                mailbox_ids,
                keywords,
                received_at,
                builder,
                body_values,
            } => match field {
                MailProperties::MailboxIds => {
                    let mailboxes = if let Some(mailboxes) = value.unwrap_object() {
                        mailboxes
                    } else {
                        return Err(JSONValue::new_error(
                            SetErrorType::InvalidProperties,
                            "Expected a MailboxId object.".to_string(),
                        ));
                    };
                    for (mailbox, value) in mailboxes {
                        if let (Some(mailbox_id), Some(set)) =
                            (JMAPId::from_jmap_string(&mailbox), value.to_bool())
                        {
                            if set {
                                let mailbox_id = mailbox_id.get_document_id();
                                if helper.data.mailbox_ids.contains(mailbox_id) {
                                    mailbox_ids.insert(mailbox_id);
                                } else {
                                    return Err(JSONValue::new_error(
                                        SetErrorType::InvalidProperties,
                                        format!("mailboxId {} does not exist.", mailbox),
                                    ));
                                }
                            }
                        } else {
                            return Err(JSONValue::new_error(
                                SetErrorType::InvalidProperties,
                                "Expected boolean value in mailboxIds",
                            ));
                        }
                    }
                }
                MailProperties::Keywords => {
                    for (keyword, value) in value.unwrap_object().ok_or_else(|| {
                        JSONValue::new_error(
                            SetErrorType::InvalidProperties,
                            "Expected object containing keywords",
                        )
                    })? {
                        if value.to_bool().ok_or_else(|| {
                            JSONValue::new_error(
                                SetErrorType::InvalidProperties,
                                "Expected boolean value in keywords",
                            )
                        })? {
                            keywords.insert(Keyword::from_jmap(keyword.to_string()));
                        }
                    }
                }
                MailProperties::ReceivedAt => {
                    *received_at = value.parse_json_date()?.into();
                }
                MailProperties::MessageId => builder.header(
                    "Message-ID",
                    MessageId::from(value.parse_json_string_list()?),
                ),
                MailProperties::InReplyTo => builder.header(
                    "In-Reply-To",
                    MessageId::from(value.parse_json_string_list()?),
                ),
                MailProperties::References => builder.header(
                    "References",
                    MessageId::from(value.parse_json_string_list()?),
                ),
                MailProperties::Sender => {
                    builder.header("Sender", Address::List(value.parse_json_addresses()?))
                }
                MailProperties::From => {
                    builder.header("From", Address::List(value.parse_json_addresses()?))
                }
                MailProperties::To => {
                    builder.header("To", Address::List(value.parse_json_addresses()?))
                }
                MailProperties::Cc => {
                    builder.header("Cc", Address::List(value.parse_json_addresses()?))
                }
                MailProperties::Bcc => {
                    builder.header("Bcc", Address::List(value.parse_json_addresses()?))
                }
                MailProperties::ReplyTo => {
                    builder.header("Reply-To", Address::List(value.parse_json_addresses()?))
                }
                MailProperties::Subject => {
                    builder.header("Subject", Text::new(value.parse_json_string()?));
                }
                MailProperties::SentAt => {
                    builder.header("Date", Date::new(value.parse_json_date()?))
                }
                MailProperties::TextBody => {
                    builder.text_body = value
                        .parse_body_parts(helper, body_values, "text/plain".into(), true)?
                        .pop()
                        .ok_or_else(|| {
                            JSONValue::new_error(
                                SetErrorType::InvalidProperties,
                                "No text body part found".to_string(),
                            )
                        })?
                        .into();
                }
                MailProperties::HtmlBody => {
                    builder.html_body = value
                        .parse_body_parts(helper, body_values, "text/html".into(), true)?
                        .pop()
                        .ok_or_else(|| {
                            JSONValue::new_error(
                                SetErrorType::InvalidProperties,
                                "No html body part found".to_string(),
                            )
                        })?
                        .into();
                }
                MailProperties::Attachments => {
                    builder.attachments = value
                        .parse_body_parts(helper, body_values, None, false)?
                        .into();
                }
                MailProperties::BodyStructure => {
                    builder.body = value.parse_body_structure(helper, body_values)?.into();
                }
                MailProperties::Header(MailHeaderProperty { form, header, all }) => {
                    if !all {
                        value.parse_header(builder, header, form)?;
                    } else {
                        for value in value.unwrap_array().ok_or_else(|| {
                            JSONValue::new_error(
                                SetErrorType::InvalidProperties,
                                "Expected an array.".to_string(),
                            )
                        })? {
                            value.parse_header(builder, header.clone(), form.clone())?;
                        }
                    }
                }

                MailProperties::Id
                | MailProperties::Size
                | MailProperties::BlobId
                | MailProperties::Preview
                | MailProperties::ThreadId
                | MailProperties::BodyValues
                | MailProperties::HasAttachment => (),
            },
            SetMail::Update {
                keyword_op_list,
                keyword_op_clear_all,
                mailbox_op_list,
                mailbox_op_clear_all,
                ..
            } => {
                match (field, value) {
                    (MailProperties::Keywords, JSONValue::Object(value)) => {
                        // Add keywords to the list
                        for (keyword, value) in value {
                            if let JSONValue::Bool(true) = value {
                                keyword_op_list.insert(Keyword::from_jmap(keyword), true);
                            }
                        }
                        *keyword_op_clear_all = true;
                    }
                    (MailProperties::MailboxIds, JSONValue::Object(value)) => {
                        // Add mailbox ids to the list
                        for (mailbox_id, value) in value {
                            match (JMAPId::from_jmap_string(mailbox_id.as_ref()), value) {
                                (Some(mailbox_id), JSONValue::Bool(true)) => {
                                    mailbox_op_list
                                        .insert(Tag::Id(mailbox_id.get_document_id()), true);
                                }
                                (None, _) => {
                                    return Err(JSONValue::new_invalid_property(
                                        format!("mailboxIds/{}", mailbox_id),
                                        "Invalid JMAP Id".to_string(),
                                    ));
                                }
                                _ => (),
                            }
                        }
                        *mailbox_op_clear_all = true;
                    }
                    (field, _) => {
                        return Err(JSONValue::new_invalid_property(
                            field.to_string(),
                            "Unsupported property or invalid format.",
                        ));
                    }
                }
            }
        };

        Ok(())
    }

    fn patch_field(
        &mut self,
        _helper: &mut SetObjectHelper<T, SetMailHelper>,
        field: Self::Property,
        property: String,
        value: JSONValue,
    ) -> Result<(), JSONValue> {
        if let SetMail::Update {
            keyword_op_list,
            mailbox_op_list,
            ..
        } = self
        {
            let is_set = match value {
                JSONValue::Null | JSONValue::Bool(false) => false,
                JSONValue::Bool(true) => true,
                _ => {
                    return Err(JSONValue::new_invalid_property(
                        format!("{}/{}", field, property),
                        "Expected a boolean or null value.",
                    ));
                }
            };

            match &field {
                MailProperties::MailboxIds => match JMAPId::from_jmap_string(property.as_ref()) {
                    Some(mailbox_id) => {
                        mailbox_op_list.insert(Tag::Id(mailbox_id.get_document_id()), is_set);
                    }
                    None => {
                        return Err(JSONValue::new_invalid_property(
                            format!("{}/{}", field, property),
                            "Invalid JMAP Id",
                        ));
                    }
                },
                MailProperties::Keywords => {
                    keyword_op_list.insert(Keyword::from_jmap(property), is_set);
                }
                _ => {
                    return Err(JSONValue::new_invalid_property(
                        format!("{}/{}", field, property),
                        "Unsupported property.",
                    ));
                }
            }
        }
        Ok(())
    }

    fn write(
        self,
        helper: &mut SetObjectHelper<T, SetMailHelper>,
    ) -> jmap::Result<Result<Option<JSONValue>, JSONValue>> {
        match self {
            SetMail::Create {
                mailbox_ids,
                keywords,
                received_at,
                builder,
                ..
            } => {
                if mailbox_ids.is_empty() {
                    return Ok(Err(JSONValue::new_error(
                        SetErrorType::InvalidProperties,
                        "Message has to belong to at least one mailbox.",
                    )));
                }

                if builder.headers.is_empty()
                    && builder.body.is_none()
                    && builder.html_body.is_none()
                    && builder.text_body.is_none()
                    && builder.attachments.is_none()
                {
                    return Ok(Err(JSONValue::new_error(
                        SetErrorType::InvalidProperties,
                        "Message has to have at least one header or body part.",
                    )));
                }

                // TODO: write parsed message directly to store, avoid parsing it again.
                let mut blob = Vec::with_capacity(1024);
                builder.write_to(&mut blob).map_err(|_| {
                    MethodError::ServerFail(StoreError::SerializeError(
                        "Failed to write to memory.".to_string(),
                    ))
                })?;

                helper
                    .store
                    .mail_import_blob(
                        helper.account_id,
                        blob,
                        mailbox_ids.into_iter().collect(),
                        keywords.into_iter().collect(),
                        received_at,
                    )
                    .map(|r| Ok(Some(r)))
            }
            SetMail::Update {
                mut document,
                keyword_op_list,
                keyword_op_clear_all,
                mailbox_op_list,
                mailbox_op_clear_all,
            } => {
                let mut changed_mailboxes = HashSet::new();
                if !mailbox_op_list.is_empty() || mailbox_op_clear_all {
                    // Deserialize mailbox list
                    let current_mailboxes = helper
                        .store
                        .get_document_tags(
                            helper.account_id,
                            Collection::Mail,
                            document.document_id,
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
                            if helper.data.mailbox_ids.contains(mailbox_id) {
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
                                return Ok(Err(JSONValue::new_invalid_property(
                                    format!("mailboxIds/{}", mailbox_id),
                                    "Mailbox does not exist.",
                                )));
                            }
                        }
                    }

                    // Messages have to be in at least one mailbox
                    if !has_mailboxes {
                        return Ok(Err(JSONValue::new_invalid_property(
                            "mailboxIds",
                            "Message must belong to at least one mailbox.",
                        )));
                    }
                }

                if !keyword_op_list.is_empty() || keyword_op_clear_all {
                    // Deserialize current keywords
                    let current_keywords = helper
                        .store
                        .get_document_tags(
                            helper.account_id,
                            Collection::Mail,
                            document.document_id,
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
                                    && matches!(keyword, Tag::Static(k_id) if k_id == &Keyword::SEEN )
                                {
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
                                && matches!(keyword, Tag::Static(k_id) if k_id == &Keyword::SEEN )
                            {
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
                                    && matches!(&keyword, Tag::Static(k_id) if k_id == &Keyword::SEEN )
                                {
                                    unread_changed = true;
                                }
                            }
                        }
                    }

                    // Mark mailboxes as changed if the message is tagged/untagged with $seen
                    if unread_changed {
                        if let Some(current_mailboxes) = helper.store.get_document_tags(
                            helper.account_id,
                            Collection::Mail,
                            document.document_id,
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
                        helper
                            .changes
                            .log_child_update(Collection::Mailbox, changed_mailbox_id);
                    }
                }

                if !document.is_empty() {
                    helper.changes.update_document(document);
                    Ok(Ok(Some(JSONValue::Null)))
                } else {
                    Ok(Ok(None))
                }
            }
        }
    }

    fn delete(
        _helper: &mut SetObjectHelper<T, SetMailHelper>,

        _jmap_id: JMAPId,
    ) -> jmap::Result<Result<(), JSONValue>> {
        Ok(Ok(()))
    }

    fn parse_property(property: &str) -> Option<Self::Property> {
        MailProperties::parse(property)
    }
}

pub trait JSONMailValue {
    fn parse_header(
        self,
        builder: &mut MessageBuilder,
        header: HeaderName,
        form: MailHeaderForm,
    ) -> Result<(), JSONValue>;
    fn parse_body_structure<T>(
        self,
        helper: &SetObjectHelper<T, SetMailHelper>,
        body_values: &mut Option<HashMap<String, JSONValue>>,
    ) -> Result<MimePart, JSONValue>
    where
        T: for<'x> Store<'x> + 'static;
    fn parse_body_part<T>(
        self,
        helper: &SetObjectHelper<T, SetMailHelper>,
        body_values: &mut Option<HashMap<String, JSONValue>>,
        implicit_type: Option<&'static str>,
        strict_implicit_type: bool,
    ) -> Result<(MimePart, Option<Vec<JSONValue>>), JSONValue>
    where
        T: for<'x> Store<'x> + 'static;
    fn parse_body_parts<T>(
        self,
        helper: &SetObjectHelper<T, SetMailHelper>,
        body_values: &mut Option<HashMap<String, JSONValue>>,
        implicit_type: Option<&'static str>,
        strict_implicit_type: bool,
    ) -> Result<Vec<MimePart>, JSONValue>
    where
        T: for<'x> Store<'x> + 'static;
    fn parse_json_string(self) -> Result<String, JSONValue>;
    fn parse_json_date(self) -> Result<i64, JSONValue>;
    fn parse_json_string_list(self) -> Result<Vec<String>, JSONValue>;
    fn parse_json_addresses(self) -> Result<Vec<Address>, JSONValue>;
    fn parse_json_grouped_addresses(self) -> Result<Vec<Address>, JSONValue>;
}

impl JSONMailValue for JSONValue {
    fn parse_header(
        self,
        builder: &mut MessageBuilder,
        header: HeaderName,
        form: MailHeaderForm,
    ) -> Result<(), JSONValue> {
        match form {
            MailHeaderForm::Raw => {
                builder.header(header.unwrap(), Raw::new(self.parse_json_string()?))
            }
            MailHeaderForm::Text => {
                builder.header(header.unwrap(), Text::new(self.parse_json_string()?))
            }
            MailHeaderForm::Addresses => {
                builder.header(header.unwrap(), Address::List(self.parse_json_addresses()?))
            }
            MailHeaderForm::GroupedAddresses => builder.header(
                header.unwrap(),
                Address::List(self.parse_json_grouped_addresses()?),
            ),
            MailHeaderForm::MessageIds => builder.header(
                header.unwrap(),
                MessageId::from(self.parse_json_string_list()?),
            ),
            MailHeaderForm::Date => {
                builder.header(header.unwrap(), Date::new(self.parse_json_date()?))
            }
            MailHeaderForm::URLs => {
                builder.header(header.unwrap(), URL::from(self.parse_json_string_list()?))
            }
        }
        Ok(())
    }

    fn parse_body_structure<T>(
        self,
        helper: &SetObjectHelper<T, SetMailHelper>,
        body_values: &mut Option<HashMap<String, JSONValue>>,
    ) -> Result<MimePart, JSONValue>
    where
        T: for<'x> Store<'x> + 'static,
    {
        let (mut mime_part, sub_parts) = self.parse_body_part(helper, body_values, None, false)?;

        if let Some(sub_parts) = sub_parts {
            let mut stack = Vec::new();
            let mut it = sub_parts.into_iter();

            loop {
                while let Some(part) = it.next() {
                    let (sub_mime_part, sub_parts) =
                        part.parse_body_part(helper, body_values, None, false)?;
                    if let Some(sub_parts) = sub_parts {
                        stack.push((mime_part, it));
                        mime_part = sub_mime_part;
                        it = sub_parts.into_iter();
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

    fn parse_body_part<T>(
        self,
        helper: &SetObjectHelper<T, SetMailHelper>,
        body_values: &mut Option<HashMap<String, JSONValue>>,
        implicit_type: Option<&'static str>,
        strict_implicit_type: bool,
    ) -> Result<(MimePart, Option<Vec<JSONValue>>), JSONValue>
    where
        T: for<'x> Store<'x> + 'static,
    {
        let mut part = self.unwrap_object().ok_or_else(|| {
            JSONValue::new_error(
                SetErrorType::InvalidProperties,
                "Expected an object in body part list.".to_string(),
            )
        })?;

        let content_type = part
            .remove("type")
            .and_then(|v| v.unwrap_string())
            .unwrap_or_else(|| implicit_type.unwrap_or("text/plain").to_string());

        if strict_implicit_type && implicit_type.unwrap() != content_type {
            return Err(JSONValue::new_error(
                SetErrorType::InvalidProperties,
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
            } else if let Some(part_id) = part.remove("partId").and_then(|v| v.unwrap_string()) {
                BodyPart::Text( body_values.as_mut()
                    .ok_or_else(|| {
                        JSONValue::new_error(
                            SetErrorType::InvalidProperties,
                            "Missing \"bodyValues\" object containing partId.".to_string(),
                        )
                    })?
                    .remove(&part_id)
                    .ok_or_else(|| {
                        JSONValue::new_error(
                            SetErrorType::InvalidProperties,
                            format!("Missing body value for partId \"{}\"", part_id),
                        )
                    })?
                    .unwrap_object()
                    .ok_or_else(|| {
                        JSONValue::new_error(
                            SetErrorType::InvalidProperties,
                            format!("Expected a bodyValues object defining partId \"{}\"", part_id),
                        )
                    })?
                    .remove("value")
                    .ok_or_else(|| {
                        JSONValue::new_error(
                            SetErrorType::InvalidProperties,
                            format!("Missing \"value\" field in bodyValues object defining partId \"{}\"", part_id),
                        )
                    })?
                    .unwrap_string()
                    .ok_or_else(|| {
                        JSONValue::new_error(
                            SetErrorType::InvalidProperties,
                            format!("Expected a string \"value\" field in bodyValues object defining partId \"{}\"", part_id),
                        )
                    })?)
            } else if let Some(blob_id) = part.remove("blobId").and_then(|v| v.unwrap_string()) {
                BodyPart::Binary(
                    helper
                        .store
                        .download_blob(
                            helper.account_id,
                            &BlobId::from_jmap_string(&blob_id).ok_or_else(|| {
                                JSONValue::new_error(
                                    SetErrorType::BlobNotFound,
                                    "Failed to parse blobId",
                                )
                            })?,
                            get_message_blob,
                        )
                        .map_err(|_| {
                            JSONValue::new_error(
                                SetErrorType::BlobNotFound,
                                "Failed to fetch blob.",
                            )
                        })?
                        .ok_or_else(|| {
                            JSONValue::new_error(
                                SetErrorType::BlobNotFound,
                                "blobId does not exist on this server.",
                            )
                        })?,
                )
            } else {
                return Err(JSONValue::new_error(
                    SetErrorType::InvalidProperties,
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
                } else if let Some(charset) = part.remove("charset") {
                    content_type.attributes.insert(
                        "charset".into(),
                        charset
                            .to_string()
                            .ok_or_else(|| {
                                JSONValue::new_error(
                                    SetErrorType::InvalidProperties,
                                    "Expected a string value for \"charset\" field.".to_string(),
                                )
                            })?
                            .into(),
                    );
                };
            }

            match (
                part.remove("disposition").and_then(|v| v.unwrap_string()),
                part.remove("name").and_then(|v| v.unwrap_string()),
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
                    content_type.attributes.insert("name".into(), filename);
                }
                (None, None) => (),
            };

            if let Some(languages) = part.remove("language").and_then(|v| v.unwrap_array()) {
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

            if let Some(cid) = part.remove("cid").and_then(|v| v.unwrap_string()) {
                mime_part
                    .headers
                    .insert("Content-ID".into(), MessageId::new(cid).into());
            }

            if let Some(location) = part.remove("location").and_then(|v| v.unwrap_string()) {
                mime_part
                    .headers
                    .insert("Content-Location".into(), Text::new(location).into());
            }
        }

        mime_part
            .headers
            .insert("Content-Type".into(), content_type.into());
        let mut sub_parts = None;

        for (property, value) in part {
            if property.starts_with("header:") {
                match property.split(':').nth(1) {
                    Some(header_name) if !header_name.is_empty() => {
                        mime_part.headers.insert(
                            header_name.into(),
                            Raw::new(value.unwrap_string().ok_or_else(|| {
                                JSONValue::new_error(
                                    SetErrorType::InvalidProperties,
                                    format!("Expected a string value for \"{}\" field.", property),
                                )
                            })?)
                            .into(),
                        );
                    }
                    _ => (),
                }
            } else if property == "headers" {
                if let Some(headers) = value.unwrap_array() {
                    for header in headers {
                        let mut header = header.unwrap_object().ok_or_else(|| {
                            JSONValue::new_error(
                                SetErrorType::InvalidProperties,
                                "Expected an object for \"headers\" field.".to_string(),
                            )
                        })?;
                        mime_part.headers.insert(
                            header
                                .remove("name")
                                .and_then(|v| v.unwrap_string())
                                .ok_or_else(|| {
                                    JSONValue::new_error(
                                        SetErrorType::InvalidProperties,
                                        "Expected a string value for \"name\" field in \"headers\" field."
                                            .to_string(),
                                    )
                                })?
                                ,
                            Raw::new(
                                header
                                    .remove("value")
                                    .and_then(|v| v.unwrap_string())
                                    .ok_or_else(|| {
                                        JSONValue::new_error(
                                        SetErrorType::InvalidProperties,
                                        "Expected a string value for \"value\" field in \"headers\" field."
                                            .to_string(),
                                    )
                                    })?,
                            )
                            .into(),
                        );
                    }
                }
            } else if property == "subParts" {
                sub_parts = value.unwrap_array();
            }
        }

        Ok((mime_part, if is_multipart { sub_parts } else { None }))
    }

    fn parse_body_parts<T>(
        self,
        helper: &SetObjectHelper<T, SetMailHelper>,
        body_values: &mut Option<HashMap<String, JSONValue>>,
        implicit_type: Option<&'static str>,
        strict_implicit_type: bool,
    ) -> Result<Vec<MimePart>, JSONValue>
    where
        T: for<'x> Store<'x> + 'static,
    {
        let parts = self.unwrap_array().ok_or_else(|| {
            JSONValue::new_error(
                SetErrorType::InvalidProperties,
                "Expected an array containing body part.".to_string(),
            )
        })?;

        let mut result = Vec::with_capacity(parts.len());
        for part in parts {
            result.push(
                part.parse_body_part(helper, body_values, implicit_type, strict_implicit_type)?
                    .0,
            );
        }

        Ok(result)
    }

    fn parse_json_string(self) -> Result<String, JSONValue> {
        self.unwrap_string().ok_or_else(|| {
            JSONValue::new_error(
                SetErrorType::InvalidProperties,
                "Expected a String property.".to_string(),
            )
        })
    }

    fn parse_json_date(self) -> Result<i64, JSONValue> {
        Ok(
            DateTime::parse_from_rfc3339(self.to_string().ok_or_else(|| {
                JSONValue::new_error(
                    SetErrorType::InvalidProperties,
                    "Expected a UTCDate property.".to_string(),
                )
            })?)
            .map_err(|_| {
                JSONValue::new_error(
                    SetErrorType::InvalidProperties,
                    "Expected a valid UTCDate property.".to_string(),
                )
            })?
            .timestamp(),
        )
    }

    fn parse_json_string_list(self) -> Result<Vec<String>, JSONValue> {
        let value = self.unwrap_array().ok_or_else(|| {
            JSONValue::new_error(
                SetErrorType::InvalidProperties,
                "Expected a String array.".to_string(),
            )
        })?;

        let mut list = Vec::with_capacity(value.len());
        for v in value {
            list.push(v.unwrap_string().ok_or_else(|| {
                JSONValue::new_error(
                    SetErrorType::InvalidProperties,
                    "Expected a String array.".to_string(),
                )
            })?);
        }

        Ok(list)
    }

    fn parse_json_addresses(self) -> Result<Vec<Address>, JSONValue> {
        let value = self.unwrap_array().ok_or_else(|| {
            JSONValue::new_error(
                SetErrorType::InvalidProperties,
                "Expected an array with EmailAddress objects.".to_string(),
            )
        })?;

        let mut result = Vec::with_capacity(value.len());
        for addr in value {
            let mut addr = addr.unwrap_object().ok_or_else(|| {
                JSONValue::new_error(
                    SetErrorType::InvalidProperties,
                    "Expected an array containing EmailAddress objects.".to_string(),
                )
            })?;
            result.push(Address::new_address(
                addr.remove("name").and_then(|n| n.unwrap_string()),
                addr.remove("email")
                    .and_then(|n| n.unwrap_string())
                    .ok_or_else(|| {
                        JSONValue::new_error(
                            SetErrorType::InvalidProperties,
                            "Missing 'email' field in EmailAddress object.".to_string(),
                        )
                    })?,
            ));
        }

        Ok(result)
    }

    fn parse_json_grouped_addresses<'x>(self) -> Result<Vec<Address>, JSONValue> {
        let value = self.unwrap_array().ok_or_else(|| {
            JSONValue::new_error(
                SetErrorType::InvalidProperties,
                "Expected an array with EmailAddressGroup objects.".to_string(),
            )
        })?;

        let mut result = Vec::with_capacity(value.len());
        for addr in value {
            let mut addr = addr.unwrap_object().ok_or_else(|| {
                JSONValue::new_error(
                    SetErrorType::InvalidProperties,
                    "Expected an array containing EmailAddressGroup objects.".to_string(),
                )
            })?;
            result.push(Address::new_group(
                addr.remove("name").and_then(|n| n.unwrap_string()),
                addr.remove("addresses")
                    .ok_or_else(|| {
                        JSONValue::new_error(
                            SetErrorType::InvalidProperties,
                            "Missing 'addresses' field in EmailAddressGroup object.".to_string(),
                        )
                    })?
                    .parse_json_addresses()?,
            ));
        }

        Ok(result)
    }
}
