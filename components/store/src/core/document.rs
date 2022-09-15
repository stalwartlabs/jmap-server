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

use crate::{blob::BlobId, nlp::Language, write::field::Field, DocumentId, FieldId};

use super::{acl::Permission, collection::Collection, number::Number, tag::Tag};

pub const MAX_TOKEN_LENGTH: usize = 25;
pub const MAX_ID_LENGTH: usize = 100;
pub const MAX_SORT_FIELD_LENGTH: usize = 255;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Text {
    pub text: String,
    pub language: Language,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Document {
    pub collection: Collection,
    pub document_id: DocumentId,
    pub term_index: Option<(BlobId, u64)>,

    pub text_fields: Vec<Field<Text>>,
    pub number_fields: Vec<Field<Number>>,
    pub binary_fields: Vec<Field<Vec<u8>>>,
    pub tag_fields: Vec<Field<Tag>>,
    pub acls: Vec<(Permission, u64)>,
    pub blobs: Vec<(BlobId, u64)>,
}

impl Document {
    pub fn new(collection: Collection, document_id: DocumentId) -> Document {
        Document {
            collection,
            document_id,
            text_fields: Vec::new(),
            number_fields: Vec::new(),
            binary_fields: Vec::new(),
            tag_fields: Vec::new(),
            blobs: Vec::new(),
            acls: Vec::new(),
            term_index: None,
        }
    }

    pub fn text(
        &mut self,
        field: impl Into<FieldId>,
        value: String,
        language: Language,
        options: u64,
    ) {
        self.text_fields.push(Field::new(
            field.into(),
            Text {
                text: value,
                language,
            },
            options,
        ));
    }

    pub fn binary(&mut self, field: impl Into<FieldId>, value: Vec<u8>, options: u64) {
        self.binary_fields
            .push(Field::new(field.into(), value, options));
    }

    pub fn number(&mut self, field: impl Into<FieldId>, value: impl Into<Number>, options: u64) {
        self.number_fields
            .push(Field::new(field.into(), value.into(), options));
    }

    pub fn tag(&mut self, field: impl Into<FieldId>, value: Tag, options: u64) {
        self.tag_fields
            .push(Field::new(field.into(), value, options));
    }

    pub fn blob(&mut self, blob: BlobId, options: u64) {
        self.blobs.push((blob, options));
    }

    pub fn acl(&mut self, acl: Permission, options: u64) {
        self.acls.push((acl, options));
    }

    pub fn term_index(&mut self, blob: BlobId, options: u64) {
        self.term_index = Some((blob, options));
    }

    pub fn is_empty(&self) -> bool {
        self.text_fields.is_empty()
            && self.number_fields.is_empty()
            && self.binary_fields.is_empty()
            && self.tag_fields.is_empty()
    }
}
