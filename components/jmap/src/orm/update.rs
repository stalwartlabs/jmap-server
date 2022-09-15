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

use store::core::document::Document;
use store::core::error::StoreError;
use store::nlp::Language;
use store::serialize::StoreSerialize;
use store::write::options::{IndexOptions, Options};

use crate::error::set::SetError;

use super::{Index, Object, TinyORM, Value};

impl<T> TinyORM<T>
where
    T: Object + 'static,
{
    pub fn insert_validate(
        self,
        document: &mut Document,
    ) -> crate::error::set::Result<(), T::Property> {
        for property in T::required() {
            if self
                .properties
                .get(property)
                .map(|v| v.is_empty())
                .unwrap_or(true)
            {
                return Err(SetError::invalid_property(
                    property.clone(),
                    "Property cannot be empty.".to_string(),
                ));
            }
        }

        for (property, max_len) in T::max_len() {
            if self
                .properties
                .get(property)
                .map(|v| v.len() > *max_len)
                .unwrap_or(false)
            {
                return Err(SetError::invalid_property(
                    property.clone(),
                    format!("Property cannot be longer than {} bytes.", max_len),
                ));
            }
        }

        self.insert(document).map_err(|err| err.into())
    }

    pub fn insert(self, document: &mut Document) -> store::Result<()> {
        self.insert_orm(document)?;
        self.update_document(document, false);
        Ok(())
    }

    pub fn delete(self, document: &mut Document) {
        TinyORM::<T>::delete_orm(document);
        self.update_document(document, true);
    }

    fn update_document(self, document: &mut Document, is_delete: bool) {
        let indexed = T::indexed();
        if indexed.is_empty() && self.tags.is_empty() {
            return;
        }

        for (property, value) in self.properties {
            let (is_indexed, index_options) = indexed
                .iter()
                .filter_map(|(p, index_options)| {
                    if p == &property {
                        Some((
                            true,
                            if !is_delete {
                                *index_options
                            } else {
                                (*index_options).clear()
                            },
                        ))
                    } else {
                        None
                    }
                })
                .next()
                .unwrap_or((false, 0));

            if is_indexed {
                match value.index_as() {
                    Index::Text(value) => {
                        document.text(property, value, Language::Unknown, index_options);
                    }
                    Index::TextList(value) => {
                        for item in value {
                            document.text(property.clone(), item, Language::Unknown, index_options);
                        }
                    }
                    Index::Integer(value) => {
                        document.number(property, value, index_options);
                    }
                    Index::LongInteger(value) => {
                        document.number(property, value, index_options);
                    }
                    Index::IntegerList(value) => {
                        for item in value {
                            document.number(property.clone(), item, index_options);
                        }
                    }
                    Index::Null => (),
                }
            }
        }

        let index_options = if !is_delete {
            IndexOptions::new()
        } else {
            IndexOptions::new().clear()
        };
        for (property, tags) in self.tags {
            for tag in tags {
                document.tag(property.clone(), tag, index_options);
            }
        }

        for acl in self.acls {
            document.acl(acl, index_options);
        }
    }

    pub fn insert_orm(&self, document: &mut Document) -> store::Result<()> {
        document.binary(
            Self::FIELD_ID,
            self.serialize().ok_or_else(|| {
                StoreError::SerializeError("Failed to serialize ORM object.".to_string())
            })?,
            IndexOptions::new().store(),
        );
        Ok(())
    }

    pub fn delete_orm(document: &mut Document) {
        document.binary(
            Self::FIELD_ID,
            Vec::with_capacity(0),
            IndexOptions::new().clear(),
        );
    }
}
