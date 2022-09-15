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
use jmap::request::get::{GetRequest, GetResponse};
use jmap::types::jmap::JMAPId;

use store::core::error::StoreError;
use store::core::vec_map::VecMap;
use store::JMAPStore;
use store::Store;

use super::schema::{Identity, Property, Value};

impl GetObject for Identity {
    type GetArguments = ();

    fn default_properties() -> Vec<Self::Property> {
        vec![
            Property::Id,
            Property::Name,
            Property::Email,
            Property::ReplyTo,
            Property::Bcc,
            Property::TextSignature,
            Property::HtmlSignature,
            Property::MayDelete,
        ]
    }

    fn get_as_id(&self, _property: &Self::Property) -> Option<Vec<JMAPId>> {
        None
    }
}

pub trait JMAPGetIdentity<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn identity_get(&self, request: GetRequest<Identity>) -> jmap::Result<GetResponse<Identity>>;
}

impl<T> JMAPGetIdentity<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn identity_get(&self, request: GetRequest<Identity>) -> jmap::Result<GetResponse<Identity>> {
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
                .get_orm::<Identity>(account_id, document_id)?
                .ok_or_else(|| StoreError::NotFound("Identity data not found".to_string()))?;
            let mut identity = VecMap::with_capacity(properties.len());

            for property in properties {
                identity.append(
                    *property,
                    match property {
                        Property::Id => Value::Id { value: id },
                        Property::MayDelete => Value::Bool { value: true },
                        _ => fields.remove(property).unwrap_or_default(),
                    },
                );
            }
            Ok(Some(Identity {
                properties: identity,
            }))
        })
    }
}
