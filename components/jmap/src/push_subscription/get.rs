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

use crate::jmap_store::get::{default_mapper, GetHelper, GetObject, SharedDocsFnc};
use crate::orm::serialize::JMAPOrm;
use crate::request::get::{GetRequest, GetResponse};
use crate::types::jmap::JMAPId;

use store::core::error::StoreError;
use store::core::vec_map::VecMap;
use store::JMAPStore;
use store::Store;

use super::schema::{Property, PushSubscription, Value};

impl GetObject for PushSubscription {
    type GetArguments = ();

    fn default_properties() -> Vec<Self::Property> {
        vec![
            Property::Id,
            Property::DeviceClientId,
            Property::VerificationCode,
            Property::Expires,
            Property::Types,
        ]
    }

    fn get_as_id(&self, _property: &Self::Property) -> Option<Vec<JMAPId>> {
        None
    }
}

pub trait JMAPGetPushSubscription<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn push_subscription_get(
        &self,
        request: GetRequest<PushSubscription>,
    ) -> crate::Result<GetResponse<PushSubscription>>;
}

impl<T> JMAPGetPushSubscription<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn push_subscription_get(
        &self,
        request: GetRequest<PushSubscription>,
    ) -> crate::Result<GetResponse<PushSubscription>> {
        let helper = GetHelper::new(self, request, default_mapper.into(), None::<SharedDocsFnc>)?;
        let account_id = helper.account_id;

        helper.get(|id, properties| {
            let document_id = id.get_document_id();
            let mut fields = self
                .get_orm::<PushSubscription>(account_id, document_id)?
                .ok_or_else(|| {
                    StoreError::NotFound("PushSubscription data not found".to_string())
                })?;
            let mut push_subscription = VecMap::with_capacity(properties.len());

            for property in properties {
                push_subscription.append(
                    *property,
                    match property {
                        Property::Id => Value::Id { value: id },
                        _ => fields.remove(property).unwrap_or_default(),
                    },
                );
            }
            Ok(Some(PushSubscription {
                properties: push_subscription,
            }))
        })
    }
}
