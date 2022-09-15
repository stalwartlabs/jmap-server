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

use jmap::jmap_store::get::{default_mapper, GetHelper, SharedDocsFnc};
use jmap::orm::serialize::JMAPOrm;
use jmap::principal::schema::{Principal, Property, Value};
use jmap::principal::store::JMAPPrincipals;
use jmap::request::get::{GetRequest, GetResponse};
use jmap::SUPERUSER_ID;
use jmap_mail::mail_send::dkim::DKIM;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::core::tag::Tag;
use store::core::vec_map::VecMap;
use store::core::JMAPIdPrefix;
use store::read::comparator::Comparator;
use store::read::filter::{Filter, Query};
use store::read::FilterMapper;
use store::JMAPStore;
use store::Store;

pub trait JMAPGetPrincipal<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn principal_get(&self, request: GetRequest<Principal>)
        -> jmap::Result<GetResponse<Principal>>;
    fn dkim_get(&self, domain_name: String) -> store::Result<Option<DKIM<'_>>>;
}

impl<T> JMAPGetPrincipal<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn principal_get(
        &self,
        request: GetRequest<Principal>,
    ) -> jmap::Result<GetResponse<Principal>> {
        let helper = GetHelper::new(self, request, default_mapper.into(), None::<SharedDocsFnc>)?;
        let account_id = helper.account_id;

        helper.get(|id, properties| {
            let document_id = id.get_document_id();
            let mut fields = self
                .get_orm::<Principal>(account_id, document_id)?
                .ok_or_else(|| StoreError::NotFound("Principal data not found".to_string()))?;
            let mut principal = VecMap::with_capacity(properties.len());

            for property in properties {
                principal.append(
                    *property,
                    match property {
                        Property::Id => Value::Id { value: id },
                        Property::ACL => {
                            let mut acl_get = VecMap::new();
                            for (account_id, acls) in fields.get_acls() {
                                if let Some(email) = self.principal_to_email(account_id)? {
                                    acl_get.append(email, acls);
                                }
                            }
                            Value::ACL(acl_get)
                        }

                        Property::Secret => Value::Null,
                        _ => fields.remove(property).unwrap_or_default(),
                    },
                );
            }
            Ok(Some(Principal {
                properties: principal,
            }))
        })
    }

    fn dkim_get(&self, domain_name: String) -> store::Result<Option<DKIM<'_>>> {
        if let Some(domain_id) = self
            .query_store::<FilterMapper>(
                SUPERUSER_ID,
                Collection::Principal,
                Filter::and(vec![
                    Filter::eq(Property::DKIM.into(), Query::Tag(Tag::Default)),
                    Filter::eq(Property::Name.into(), Query::Index(domain_name.clone())),
                ]),
                Comparator::None,
            )?
            .next()
        {
            if let Some((Value::Text { value: dkim }, dkim_settings)) = self
                .get_orm::<Principal>(SUPERUSER_ID, domain_id.get_document_id())?
                .map(|mut p| {
                    (
                        p.remove(&Property::Secret).unwrap_or(Value::Null),
                        p.remove(&Property::DKIM).unwrap_or(Value::Null),
                    )
                })
            {
                let mut dkim = DKIM::from_pkcs1_pem(&dkim)
                    .map_err(|err| {
                        StoreError::InternalError(format!("Failed to DKIM sign: {}", err))
                    })?
                    .domain(domain_name)
                    .selector("default");

                if let Value::DKIM { value } = dkim_settings {
                    if let Some(expiration) = value.dkim_expiration {
                        dkim = dkim.expiration(expiration as u64);
                    }
                    if let Some(selector) = value.dkim_selector {
                        dkim = dkim.selector(selector);
                    }
                }

                return Ok(Some(dkim));
            }
        }

        Ok(None)
    }
}
