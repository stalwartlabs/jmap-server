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

use super::schema::{Property, Thread};
use crate::mail::{sharing::JMAPShareMail, MessageField};
use jmap::{
    jmap_store::get::{GetHelper, GetObject, IdMapper, SharedDocsFnc},
    request::{
        get::{GetRequest, GetResponse},
        ACLEnforce,
    },
    types::jmap::JMAPId,
};
use store::{
    core::{acl::ACL, collection::Collection, tag::Tag, JMAPIdPrefix},
    read::{
        comparator::{Comparator, FieldComparator},
        filter::Filter,
        FilterMapper,
    },
    JMAPStore, Store,
};

impl GetObject for Thread {
    type GetArguments = ();

    fn default_properties() -> Vec<Self::Property> {
        vec![Property::Id, Property::EmailIds]
    }

    fn get_as_id(&self, property: &Self::Property) -> Option<Vec<JMAPId>> {
        match property {
            Property::Id => vec![self.id],
            Property::EmailIds => self.email_ids.clone(),
        }
        .into()
    }
}

pub trait JMAPGetThread<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn thread_get(&self, request: GetRequest<Thread>) -> jmap::Result<GetResponse<Thread>>;
}

impl<T> JMAPGetThread<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn thread_get(&self, request: GetRequest<Thread>) -> jmap::Result<GetResponse<Thread>> {
        let mut helper = GetHelper::new(self, request, None::<IdMapper>, None::<SharedDocsFnc>)?;
        let account_id = helper.account_id;
        let shared_messages = if helper.acl.is_shared(account_id) {
            Some(self.mail_shared_messages(account_id, &helper.acl.member_of, ACL::ReadItems)?)
        } else {
            None
        };

        // Add Id Property
        if !helper.properties.contains(&Property::Id) {
            helper.properties.push(Property::Id);
        }

        let response = helper.get(|id, _properties| {
            let thread_id = id.get_document_id();
            if let Some(mut doc_ids) = self.get_tag(
                account_id,
                Collection::Mail,
                MessageField::ThreadId.into(),
                Tag::Id(thread_id),
            )? {
                // Filter out messages that were not shared
                if let Some(shared_messages) = &shared_messages {
                    if let Some(shared_messages) = shared_messages.as_ref() {
                        doc_ids &= shared_messages;
                    } else {
                        doc_ids.clear();
                    }
                }

                Ok(Some(Thread {
                    id,
                    email_ids: self
                        .query_store::<FilterMapper>(
                            account_id,
                            Collection::Mail,
                            Filter::DocumentSet(doc_ids),
                            Comparator::Field(FieldComparator {
                                field: MessageField::ReceivedAt.into(),
                                ascending: true,
                            }),
                        )?
                        .into_iter()
                        .map(|doc_id| JMAPId::from_parts(thread_id, doc_id.get_document_id()))
                        .collect(),
                }))
            } else {
                Ok(None)
            }
        })?;

        Ok(response)
    }
}
