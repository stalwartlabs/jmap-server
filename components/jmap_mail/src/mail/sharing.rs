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

use std::sync::Arc;

use store::{
    core::{acl::ACL, collection::Collection, error::StoreError, tag::Tag},
    roaring::RoaringBitmap,
    AccountId, JMAPStore, SharedResource, Store,
};

use super::MessageField;

pub trait JMAPShareMail<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_shared_folders(
        &self,
        owner_id: AccountId,
        shared_to: &[AccountId],
        acl: ACL,
    ) -> store::Result<Arc<Option<RoaringBitmap>>>;
    fn mail_shared_messages(
        &self,
        owner_id: AccountId,
        shared_to: &[AccountId],
        acl: ACL,
    ) -> store::Result<Arc<Option<RoaringBitmap>>>;
}

impl<T> JMAPShareMail<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_shared_folders(
        &self,
        owner_id: AccountId,
        shared_to: &[AccountId],
        acl: ACL,
    ) -> store::Result<Arc<Option<RoaringBitmap>>> {
        self.shared_documents
            .try_get_with::<_, StoreError>(
                SharedResource::new(
                    owner_id,
                    shared_to.first().copied().unwrap(),
                    Collection::Mail,
                    acl,
                ),
                || {
                    Ok(Arc::new(self.get_shared_documents(
                        shared_to,
                        owner_id,
                        Collection::Mailbox,
                        acl.into(),
                    )?))
                },
            )
            .map_err(|e| e.as_ref().clone())
    }

    fn mail_shared_messages(
        &self,
        owner_id: AccountId,
        shared_to: &[AccountId],
        acl: ACL,
    ) -> store::Result<Arc<Option<RoaringBitmap>>> {
        Ok(Arc::new(
            if let Some(shared_folders) =
                self.mail_shared_folders(owner_id, shared_to, acl)?.as_ref()
            {
                let mut shared_messages = RoaringBitmap::new();
                for mailbox_id in shared_folders {
                    if let Some(message_ids) = self.get_tag(
                        owner_id,
                        Collection::Mail,
                        MessageField::Mailbox.into(),
                        Tag::Id(mailbox_id),
                    )? {
                        shared_messages |= message_ids;
                    }
                }
                if !shared_messages.is_empty() {
                    shared_messages.into()
                } else {
                    None
                }
            } else {
                None
            },
        ))
    }
}
