use roaring::RoaringBitmap;

use crate::serialize::leb128::Leb128;
use crate::serialize::StoreDeserialize;
use crate::DocumentId;
use crate::{
    core::{acl::ACL, bitmap::Bitmap, collection::Collection, error::StoreError},
    serialize::key::ValueKey,
    AccountId, ColumnFamily, Direction, JMAPStore, Store,
};

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    /*pub fn member_of(&self, mut account_id: AccountId) -> crate::Result<Vec<AccountId>> {
        let mut member_of = Vec::new();
        let mut iter_stack = Vec::new();

        'outer: loop {
            let mut prefix = ValueKey::serialize_member_of_prefix(account_id);
            let mut iter = self
                .db
                .iterator(ColumnFamily::Values, &prefix, Direction::Forward)?;

            loop {
                while let Some((key, _)) = iter.next() {
                    if key.starts_with(&prefix) && key.len() > prefix.len() {
                        let (member_account, _) = AccountId::from_leb128_bytes(
                            &key[prefix.len()..],
                        )
                        .ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Corrupted ACL members key for [{:?}]",
                                key
                            ))
                        })?;

                        if !member_of.contains(&member_account) {
                            member_of.push(member_account);
                            if iter_stack.len() < 10 {
                                iter_stack.push((iter, prefix));
                                account_id = member_account;
                                continue 'outer;
                            }
                        }
                    } else {
                        break;
                    }
                }

                if let Some((prev_it, prev_prefix)) = iter_stack.pop() {
                    iter = prev_it;
                    prefix = prev_prefix;
                } else {
                    break 'outer;
                }
            }
        }
        Ok(member_of)
    }*/

    pub fn shared_accounts(&self, member_of: Vec<AccountId>) -> crate::Result<Vec<AccountId>> {
        let mut shared_accounts = Vec::new();
        for account_id in member_of {
            let prefix =
                ValueKey::serialize_acl_prefix(account_id, AccountId::MAX, Collection::None);
            for (key, value) in
                self.db
                    .iterator(ColumnFamily::Values, &prefix, Direction::Forward)?
            {
                if key.starts_with(&prefix)
                    && key.len() > prefix.len() + 2
                    && key[prefix.len()] != u8::MAX
                {
                    let (to_account_id, _) = ValueKey::deserialize_acl_target(
                        &key[prefix.len() + 1..],
                    )
                    .ok_or_else(|| {
                        StoreError::InternalError(format!("Corrupted ACL key for [{:?}]", key))
                    })?;
                    let acl = Bitmap::from(u64::deserialize(&value).ok_or_else(|| {
                        StoreError::InternalError(format!("Corrupted ACL value for [{:?}]", key))
                    })?);
                    if acl.contains(ACL::Read) || acl.contains(ACL::ReadItems) {
                        shared_accounts.push(to_account_id);
                    }
                } else {
                    break;
                }
            }
        }
        Ok(shared_accounts)
    }

    pub fn shared_documents(
        &self,
        member_of: Vec<AccountId>,
        to_account_id: AccountId,
        to_collection: Collection,
        acls: Bitmap<ACL>,
    ) -> crate::Result<RoaringBitmap> {
        let mut shared_documents = RoaringBitmap::new();
        for account_id in member_of {
            let prefix = ValueKey::serialize_acl_prefix(account_id, to_account_id, to_collection);
            for (key, value) in
                self.db
                    .iterator(ColumnFamily::Values, &prefix, Direction::Forward)?
            {
                if key.starts_with(&prefix) && key.len() > prefix.len() {
                    let (document_id, _) = DocumentId::from_leb128_bytes(&key[prefix.len()..])
                        .ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Corrupted ACL members key for [{:?}]",
                                key
                            ))
                        })?;

                    let mut acl = Bitmap::from(u64::deserialize(&value).ok_or_else(|| {
                        StoreError::InternalError(format!("Corrupted ACL value for [{:?}]", key))
                    })?);
                    acl.intersection(&acls);
                    if !acl.is_empty() {
                        shared_documents.insert(document_id);
                    }
                } else {
                    break;
                }
            }
        }
        Ok(shared_documents)
    }

    pub fn get_acl(
        &self,
        account_id: AccountId,
        to_account_id: AccountId,
        to_collection: Collection,
        to_document_id: DocumentId,
    ) -> crate::Result<Bitmap<ACL>> {
        Ok(Bitmap::from(
            self.db
                .get(
                    ColumnFamily::Values,
                    &ValueKey::serialize_acl(
                        account_id,
                        to_account_id,
                        to_collection,
                        to_document_id,
                    ),
                )?
                .unwrap_or(0u64),
        ))
    }
}
