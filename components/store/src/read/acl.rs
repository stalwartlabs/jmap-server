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
    pub fn get_shared_accounts(&self, member_of: &[AccountId]) -> crate::Result<Vec<AccountId>> {
        let mut shared_accounts = Vec::new();
        for account_id in member_of {
            let prefix =
                ValueKey::serialize_acl_prefix(*account_id, AccountId::MAX, Collection::None);
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
                    if !member_of.contains(&to_account_id)
                        && (acl.contains(ACL::Read) || acl.contains(ACL::ReadItems))
                    {
                        shared_accounts.push(to_account_id);
                    }
                } else {
                    break;
                }
            }
        }
        Ok(shared_accounts)
    }

    pub fn get_shared_documents(
        &self,
        member_of: &[AccountId],
        to_account_id: AccountId,
        to_collection: Collection,
        acls: Bitmap<ACL>,
    ) -> crate::Result<Option<RoaringBitmap>> {
        let mut shared_documents = RoaringBitmap::new();
        for account_id in member_of {
            let prefix = ValueKey::serialize_acl_prefix(*account_id, to_account_id, to_collection);
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
        Ok(if !shared_documents.is_empty() {
            shared_documents.into()
        } else {
            None
        })
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
