use std::collections::HashMap;

use crate::{
    core::{bitmap::Bitmap, collection::Collection, document::Document},
    serialize::{key::ValueKey, StoreSerialize},
    write::operation::WriteOperation,
    AccountId, ColumnFamily, DocumentId,
};

use super::ACL;

#[derive(Debug, Default)]
pub struct ACLUtil {
    pub account_id: AccountId,
    pub collection: Collection,
    pub document_id: DocumentId,
    pub batch: Vec<WriteOperation>,
}

impl ACLUtil {
    pub fn new(account_id: AccountId, collection: Collection, document_id: DocumentId) -> ACLUtil {
        ACLUtil {
            account_id,
            collection,
            document_id,
            batch: Vec::new(),
        }
    }

    pub fn insert_acl(&mut self, map: &HashMap<AccountId, Vec<ACL>>) {
        for (id, permissions) in map {
            self.batch.push(WriteOperation::Set {
                cf: ColumnFamily::Values,
                key: ValueKey::serialize_acl(
                    *id,
                    self.account_id,
                    self.collection,
                    self.document_id,
                ),
                value: Bitmap::from(permissions).serialize().unwrap(),
            });
        }
    }

    pub fn remove_acl(&mut self, map: &HashMap<AccountId, Vec<ACL>>) {
        for id in map.keys() {
            self.batch.push(WriteOperation::Delete {
                cf: ColumnFamily::Values,
                key: ValueKey::serialize_acl(
                    *id,
                    self.account_id,
                    self.collection,
                    self.document_id,
                ),
            });
        }
    }

    pub fn merge_acl(
        &mut self,
        prev_map: &HashMap<AccountId, Vec<ACL>>,
        new_map: &HashMap<AccountId, Vec<ACL>>,
    ) {
        for id in prev_map.keys() {
            if !new_map.contains_key(id) {
                self.batch.push(WriteOperation::Delete {
                    cf: ColumnFamily::Values,
                    key: ValueKey::serialize_acl(
                        *id,
                        self.account_id,
                        self.collection,
                        self.document_id,
                    ),
                });
            }
        }

        for (id, permissions) in new_map {
            if prev_map.get(id).map_or(true, |v| v != permissions) {
                self.batch.push(WriteOperation::Set {
                    cf: ColumnFamily::Values,
                    key: ValueKey::serialize_acl(
                        *id,
                        self.account_id,
                        self.collection,
                        self.document_id,
                    ),
                    value: Bitmap::from(permissions).serialize().unwrap(),
                });
            }
        }
    }
}

impl Document {
    pub fn insert_members(&mut self, members: &[AccountId]) {
        for member in members {
            self.operations.push(WriteOperation::Set {
                cf: ColumnFamily::Values,
                key: ValueKey::serialize_member_of(*member, self.document_id),
                value: vec![],
            });
        }
    }

    pub fn remove_members(&mut self, members: &[AccountId]) {
        for member in members {
            self.operations.push(WriteOperation::Delete {
                cf: ColumnFamily::Values,
                key: ValueKey::serialize_member_of(*member, self.document_id),
            });
        }
    }

    pub fn merge_members(&mut self, prev_members: &[AccountId], new_members: &[AccountId]) {
        if prev_members == new_members {
            return;
        }

        for prev_member in prev_members {
            if !new_members.contains(prev_member) {
                self.operations.push(WriteOperation::Delete {
                    cf: ColumnFamily::Values,
                    key: ValueKey::serialize_member_of(*prev_member, self.document_id),
                });
            }
        }

        for new_member in new_members {
            if !prev_members.contains(new_member) {
                self.operations.push(WriteOperation::Set {
                    cf: ColumnFamily::Values,
                    key: ValueKey::serialize_member_of(*new_member, self.document_id),
                    value: vec![],
                });
            }
        }
    }
}
