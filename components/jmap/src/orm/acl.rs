use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use store::{
    core::{
        acl::{Permission, ACL},
        bitmap::Bitmap,
    },
    AccountId,
};

use crate::{jmap_store::Object, types::jmap::JMAPId};

use super::TinyORM;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ACLUpdate {
    pub acl: HashMap<JMAPId, Vec<ACL>>,
    pub set: bool,
}

impl<T> TinyORM<T>
where
    T: Object + 'static,
{
    pub fn acl_revoke(&mut self, account_id: AccountId) {
        if let Some(pos) = self.acls.iter().position(|p| p.id == account_id) {
            self.acls.swap_remove(pos);
        }
    }

    pub fn acl_set(&mut self, account_id: AccountId, acl: impl Into<Bitmap<ACL>>) {
        let acl = acl.into();
        if !acl.is_empty() {
            if let Some(permission) = self.acls.iter_mut().find(|p| p.id == account_id) {
                if permission.acl != acl {
                    permission.acl = acl;
                }
            } else {
                self.acls.push(Permission {
                    id: account_id,
                    acl,
                });
            }
        } else {
            self.acl_revoke(account_id);
        }
    }

    pub fn acl_update(&mut self, update: ACLUpdate) {
        if update.set {
            self.acls.clear();
            for (id, acl) in update.acl {
                self.acls.push(Permission {
                    id: id.get_document_id(),
                    acl: acl.into(),
                });
            }
            self.acls.sort_unstable();
        } else if let Some((id, acl)) = update.acl.into_iter().next() {
            self.acl_set(id.get_document_id(), acl);
        }
    }

    pub fn acl_check(&self, account_id: AccountId, acl: ACL) -> bool {
        self.acls
            .iter()
            .find(|p| p.id == account_id)
            .map_or(false, |p| p.acl.contains(acl))
    }

    pub fn get_acls(&self) -> HashMap<JMAPId, Vec<ACL>> {
        let mut acls = HashMap::with_capacity(self.acls.len());
        for acl in &self.acls {
            acls.insert(JMAPId::from(acl.id), acl.acl.clone().into_iter().collect());
        }
        acls
    }
}
