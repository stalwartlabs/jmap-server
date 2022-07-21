use std::collections::{hash_map::Entry, HashMap};

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
pub enum ACLUpdate {
    Replace {
        acls: HashMap<JMAPId, Vec<ACL>>,
    },
    Update {
        account_id: JMAPId,
        acls: Vec<ACL>,
    },
    Set {
        account_id: JMAPId,
        acl: ACL,
        is_set: bool,
    },
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

    pub fn acl_set_all(&mut self, account_id: AccountId, acl: impl Into<Bitmap<ACL>>) {
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

    pub fn acl_set(&mut self, account_id: AccountId, acl: ACL, is_set: bool) {
        if let Some(permission) = self.acls.iter_mut().find(|p| p.id == account_id) {
            if is_set {
                permission.acl.insert(acl);
            } else {
                permission.acl.remove(acl);
                if permission.acl.is_empty() {
                    self.acl_revoke(account_id);
                }
            }
        } else if is_set {
            self.acls.push(Permission {
                id: account_id,
                acl: acl.into(),
            });
        }
    }

    pub fn acl_update(&mut self, updates: Vec<ACLUpdate>) {
        for update in updates {
            match update {
                ACLUpdate::Replace { acls } => {
                    self.acls.clear();
                    for (id, acl) in acls {
                        self.acls.push(Permission {
                            id: id.get_document_id(),
                            acl: acl.into(),
                        });
                    }
                    self.acls.sort_unstable();
                }
                ACLUpdate::Update { account_id, acls } => {
                    self.acl_set_all(account_id.get_document_id(), acls);
                }
                ACLUpdate::Set {
                    account_id,
                    acl,
                    is_set,
                } => {
                    self.acl_set(account_id.get_document_id(), acl, is_set);
                }
            }
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

    pub fn get_changed_acls(&self, changes: Option<&Self>) -> Option<Vec<Permission>> {
        if let Some(changes) = changes {
            if changes.acls != self.acls {
                let mut acls: HashMap<AccountId, Bitmap<ACL>> = HashMap::new();
                for (a, b) in [(&self.acls, &changes.acls), (&changes.acls, &self.acls)] {
                    for p in a {
                        if !b.contains(p) {
                            match acls.entry(p.id) {
                                Entry::Occupied(mut entry) => {
                                    entry.get_mut().union(&p.acl);
                                }
                                Entry::Vacant(entry) => {
                                    entry.insert(p.acl.clone());
                                }
                            }
                        }
                    }
                }
                acls.into_iter()
                    .map(|(id, acl)| Permission { id, acl })
                    .collect::<Vec<_>>()
                    .into()
            } else {
                None
            }
        } else if !self.acls.is_empty() {
            self.acls.clone().into()
        } else {
            None
        }
    }
}

impl ACLUpdate {
    pub fn get_acls(&self) -> &HashMap<JMAPId, Vec<ACL>> {
        match self {
            ACLUpdate::Replace { acls } => acls,
            _ => unreachable!(),
        }
    }
}
