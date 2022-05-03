use crate::core::collection::Collections;
use crate::serialize::leb128::Leb128;
use crate::serialize::StoreDeserialize;
use crate::write::batch;
use crate::AccountId;
use std::convert::TryInto;

#[derive(Debug)]
pub enum Entry {
    Item {
        account_id: AccountId,
        changed_collections: Collections,
    },
    Snapshot {
        changed_accounts: Vec<(Collections, Vec<AccountId>)>,
    },
}

impl Entry {
    pub fn next_account(&mut self) -> Option<(AccountId, Collections)> {
        match self {
            Entry::Item {
                account_id,
                changed_collections,
            } => {
                if !changed_collections.is_empty() {
                    Some((*account_id, changed_collections.clear()))
                } else {
                    None
                }
            }
            Entry::Snapshot { changed_accounts } => loop {
                let (collections, account_ids) = changed_accounts.last_mut()?;
                if let Some(account_id) = account_ids.pop() {
                    return Some((account_id, collections.clone()));
                } else {
                    changed_accounts.pop();
                }
            },
        }
    }
}

impl StoreDeserialize for Entry {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        match *bytes.get(0)? {
            batch::Change::ENTRY => Entry::Item {
                account_id: AccountId::from_le_bytes(
                    bytes
                        .get(1..1 + std::mem::size_of::<AccountId>())?
                        .try_into()
                        .ok()?,
                ),
                changed_collections: u64::from_le_bytes(
                    bytes
                        .get(1 + std::mem::size_of::<AccountId>()..)?
                        .try_into()
                        .ok()?,
                )
                .into(),
            },
            batch::Change::SNAPSHOT => {
                let mut bytes_it = bytes.get(1..)?.iter();
                let total_collections = usize::from_leb128_it(&mut bytes_it)?;
                let mut changed_accounts = Vec::with_capacity(total_collections);

                for _ in 0..total_collections {
                    let collections = u64::from_leb128_it(&mut bytes_it)?.into();
                    let total_accounts = usize::from_leb128_it(&mut bytes_it)?;
                    let mut accounts = Vec::with_capacity(total_accounts);

                    for _ in 0..total_accounts {
                        accounts.push(AccountId::from_leb128_it(&mut bytes_it)?);
                    }

                    changed_accounts.push((collections, accounts));
                }

                Entry::Snapshot { changed_accounts }
            }
            _ => {
                return None;
            }
        }
        .into()
    }
}
