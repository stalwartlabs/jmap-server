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

use crate::{orm, types::jmap::JMAPId};
use core::hash::Hash;
use std::fmt::Debug;
use store::{
    blob::BlobId,
    core::{collection::Collection, document::Document},
    write::batch::WriteBatch,
    AccountId, DocumentId, JMAPStore, Store,
};

pub mod changes;
pub mod copy;
pub mod get;
pub mod query;
pub mod query_changes;
pub mod set;

pub trait Object: Sized + for<'de> serde::Deserialize<'de> + serde::Serialize {
    type Property: for<'de> serde::Deserialize<'de>
        + serde::Serialize
        + for<'x> TryFrom<&'x str>
        + From<u8>
        + Into<u8>
        + Eq
        + PartialEq
        + Debug
        + Hash
        + Clone
        + Sync
        + Send;
    type Value: orm::Value;

    fn new(id: JMAPId) -> Self;
    fn id(&self) -> Option<&JMAPId>;
    fn required() -> &'static [Self::Property];
    fn indexed() -> &'static [(Self::Property, u64)];
    fn max_len() -> &'static [(Self::Property, usize)];
    fn collection() -> Collection;
}

pub trait RaftObject<T>: Object
where
    T: for<'x> Store<'x> + 'static,
{
    fn on_raft_update(
        store: &JMAPStore<T>,
        write_batch: &mut WriteBatch,
        document: &mut Document,
        jmap_id: store::JMAPId,
        as_insert: Option<Vec<BlobId>>,
    ) -> store::Result<()>;

    fn get_jmap_id(
        store: &JMAPStore<T>,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Option<store::JMAPId>>;

    fn get_blobs(
        store: &JMAPStore<T>,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Vec<BlobId>>;
}
