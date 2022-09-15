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

pub mod blobs;
pub mod log;
pub mod query;
pub mod utils;

use std::{path::PathBuf, sync::Arc};

use store::{config::jmap::JMAPConfig, JMAPStore, Store};
use store_rocksdb::RocksDB;

use self::utils::{destroy_temp_dir, init_settings};

pub fn init_db<T>(name: &str, delete_if_exists: bool) -> (JMAPStore<T>, PathBuf)
where
    T: for<'x> Store<'x> + 'static,
{
    init_db_params(name, 1, 1, delete_if_exists)
}

pub fn init_db_params<T>(
    name: &str,
    peer_num: u32,
    total_peers: u32,
    delete_if_exists: bool,
) -> (JMAPStore<T>, PathBuf)
where
    T: for<'x> Store<'x> + 'static,
{
    let (settings, temp_dir) = init_settings(name, peer_num, total_peers, delete_if_exists);

    (
        JMAPStore::new(
            T::open(&settings).unwrap(),
            JMAPConfig::from(&settings),
            &settings,
        ),
        temp_dir,
    )
}

#[test]
#[ignore]
fn store_tests() {
    let (db, temp_dir) = init_db::<RocksDB>("strdb_store", true);
    let db = Arc::new(db);

    blobs::test(db.clone());
    log::test(db.clone());
    query::test(db, true);

    destroy_temp_dir(&temp_dir);
}
