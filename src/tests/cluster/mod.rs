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

use store_rocksdb::RocksDB;

pub mod crud;
pub mod election;
pub mod fuzz;
pub mod log_conflict;
pub mod mail_thread_merge;
pub mod utils;

#[actix_web::test]
#[ignore]
async fn cluster_tests() {
    store::tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(store::tracing::Level::DEBUG)
            .finish(),
    )
    .expect("Setting default subscriber failed.");

    election::test::<RocksDB>().await;
    crud::test::<RocksDB>().await;
    mail_thread_merge::test::<RocksDB>().await;
    log_conflict::test::<RocksDB>().await;
}

#[actix_web::test]
#[ignore]
async fn cluster_fuzz() {
    store::tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(store::tracing::Level::DEBUG)
            .finish(),
    )
    .expect("Setting default subscriber failed.");

    fuzz::test::<RocksDB>(vec![]).await;

    // Used to replay a fuzz test.
    //fuzz::test::<RocksDB>(serde_json::from_slice(br#""#).unwrap()).await;
}

/*#[test]
//#[ignore]
fn postmortem() {
    let dbs = (1..=6)
        .map(|n| super::store::init_db_params::<RocksDB>("st_cluster", n, 5, false).0)
        .collect::<Vec<_>>();

    for (pos1, db1) in dbs.iter().enumerate() {
        for (pos2, db2) in dbs.iter().enumerate() {
            if pos1 != pos2 {
                print!("{}/{} -> ", pos1, pos2);
                println!("{:?}", db1.compare_with(db2));
            }
        }
    }
}*/
