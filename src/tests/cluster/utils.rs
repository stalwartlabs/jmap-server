use crate::{
    cluster::init::{init_cluster, start_cluster, ClusterInit},
    server::http::{build_jmap_server, init_jmap_server},
    tests::{jmap::bypass_authentication, store::utils::StoreCompareWith},
    JMAPServer,
};

use actix_web::{dev::ServerHandle, web};
use futures::future::join_all;
use jmap::types::jmap::JMAPId;
use jmap_client::{
    client::{Client, Credentials},
    core::set::SetObject,
    mailbox::Role,
};
use std::{path::PathBuf, sync::Arc, time::Duration};
use store::{
    ahash::AHashMap,
    config::env_settings::EnvSettings,
    log::raft::RaftId,
    parking_lot::Mutex,
    rand::{self, Rng},
    AccountId, Store,
};
use tokio::{sync::oneshot, time::sleep};

use crate::tests::store::utils::{destroy_temp_dir, init_settings};

pub struct Peer<T> {
    pub init: ClusterInit,
    pub temp_dir: PathBuf,
    pub jmap_server: web::Data<JMAPServer<T>>,
    pub settings: EnvSettings,
}

pub struct Cluster<T> {
    pub peers: Vec<Peer<T>>,
    pub temp_dirs: Vec<PathBuf>,
    pub handles: Vec<ServerHandle>,
    pub num_peers: u32,
    pub delete_if_exists: bool,
}

pub struct Clients {
    pub clients: Vec<Client>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Ac {
    NewEmail((u64, u64)),
    UpdateEmail(u64),
    DeleteEmail(u64),
    InsertMailbox(u64),
    UpdateMailbox(u64),
    DeleteMailbox(u64),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Cmd {
    StopLeader,
    StopFollower,
    StartOneOffline,
    StartAllOffline,
    Update { account_id: AccountId, action: Ac },
}

#[derive(Debug, Default)]
pub struct Cmds {
    pub cmds: Vec<Cmd>,
    pub clean_exit: bool,
}

impl Drop for Cmds {
    fn drop(&mut self) {
        if !self.clean_exit && !self.cmds.is_empty() {
            println!(
                "Executed commands: {}",
                serde_json::to_string(&self.cmds).unwrap()
            );
        }
    }
}

impl<T> Peer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn new(name: &str, peer_num: u32, num_peers: u32, delete_if_exists: bool) -> Self {
        let (settings, temp_dir) = init_settings(name, peer_num, num_peers, delete_if_exists);

        let (ipc, init) = init_cluster(&settings).unwrap();
        let jmap_server = init_jmap_server(&settings, ipc.into());

        // Bypass authentication
        bypass_authentication(&jmap_server).await;

        Peer {
            init,
            settings,
            temp_dir,
            jmap_server,
        }
    }

    pub async fn start_cluster(self) -> (web::Data<JMAPServer<T>>, PathBuf, ServerHandle) {
        // Start cluster services
        start_cluster(self.init, self.jmap_server.clone(), &self.settings).await;

        // Start web server
        let server = self.jmap_server.clone();
        let settings = self.settings;
        let _server = server.clone();
        let (tx, rx) = oneshot::channel();
        actix_web::rt::spawn(async move {
            let server = build_jmap_server(server, settings).await.unwrap();
            tx.send(server.handle()).unwrap();
            server.await
        });
        let handle = rx.await.unwrap();

        (self.jmap_server, self.temp_dir, handle)
    }
}

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn new(name: &str, num_peers: u32, delete_if_exists: bool) -> Self {
        let mut peers = Vec::with_capacity(num_peers as usize);
        for peer_num in 1..=num_peers {
            peers.push(Peer::new(name, peer_num, num_peers, delete_if_exists).await);
        }

        Cluster {
            peers,
            temp_dirs: vec![],
            handles: vec![],
            num_peers,
            delete_if_exists,
        }
    }

    pub async fn start_cluster(&mut self) -> Arc<Vec<web::Data<JMAPServer<T>>>> {
        let mut peers = Vec::new();

        for peer in self.peers.drain(..) {
            let (server, temp_dir, handle) = peer.start_cluster().await;
            self.temp_dirs.push(temp_dir);
            self.handles.push(handle);
            peers.push(server);
        }

        Arc::new(peers)
    }

    pub async fn stop_cluster(&self) {
        for handler in &self.handles {
            handler.stop(true).await;
        }
    }

    pub async fn extend_cluster(
        &mut self,
        name: &str,
        peers: Arc<Vec<web::Data<JMAPServer<T>>>>,
        n: u32,
    ) -> Arc<Vec<web::Data<JMAPServer<T>>>> {
        let mut peers = match Arc::try_unwrap(peers) {
            Ok(peers) => peers,
            Err(_) => panic!("Unable to unwrap peers"),
        };

        for peer_num in 1..=n {
            let (server, temp_dir, handle) = Peer::new(
                name,
                peer_num + self.num_peers,
                self.num_peers,
                self.delete_if_exists,
            )
            .await
            .start_cluster()
            .await;
            self.temp_dirs.push(temp_dir);
            self.handles.push(handle);
            peers.push(server);
        }

        self.num_peers += n;

        Arc::new(peers)
    }

    pub fn cleanup(self) {
        for temp_dir in self.temp_dirs {
            destroy_temp_dir(&temp_dir);
        }
    }
}

impl Clients {
    pub async fn new(num_peers: usize) -> Self {
        let mut clients = Vec::with_capacity(num_peers);
        for peer_num in 1..=num_peers {
            clients.push(
                Client::new()
                    .credentials(Credentials::bearer("DO_NOT_ATTEMPT_THIS_AT_HOME"))
                    .follow_redirects(["127.0.0.1"])
                    .connect(&format!(
                        "http://127.0.0.1:{}/.well-known/jmap",
                        8000 + peer_num
                    ))
                    .await
                    .unwrap(),
            );
        }
        Clients { clients }
    }

    fn get_client(&self, mut peer_num: usize) -> &Client {
        if peer_num == 0 {
            peer_num = rand::thread_rng().gen_range(0..self.clients.len());
        }
        &self.clients[peer_num]
    }

    pub async fn insert_email(
        &self,
        peer_num: usize,
        account_id: AccountId,
        raw_message: Vec<u8>,
        mailbox_ids: Vec<String>,
        keywords: Vec<String>,
    ) -> String {
        self.get_client(peer_num)
            .email_import_account(
                &JMAPId::from(account_id).to_string(),
                raw_message,
                mailbox_ids,
                keywords.into(),
                None,
            )
            .await
            .unwrap()
            .take_id()
    }

    pub async fn update_email(
        &self,
        peer_num: usize,
        account_id: AccountId,
        email_id: String,
        mailbox_ids: Option<Vec<String>>,
        keywords: Option<Vec<String>>,
    ) {
        let mut request = self.get_client(peer_num).build();
        let update = request
            .set_email()
            .account_id(JMAPId::from(account_id).to_string())
            .update(&email_id);
        if let Some(mailbox_ids) = mailbox_ids {
            update.mailbox_ids(mailbox_ids);
        }
        if let Some(keywords) = keywords {
            update.keywords(keywords);
        }

        request
            .send_set_email()
            .await
            .unwrap()
            .updated(&email_id)
            .unwrap();
    }

    pub async fn delete_email(&self, peer_num: usize, account_id: AccountId, email_id: String) {
        let mut request = self.get_client(peer_num).build();
        request
            .set_email()
            .account_id(JMAPId::from(account_id).to_string())
            .destroy(vec![email_id.to_string()]);
        request
            .send_set_email()
            .await
            .unwrap()
            .destroyed(&email_id)
            .unwrap();
    }

    pub async fn insert_mailbox(
        &self,
        peer_num: usize,
        account_id: AccountId,
        name: String,
        role: Role,
    ) -> String {
        let mut request = self.get_client(peer_num).build();
        let create = request
            .set_mailbox()
            .account_id(JMAPId::from(account_id).to_string())
            .create()
            .name(name)
            .role(role);
        let create_id = create.create_id().unwrap();

        request
            .send_set_mailbox()
            .await
            .unwrap()
            .created(&create_id)
            .unwrap()
            .take_id()
    }

    pub async fn update_mailbox(
        &self,
        peer_num: usize,
        account_id: AccountId,
        mailbox_id: String,
        id1: u32,
        id2: u32,
    ) {
        let mut request = self.get_client(peer_num).build();
        request
            .set_mailbox()
            .account_id(JMAPId::from(account_id).to_string())
            .update(&mailbox_id)
            .name(format!("Mailbox {}/{}", id1, id2))
            .sort_order(id2);

        request
            .send_set_mailbox()
            .await
            .unwrap()
            .updated(&mailbox_id)
            .unwrap();
    }

    pub async fn delete_mailbox(&self, peer_num: usize, account_id: AccountId, mailbox_id: String) {
        let mut request = self.get_client(peer_num).build();
        request
            .set_mailbox()
            .account_id(JMAPId::from(account_id).to_string())
            .destroy(vec![mailbox_id.to_string()])
            .arguments()
            .on_destroy_remove_emails(true);
        request
            .send_set_mailbox()
            .await
            .unwrap()
            .destroyed(&mailbox_id)
            .unwrap();
    }
}

impl Ac {
    pub async fn execute<T>(
        &self,
        clients: &Clients,
        mailbox_map: &Arc<Mutex<AHashMap<u64, String>>>,
        email_map: &Arc<Mutex<AHashMap<u64, String>>>,
        batch_num: usize,
    ) where
        T: for<'x> Store<'x> + 'static,
    {
        let peer_num = 0;
        match self {
            Ac::NewEmail((local_id, mailbox_id)) => {
                let mailbox_id = mailbox_map.lock().get(mailbox_id).unwrap().to_string();
                let email_id = clients
                    .insert_email(
                        peer_num,
                        2,
                        format!(
                            "From: test@test.com\nSubject: test {}\n\nTest message {}",
                            local_id, local_id
                        )
                        .into_bytes(),
                        vec![mailbox_id],
                        vec![],
                    )
                    .await;

                email_map.lock().insert(*local_id, email_id);
            }
            Ac::UpdateEmail(local_id) => {
                let email_id = email_map.lock().get(local_id).unwrap().to_string();
                clients
                    .update_email(
                        peer_num,
                        2,
                        email_id,
                        None,
                        Some(vec![format!("tag_{}", batch_num)]),
                    )
                    .await;
            }
            Ac::DeleteEmail(local_id) => {
                let email_id = email_map.lock().remove(local_id).unwrap();
                clients.delete_email(peer_num, 2, email_id).await;
            }
            Ac::InsertMailbox(local_id) => {
                let mailbox_id = clients
                    .insert_mailbox(peer_num, 2, format!("Mailbox {}", local_id), Role::None)
                    .await;
                mailbox_map.lock().insert(*local_id, mailbox_id);
            }
            Ac::UpdateMailbox(local_id) => {
                let mailbox_id = mailbox_map.lock().get(local_id).unwrap().to_string();
                clients
                    .update_mailbox(peer_num, 2, mailbox_id, *local_id as u32, batch_num as u32)
                    .await;
            }
            Ac::DeleteMailbox(local_id) => {
                let mailbox_id = mailbox_map.lock().remove(local_id).unwrap();
                clients.delete_mailbox(peer_num, 2, mailbox_id).await;
            }
        }
    }
}

pub fn test_batch() -> Vec<Vec<Ac>> {
    vec![
        vec![
            Ac::InsertMailbox(1),
            Ac::NewEmail((1, 1)),
            Ac::UpdateEmail(1),
            Ac::DeleteEmail(1),
        ],
        vec![
            Ac::InsertMailbox(2),
            Ac::UpdateMailbox(2),
            Ac::DeleteMailbox(2),
        ],
        vec![
            Ac::NewEmail((1, 1)),
            Ac::NewEmail((2, 1)),
            Ac::NewEmail((3, 1)),
            Ac::InsertMailbox(3),
            Ac::InsertMailbox(4),
            Ac::InsertMailbox(5),
        ],
        vec![Ac::UpdateEmail(1), Ac::UpdateEmail(2), Ac::UpdateEmail(3)],
        vec![
            Ac::UpdateMailbox(3),
            Ac::UpdateMailbox(4),
            Ac::UpdateMailbox(5),
        ],
        vec![Ac::DeleteEmail(1)],
        vec![Ac::DeleteEmail(2)],
        vec![
            Ac::UpdateMailbox(3),
            Ac::DeleteMailbox(4),
            Ac::UpdateMailbox(5),
        ],
        vec![Ac::DeleteEmail(3)],
        vec![Ac::DeleteMailbox(3), Ac::DeleteMailbox(5)],
        vec![
            Ac::InsertMailbox(2),
            Ac::NewEmail((1, 2)),
            Ac::NewEmail((2, 2)),
            Ac::NewEmail((3, 2)),
        ],
        vec![Ac::UpdateEmail(1)],
        vec![Ac::DeleteEmail(2)],
        vec![Ac::DeleteMailbox(2)],
    ]
}

pub async fn assert_leader_elected<T>(
    peers: &[web::Data<JMAPServer<T>>],
) -> &web::Data<JMAPServer<T>>
where
    T: for<'x> Store<'x> + 'static,
{
    for _ in 0..100 {
        for (peer_num, peer) in peers.iter().enumerate() {
            if peer.is_leader() {
                for (pos, peer) in peers.iter().enumerate() {
                    // Clients might try to contact an "offline" peer, redirect them
                    // to the right leader.
                    if pos != peer_num && peer.is_offline() {
                        *peer.cluster.as_ref().unwrap().leader_hostname.lock() =
                            format!("http://127.0.0.1:{}", 8000 + peer_num + 1).into();
                    }
                }

                return peer;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("No leader elected.");
}

pub async fn assert_no_quorum<T>(peers: &[web::Data<JMAPServer<T>>])
where
    T: for<'x> Store<'x> + 'static,
{
    sleep(Duration::from_millis(1000)).await;
    'outer: for _ in 0..100 {
        for peer in peers.iter() {
            if peer.is_leader() {
                continue 'outer;
            }
            sleep(Duration::from_millis(500)).await;
        }
        return;
    }
    panic!("Leader still active, expected no quorum.");
}

pub fn num_online_peers<T>(peers: &[web::Data<JMAPServer<T>>]) -> usize
where
    T: for<'x> Store<'x> + 'static,
{
    let mut num_online = 0;
    for peer in peers.iter() {
        if !peer.is_offline() {
            num_online += 1;
        }
    }
    num_online
}

pub async fn activate_all_peers<T>(peers: &[web::Data<JMAPServer<T>>])
where
    T: for<'x> Store<'x> + 'static,
{
    for peer in peers.iter() {
        if peer.is_offline() {
            peer.set_offline(false, true).await;
        }
    }
}

pub async fn assert_cluster_updated<T>(peers: &[web::Data<JMAPServer<T>>])
where
    T: for<'x> Store<'x> + 'static,
{
    let mut updated = vec![false; peers.len()];

    'outer: for _ in 0..100 {
        let mut last_logs = vec![RaftId::none(); peers.len()];

        sleep(Duration::from_millis(100)).await;

        for (peer_num, peer) in peers.iter().enumerate() {
            if peer.is_offline() {
                updated[peer_num] = true;
            } else if peer.is_up_to_date() {
                updated[peer_num] = true;
                last_logs[peer_num] = peer
                    .get_last_log()
                    .await
                    .unwrap()
                    .unwrap_or_else(RaftId::none);
            } else {
                updated[peer_num] = false;
            }
        }

        if updated.iter().all(|u| *u) {
            let mut last_log = RaftId::none();
            for peer_last_log in &last_logs {
                if !peer_last_log.is_none() {
                    if last_log.is_none() {
                        last_log = *peer_last_log;
                    } else if last_log != *peer_last_log {
                        continue 'outer;
                    }
                }
            }
            return;
        }
    }

    for peer in peers.iter() {
        println!(
            "{}: {:?} ({})",
            if peer.is_offline() {
                "offline"
            } else if peer.is_leader() {
                "leader"
            } else {
                "follower"
            },
            peer.get_last_log().await,
            peer.is_up_to_date()
        );
    }
    panic!("Some nodes are not up to date.");
}

pub async fn assert_mirrored_stores<T>(
    peers: Arc<Vec<web::Data<JMAPServer<T>>>>,
    ignore_offline: bool,
) where
    T: for<'x> Store<'x> + 'static,
{
    tokio::task::spawn_blocking(move || {
        for (leader_pos, leader) in peers.iter().enumerate() {
            if leader.is_leader() {
                for (follower_pos, follower) in peers.iter().enumerate() {
                    if follower_pos != leader_pos {
                        if follower.is_offline() {
                            if ignore_offline {
                                continue;
                            } else {
                                panic!("Follower {} is offline.", follower_pos);
                            }
                        }
                        assert!(follower.is_up_to_date());
                        println!(
                            "Comparing leader {} with follower {}.",
                            leader_pos + 1,
                            follower_pos + 1
                        );
                        let keys_leader = leader.store.compare_with(&follower.store);
                        println!(
                            "Comparing follower {} with leader {}.",
                            leader_pos + 1,
                            follower_pos + 1
                        );
                        let keys_follower = follower.store.compare_with(&leader.store);
                        assert!(
                            keys_leader.iter().map(|(_, v)| *v).sum::<usize>() > 0,
                            "{:?}",
                            keys_leader
                        );
                        assert_eq!(keys_leader, keys_follower);
                    }
                }
                return;
            }
        }
        panic!("Leader not elected.");
    })
    .await
    .unwrap();
}

pub async fn shutdown_all<T>(peers: Arc<Vec<web::Data<JMAPServer<T>>>>)
where
    T: for<'x> Store<'x> + 'static,
{
    for peer in peers.iter() {
        peer.shutdown().await;
    }
    drop(peers);
    //sleep(Duration::from_millis(1000)).await;
}

pub async fn compact_log<T>(peers: Arc<Vec<web::Data<JMAPServer<T>>>>)
where
    T: for<'x> Store<'x> + 'static,
{
    let last_log = peers.last().unwrap().get_last_log().await.unwrap().unwrap();

    join_all(peers.iter().map(|peer| {
        let store = peer.store.clone();
        tokio::task::spawn_blocking(move || store.compact_log_up_to(last_log.index).unwrap())
    }))
    .await;
}
