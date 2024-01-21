use crate::process_request::process_request;
use async_std::io::WriteExt;
use futures::AsyncWriteExt;

use crate::security::permissions_key_from_user_name;
use crate::security::user_name_key_from_user_name;
use async_std::net::TcpStream;
use std::fs::File;
use std::io::Write;
use std::thread;

use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::executor::block_on;
use futures::join;
use futures::stream::StreamExt;

use futures::io::AsyncBufReadExt;

use std::io::BufWriter;

use log;

use std::sync::Arc;

use crate::bo::*;
use crate::db_ops::*;
use crate::disk_ops::*;

impl Databases {
    pub fn replicate_message(&self, message: String) -> Result<u64, String> {
        replicate_message_with_sender(&self.replication_sender, message)
    }
}

pub fn replicate_message_with_sender(
    replication_sender: &Sender<String>,
    message: String,
) -> Result<u64, String> {
    let opp_id = Databases::next_op_log_id();
    match replication_sender
        .clone()
        // Replicate the message "opp_id message"
        .try_send(format!("rp {} {}", opp_id, message))
    {
        Ok(_) => Ok(opp_id),
        Err(e) => Err(format!(
            "Error sending message to replication channel: {}",
            e
        )),
    }
}

pub fn replicate_web(replication_sender: &Sender<String>, message: String) {
    match replicate_message_with_sender(replication_sender, message) {
        Ok(_) => (),
        Err(e) => log::error!(" [replicate_web] error {}", e),
    }
}

pub fn get_replicate_remove_message(db_name: String, key: String) -> String {
    return format!("replicate-remove {} {}", db_name, key);
}

pub fn get_replicate_message(db_name: String, key: String, value: String, version: i32) -> String {
    return format!("replicate {} {} {} {}", db_name, key, version, value);
}

pub fn get_replicate_resolve_message(
    opp_id: u64,
    db_name: String,
    key: String,
    value: String,
    version: i32,
) -> String {
    return format!(
        "resolve {} {} {} {} {}",
        opp_id, db_name, key, version, value
    );
}

pub fn get_resolve_message(
    opp_id: u64,
    db_name: String,
    key: String,
    value: String,
    version: i32,
) -> String {
    return format!(
        "resolve {} {} {} {} {}",
        opp_id, db_name, key, version, value
    );
}

pub fn get_replicate_increment_message(db_name: String, key: String, inc: String) -> String {
    return format!("replicate-increment {} {} {}", db_name, key, inc);
}

pub fn replicate_change(change: &Change, db: &Database, dbs: &Arc<Databases>) -> Response {
    if dbs.is_primary() || dbs.is_eligible() {
        replicate_web(
            &dbs.replication_sender,
            get_replicate_message(
                db.name.clone(),
                change.key.clone(),
                change.value.clone(),
                change.version,
            ),
        );
    } else {
        send_message_to_primary(
            get_replicate_message(
                db.name.clone(),
                change.key.clone(),
                change.value.clone(),
                change.version,
            ),
            &dbs,
        );
    }
    Response::Ok {}
}
pub fn replicate_request(
    input: Request,
    db_name: &String,
    response: Response,
    replication_sender: &Sender<String>,
) -> Response {
    match response {
        Response::Error { msg: _ } => response,
        Response::VersionError {
            msg: _,
            key: _,
            old_version: _,
            version: _,
            old_value: _,
            change: _,
            db: _,
            state: _,
        } => response,
        _ => match input {
            Request::CreateDb {
                name,
                token,
                strategy,
            } => {
                log::debug!("Will replicate command a created database name {}", name);
                replicate_web(
                    replication_sender,
                    format!("create-db {} {} {}", name, token, strategy.to_string()),
                );
                Response::Ok {}
            }
            Request::Snapshot {
                reclaim_space,
                db_names,
            } => {
                log::debug!("Will replicate a snapshot to the database {}", db_name);
                let db_names = if db_names.is_empty() {
                    vec![db_name.to_string()]
                } else {
                    db_names
                };
                replicate_web(
                    replication_sender,
                    format!(
                        "replicate-snapshot {} {}",
                        db_names.join("|"),
                        reclaim_space
                    ),
                );
                Response::Ok {}
            }

            Request::ReplicateSnapshot {
                reclaim_space,
                db_names,
            } => {
                log::debug!(
                    "Will replicate a snapshot to the database {}",
                    db_names.join("|")
                );
                replicate_web(
                    replication_sender,
                    format!(
                        "replicate-snapshot {} {}",
                        db_names.join("|"),
                        reclaim_space
                    ),
                );
                Response::Ok {}
            }

            Request::Set {
                value,
                key,
                version,
            } => {
                log::debug!("Will replicate the set of the key {} to {} ", key, value);
                replicate_web(
                    replication_sender,
                    get_replicate_message(db_name.to_string(), key, value, version),
                );
                Response::Ok {}
            }

            Request::Resolve {
                opp_id,
                db_name,
                key,
                value,
                version,
            } => {
                log::debug!(
                    "Will replicate the resolve of the key {} to {} ",
                    key,
                    value
                );
                replicate_web(
                    replication_sender,
                    get_replicate_resolve_message(
                        opp_id,
                        db_name.to_string(),
                        key.clone(),
                        value.clone(),
                        version,
                    ),
                );
                Response::Ok {}
            }

            Request::ReplicateSet {
                db,
                value,
                key,
                version,
            } => {
                log::debug!("Will replicate the set of the key {} to {} ", key, value);
                replicate_web(
                    replication_sender,
                    get_replicate_message(db.to_string(), key, value, version),
                );
                Response::Ok {}
            }

            Request::Remove { key } => {
                log::debug!("Will replicate the remove of the key {} ", key);
                replicate_web(
                    replication_sender,
                    get_replicate_remove_message(db_name.to_string(), key),
                );
                Response::Ok {}
            }

            Request::ReplicateRemove { db, key } => {
                log::debug!("Will replicate the remove of the key {} ", key);
                replicate_web(
                    replication_sender,
                    get_replicate_remove_message(db.to_string(), key),
                );
                Response::Ok {}
            }

            Request::Election { id, node_name } => {
                replicate_web(
                    replication_sender,
                    format!("election candidate {} {}", id, node_name),
                );
                Response::Ok {}
            }

            Request::ElectionActive { node_name } => {
                replicate_web(replication_sender, format!("election active {}", node_name));
                Response::Ok {}
            }

            Request::Leave { name } => {
                replicate_web(replication_sender, format!("replicate-leave {}", name));
                Response::Ok {}
            }

            Request::ReplicateIncrement { db, inc, key } => {
                log::debug!("Will replicate the inc of the key {} to {} ", key, inc);
                replicate_web(
                    replication_sender,
                    get_replicate_increment_message(db.to_string(), key, inc.to_string()),
                );
                Response::Ok {}
            }
            Request::Increment { key, inc } => {
                replicate_web(
                    replication_sender,
                    get_replicate_increment_message(db_name.to_string(), key, inc.to_string()),
                );
                Response::Ok {}
            }
            Request::CreateUser { token, user_name } => {
                let key = user_name_key_from_user_name(&user_name);
                let value = token.to_string();
                log::debug!(
                    "Will replicate user creating set of the key {} to {} ",
                    key,
                    value
                );
                replicate_web(
                    replication_sender,
                    get_replicate_message(db_name.to_string(), key, value, -1),
                );
                Response::Ok {}
            }
            Request::SetPermissions { user, permissions } => {
                let key = permissions_key_from_user_name(&user);
                let value = Permission::permissions_to_str_value(&permissions);
                log::debug!(
                    "Will replicate user set-permission set of the key {} to {} ",
                    key,
                    value
                );
                replicate_web(
                    replication_sender,
                    get_replicate_message(db_name.to_string(), key, value, -1),
                );
                Response::Ok {}
            }
            _ => response,
        },
    }
}

/**
 * Will replicate the message if the sender is Some reference, if not will print and message
 *
 */
fn replicate_if_some(opt_sender: &Option<Sender<String>>, message: &String, name: &String) {
    match opt_sender {
        Some(member_sender) => {
            log::debug!("Replicating {} to {}", message, name);
            match member_sender.clone().try_send(message.to_string()) {
                Ok(_) => (),
                Err(e) => log::warn!("replicate_if_some sender.send Error: {}", e),
            }
        }
        None => log::info!(
            "start_replication_thread:: Not replicatin {}, None sender",
            message
        ),
    }
}

fn replicate_message_to_all(op_log_id: u64, message: String, dbs: &Arc<Databases>) {
    log::debug!("Got the message {} to replicate ", message);
    let state = dbs.cluster_state.lock().unwrap();
    for (name, member) in state.members.lock().unwrap().iter() {
        if dbs.external_tcp_address.to_string() == name.to_string() {
            log::debug!("Not replicating to myself {}", name);
            continue;
        } else {
            let message_to_replicate = dbs.register_pending_opp(op_log_id, message.clone(), name);
            replicate_if_some(&member.sender, &message_to_replicate, &member.name)
        }
    }
}

fn replicate_message_to_secoundary(op_log_id: u64, message: String, dbs: &Arc<Databases>) {
    log::debug!("Got the message {} to replicate ", message);
    let state = dbs.cluster_state.lock().unwrap();
    for (name, member) in state.members.lock().unwrap().iter() {
        match member.role {
            ClusterRole::Secoundary => {
                if dbs.external_tcp_address.to_string() != name.to_string() {
                    let message_to_replicate =
                        dbs.register_pending_opp(op_log_id, message.clone(), name);
                    replicate_if_some(&member.sender, &message_to_replicate, &member.name)
                } else {
                    log::debug!("Not replicating to myself {}", name);
                }
            }
            ClusterRole::Primary => (),
            ClusterRole::StartingUp => (),
        }
    }
}

pub fn send_message_to_primary(message: String, dbs: &Arc<Databases>) {
    // in test intoduces latency to replication in app noop
    latency_trap();
    log::debug!("Got the message {} to send to primary", message);
    let state = dbs.cluster_state.lock().unwrap();
    for (_name, member) in state.members.lock().unwrap().iter() {
        match member.role {
            ClusterRole::Secoundary => (),
            ClusterRole::Primary => replicate_if_some(&member.sender, &message, &member.name),
            ClusterRole::StartingUp => (),
        }
    }
}

fn get_db_id(db_name: String, dbs: &Arc<Databases>) -> u64 {
    dbs.acquire_dbs_read_lock()
        .get(&db_name)
        .unwrap()
        .metadata
        .id as u64
}

fn generate_key_id(
    key: String,
    dbs: &Arc<Databases>,
    invalidate_stream: &mut BufWriter<File>,
) -> u64 {
    let keys_map = { dbs.keys_map.read().unwrap().clone() };
    if keys_map.contains_key(&key) {
        *keys_map.get(&key).unwrap()
    } else {
        let id = keys_map.len() as u64;
        let mut keys_map = { dbs.keys_map.write().unwrap() };
        let mut id_keys_map = { dbs.id_keys_map.write().unwrap() };
        keys_map.insert(key.clone(), id);
        id_keys_map.insert(id, key.to_string());
        log::debug!("Key {}, id {}", key, id);
        invalidate_oplog(invalidate_stream, dbs).unwrap();
        id
    }
}
/**
 *
 * This function will wait for  requests to replicates the messages to the menbers receiver
 * Here I replicate the message the all the members in the cluster
 *
 */
pub async fn start_replication_thread(
    mut replication_receiver: Receiver<String>,
    dbs: Arc<Databases>,
) {
    let mut op_log_stream = get_log_file_append_mode();
    let mut invalidate_stream = get_invalidate_file_write_mode();
    // Loop replicating messages
    loop {
        let message_opt = replication_receiver.next().await;
        match message_opt {
            Some(message) => {
                if message == "exit" {
                    log::info!("replication_ops::start_replication_thread will exist!");
                    break;
                }
                let rp_request = Request::parse(&message.to_string()).unwrap();
                let (request_str, op_log_id_in) = match rp_request {
                    Request::ReplicateRequest {
                        request_str,
                        opp_id,
                    } => (request_str.to_string(), opp_id),
                    _ => {
                        log::error!(
                            "replication_ops::start_replication_thread:: Unknown message {}",
                            message
                        );
                        panic!(
                            "replication_ops::start_replication_thread:: Unknown message {}",
                            message
                        );
                    }
                };
                let request = Request::parse(&request_str).unwrap();

                let op_log_id: u64 = match request {
                    Request::CreateDb {
                        name,
                        token: _,
                        strategy: _,
                    } => {
                        let db_id = get_db_id(name, &dbs);
                        let key_id = 1;
                        log::debug!("Will write CreateDb");
                        write_op_log(
                            &mut op_log_stream,
                            db_id,
                            key_id,
                            ReplicateOpp::CreateDb,
                            op_log_id_in,
                        )
                    }
                    Request::ReplicateSnapshot {
                        db_names,
                        reclaim_space: _reclaim_space,
                    } => {
                        // Todo replicate all databases
                        let db = db_names[0].clone();
                        let db_id = get_db_id(db, &dbs);
                        let key_id = 2; //has to be different
                        log::debug!("Will write ReplicateSnapshot");
                        write_op_log(
                            &mut op_log_stream,
                            db_id,
                            key_id,
                            ReplicateOpp::Snapshot,
                            op_log_id_in,
                        )
                    }
                    Request::ReplicateSet {
                        db,
                        key,
                        value: _,
                        version: _,
                    } => {
                        let db_id = get_db_id(db, &dbs);
                        let key_id = generate_key_id(key, &dbs, &mut invalidate_stream);
                        write_op_log(
                            &mut op_log_stream,
                            db_id,
                            key_id,
                            ReplicateOpp::Update,
                            op_log_id_in,
                        )
                    }

                    Request::ReplicateIncrement { db, key, inc: _ } => {
                        let db_id = get_db_id(db, &dbs);
                        let key_id = generate_key_id(key, &dbs, &mut invalidate_stream);
                        write_op_log(
                            &mut op_log_stream,
                            db_id,
                            key_id,
                            ReplicateOpp::Update,
                            op_log_id_in,
                        )
                    }

                    Request::ReplicateRemove { db, key } => {
                        let db_id = get_db_id(db, &dbs);
                        let key_id = generate_key_id(key, &dbs, &mut invalidate_stream);
                        write_op_log(
                            &mut op_log_stream,
                            db_id,
                            key_id,
                            ReplicateOpp::Remove,
                            op_log_id_in,
                        )
                    }

                    // Even if not in op log we need to return a valid id so the message can be ack
                    Request::SetPrimary { name: _ } => op_log_id_in, //Election events won't be registed in OpLog
                    _ => {
                        log::debug!(
                            "Ignoring command {} in replication oplog register! not unimplemented!",
                            message
                        );
                        // Even if not in op log we need to return a valid id so the message can be ack
                        op_log_id_in
                    }
                };

                match dbs.get_role() {
                    ClusterRole::Primary => {
                        log::debug!("is_primary replicating message to secoundary");
                        replicate_message_to_secoundary(op_log_id, request_str.to_string(), &dbs);
                    }
                    ClusterRole::Secoundary => {
                        log::debug!("Won't replicate message from secoundary");
                    }
                    ClusterRole::StartingUp => {
                        // When running elections Nodes stay in StartingUp state, therefore we need
                        // to replicate to all nodes
                        log::debug!("is_starting replicating message to all");
                        replicate_message_to_all(op_log_id, request_str.to_string(), &dbs);
                    }
                }
            }
            _ => log::warn!("Non part in replication"),
        }
    }
}

fn replicate_join(sender: Sender<String>, name: String) {
    match sender.clone().try_send(format!("replicate-join {}", name)) {
        Ok(_n) => (),
        Err(e) => log::warn!("secoundary::replicate_join sender.send Error: {}", e),
    }
}

fn send_cluster_state_to_the_new_member(
    sender: &Sender<String>,
    dbs: &Arc<Databases>,
    name: &String,
) {
    let cluster_state = dbs.cluster_state.lock().unwrap();
    let members = cluster_state
        .members
        .lock()
        .expect("Could not lock members!")
        .clone();
    for (_name, member) in members.iter() {
        match member.role {
            ClusterRole::Secoundary => {
                //Notify the new secoundary about the already connected menbers.
                replicate_join(sender.clone(), member.name.to_string());
                //Notify the old secoundary about the new secoundary
                match &member.sender {
                    Some(member_sender) => {
                        replicate_join(member_sender.clone(), name.to_string());
                    }
                    None => {
                        log::warn!(
                            "[start_replication_creator_thread] not replicate_join None found"
                        )
                    }
                }
            }
            ClusterRole::Primary => (),
            ClusterRole::StartingUp => (),
        }
    }
}

fn add_primary_to_secoundary(
    sender: &Sender<String>,
    name: String,
    tcp_addr: &String,
    dbs: Arc<Databases>,
    receiver: Receiver<String>,
) -> std::thread::JoinHandle<()> {
    let user = dbs.user.to_string();
    let pwd = dbs.pwd.to_string();
    dbs.add_cluster_member(ClusterMember {
        name: name.clone(),
        role: ClusterRole::Primary,
        sender: Some(sender.clone()),
    });
    let tcp_addr = tcp_addr.clone();
    let guard = thread::spawn(move || {
        start_replication(name, receiver, user, pwd, tcp_addr.to_string(), false, &dbs);
    });
    guard
}

fn add_sencoundary_to_primary(
    sender: &Sender<String>,
    name: String,
    tcp_addr: &String,
    dbs: Arc<Databases>,
    receiver: Receiver<String>,
) -> std::thread::JoinHandle<()> {
    let user = dbs.user.to_string();
    let pwd = dbs.pwd.to_string();
    replicate_join(sender.clone(), name.to_string());
    dbs.add_cluster_member(ClusterMember {
        name: name.clone(),
        role: ClusterRole::Secoundary,
        sender: Some(sender.clone()),
    });

    let tcp_addr = tcp_addr.clone();
    let dbs = dbs.clone();
    let guard = thread::spawn(move || {
        start_replication(
            name.clone(),
            receiver,
            user,
            pwd,
            tcp_addr.to_string(),
            true,
            &dbs,
        );

        log::info!(
            "Removing member {} from cluster add_sencoundary_to_primary died!",
            name
        );
        dbs.remove_cluster_member(&name);
    });
    guard
}

fn add_sencoundary_to_secoundary(
    sender: &Sender<String>,
    name: String,
    tcp_addr: &String,
    dbs: Arc<Databases>,
    receiver: Receiver<String>,
) -> std::thread::JoinHandle<()> {
    let user = dbs.user.to_string();
    let pwd = dbs.pwd.to_string();
    dbs.add_cluster_member(ClusterMember {
        name: name.clone(),
        role: ClusterRole::Secoundary,
        sender: Some(sender.clone()),
    });

    let tcp_addr = tcp_addr.clone();
    // let dbs = dbs.clone();

    let guard = thread::spawn(move || {
        start_replication(
            name.clone(),
            receiver,
            user,
            pwd,
            tcp_addr.to_string(),
            false,
            &dbs,
        );

        log::info!(
            "Removing member {} from cluster add_sencoundary_to_secoundary!",
            name
        );

        // I think I don't need to do this here but still not sure
        // this fixed the issue #10 on github
        // Primary should tell the secoundary that one of them leave
        //dbs.remove_cluster_member(&name);
    });
    guard
}

/**
 * This function goal is to start the thread to connect to the other cluster members
 *
 * The command send to the replication_start_receiver are like
 *
 * commads e.g:
 * secoundary some_sever:1412
 * primary some_sever:1412
 * <kind> <name port>
 *
 */

pub async fn start_replication_supervisor(
    mut replication_supervisor_receiver: Receiver<String>,
    dbs: Arc<Databases>,
    tcp_addr: Arc<String>,
) {
    log::debug!("Will start the start_replication_creator_thread");
    let mut guards = Vec::with_capacity(10); // Max 10 servers
    loop {
        log::debug!("[start_replication_creator_thread] loop");
        let message_opt = replication_supervisor_receiver.next().await;
        match message_opt {
            Some(start_replicate_message) => {
                log::debug!(
                    "[start_replication_creator_thread] got {} at {}",
                    start_replicate_message,
                    tcp_addr
                );
                let mut command = start_replicate_message.splitn(2, " ");

                let command_str = command.next();
                let name = String::from(command.next().unwrap());
                let (sender, receiver): (Sender<String>, Receiver<String>) = channel(100);

                match command_str {
                    // Add secoundary to primary
                    Some("secoundary") => {
                        if !dbs.has_cluster_memeber(&name) {
                            // Notify the members in the cluster about the new member
                            send_cluster_state_to_the_new_member(&sender, &dbs, &name);
                            let guard = add_sencoundary_to_primary(
                                &sender,
                                name.clone(),
                                &tcp_addr,
                                dbs.clone(),
                                receiver,
                            );
                            guards.push(guard);
                        } else {
                            panic!("Re-adding a secoundary that alrady exists!!!")
                        }
                    }

                    Some("leave") => {
                        if name != tcp_addr.to_string() {
                            log::info!(
                                "[start_replication_creator_thread] removing {} from the cluster!!",
                                name
                            );
                            dbs.remove_cluster_member(&name);
                        } else {
                            log::info!("[start_replication_creator_thread-leave] Ignoring {} because it is from the same server sending the message!",  start_replicate_message)
                        }
                    }
                    // Add primary to secoundary
                    Some("primary") => {
                        if !dbs.has_cluster_memeber(&name) {
                            // Notify the members in the cluster about the new member
                            send_cluster_state_to_the_new_member(&sender, &dbs, &name);
                            let guard = add_primary_to_secoundary(
                                &sender,
                                name.clone(),
                                &tcp_addr,
                                dbs.clone(),
                                receiver,
                            );
                            guards.push(guard);
                        } else {
                            // If it is already exists I just need to promote it
                            dbs.promote_member(&name);
                        }
                    }
                    // Add secoundary to secoundary
                    Some("new-secoundary") => {
                        if !dbs.has_cluster_memeber(&name) {
                            // Notify the members in the cluster about the new member
                            send_cluster_state_to_the_new_member(&sender, &dbs, &name);
                            let guard = add_sencoundary_to_secoundary(
                                &sender,
                                name.clone(),
                                &tcp_addr,
                                dbs.clone(),
                                receiver,
                            );
                            guards.push(guard);
                        }
                    }
                    // Add secoundary to secoundary
                    Some("replicate-since-to") => {
                        let mut parts = name.splitn(2, " ");
                        let name = String::from(parts.next().unwrap());
                        let start_at_str = String::from(parts.next().unwrap());
                        let start_at = start_at_str.parse::<u64>().unwrap();

                        //send missing data to primary
                        let cluster_state = dbs.cluster_state.lock().unwrap();
                        let members = cluster_state.members.lock().unwrap();
                        match members.get(&name) {
                            Some(member) => {
                                let commands = get_pendding_opps_since(start_at, &dbs);
                                log::debug!(
                                    "Will replicate {} messages to {}",
                                    commands.len(),
                                    member.name
                                );
                                for message in commands {
                                    replicate_if_some(&member.sender, &message, &member.name);
                                }
                            }
                            _ => {
                                log::warn!("Error tryting to replicate to a non existent member")
                            }
                        }
                    }
                    // Add primary to primary
                    Some("election-win") => {
                        dbs.add_cluster_member(ClusterMember {
                            name: tcp_addr.to_string(),
                            role: ClusterRole::Primary,
                            sender: None,
                        });
                        //noity others about primary
                        match dbs.replicate_message(format!("set-primary {}", tcp_addr)) {
                            Ok(_) => (),
                            Err(_) => log::warn!("Error election candidate"),
                        }
                    }
                    Some(n) => {
                        log::warn!(
                            "[start_replication_creator_thread] ignoring command not implement {}",
                            n
                        );
                        ()
                    }
                    _ => (),
                }
            }
            None => log::warn!("replication::try_next::Empty message"),
        }
    }
}

pub fn ask_to_join_all_replicas(
    replicate_address_to_thread: &String,
    tcp_addr: &String,
    external_tcp_addr: &String,
    user: &String,
    pwd: &String,
) {
    if replicate_address_to_thread.len() > 0 {
        let mut parts: Vec<&str> = replicate_address_to_thread.split(",").collect();
        parts.sort();
        for replica in parts {
            if replica == external_tcp_addr {
                log::warn!(
                    "Ignoring external_tcp_addr {} from replica equal join addr",
                    external_tcp_addr
                );
            }
            if replica != tcp_addr && replica != external_tcp_addr {
                // Don't ask to join the server sending it
                let replica_str = String::from(replica);
                ask_to_join(&replica_str, &external_tcp_addr, &user, &pwd);
            }
        }
    }
}

pub fn ask_to_join(replica_addr: &String, external_addr: &String, user: &String, pwd: &String) {
    log::debug!(
        "Will ask to join {}, from external_addr {}",
        replica_addr,
        external_addr
    );
    match std::net::TcpStream::connect(replica_addr.clone()) {
        Ok(socket) => {
            let writer = &mut std::io::BufWriter::new(&socket);
            writer
                .write_fmt(format_args!("auth {} {}\n", user, pwd))
                .unwrap();

            writer
                .write_fmt(format_args!("join {}\n", external_addr))
                .unwrap();
            writer.flush().unwrap();
        }
        Err(_) => {
            log::warn!(
                "Could not stablish the connection to replicate to {}, verify if the replica set is up", replica_addr
            )
        }
    }
}

fn start_replication(
    replicate_address: String,
    mut command_receiver: Receiver<String>,
    user: String,
    pwd: String,
    tcp_addr: String,
    is_primary: bool,
    dbs: &Arc<Databases>,
) {
    log::info!(
        "replicating to tcp client in the addr: {}",
        replicate_address
    );
    let global_fut = async {
        let (mut client, _receiver) = Client::new_empty_and_receiver();
        client
            .auth
            .store(true, std::sync::atomic::Ordering::Relaxed); // Auth as node
        let member = Some(ClusterMember {
            name: tcp_addr.clone(),
            role: ClusterRole::Secoundary, // Todo not sure if this is correct
            sender: None,
        });
        {
            let mut member_lock = client.cluster_member.lock().unwrap();
            *member_lock = member;
        }

        log::warn!("replication::start_replication::reader");
        let stream = TcpStream::connect(replicate_address.clone()).await?;
        let mut line = String::new();
        let mut reader = futures::io::BufReader::new(&stream);
        let mut writer = futures::io::BufWriter::new(&stream);
        auth_on_replication(&user, &pwd, &tcp_addr, is_primary, &mut writer).await?;
        let reader_fut = async {
            loop {
                let len = reader.read_line(&mut line).await?;
                if len == 0 {
                    log::warn!("replication::next::Empty message");
                    break;
                } else {
                    log::debug!("replication::next::{}", line);
                    let message = line.trim().to_string();
                    if message == "ok" {
                        log::debug!("Ignoring ok message");
                    } else {
                        match process_request(&message, &dbs, &mut client) {
                            _ => log::info!("Processed"),
                        }
                    }
                }
                line.clear();
            }
            Result::<(), std::io::Error>::Ok(())
        };

        let fut = async {
            log::warn!("replication::start_replication::writer");
            loop {
                let message_opt = command_receiver.next().await;
                match message_opt {
                    Some(message) => {
                        log::debug!(
                            "[start_replication] Will replicate {} to {}",
                            message,
                            replicate_address
                        );
                        match writer.write_fmt(format_args!("{}\n", message)).await {
                            Ok(_) => (),
                            Err(_) => {
                                log::warn!("Error replicating message write_fmt");
                            }
                        }
                        match futures::AsyncWriteExt::flush(&mut writer).await {
                            Ok(_) => (),
                            Err(_) => {
                                log::warn!("Error replicating message");
                            }
                        }
                    }
                    None => {
                        log::warn!("replication::next::Empty message");
                        break;
                    }
                }
            }
        };
        let join_all_promises = async { join!(fut, reader_fut) };
        match join_all_promises.await {
            _ => (),
        };
        Result::<(), std::io::Error>::Ok(())
    };
    match block_on(global_fut) {
        Ok(_) => (),
        Err(e) => log::warn!("Error global_fut {}", e),
    }
}

pub async fn auth_on_replication(
    user: &String,
    pwd: &String,
    tcp_addr: &String,
    is_primary: bool,
    writer: &mut futures::io::BufWriter<&TcpStream>,
) -> Result<(), std::io::Error> {
    log::debug!("authenticating on replication {}", tcp_addr);
    writer
        .write_fmt(format_args!("auth {} {}\n", user, pwd))
        .await?;
    if is_primary {
        // do I need this?
        writer
            .write_fmt(format_args!("set-primary {}\n", tcp_addr))
            .await?;
    } else {
        writer
            .write_fmt(format_args!("set-secoundary {}\n", tcp_addr))
            .await?;
        start_sync_process(writer, &tcp_addr).await;
        ()
    }

    match AsyncWriteExt::flush(writer).await {
        Err(e) => {
            log::warn!("auth replication error: {}", e);
            Err(e)
        }
        _ => Ok(()),
    }
}

fn make_create_db_command(db: &Database) -> String {
    let db_name = db.name.clone();
    let key_str = String::from(TOKEN_KEY);
    let token = match get_key_value_new(&key_str, &db) {
        Response::Value {
            key: _key,
            value,
            version: _,
        } => value,
        _ => String::from("none"),
    };
    format!("create-db {} {}", db_name, token)
}

fn get_full_sync_opps(dbs: &Arc<Databases>) -> Vec<String> {
    log::info!("Will perform a full sync!");
    let mut opps_vec = Vec::new();
    let dbs = dbs.map.read().unwrap(); // Will lock db creation and deletion for a long time...
    let db_list: Vec<&Database> = dbs.values().collect();
    for db in db_list {
        let db_name = db.name.clone();
        if db_name != ADMIN_DB {
            log::debug!("Praparing the db {}", db_name);
            opps_vec.push(make_create_db_command(&db));
            let map_values = {
                let values = db.map.read().unwrap();
                values.clone()
            };
            for (key, value) in &map_values {
                if key != TOKEN_KEY && key != CONNECTIONS_KEY {
                    opps_vec.push(format!("replicate {} {} {}", db_name, key, value));
                }
            }

            opps_vec.push(format!("replicate-snapshot {}", db_name));
            log::debug!("Done the the db {}", db_name);
        }
    }

    opps_vec
}

fn get_pendding_opps_since_from_sync(since: u64, dbs: &Arc<Databases>) -> Vec<String> {
    let opps = read_operations_since(since);
    let mut opps_vec = Vec::new();
    let id_keys_map = { dbs.id_keys_map.read().unwrap().clone() };
    let id_name_db_map = { dbs.id_name_db_map.read().unwrap().clone() };
    let dbs_map = dbs.map.read().expect("Error getting the dbs.map.lock"); // Will lock db creation I think
    let mut last_db = "$admin";
    let mut db: &Database = dbs_map.get(last_db).unwrap();
    let mut vec_ops_to_process: Vec<&OpLogRecord> = opps.values().collect();
    vec_ops_to_process.sort_by(|a, b| a.opp_position.cmp(&b.opp_position)); //sort by insert order
    for op_record in vec_ops_to_process {
        log::debug!("{}", op_record.to_string());
        //@todo sort by key to optmize speed
        let opp = match op_record.opp {
            ReplicateOpp::Update => {
                let db_name = id_name_db_map.get(&op_record.db).unwrap();
                log::debug!("db_name: {} and key: {}", db_name, op_record.key);
                let key_str = id_keys_map.get(&op_record.key).unwrap();
                if last_db != db_name {
                    db = dbs_map.get(db_name).unwrap();
                    last_db = db_name;
                }
                let value = match get_key_value_new(key_str, &db) {
                    Response::Value {
                        key: _key,
                        value,
                        version: _,
                    } => value,
                    _ => String::from(""),
                };
                format!("replicate {} {} {}", db_name, key_str, value)
            }
            ReplicateOpp::Remove => {
                let db_name = id_name_db_map.get(&op_record.db).unwrap();
                let key_str = id_keys_map.get(&op_record.key).unwrap();
                format!("replicate-remove {} {}", db_name, key_str)
            }
            ReplicateOpp::CreateDb => {
                let db_name = id_name_db_map.get(&op_record.db).unwrap();
                let db = dbs_map.get(db_name).unwrap();
                make_create_db_command(&db)
            }

            ReplicateOpp::Snapshot => {
                let db_name = id_name_db_map.get(&op_record.db).unwrap();
                format!("replicate-snapshot {}", db_name)
            }
        };
        opps_vec.push(opp);
    }
    opps_vec
}

pub fn get_pendding_opps_since(since: u64, dbs: &Arc<Databases>) -> Vec<String> {
    if since == 0 {
        get_full_sync_opps(dbs)
    } else {
        get_pendding_opps_since_from_sync(since, dbs)
    }
}

async fn start_sync_process(writer: &mut futures::io::BufWriter<&TcpStream>, tcp_addr: &String) {
    log::debug!("start_sync_process to {}", tcp_addr);
    // todo make sure it replicates
    match writer
        .write_fmt(format_args!(
            "replicate-since {} {}\n",
            tcp_addr.to_string(),
            last_op_time()
        ))
        .await
    {
        Ok(_n) => (),
        Err(e) => log::warn!(
            "start_sync_process writer.write_fmt Error: {} replicate-since",
            e
        ),
    }
}

pub fn add_as_secoundary(dbs: &Arc<Databases>, name: &String) {
    match dbs
        .replication_supervisor_sender
        .clone()
        .try_send(format!("secoundary {}", name))
    {
        Ok(_n) => (),
        Err(e) => log::warn!("Request::Join sender.send Error: {}", e),
    }
}

#[cfg(test)]
fn latency_trap() {
    let ten_millis = std::time::Duration::from_millis(100);
    thread::sleep(ten_millis);
}

#[cfg(not(test))]
fn latency_trap() {}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::mpsc::{channel, Receiver, Sender};
    use std::collections::HashMap;
    use std::fs;
    use std::sync::atomic::Ordering;
    use std::time;
    use tokio_test;

    pub const SAMPLE_NAME: &'static str = "sample";

    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    fn clean_env() {
        fs::remove_file(get_op_log_file_name()).unwrap(); //clean file
        let _f = get_log_file_append_mode(); //Get here to ensure the file exists
    }

    fn prep_env() -> (Arc<Databases>, Sender<String>, Receiver<String>) {
        thread::sleep(time::Duration::from_millis(50)); //give it time to the opperation to happen
        let (sender, replication_receiver): (Sender<String>, Receiver<String>) = channel(100);

        let keys_map = HashMap::new();
        let dbs = Arc::new(Databases::new(
            String::from(""),
            String::from(""),
            String::from(""),
            String::from(""),
            sender.clone(),
            sender.clone(),
            keys_map,
            1 as u128,
            true,
        ));

        dbs.node_state
            .swap(ClusterRole::Primary as usize, Ordering::Relaxed);

        let name = String::from(SAMPLE_NAME);
        let token = String::from(SAMPLE_NAME);
        let (client, _) = Client::new_empty_and_receiver();
        create_db(&name, &token, &dbs, &client, ConsensuStrategy::Newer);
        (dbs, sender, replication_receiver)
    }

    #[test]
    fn should_return_0_if_no_pending_ops() {
        let (dbs, mut sender, replication_receiver) = prep_env();
        let dbs_to_thread = dbs.clone();

        let replication_thread = thread::spawn(|| async {
            start_replication_thread(replication_receiver, dbs_to_thread).await;
        });
        {
            let map = dbs.map.read().unwrap();
            let db = map.get("$admin").unwrap();
            set_key_value("key".to_string(), "value1".to_string(), -1, db, &dbs);
            set_key_value("key".to_string(), "value3".to_string(), -1, db, &dbs);
        }

        replicate_message_with_sender(&sender.clone(), "replicate $admin key value".to_string())
            .unwrap();
        replicate_message_with_sender(&sender, "replicate $admin key value1".to_string()).unwrap();
        replicate_message_with_sender(&sender, "replicate $admin key value3".to_string()).unwrap();
        thread::sleep(time::Duration::from_millis(20));

        let test_start = Databases::next_op_log_id();
        let commands = get_pendding_opps_since(test_start, &dbs);
        log::warn!("{:?}", commands);
        assert!(commands.len() == 0, "Only one command expected");
        sender.try_send("exit".to_string()).unwrap();
        aw!(replication_thread.join().expect("thread died"));
        clean_env();
    }

    #[test]
    fn should_return_all_the_opps_if_since_is_0() {
        let (dbs, mut sender, replication_receiver) = prep_env();
        let dbs_to_thread = dbs.clone();
        let replication_thread = thread::spawn(|| async {
            start_replication_thread(replication_receiver, dbs_to_thread).await;
        });

        {
            let map = dbs.acquire_dbs_read_lock();
            let db = map.get(&SAMPLE_NAME.to_string()).unwrap();
            set_key_value("key".to_string(), "value1".to_string(), -1, db, &dbs);
            set_key_value("key".to_string(), "value3".to_string(), -1, db, &dbs);
        }

        thread::sleep(time::Duration::from_millis(10));
        replicate_message_with_sender(&sender, "create-db sample sample".to_string()).unwrap();
        replicate_message_with_sender(&sender, "replicate sample key value".to_string()).unwrap();

        replicate_message_with_sender(&sender, "replicate sample key value1".to_string()).unwrap();
        replicate_message_with_sender(&sender, "replicate sample key value3".to_string()).unwrap();
        replicate_message_with_sender(&sender, "replicate-snapshot sample".to_string()).unwrap();

        sender.try_send("exit".to_string()).unwrap();
        aw!(replication_thread.join().expect("thread died"));
        let commands = get_pendding_opps_since(0, &dbs);
        log::debug!("{:?}", commands);
        assert!(commands.len() == 3, "Only 3 command expected");
        assert!(
            commands[0] == "create-db sample sample",
            "Create sample comman error"
        );

        assert!(
            commands[1] == "replicate sample key value3",
            "Expected secound message to be sample key value3"
        );

        assert!(
            commands[2] == "replicate-snapshot sample",
            "Replicate message expected"
        );

        clean_env();
    }

    #[test]
    fn should_return_the_pending_ops() {
        let (dbs, _sender, _replication_receiver) = prep_env();
        let test_start = Databases::next_op_log_id();
        let (mut sender, replication_receiver): (Sender<String>, Receiver<String>) = channel(100);
        let dbs_to_thread = dbs.clone();
        let replication_thread = thread::spawn(|| async {
            start_replication_thread(replication_receiver, dbs_to_thread).await;
        });

        {
            let map = dbs.map.read().unwrap();
            let db = map.get(&SAMPLE_NAME.to_string()).unwrap();
            //set_key_value("key".to_string(), "value".to_string(), db);
            set_key_value("key".to_string(), "value1".to_string(), -1, db, &dbs);
            set_key_value("key".to_string(), "value3".to_string(), -1, db, &dbs);
        }

        thread::sleep(time::Duration::from_millis(10));
        replicate_message_with_sender(&sender, "create-db sample sample".to_string()).unwrap();
        replicate_message_with_sender(&sender, "replicate sample key value".to_string()).unwrap();

        replicate_message_with_sender(&sender, "replicate sample key value1".to_string()).unwrap();
        replicate_message_with_sender(&sender, "replicate sample key value3".to_string()).unwrap();

        replicate_message_with_sender(&sender, "replicate-snapshot sample".to_string()).unwrap();

        sender.try_send("exit".to_string()).unwrap();
        aw!(replication_thread.join().expect("thread died"));
        let commands = get_pendding_opps_since(test_start, &dbs);
        log::debug!("{:?}", commands);
        assert!(commands.len() == 3, "Only 3 command expected");
        assert!(
            commands[0] == "create-db sample sample",
            "Create sample comman error"
        );

        assert!(
            commands[1] == "replicate sample key value3",
            "Expected secound message to be sample key value3"
        );

        assert!(
            commands[2] == "replicate-snapshot sample",
            "Replicate message expected"
        );

        clean_env();
    }

    #[test]
    fn should_not_fail_with_a_prime_number_of_records() {
        let test_start = Databases::next_op_log_id();
        let (dbs, mut sender, replication_receiver) = prep_env();
        let dbs_to_thread = dbs.clone();

        let replication_thread = thread::spawn(|| async {
            start_replication_thread(replication_receiver, dbs_to_thread).await;
        });
        {
            let map = dbs.map.read().unwrap();
            let db = map.get("$admin").unwrap();
            set_key_value("key".to_string(), "value".to_string(), -1, db, &dbs);
            set_key_value("key".to_string(), "value1".to_string(), -1, db, &dbs);
            set_key_value("key".to_string(), "value3".to_string(), -1, db, &dbs);
            set_key_value("key".to_string(), "value4".to_string(), -1, db, &dbs);
            set_key_value("key".to_string(), "value5".to_string(), -1, db, &dbs);
            set_key_value("key".to_string(), "value6".to_string(), -1, db, &dbs);
            set_key_value("key".to_string(), "value7".to_string(), -1, db, &dbs);
            set_key_value("key".to_string(), "value8".to_string(), -1, db, &dbs);
            // 11 recoreds on the op log
        }

        replicate_message_with_sender(&sender, "replicate $admin key value".to_string()).unwrap();
        replicate_message_with_sender(&sender, "replicate $admin key value1".to_string()).unwrap();
        replicate_message_with_sender(&sender, "replicate $admin key value3".to_string()).unwrap();

        sender.try_send("exit".to_string()).unwrap();
        aw!(replication_thread.join().unwrap());
        let commands = get_pendding_opps_since(test_start, &dbs);
        assert!(commands.len() == 1, "Only one command expected");
        assert!(
            commands[0] == "replicate $admin key value8",
            "Only one command expected"
        );

        clean_env();
    }

    #[test]
    fn should_not_replicate_error_messages() {
        let (sender, _): (Sender<String>, Receiver<String>) = channel(100);
        let resp_error = Response::Error {
            msg: "Any error".to_string(),
        };

        let db_name = "some".to_string();
        let result = match replicate_request(
            Request::Set {
                key: "any".to_string(),
                value: "any".to_string(),
                version: -1,
            },
            &db_name,
            resp_error,
            &sender,
        ) {
            Response::Error { msg: _ } => true,
            _ => false,
        };
        assert!(result, "should have returned an error!")
    }

    #[test]
    fn should_replicate_if_the_command_is_a_set_and_node_is_primary() {
        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);
        let resp_set = Response::Set {
            key: "any_key".to_string(),
            value: "any_value".to_string(),
        };

        let req_set = Request::Set {
            key: "any_key".to_string(),
            value: "any_value".to_string(),
            version: -1,
        };
        let db_name = "some".to_string();
        let result = match replicate_request(req_set, &db_name, resp_set, &sender) {
            Response::Ok {} => true,
            _ => false,
        };
        assert!(result, "should have returned an ok response!");
        let replicate_command = receiver.try_next().unwrap().unwrap();
        assert!(replicate_command.ends_with("replicate some any_key -1 any_value"));
    }

    #[test]
    fn should_not_replicate_if_the_command_is_a_set_and_node_is_not_the_primary() {
        let (sender, _): (Sender<String>, Receiver<String>) = channel(100);
        let resp_set = Response::Set {
            key: "any_key".to_string(),
            value: "any_value".to_string(),
        };

        let req_set = Request::Set {
            key: "any_key".to_string(),
            value: "any_value".to_string(),
            version: -1,
        };
        let db_name = "some".to_string();
        let _ = match replicate_request(req_set, &db_name, resp_set, &sender) {
            Response::Set {
                key: _key,
                value: _value,
            } => true,
            _ => false,
        };
    }

    #[test]
    fn should_not_replicate_if_the_command_is_a_get() {
        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);
        let resp_get = Response::Value {
            key: "any_key".to_string(),
            value: "any_value".to_string(),
            version: 1,
        };

        let db_name = "some".to_string();
        let result = match replicate_request(
            Request::Get {
                key: "any_key".to_string(),
            },
            &db_name,
            resp_get,
            &sender,
        ) {
            Response::Value {
                key: _,
                value: _,
                version: _,
            } => true,
            _ => false,
        };
        assert!(result, "should have returned an value response!");
        let receiver_replicate_result = receiver.try_next().unwrap_or(None);
        assert_eq!(receiver_replicate_result, None);
    }

    #[test]
    fn should_replicate_if_the_command_is_a_snapshot_and_node_is_primary() {
        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);
        let request = Request::Snapshot {
            reclaim_space: false,
            db_names: vec![],
        };

        let resp_get = Response::Ok {};

        let db_name = "some_db_name".to_string();
        let result = match replicate_request(request, &db_name, resp_get, &sender) {
            Response::Ok {} => true,
            _ => false,
        };
        assert!(result, "should have returned an Ok response!");
        let receiver_replicate_result = receiver.try_next().unwrap().unwrap();
        assert!(receiver_replicate_result.ends_with("replicate-snapshot some_db_name false"));
    }
    #[test]
    fn should_replicate_if_the_command_is_a_create_db_and_node_is_primary() {
        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);
        let request = Request::CreateDb {
            name: "mateus_db".to_string(),
            token: "jose".to_string(),
            strategy: ConsensuStrategy::Newer,
        };

        let resp_get = Response::Ok {};

        let db_name = "some".to_string();
        let result = match replicate_request(request, &db_name, resp_get, &sender) {
            Response::Ok {} => true,
            _ => false,
        };
        assert!(result, "should have returned an Ok response!");
        let receiver_replicate_result = receiver.try_next().unwrap().unwrap();
        assert!(receiver_replicate_result.contains("create-db mateus_db jose newer"));
    }
}
