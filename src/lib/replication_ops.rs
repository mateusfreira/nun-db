use std::net::TcpStream;
use std::thread;

use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::executor::block_on;
use futures::stream::StreamExt;
use log;

use std::io::{BufWriter, Write};
use std::sync::Arc;

use crate::bo::*;
use crate::db_ops::*;
use crate::disk_ops::*;

pub fn replicate_web(replication_sender: &Sender<String>, message: String) {
    match replication_sender.clone().try_send(message.clone()) {
        Ok(_) => (),
        Err(_) => log::error!("Error replicating message {}", message),
    }
}

pub fn get_replicate_remove_message(db_name: String, key: String) -> String {
    return format!("replicate-remove {} {}", db_name, key);
}

pub fn get_replicate_message(db_name: String, key: String, value: String) -> String {
    return format!("replicate {} {} {}", db_name, key, value);
}

pub fn replicate_request(
    input: Request,
    db_name: &String,
    response: Response,
    replication_sender: &Sender<String>,
    is_primary: bool,
) -> Response {
    if is_primary {
        match response {
            Response::Error { msg: _ } => response,
            _ => match input {
                Request::CreateDb { name, token } => {
                    log::debug!("Will replicate command a created database name {}", name);
                    replicate_web(replication_sender, format!("create-db {} {}", name, token));
                    Response::Ok {}
                }
                Request::Snapshot {} => {
                    log::debug!("Will replicate a snapshot to the database {}", db_name);
                    replicate_web(
                        replication_sender,
                        format!("replicate-snapshot {}", db_name),
                    );
                    Response::Ok {}
                }
                Request::Set { value, key } => {
                    log::debug!("Will replicate the set of the key {} to {} ", key, value);
                    replicate_web(
                        replication_sender,
                        get_replicate_message(db_name.to_string(), key, value),
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

                Request::Election { id } => {
                    replicate_web(replication_sender, format!("election cadidate {}", id));
                    Response::Ok {}
                }

                Request::ElectionActive {} => {
                    replicate_web(replication_sender, format!("election active"));
                    Response::Ok {}
                }

                Request::Leave { name } => {
                    replicate_web(replication_sender, format!("replicate-leave {}", name));
                    Response::Ok {}
                }

                Request::ReplicateSet { db, value, key } => {
                    if is_primary {
                        log::debug!("Will replicate the set of the key {} to {} ", key, value);
                        replicate_web(
                            replication_sender,
                            get_replicate_message(db.to_string(), key, value),
                        );
                    }
                    Response::Ok {}
                }
                _ => response,
            },
        }
    } else {
        response
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
fn replicate_message_to_secoundary(message: String, dbs: &Arc<Databases>) {
    log::debug!("Got the message {} to replicate ", message);
    let state = dbs.cluster_state.lock().unwrap();
    for (_name, member) in state.members.lock().unwrap().iter() {
        match member.role {
            ClusterRole::Secoundary => replicate_if_some(&member.sender, &message, &member.name),
            ClusterRole::Primary => (),
            ClusterRole::StartingUp => (),
        }
    }
}

pub fn send_message_to_primary(message: String, dbs: &Arc<Databases>) {
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
    dbs.map.read().unwrap().get(&db_name).unwrap().metadata.id as u64
}

fn get_key_id(key: String, dbs: &Arc<Databases>) -> u64 {
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
    loop {
        let message_opt = replication_receiver.next().await;
        match message_opt {
            Some(message) => {
                if message == "exit" {
                    log::info!("replication_ops::start_replication_thread will exist!");
                    break;
                }
                replicate_message_to_secoundary(message.to_string(), &dbs);
                let request = Request::parse(&message.to_string()).unwrap();
                match request {
                    Request::CreateDb { name, token: _ } => {
                        let db_id = get_db_id(name, &dbs);
                        let key_id = 1;
                        log::debug!("Will write CreateDb");
                        write_op_log(&mut op_log_stream, db_id, key_id, ReplicateOpp::CreateDb);
                    }
                    Request::ReplicateSnapshot { db } => {
                        let db_id = get_db_id(db, &dbs);
                        let key_id = 2; //has to be different
                        log::debug!("Will write ReplicateSnapshot");
                        write_op_log(&mut op_log_stream, db_id, key_id, ReplicateOpp::Snapshot);
                    }
                    Request::ReplicateSet { db, key, value: _ } => {
                        let db_id = get_db_id(db, &dbs);
                        let key_id = get_key_id(key, &dbs);
                        write_op_log(&mut op_log_stream, db_id, key_id, ReplicateOpp::Update);
                    }

                    Request::ReplicateRemove { db, key } => {
                        let db_id = get_db_id(db, &dbs);
                        let key_id = get_key_id(key, &dbs);
                        write_op_log(&mut op_log_stream, db_id, key_id, ReplicateOpp::Remove);
                    }

                    Request::SetPrimary { name: _ } => (), //Election events won't be registed in OpLog
                    _ => log::debug!(
                        "Ignoring command {} in replication oplog register! not unimplemented!",
                        message
                    ),
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
        start_replication(name, receiver, user, pwd, tcp_addr.to_string(), false);
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
 * This function goal is to start the theread to connecto to the other cluster members
 *
 * The command send to the replication_start_receiver are like
 *
 * commads e.g:
 * secoundary some_sever:1412
 * primary some_sever:1412
 * <kind> <name:port>
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
                    "[start_replication_creator_thread] got {}",
                    start_replicate_message
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
                        match dbs
                            .replication_sender
                            .clone()
                            .try_send(format!("set-primary {}", tcp_addr))
                        {
                            Ok(_) => (),
                            Err(_) => log::warn!("Error election cadidate"),
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
    user: &String,
    pwd: &String,
) {
    if replicate_address_to_thread.len() > 0 {
        let mut parts: Vec<&str> = replicate_address_to_thread.split(",").collect();
        parts.sort();
        for replica in parts {
            if replica != tcp_addr {
                let replica_str = String::from(replica);
                ask_to_join(&replica_str, &tcp_addr, &user, &pwd);
            }
        }
    }
}

pub fn ask_to_join(replica_addr: &String, tcp_addr: &String, user: &String, pwd: &String) {
    match TcpStream::connect(replica_addr.clone()) {
        Ok(socket) => {
            let writer = &mut BufWriter::new(&socket);
            writer
                .write_fmt(format_args!("auth {} {}\n", user, pwd))
                .unwrap();

            writer
                .write_fmt(format_args!("join {}\n", tcp_addr))
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
) {
    log::info!(
        "replicating to tcp client in the addr: {}",
        replicate_address
    );
    match TcpStream::connect(replicate_address.clone()) {
        Ok(socket) => {
            let writer = &mut BufWriter::new(&socket);
            auth_on_replication(user, pwd, tcp_addr, is_primary, writer);
            let fut = async {
                loop {
                    let message_opt = command_receiver.next().await;
                    match message_opt {
                        Some(message) => {
                            log::debug!("Will replicate {}", message);
                            writer.write_fmt(format_args!("{}\n", message)).unwrap();
                            match writer.flush() {
                                Err(e) => {
                                    log::warn!("replication error: {}", e);
                                    break;
                                }
                                _ => (),
                            }
                        }
                        None => {
                            log::warn!("replication::next::Empty message");
                            break;
                        }
                    }
                }
            };
            block_on(fut);
        }
        Err(e) => {
            log::warn!(
                "Could not stablish the connection to replicate to {} error : {}",
                replicate_address,
                e
            )
        }
    };
}

pub fn auth_on_replication(
    user: String,
    pwd: String,
    tcp_addr: String,
    is_primary: bool,
    writer: &mut std::io::BufWriter<&std::net::TcpStream>,
) {
    writer
        .write_fmt(format_args!("auth {} {}\n", user, pwd))
        .unwrap();
    if is_primary {
        // do I need this?
        writer
            .write_fmt(format_args!("set-primary {}\n", tcp_addr))
            .unwrap();
    } else {
        writer
            .write_fmt(format_args!("set-secoundary {}\n", tcp_addr))
            .unwrap();

        start_sync_process(writer, &tcp_addr);
    }
    match writer.flush() {
        Err(e) => log::warn!("auth replication error: {}", e),
        _ => (),
    }
}

fn make_create_db_command(db: &Database) -> String {
    let db_name = db.name.clone();
    let key_str = String::from(TOKEN_KEY);
    let token = match get_key_value_new(&key_str, &db) {
        Response::Value { key: _key, value } => value,
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
                let key_str = id_keys_map.get(&op_record.key).unwrap();
                if last_db != db_name {
                    db = dbs_map.get(db_name).unwrap();
                    last_db = db_name;
                }
                let value = match get_key_value_new(key_str, &db) {
                    Response::Value { key: _key, value } => value,
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

fn start_sync_process(writer: &mut std::io::BufWriter<&std::net::TcpStream>, tcp_addr: &String) {
    writer
        .write_fmt(format_args!(
            "replicate-since {} {}\n",
            tcp_addr.to_string(),
            last_op_time()
        ))
        .unwrap();
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
mod tests {
    use super::*;
    use futures::channel::mpsc::{channel, Receiver, Sender};
    use std::collections::HashMap;
    use std::fs;
    use std::sync::atomic::Ordering;
    use std::time;
    use std::time::{SystemTime, UNIX_EPOCH};
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
            sender.clone(),
            sender.clone(),
            keys_map,
            1 as u128,
        ));

        dbs.node_state
            .swap(ClusterRole::Primary as usize, Ordering::Relaxed);

        let name = String::from(SAMPLE_NAME);
        let token = String::from(SAMPLE_NAME);
        let (client, _) = Client::new_empty_and_receiver();
        create_db(&name, &token, &dbs, &client);
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
            set_key_value("key".to_string(), "value1".to_string(), db);
            set_key_value("key".to_string(), "value3".to_string(), db);
        }

        sender
            .try_send("replicate $admin key value".to_string())
            .unwrap();
        sender
            .try_send("replicate $admin key value1".to_string())
            .unwrap();
        sender
            .try_send("replicate $admin key value3".to_string())
            .unwrap();
        thread::sleep(time::Duration::from_millis(20));

        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let test_start =
            since_the_epoch.as_secs() * 1000 + since_the_epoch.subsec_nanos() as u64 / 1_000_000;
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
            let map = dbs.map.read().unwrap();
            let db = map.get(&SAMPLE_NAME.to_string()).unwrap();
            set_key_value("key".to_string(), "value1".to_string(), db);
            set_key_value("key".to_string(), "value3".to_string(), db);
        }

        thread::sleep(time::Duration::from_millis(10));
        sender
            .try_send("create-db sample sample".to_string())
            .unwrap();
        sender
            .try_send("replicate sample key value".to_string())
            .unwrap();

        sender
            .try_send("replicate sample key value1".to_string())
            .unwrap();
        sender
            .try_send("replicate sample key value3".to_string())
            .unwrap();
        sender
            .try_send("replicate-snapshot sample".to_string())
            .unwrap();

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
    fn should_return_the_pedding_ops() {
        let (dbs, _sender, _replication_receiver) = prep_env();
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let test_start =
            since_the_epoch.as_secs() * 1000 + since_the_epoch.subsec_nanos() as u64 / 1_000_000;
        let (mut sender, replication_receiver): (Sender<String>, Receiver<String>) = channel(100);
        let dbs_to_thread = dbs.clone();
        let replication_thread = thread::spawn(|| async {
            start_replication_thread(replication_receiver, dbs_to_thread).await;
        });

        {
            let map = dbs.map.read().unwrap();
            let db = map.get(&SAMPLE_NAME.to_string()).unwrap();
            //set_key_value("key".to_string(), "value".to_string(), db);
            set_key_value("key".to_string(), "value1".to_string(), db);
            set_key_value("key".to_string(), "value3".to_string(), db);
        }

        thread::sleep(time::Duration::from_millis(10));
        sender
            .try_send("create-db sample sample".to_string())
            .unwrap();
        sender
            .try_send("replicate sample key value".to_string())
            .unwrap();

        sender
            .try_send("replicate sample key value1".to_string())
            .unwrap();
        sender
            .try_send("replicate sample key value3".to_string())
            .unwrap();
        //thread::sleep(time::Duration::from_millis(200));
        sender
            .try_send("replicate-snapshot sample".to_string())
            .unwrap();

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
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let test_start =
            since_the_epoch.as_secs() * 1000 + since_the_epoch.subsec_nanos() as u64 / 1_000_000;
        let (dbs, mut sender, replication_receiver) = prep_env();
        let dbs_to_thread = dbs.clone();

        let replication_thread = thread::spawn(|| async {
            start_replication_thread(replication_receiver, dbs_to_thread).await;
        });
        {
            let map = dbs.map.read().unwrap();
            let db = map.get("$admin").unwrap();
            set_key_value("key".to_string(), "value".to_string(), db);
            set_key_value("key".to_string(), "value1".to_string(), db);
            set_key_value("key".to_string(), "value3".to_string(), db);
            set_key_value("key".to_string(), "value4".to_string(), db);
            set_key_value("key".to_string(), "value5".to_string(), db);
            set_key_value("key".to_string(), "value6".to_string(), db);
            set_key_value("key".to_string(), "value7".to_string(), db);
            set_key_value("key".to_string(), "value8".to_string(), db); // 11 recoreds on the op log
        }

        sender
            .try_send("replicate $admin key value".to_string())
            .unwrap();
        sender
            .try_send("replicate $admin key value1".to_string())
            .unwrap();
        sender
            .try_send("replicate $admin key value3".to_string())
            .unwrap();

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
            },
            &db_name,
            resp_error,
            &sender,
            false,
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
        };
        let db_name = "some".to_string();
        let result = match replicate_request(req_set, &db_name, resp_set, &sender, true) {
            Response::Ok {} => true,
            _ => false,
        };
        assert!(result, "should have returned an ok response!");
        let replicate_command = receiver.try_next().unwrap().unwrap();
        assert_eq!(replicate_command, "replicate some any_key any_value")
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
        };
        let db_name = "some".to_string();
        let _ = match replicate_request(req_set, &db_name, resp_set, &sender, false) {
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
        };

        let db_name = "some".to_string();
        let result = match replicate_request(
            Request::Get {
                key: "any_key".to_string(),
            },
            &db_name,
            resp_get,
            &sender,
            false,
        ) {
            Response::Value { key: _, value: _ } => true,
            _ => false,
        };
        assert!(result, "should have returned an value response!");
        let receiver_replicate_result = receiver.try_next().unwrap_or(None);
        assert_eq!(receiver_replicate_result, None);
    }

    #[test]
    fn should_replicate_if_the_command_is_a_snapshot_and_node_is_primary() {
        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);
        let request = Request::Snapshot {};

        let resp_get = Response::Ok {};

        let db_name = "some_db_name".to_string();
        let result = match replicate_request(request, &db_name, resp_get, &sender, true) {
            Response::Ok {} => true,
            _ => false,
        };
        assert!(result, "should have returned an Ok response!");
        let receiver_replicate_result = receiver.try_next().unwrap().unwrap();
        assert_eq!(receiver_replicate_result, "replicate-snapshot some_db_name");
    }
    #[test]
    fn should_replicate_if_the_command_is_a_create_db_and_node_is_primary() {
        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);
        let request = Request::CreateDb {
            name: "mateus_db".to_string(),
            token: "jose".to_string(),
        };

        let resp_get = Response::Ok {};

        let db_name = "some".to_string();
        let result = match replicate_request(request, &db_name, resp_get, &sender, true) {
            Response::Ok {} => true,
            _ => false,
        };
        assert!(result, "should have returned an Ok response!");
        let receiver_replicate_result = receiver.try_next().unwrap().unwrap();
        assert_eq!(receiver_replicate_result, "create-db mateus_db jose");
    }
}
