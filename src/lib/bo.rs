use atomic_float::*;
use futures::channel::mpsc::{channel, Receiver, Sender};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use crate::db_ops::*;
use crate::disk_ops::*;

pub const TOKEN_KEY: &'static str = "$$token";
pub const ADMIN_DB: &'static str = "$admin";

pub struct Client {
    pub auth: Arc<AtomicBool>,
    pub cluster_member: Mutex<Option<ClusterMember>>,
    pub selected_db: Arc<SelectedDatabase>,
    pub sender: Sender<String>,
}

impl Client {
    pub fn is_primary(&self) -> bool {
        let member = &*self.cluster_member.lock().unwrap();
        if let Some(m) = member {
            m.role == ClusterRole::Primary
        } else {
            false
        }
    }
    pub fn left(&self, dbs: &Arc<Databases>) {
        let dbs = dbs.map.read().expect("Error getting the dbs.map.lock");
        let db_box = dbs.get(&self.selected_db_name());
        match db_box {
            Some(db) => {
                db.dec_connections();
                set_connection_counter(db);
            }
            _ => (),
        }
    }

    pub fn new_empty(sender: Sender<String>) -> Client {
        Client {
            auth: Arc::new(AtomicBool::new(false)),
            cluster_member: Mutex::new(None),
            selected_db: Arc::new(SelectedDatabase {
                name: RwLock::new("init".to_string()),
            }),
            sender,
        }
    }

    pub fn new_empty_and_receiver() -> (Client, Receiver<String>) {
        let (sender, receiver): (Sender<String>, Receiver<String>) = channel(100);
        (Client::new_empty(sender), receiver)
    }

    pub fn selected_db_name(&self) -> String {
        let db_name = {
            let name = self.selected_db.name.read().unwrap();
            name.clone()
        };
        db_name
    }
}

#[derive(Clone)]
pub struct ClusterMember {
    pub name: String,
    pub role: ClusterRole,
    pub sender: Option<Sender<String>>,
}

#[derive(Clone, PartialEq, Copy)]
pub enum ClusterRole {
    StartingUp = 0,
    Primary = 1,
    Secoundary = 2,
}

impl From<usize> for ClusterRole {
    fn from(val: usize) -> Self {
        use self::ClusterRole::*;
        match val {
            0 => StartingUp,
            1 => Primary,
            2 => Secoundary,
            _ => unreachable!(),
        }
    }
}

impl fmt::Display for ClusterRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            ClusterRole::Primary => write!(f, "Primary"),
            ClusterRole::Secoundary => write!(f, "Secoundary"),
            ClusterRole::StartingUp => write!(f, "StartingUp"),
        }
    }
}

#[derive(Clone, PartialEq, Copy, Debug)]
pub enum ValueStatus {
    /// Value is ok memory == disk values
    Ok = 0,
    /// Value value needs to be deleted in the disk
    Deleted = 1,
    /// Value is updated on memory not yet stored in disk
    Updated = 2,
    /// Value is new it is not present on disk yet
    New = 3,
}

impl From<i32> for ValueStatus {
    fn from(val: i32) -> Self {
        use self::ValueStatus::*;
        match val {
            1 => Deleted,
            2 => Updated,
            3 => New,
            _ => Ok,
        }
    }
}

impl ValueStatus {
    pub fn to_le_bytes(&self) -> [u8; 4] {
        match self {
            ValueStatus::Ok => (0 as i32).to_le_bytes(),
            ValueStatus::Deleted => (1 as i32).to_le_bytes(),
            ValueStatus::Updated => (2 as i32).to_le_bytes(),
            ValueStatus::New => (3 as i32).to_le_bytes(),
        }
    }
}

pub struct ClusterState {
    pub members: Mutex<HashMap<String, ClusterMember>>,
}

pub struct SelectedDatabase {
    pub name: RwLock<String>,
}

pub struct DatabaseMataData {
    pub id: usize,
}

impl DatabaseMataData {
    pub fn new(id: usize) -> DatabaseMataData {
        return DatabaseMataData { id };
    }
}

#[derive(Clone, Debug)]
pub struct Value {
    pub value: String,
    pub version: i32,
    pub state: ValueStatus,
    pub value_disk_addr: u64,
    pub key_disk_addr: u64,
}

impl From<String> for Value {
    fn from(value: String) -> Value {
        Value {
            value,
            version: 1,
            state: ValueStatus::New,
            value_disk_addr: 0,
            key_disk_addr: 0,
        }
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Value {
        Value::from(String::from(value))
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value.to_string())
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Value) -> bool {
        self.value == other.value
    }
}

impl PartialEq<String> for Value {
    fn eq(&self, other: &String) -> bool {
        self.value.eq(other)
    }
}

impl PartialEq<str> for Value {
    fn eq(&self, other: &str) -> bool {
        self.value.eq(other)
    }
}

impl PartialEq<&str> for Value {
    fn eq(&self, other: &&str) -> bool {
        self.value.eq(other)
    }
}
//Based on sabinchitrakar/ema-rs
pub struct NunEma {
    //Exponential Moving Average
    pub ema: AtomicF64,
    pub init: bool,
    pub k: f64,
}

pub struct Database {
    pub map: std::sync::RwLock<HashMap<String, Value>>,
    pub name: String,
    pub watchers: Watchers,
    pub connections: RwLock<AtomicUsize>,
    pub metadata: DatabaseMataData,
}

pub struct Databases {
    pub query_ema: std::sync::RwLock<NunEma>,
    pub replication_ema: std::sync::RwLock<NunEma>,
    pub map: std::sync::RwLock<HashMap<String, Database>>,
    pub id_name_db_map: std::sync::RwLock<HashMap<u64, String>>,
    pub pending_opps: std::sync::RwLock<HashMap<u64, ReplicationMessage>>,
    pub keys_map: std::sync::RwLock<HashMap<String, u64>>,
    pub id_keys_map: std::sync::RwLock<HashMap<u64, String>>,
    pub to_snapshot: RwLock<Vec<(String, bool)>>, // (database_name, reclaim_space)
    pub cluster_state: Mutex<ClusterState>,
    pub replication_supervisor_sender: Sender<String>,
    pub replication_sender: Sender<String>,
    pub node_state: Arc<AtomicUsize>,
    pub tcp_address: String,
    pub process_id: u128,
    pub user: String,
    pub pwd: String,
    pub is_oplog_valid: Arc<AtomicBool>,
}

impl Database {
    #[cfg(test)]
    pub fn to_string_hash(&self) -> HashMap<String, String> {
        let data = self.map.read().expect("Error getting the db.map.read");
        let mut value_data: HashMap<String, String> = HashMap::new();
        for (key, value) in &*data {
            value_data.insert(key.to_string(), value.to_string());
        }
        value_data
    }

    #[cfg(test)]
    pub fn count_keys(&self) -> usize {
        let data = self.map.read().expect("Error getting the db.map.read");
        data.len()
    }
    pub fn connections_count(&self) -> usize {
        let connections = self
            .connections
            .read()
            .expect("Error getting the db.connections.lock to decrement");
        return connections.load(Ordering::Relaxed);
    }
    pub fn create_db_from_value_hash(
        name: String,
        value_data: HashMap<String, Value>,
        metadata: DatabaseMataData,
    ) -> Database {
        return Database {
            map: RwLock::new(value_data),
            name,
            watchers: Watchers {
                map: RwLock::new(HashMap::new()),
            },
            connections: RwLock::new(AtomicUsize::new(0)),
            metadata,
        };
    }

    pub fn create_db_from_hash(
        name: String,
        data: HashMap<String, String>,
        metadata: DatabaseMataData,
    ) -> Database {
        let mut value_data: HashMap<String, Value> = HashMap::new();

        for (key, value) in &data {
            value_data.insert(key.to_string(), Value::from(value.to_string()));
        }
        Database::create_db_from_value_hash(name, value_data, metadata)
    }

    pub fn dec_connections(&self) {
        let mut connections = self
            .connections
            .write()
            .expect("Error getting the db.connections.lock to decrement");
        *connections.get_mut() = *connections.get_mut() - 1;
    }

    pub fn inc_connections(&self) {
        let mut connections = self
            .connections
            .write()
            .expect("Error getting the db.connections.lock to increment");
        *connections.get_mut() = *connections.get_mut() + 1;
    }

    pub fn inc_value(&self, key: String, inc: i32) -> Response {
        // This will reduce the lock time of map. It won't wait the notifyt time, we don't need to
        // wait for the update_watchers to release the key
        let value = {
            let mut db = self.map.write().unwrap();
            match i32::from_str_radix(
                &db.get(&key.to_string())
                    .unwrap_or(&Value::from("0"))
                    .to_string(),
                10,
            ) {
                Ok(current) => {
                    let next = (current + inc).to_string();
                    db.insert(key.clone(), Value::from(next.clone()));
                    next
                }
                _ => {
                    return Response::Error {
                        msg: "Key is not numeric".to_string(),
                    }
                }
            }
        };

        self.notify_watchers(key.clone(), value.clone());
        Response::Ok {}
    }

    pub fn new(name: String, metadata: DatabaseMataData) -> Database {
        return Database {
            metadata,
            map: std::sync::RwLock::new(HashMap::new()),
            connections: RwLock::new(AtomicUsize::new(0)),
            name,
            watchers: Watchers {
                map: RwLock::new(HashMap::new()),
            },
        };
    }

    fn notify_watchers(&self, key: String, value: String) {
        let watchers = self.watchers.map.read().unwrap();
        match watchers.get(&key) {
            Some(senders) => {
                for sender in senders {
                    log::debug!("Sending to another client");
                    match sender.clone().try_send(
                        format_args!("changed {} {}\n", key.to_string(), value.to_string())
                            .to_string(),
                    ) {
                        Ok(_n) => (),
                        Err(e) => log::warn!("Request::Set sender.send Error: {}", e),
                    }
                }
            }
            _ => {}
        }
    }

    pub fn remove_value(&self, key: String) -> Response {
        let mut watchers = self.watchers.map.write().unwrap();
        {
            if let Some(value) = self.get_value(key.clone()) {
                // If deleted before the key is in disk remove direct from memory
                if value.state == ValueStatus::New {
                    let mut db = self.map.write().unwrap();
                    db.remove(&key);
                } else {
                    // value.
                    self.set_value_version(
                        &key,
                        &String::from("<Empty>"),
                        value.version + 1,
                        ValueStatus::Deleted,
                        value.value_disk_addr,
                        value.key_disk_addr,
                    );
                }
            }
        } // Release the lock
        match watchers.get_mut(&key) {
            Some(senders) => {
                for sender in senders {
                    log::debug!("Sending to another client");
                    match sender
                        .try_send(format_args!("changed {} <Empty>\n", key.to_string()).to_string())
                    {
                        Ok(_n) => (),
                        Err(e) => log::warn!("Request::Remove sender.send Error: {}", e),
                    }
                }
            }
            _ => {}
        }
        Response::Ok {}
    }

    pub fn get_value(&self, key: String) -> Option<Value> {
        let db = self.map.read().unwrap();
        if let Some(value) = db.get(&key.to_string()) {
            Some(Value {
                value: value.value.to_string(),
                version: value.version,
                state: value.state,
                value_disk_addr: value.value_disk_addr,
                key_disk_addr: value.key_disk_addr,
            })
        } else {
            None
        }
    }

    fn set_value_version(
        &self,
        key: &String,
        value: &String,
        new_version: i32,
        state: ValueStatus,
        value_disk_addr: u64,
        key_disk_addr: u64,
    ) {
        {
            let mut db = self.map.write().unwrap();
            db.insert(
                key.clone(),
                Value {
                    value: value.clone(),
                    version: new_version,
                    state,
                    value_disk_addr, // will change on the store
                    key_disk_addr,   // will change on the store
                },
            );
        } // release the db
    }

    pub fn set_value_as_ok(
        &self,
        key: &String,
        value: &Value,
        value_disk_addr: u64,
        key_disk_addr: u64,
    ) {
        self.set_value_version(
            key,
            &value.value,
            value.version,
            ValueStatus::Ok,
            value_disk_addr,
            key_disk_addr,
        );
    }

    pub fn set_value(&self, key: String, value: String, version: i32) -> Response {
        if let Some(old_version) = self.get_value(key.clone()) {
            if version != -1 && version < old_version.version {
                return Response::Error {
                    msg: String::from("Invalid version!"),
                };
            }
            let new_version = old_version.version + 1;
            log::debug!(
                "Updating existing value Old version: {}, New version: {}, PassedVersion : {}",
                old_version.version,
                new_version,
                version
            );
            let state = if old_version.state == ValueStatus::New {
                ValueStatus::New
            } else {
                ValueStatus::Updated
            };
            self.set_value_version(
                &key,
                &value,
                new_version,
                state,
                old_version.value_disk_addr,
                old_version.key_disk_addr,
            )
        } else {
            //new key
            self.set_value_version(&key, &value, 1, ValueStatus::New, 0, 0) // not in disk yet
        }
        self.notify_watchers(key.clone(), value.clone());

        Response::Set {
            key: key.clone(),
            value: value.to_string(),
        }
    }
}
impl Databases {
    pub fn add_cluster_member(&self, member: ClusterMember) {
        //todo receive the data separated!!!
        let cluster_state = (*self).cluster_state.lock().unwrap();
        let mut members = cluster_state.members.lock().unwrap();
        if member.role == ClusterRole::Primary {
            log::debug!("New primary added channging all old to secundary");
            for (name, old_member) in members.clone().iter() {
                members.insert(
                    name.to_string(),
                    ClusterMember {
                        name: name.clone(),
                        role: ClusterRole::Secoundary,
                        sender: old_member.sender.clone(),
                    },
                );
            }
        }
        members.insert(
            member.name.to_string(),
            ClusterMember {
                name: member.name.clone(),
                role: member.role,
                sender: member.sender.clone(),
            },
        );
    }

    pub fn add_database(&self, name: &String, database: Database) -> Response {
        log::debug!("add_database {}", name.to_string());
        let mut dbs = self.map.write().unwrap();
        match dbs.get(name) {
            None => {
                let mut id_name_db_map = self.id_name_db_map.write().unwrap();
                id_name_db_map.insert(database.metadata.id as u64, name.to_string());
                dbs.insert(name.to_string(), database);
                dbs.get(&String::from(ADMIN_DB)).unwrap().set_value(
                    name.to_string(),
                    String::from("{}"),
                    -1,
                );
                Response::Ok {}
            }
            _ => Response::Error {
                msg: "database already exists".to_string(),
            },
        }
    }

    pub fn get_role(&self) -> ClusterRole {
        let role_int = (*self.node_state).load(Ordering::SeqCst);
        return ClusterRole::from(role_int);
    }

    pub fn has_cluster_memeber(&self, name: &String) -> bool {
        let cluster_state = (*self).cluster_state.lock().unwrap();
        let members = cluster_state.members.lock().unwrap();
        return members.contains_key(name);
    }

    pub fn is_eligible(&self) -> bool {
        return self.get_role() == ClusterRole::StartingUp;
    }

    pub fn is_primary(&self) -> bool {
        return self.get_role() == ClusterRole::Primary;
    }

    pub fn new(
        user: String,
        pwd: String,
        tcp_address: String,
        replication_supervisor_sender: Sender<String>,
        replication_sender: Sender<String>,
        keys_map: HashMap<String, u64>,
        process_id: u128,
        is_oplog_valid: bool,
    ) -> Databases {
        let mut id_keys_map: HashMap<u64, String> = HashMap::new();
        let initial_dbs = HashMap::new();
        let id_name_db_map = HashMap::new();
        let pending_opps = HashMap::new();

        for (key, val) in keys_map.iter() {
            id_keys_map.insert(*val, (*key).to_string());
        }
        let dbs = Databases {
            query_ema: std::sync::RwLock::new(NunEma::new(100)), //@todo is 100 good?
            replication_ema: std::sync::RwLock::new(NunEma::new(100)), //@todo is 100 good?
            map: std::sync::RwLock::new(initial_dbs),
            id_name_db_map: std::sync::RwLock::new(id_name_db_map),
            keys_map: std::sync::RwLock::new(keys_map),
            id_keys_map: std::sync::RwLock::new(id_keys_map),
            to_snapshot: RwLock::new(Vec::new()),
            cluster_state: Mutex::new(ClusterState {
                members: Mutex::new(HashMap::new()),
            }),
            replication_supervisor_sender,
            replication_sender,
            node_state: Arc::new(AtomicUsize::new(ClusterRole::StartingUp as usize)),
            tcp_address,
            process_id,
            user,
            pwd: pwd.to_string(),
            is_oplog_valid: Arc::new(AtomicBool::new(is_oplog_valid)),
            pending_opps: std::sync::RwLock::new(pending_opps),
        };

        let admin_db_name = String::from(ADMIN_DB);
        let admin_db = Database::new(admin_db_name.to_string(), DatabaseMataData::new(0)); // id 0 adnmin db
        admin_db.set_value(String::from(TOKEN_KEY), pwd.to_string(), -1);
        dbs.add_database(&admin_db_name.to_string(), admin_db);

        dbs
    }

    pub fn next_op_log_id() -> u64 {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        since_the_epoch.as_nanos() as u64
    }

    pub fn promote_member(&self, name: &String) {
        log::info!("Promoting {} to primary", name);
        let sender = {
            let cluster_state = (*self).cluster_state.lock().unwrap();
            let members = cluster_state.members.lock().unwrap();
            let old_member = members.get(name).unwrap();
            old_member.sender.clone()
        };
        self.add_cluster_member(ClusterMember {
            name: name.clone(),
            role: ClusterRole::Primary,
            sender: sender,
        });
    }

    pub fn remove_cluster_member(&self, name: &String) {
        let cluster_state = (*self).cluster_state.lock().unwrap();
        let mut members = cluster_state.members.lock().unwrap();
        members.remove(&name.to_string());
    }

    /**
     * Registers the message as pending from replication and return the message_to_replicate.
     * The method ensures that the message are registered only once
     * @todo this method has 2 locks for the pending_opps to make it safe ideally no lock would be
     * needed but for now they are needed.
     */
    pub fn register_pending_opp(&self, op_log_id: u64, message: String) -> String {
        let exists = { self.pending_opps.read().unwrap().contains_key(&op_log_id) }; //To limit the scope of the read lock
        if !exists {
            let replication_message = ReplicationMessage::new(op_log_id, message.clone());
            let message_to_replicate = replication_message.message_to_replicate();
            replication_message.replicated();
            let mut pending_opps = self.pending_opps.write().unwrap();
            pending_opps.insert(replication_message.opp_id, replication_message);
            message_to_replicate
        } else {
            let message_to_replicate = {
                self.pending_opps
                    .write()
                    .unwrap()
                    .get(&op_log_id)
                    .unwrap()
                    .replicated()
                    .message_to_replicate()
            }; // To limit the scope of the read lock
            message_to_replicate
        }
    }

    pub fn acknowledge_pending_opp(&self, opp_id: u64, server_name: String) {
        let mut pending_opps = self.pending_opps.write().unwrap();
        match pending_opps.get_mut(&opp_id) {
            Some(replicated_opp) => {
                replicated_opp.ack();
                let elapsed = replicated_opp.start_time.elapsed();
                self.update_replication_time_moving_avg(elapsed.as_millis());
                log::debug!(
                    "Acknowledged opp {} from {} in {:?}",
                    opp_id,
                    server_name,
                    elapsed
                );
                if replicated_opp.is_full_acknowledged() {
                    pending_opps.remove(&opp_id);
                    log::debug!(
                        "All replications Acknowledged removing opp {} from {} in {:?}, {} pending",
                        opp_id,
                        server_name,
                        elapsed,
                        pending_opps.keys().len()
                    );
                }
            }
            None => {
                log::warn!("Acknowledging invalid opp {}", opp_id);
            }
        };
    }

    pub fn get_oplog_state(&self) -> String {
        let pending_opps = self.pending_opps.read().unwrap();
        let (file_size, count) = get_op_log_size();
        format!(
            "pending_ops: {}, op_log_file_size: {}, op_log_count: {}",
            pending_opps.keys().len(),
            file_size,
            count
        )
    }

    pub fn get_pending_messages_debug(&self) -> Vec<String> {
        let pending_opps = self.pending_opps.read().unwrap();
        let messages = pending_opps.values();
        messages
            .into_iter()
            .map(|p_m|
                format!("message: \"{message}\", opp_id: {opp_id}, ack_count: {ack_count}, replicate_count: {replicate_count} ",
                opp_id = p_m.opp_id,
                message = p_m.message.to_string(),
                ack_count = p_m.ack_count.load(Ordering::Relaxed),
                replicate_count = p_m.replicate_count.load(Ordering::Relaxed),
                )
            )
            .collect()
    }
}

pub struct Watchers {
    pub map: RwLock<HashMap<String, Vec<Sender<String>>>>,
}

pub enum ReplicateOpp {
    Update = 0,
    Remove = 1,
    CreateDb = 2,
    Snapshot = 3,
}

impl From<u8> for ReplicateOpp {
    fn from(val: u8) -> Self {
        use self::ReplicateOpp::*;
        match val {
            0 => Update,
            1 => Remove,
            2 => CreateDb,
            3 => Snapshot,
            _ => Update,
        }
    }
}

pub struct OpLogRecord {
    pub db: u64,
    pub key: u64,
    pub timestamp: u64,
    pub opp_position: u64,
    pub opp: ReplicateOpp,
}

impl OpLogRecord {
    pub fn new(
        db: u64,
        key: u64,
        timestamp: u64,
        opp_position: u64,
        opp: ReplicateOpp,
    ) -> OpLogRecord {
        OpLogRecord {
            db,
            key,
            opp,
            opp_position,
            timestamp,
        }
    }

    pub fn to_key(&self) -> String {
        format!("{}_{}", self.db, self.key)
    }

    pub fn to_string(&self) -> String {
        format!(
            "db: {} key: {} timestamp: {} file_position: {}",
            self.db, self.key, self.timestamp, self.opp_position
        )
    }
}

#[derive(Clone)]
pub enum Request {
    Get {
        key: String,
    },
    GetSafe {
        key: String,
    },
    Remove {
        key: String,
    },
    ReplicateRemove {
        db: String,
        key: String,
    },
    Set {
        key: String,
        value: String,
        version: i32,
    },
    Increment {
        key: String,
        inc: i32,
    },
    ReplicateIncrement {
        db: String,
        key: String,
        inc: i32,
    },
    ReplicateSet {
        db: String,
        key: String,
        value: String,
        version: i32,
    },
    Watch {
        key: String,
    },
    UnWatch {
        key: String,
    },
    UnWatchAll {},
    Auth {
        user: String,
        password: String,
    },
    CreateDb {
        token: String,
        name: String,
    },
    UseDb {
        token: String,
        name: String,
    },
    Snapshot {
        reclaim_space: bool,
    },
    ReplicateSnapshot {
        db: String,
        reclaim_space: bool,
    },
    Leave {
        name: String,
    },
    ReplicateLeave {
        name: String,
    },
    Join {
        name: String,
    },

    ReplicateJoin {
        name: String,
    },
    SetPrimary {
        name: String,
    },
    SetScoundary {
        name: String,
    },
    ReplicateSince {
        node_name: String,
        start_at: u64,
    },
    ClusterState {},
    MetricsState {},
    ElectionWin {},
    Election {
        id: u128,
    },
    ElectionActive {},
    Keys {},
    ReplicateRequest {
        request_str: String,
        opp_id: u64,
    },
    Acknowledge {
        opp_id: u64,
        server_name: String,
    },
    Debug {
        command: String,
    },
}

#[derive(PartialEq, Debug)]
pub enum Response {
    Value { key: String, value: String },
    Ok {},
    Set { key: String, value: String },
    Error { msg: String },
}

pub struct ReplicationMessage {
    pub opp_id: u64,
    pub message: String,
    pub ack_count: AtomicUsize,
    pub replicate_count: AtomicUsize,
    pub start_time: Instant,
}
impl ReplicationMessage {
    pub fn new(opp_id: u64, message: String) -> ReplicationMessage {
        ReplicationMessage {
            opp_id,
            message,
            ack_count: AtomicUsize::new(0),
            replicate_count: AtomicUsize::new(0),
            start_time: Instant::now(),
        }
    }

    pub fn ack(&self) {
        self.ack_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn replicated(&self) -> &ReplicationMessage {
        self.replicate_count.fetch_add(1, Ordering::Relaxed);
        return self;
    }

    pub fn is_full_acknowledged(&self) -> bool {
        self.replicate_count.load(Ordering::Relaxed) == self.ack_count.load(Ordering::Relaxed)
    }

    pub fn message_to_replicate(&self) -> String {
        format!("rp {} {}", self.opp_id, self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::mpsc::{channel, Receiver, Sender};

    #[test]
    fn connection_count_should_start_at_0() {
        let db = Database::new(String::from("some"), DatabaseMataData::new(1));
        assert_eq!(db.connections_count(), 0);
    }

    #[test]
    fn connection_count_should_increment() {
        let db = Database::new(String::from("some"), DatabaseMataData::new(1));
        assert_eq!(db.connections_count(), 0);

        db.inc_connections();

        assert_eq!(db.connections_count(), 1);
    }

    #[test]
    fn connection_count_should_decrement() {
        let db = Database::new(String::from("some"), DatabaseMataData::new(1));
        assert_eq!(db.connections_count(), 0);

        db.inc_connections();
        db.inc_connections();
        db.dec_connections();

        assert_eq!(db.connections_count(), 1);
    }

    #[test]
    fn inc_should_increment_empty_key() {
        let db = Database::new(String::from("some"), DatabaseMataData::new(1));
        let key = String::from("new");
        db.inc_value(key.clone(), 1);
        {
            let values = db.map.read().unwrap();
            assert_eq!(
                values
                    .get(&key.to_string())
                    .unwrap_or(&Value::from(String::from("0")))
                    .clone(),
                "1"
            );
        }
        db.inc_value(key.clone(), 1);
        {
            let values = db.map.read().unwrap();
            assert_eq!(
                values
                    .get(&key.to_string())
                    .unwrap_or(&Value::from("0"))
                    .clone(),
                "2"
            );
        }
    }

    #[test]
    fn add_database_should_add_a_database() {
        let (sender, _): (Sender<String>, Receiver<String>) = channel(100);
        let (sender1, _): (Sender<String>, Receiver<String>) = channel(100);
        let keys_map = HashMap::new();
        let dbs = Databases::new(
            String::from(""),
            String::from(""),
            String::from(""),
            sender,
            sender1,
            keys_map,
            1 as u128,
            true,
        );
        assert_eq!(dbs.map.read().expect("error to lock").keys().len(), 1); //Admin db

        let db = Database::new(String::from("some"), DatabaseMataData::new(1));

        dbs.add_database(&String::from("jose"), db);
        assert_eq!(dbs.map.read().expect("error to lock").keys().len(), 2); // Admin db and the db
    }

    #[test]
    fn should_return_op_log_size() {
        let (sender, _): (Sender<String>, Receiver<String>) = channel(100);
        let (sender1, _): (Sender<String>, Receiver<String>) = channel(100);
        let keys_map = HashMap::new();
        let dbs = Databases::new(
            String::from(""),
            String::from(""),
            String::from(""),
            sender,
            sender1,
            keys_map,
            1 as u128,
            true,
        );
        assert_eq!(
            dbs.get_oplog_state(),
            "pending_ops: 0, op_log_file_size: 0, op_log_count: 0"
        );
    }
}
