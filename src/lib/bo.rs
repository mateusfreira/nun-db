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

pub const IN_CONFLICT_RESOLUTION_KEY_VERSION: i32 = -2;

pub const TOKEN_KEY: &'static str = "$$token";
pub const ADMIN_DB: &'static str = "$admin";

const INVALID_VERSION_ERROR: &'static str = "Invalid version!";

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

    pub fn is_admin_auth(&self) -> bool {
        self.auth.load(Ordering::SeqCst)
    }

    pub fn left(&self, dbs: &Arc<Databases>) {
        let dbs_maps = dbs.map.read().expect("Error getting the dbs.map.lock");
        let db_box = dbs_maps.get(&self.selected_db_name());
        match db_box {
            Some(db) => {
                db.dec_connections();
                set_connection_counter(db, &dbs);
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

#[derive(Clone, PartialEq, Copy, Debug)]
pub enum ConsensuStrategy {
    Arbiter = 2,
    Newer = 1,
    None = 0,
}

impl fmt::Display for ConsensuStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", self.to_string())
    }
}
impl From<i32> for ConsensuStrategy {
    fn from(val: i32) -> Self {
        log::debug!("Val in ConsensuStrategy {}", val);
        use self::ConsensuStrategy::*;
        match val {
            2 => Arbiter,
            1 => Newer,
            _ => None,
        }
    }
}

impl From<String> for ConsensuStrategy {
    fn from(val: String) -> Self {
        log::debug!("Val in ConsensuStrategy {}", val);
        use self::ConsensuStrategy::*;
        match val.as_str() {
            "arbiter" => Arbiter,
            "newer" => Newer,
            _ => None,
        }
    }
}

impl ConsensuStrategy {
    pub fn to_le_bytes(&self) -> [u8; 4] {
        log::debug!("Val in ConsensuStrategy to_le_bytes {:#?}", self);
        match self {
            ConsensuStrategy::None => (0 as i32).to_le_bytes(),
            ConsensuStrategy::Newer => (1 as i32).to_le_bytes(),
            ConsensuStrategy::Arbiter => (2 as i32).to_le_bytes(),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            ConsensuStrategy::None => "none".to_string(),
            ConsensuStrategy::Newer => "newer".to_string(),
            ConsensuStrategy::Arbiter => "arbiter".to_string(),
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
    pub consensus_strategy: ConsensuStrategy,
}

impl DatabaseMataData {
    pub fn new(id: usize, consensus_strategy: ConsensuStrategy) -> DatabaseMataData {
        return DatabaseMataData {
            id,
            consensus_strategy,
        };
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Change {
    pub value: String,
    pub version: i32,
    pub opp_id: u64,
    pub key: String,
    pub resolve_conflict: bool,
}

impl Change {
    pub fn new(key: String, value: String, version: i32) -> Change {
        Change {
            key,
            value,
            version,
            opp_id: Databases::next_op_log_id(),
            resolve_conflict: false,
        }
    }

    pub fn to_resolve_change(&self) -> Change {
        Change {
            key: self.key.clone(),
            value: self.value.clone(),
            version: self.version,
            opp_id: self.opp_id,
            resolve_conflict: true,
        }
    }

    pub fn to_different_version(&self, version: i32) -> Change {
        Change {
            key: self.key.clone(),
            value: self.value.clone(),
            version: version,
            opp_id: self.opp_id,
            resolve_conflict: self.resolve_conflict,
        }
    }

    pub fn allow_save_version(&self) -> bool {
        self.keep_in_conflict_resolution()
    }

    pub fn keep_in_conflict_resolution(&self) -> bool {
        self.version == IN_CONFLICT_RESOLUTION_KEY_VERSION
    }

    pub fn resolving_conflict(&self) -> bool {
        self.resolve_conflict
    }
    /**
     * Returns the next version from a change, for conflict cases it will return conflicted keys
     */
    pub fn next_version(&self, old_value: &Value) -> i32 {
        if self.keep_in_conflict_resolution() {
            self.version
        } else if self.resolving_conflict() {
            let source_version = if old_value.is_in_conflict_resolution() {
                self.version
            } else {
                old_value.version
            };
            source_version + 1
        } else if old_value.is_in_conflict_resolution() {
            old_value.version
        } else if self.version == -1 {
            old_value.version + 1
        } else {
            self.version + 1
        }
    }
}

#[derive(Clone, Debug)]
pub struct Value {
    pub value: String,
    pub version: i32,
    pub opp_id: u64,
    pub state: ValueStatus,
    pub value_disk_addr: u64,
    pub key_disk_addr: u64,
}

impl From<String> for Value {
    fn from(value: String) -> Value {
        Value {
            value,
            version: 1,
            opp_id: Databases::next_op_log_id(),
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

impl Value {
    /**
     * If the key is new means it has not been stored in disk yet therefore status should still be
     * ValueStatus::New, if it is Deleted became updated to be repopulated
     */
    pub fn get_update_value_sate(&self) -> ValueStatus {
        if self.state == ValueStatus::New {
            ValueStatus::New
        } else {
            ValueStatus::Updated
        }
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
        let (value, version) = {
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
                    (next, -1)
                }
                _ => {
                    return Response::Error {
                        msg: "Key is not numeric".to_string(),
                    }
                }
            }
        };

        self.notify_watchers(key.clone(), value.clone(), version);
        Response::Ok {}
    }

    pub fn list_keys(&self, pattern: &String, list_system_keys: bool) -> Vec<String> {
        let query_function = get_function_by_pattern(&pattern);
        let mut keys: Vec<String> = {
            self.map
                .read()
                .unwrap()
                .iter()
                .filter(|&(_k, v)| v.state != ValueStatus::Deleted)
                .filter(|(key, _v)| query_function(&key, &pattern))
                .filter(|(key, _v)| list_system_keys || !key.starts_with("$$"))
                .map(|(key, _v)| format!("{}", key))
                .collect()
        };
        keys.sort();
        keys
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

    fn notify_watchers(&self, key: String, value: String, version: i32) {
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
                    match sender.clone().try_send(
                        format_args!(
                            "changed-version {} {} {}\n",
                            key.to_string(),
                            version,
                            value.to_string()
                        )
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
        match key {
            key if key == TOKEN_KEY => Response::Error {
                msg: "$$token key cannot be removed".to_string(),
            },
            key => {
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
                                value.opp_id,
                            );
                        }
                    }
                } // Release the lock
                let mut watchers = self.watchers.map.write().unwrap();
                match watchers.get_mut(&key) {
                    Some(senders) => {
                        for sender in senders {
                            match sender
                                .clone()
                                .try_send(format_args!("removed {}\n", key.to_string()).to_string())
                            {
                                Ok(_n) => (),
                                Err(e) => log::warn!("Request::Set sender.send Error: {}", e),
                            }
                        }
                    }
                    _ => {}
                }
                Response::Ok {}
            }
        }
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
                opp_id: value.opp_id,
            })
        } else {
            None
        }
    }

    pub fn set_value_version(
        &self,
        key: &String,
        value: &String,
        new_version: i32,
        state: ValueStatus,
        value_disk_addr: u64,
        key_disk_addr: u64,
        opp_id: u64,
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
                    opp_id,
                },
            );
        } // release the db
    }

    pub fn watch_key(&self, key: &String, sender: &Sender<String>) -> Response {
        let mut watchers = self.watchers.map.write().unwrap();
        let mut senders: Vec<Sender<String>> = match watchers.get(key) {
            Some(watchers_vec) => watchers_vec.clone(),
            _ => Vec::new(),
        };
        senders.push(sender.clone());
        watchers.insert(key.clone(), senders);
        Response::Ok {}
    }

    pub fn set_value_as_ok(
        &self,
        key: &String,
        value: &Value,
        value_disk_addr: u64,
        key_disk_addr: u64,
        opp_id: u64,
    ) {
        self.set_value_version(
            key,
            &value.value,
            value.version,
            ValueStatus::Ok,
            value_disk_addr,
            key_disk_addr,
            opp_id,
        );
    }

    /// apply the change to the database
    /// Does not fix conflicts if they happen
    ///
    /// # Arguments
    ///
    /// * `change` - The change to be applied to the database
    /// A change contains a key, a value and a version
    /// In case the version conflicts with that the database has it will return an error
    /// Do not use this method if you need conflict resolution use
    /// db_ops::apply_change_to_db_try_fix_conflicts instead
    /// Response::VersionError
    ///
    /// # Examples
    ///
    /// ```
    /// let change1 = Change::new(String::from("key"), String::from("foo"), 0);
    ///
    /// let db = Database::new(
    ///     String::from("some"),
    ///     DatabaseMataData::new(1, ConsensuStrategy::Newer),
    /// );
    /// db.set_value(&change1);
    /// let v = db.get_value(String::from("key"))
    /// assert_eq!(v.value, "foo");
    /// assert_eq!(v.version, 1);
    /// ```
    ///
    pub fn set_value(&self, change: &Change) -> Response {
        if let Some(old_version) = self.get_value(change.key.clone()) {
            let new_version = change.next_version(&old_version);
            if new_version <= old_version.version && !change.allow_save_version() {
                let state = old_version.get_update_value_sate();
                log::debug!(
                    "Version conflicted will try to resolve: {}, New version: {}, PassedVersion : {}",
                    old_version.version,
                    new_version,
                    change.version,
                );
                return Response::VersionError {
                    msg: String::from(INVALID_VERSION_ERROR),
                    old_version: old_version.version,
                    key: change.key.clone(),
                    version: change.version,
                    old_value: old_version.clone(),
                    change: change.clone(),
                    state: state,
                    db: self.name.clone(),
                };
            }
            log::debug!(
                "Updating existing value Old version: {}, New version: {}, PassedVersion : {}",
                old_version.version,
                new_version,
                change.version,
            );
            let state = old_version.get_update_value_sate();
            self.set_value_version(
                &change.key,
                &change.value,
                new_version,
                state,
                old_version.value_disk_addr,
                old_version.key_disk_addr,
                change.opp_id,
            );
            self.notify_watchers(change.key.clone(), change.value.clone(), new_version);
        } else {
            let new_version = change.version + 1;
            //new key
            self.set_value_version(
                &change.key,
                &change.value,
                new_version,
                ValueStatus::New,
                0,
                0,
                change.opp_id,
            );
            // not in disk yet
            self.notify_watchers(change.key.clone(), change.value.clone(), new_version);
        }

        Response::Set {
            key: change.key.clone(),
            value: change.value.to_string(),
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

    pub fn add_database(&self, database: Database) -> Response {
        let db_name = database.name.to_string();
        log::debug!("add_database {}", db_name);
        let mut dbs = self.map.write().unwrap();
        match dbs.get(&database.name.to_string()) {
            None => {
                let mut id_name_db_map = self.id_name_db_map.write().unwrap();
                id_name_db_map.insert(database.metadata.id as u64, database.name.to_string());
                dbs.insert(db_name.to_string(), database);
                dbs.get(&String::from(ADMIN_DB))
                    .unwrap()
                    .set_value(&Change::new(db_name.to_string(), String::from("{}"), -1));
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
        let admin_db = Database::new(
            admin_db_name.to_string(),
            DatabaseMataData::new(0, ConsensuStrategy::Newer),
        ); // id 0 adnmin db
        admin_db.set_value(&Change::new(String::from(TOKEN_KEY), pwd.to_string(), -1));
        dbs.add_database(admin_db);
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
     * needed but for now they are.
     */
    pub fn register_pending_opp(
        &self,
        op_log_id: u64,
        message: String,
        server_name: &String,
    ) -> String {
        let mut pending_opps = self.pending_opps.write().unwrap();
        match pending_opps.get(&op_log_id) {
            Some(pendding_opp) => pendding_opp.replicated(server_name).message_to_replicate(),
            None => {
                let replication_message = ReplicationMessage::new(op_log_id, message.clone());
                let message_to_replicate = replication_message.message_to_replicate();
                replication_message.replicated(server_name);
                pending_opps.insert(replication_message.opp_id, replication_message);
                message_to_replicate
            }
        }
    }

    pub fn acknowledge_pending_opp(&self, opp_id: u64, server_name: &String) -> bool {
        let mut pending_opps = self.pending_opps.write().unwrap();
        match pending_opps.get_mut(&opp_id) {
            Some(replicated_opp) => {
                let is_replicated = replicated_opp.ack(server_name);
                if is_replicated {
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
                is_replicated
            }
            None => {
                log::warn!("Acknowledging invalid opp {}", opp_id);
                false
            }
        }
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
            .map(|p_m|{
                   let replications =  p_m.replications.lock().unwrap();
                   let replications = replications.clone().into_iter().fold(String::from(""),|c:String, a:(String, bool)|String::from(format!("{}, {}:{}", c.to_owned(), a.0.to_owned(), a.1.to_owned())));
                return format!("message: \"{message}\", opp_id: {opp_id}, ack_count: {ack_count}, replicate_count: {replicate_count}, replications: {replications}",
                opp_id = p_m.opp_id,
                message = p_m.message.to_string(),
                ack_count = p_m.ack_count.load(Ordering::Relaxed),
                replicate_count = p_m.replicate_count.load(Ordering::Relaxed),
                replications = replications
                )
            }).collect()
    }

    pub fn get_dbs_name_strategy(&self) -> Vec<String> {
        let dbs = self.map.read().unwrap();
        let dbs = dbs.values();
        dbs.into_iter()
            .map(|db| format!("{} : {}", db.name, db.metadata.consensus_strategy))
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

#[derive(Clone, Debug, PartialEq)]
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
        strategy: ConsensuStrategy,
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
    Keys {
        pattern: String,
    },
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
    Arbiter {},
    Resolve {
        opp_id: u64,
        db_name: String,
        key: String,
        value: String,
        version: i32,
    },
}

#[derive(PartialEq, Debug, Clone)]
pub enum Response {
    Value {
        key: String,
        value: String,
        version: i32,
    },
    Ok {},
    Set {
        key: String,
        value: String,
    },
    Error {
        msg: String,
    },
    VersionError {
        msg: String,
        key: String,
        old_version: i32,
        version: i32,
        old_value: Value,
        state: ValueStatus,
        change: Change,
        db: String,
    },
}

pub struct ReplicationMessage {
    pub opp_id: u64,
    pub message: String,
    pub ack_count: AtomicUsize,
    pub replicate_count: AtomicUsize,
    pub start_time: Instant,
    pub replications: Mutex<HashMap<String, bool>>, // Key ServerName value ack or not
}
impl ReplicationMessage {
    pub fn new(opp_id: u64, message: String) -> ReplicationMessage {
        ReplicationMessage {
            opp_id,
            message,
            ack_count: AtomicUsize::new(0),
            replicate_count: AtomicUsize::new(0),
            start_time: Instant::now(),
            replications: Mutex::new(HashMap::new()),
        }
    }

    pub fn ack(&self, server_name: &String) -> bool {
        let not_full_ack: bool = {
            //let relations =
            match self
                .replications
                .lock()
                .unwrap()
                .insert(server_name.to_string(), true)
            {
                None => {
                    log::warn!(
                        "trying to ack {} but not pedding from the server {}",
                        self.opp_id,
                        server_name
                    );
                    false
                }
                Some(is_ack) => {
                    if !is_ack {
                        self.ack_count.fetch_add(1, Ordering::Relaxed);
                        true
                    } else {
                        log::warn!(
                            "trying to ack {} but already ack from the server {}",
                            self.opp_id,
                            server_name
                        );
                        false
                    }
                }
            }
        };
        not_full_ack
    }

    pub fn replicated(&self, server_name: &String) -> &ReplicationMessage {
        self.replicate_count.fetch_add(1, Ordering::Relaxed);
        self.replications
            .lock()
            .unwrap()
            .insert(server_name.to_string(), false);
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

    fn get_empty_dbs() -> Databases {
        let (sender, _): (Sender<String>, Receiver<String>) = channel(100);
        let (sender1, _): (Sender<String>, Receiver<String>) = channel(100);
        let keys_map = HashMap::new();
        Databases::new(
            String::from(""),
            String::from(""),
            String::from(""),
            sender,
            sender1,
            keys_map,
            1 as u128,
            true,
        )
    }

    #[test]
    fn connection_count_should_start_at_0() {
        let db = Database::new(
            String::from("some"),
            DatabaseMataData::new(1, ConsensuStrategy::Newer),
        );
        assert_eq!(db.connections_count(), 0);
    }

    #[test]
    fn connection_count_should_increment() {
        let db = Database::new(
            String::from("some"),
            DatabaseMataData::new(1, ConsensuStrategy::Newer),
        );
        assert_eq!(db.connections_count(), 0);

        db.inc_connections();

        assert_eq!(db.connections_count(), 1);
    }

    #[test]
    fn connection_count_should_decrement() {
        let db = Database::new(
            String::from("some"),
            DatabaseMataData::new(1, ConsensuStrategy::Newer),
        );
        assert_eq!(db.connections_count(), 0);

        db.inc_connections();
        db.inc_connections();
        db.dec_connections();

        assert_eq!(db.connections_count(), 1);
    }

    #[test]
    fn inc_should_increment_empty_key() {
        let db = Database::new(
            String::from("some"),
            DatabaseMataData::new(1, ConsensuStrategy::Newer),
        );
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
    fn set_should_set_the_latest_version() {
        let db = Database::new(
            String::from("some"),
            DatabaseMataData::new(1, ConsensuStrategy::Newer),
        );
        let key = String::from("new");
        db.set_value(&Change::new(key.clone(), String::from("1"), 1));
        db.set_value(&Change::new(key.clone(), String::from("23"), 23));
        let value = db.get_value(key);
        assert_eq!(value.unwrap().version, 24);
    }

    #[test]
    fn set_should_set_the_latest_version_on_creation() {
        let db = Database::new(
            String::from("some"),
            DatabaseMataData::new(1, ConsensuStrategy::Newer),
        );
        let key = String::from("new");
        db.set_value(&Change::new(key.clone(), String::from("1"), 23));
        let value = db.get_value(key);
        assert_eq!(value.unwrap().version, 24);
    }

    #[test]
    fn set_should_return_error_if_invalid_version_passed() {
        let db = Database::new(
            String::from("some"),
            DatabaseMataData::new(1, ConsensuStrategy::None),
        );
        let key = String::from("new");
        let change1 = Change::new(key.clone(), String::from("1"), 23);
        db.set_value(&change1);
        let change2 = Change::new(key.clone(), String::from("2"), 22);
        let r = db.set_value(&change2);
        assert_eq!(
            r,
            Response::VersionError {
                msg: String::from("Invalid version!"),
                old_version: 24,
                version: 22,
                key,
                old_value: Value {
                    value: String::from("1"),
                    version: 22,
                    opp_id: change1.opp_id,
                    state: ValueStatus::New,
                    value_disk_addr: 0,
                    key_disk_addr: 0
                },
                change: change2,
                db: String::from("some"),
                state: ValueStatus::New
            }
        );
    }

    #[test]
    fn set_with_negative_value_should_increment_last_version() {
        let db = Database::new(
            String::from("some"),
            DatabaseMataData::new(1, ConsensuStrategy::Newer),
        );
        let key = String::from("new");
        db.set_value(&Change::new(key.clone(), String::from("1"), 23));
        let value = db.get_value(key.clone());
        assert_eq!(value.unwrap().version, 24);
        db.set_value(&Change::new(key.clone(), String::from("2"), -1));
        let value = db.get_value(key);
        assert_eq!(value.unwrap().version, 25);
    }

    #[test]
    fn add_database_should_add_a_database() {
        let dbs = get_empty_dbs();
        assert_eq!(dbs.map.read().expect("error to lock").keys().len(), 1); //Admin db

        let db = Database::new(
            String::from("some"),
            DatabaseMataData::new(1, ConsensuStrategy::Newer),
        );

        dbs.add_database(db);
        assert_eq!(dbs.map.read().expect("error to lock").keys().len(), 2); // Admin db and the db
    }

    #[test]
    fn should_return_op_log_size() {
        let dbs = get_empty_dbs();
        assert_eq!(
            dbs.get_oplog_state(),
            "pending_ops: 0, op_log_file_size: 0, op_log_count: 0"
        );
    }

    #[test]
    fn should_count_the_number_of_replications() {
        let dbs = get_empty_dbs();
        let id = Databases::next_op_log_id();
        let server_1_name = String::from("server_1");
        let server_2_name = String::from("server_2");

        let message_to_replicate =
            dbs.register_pending_opp(id, "replicate test 1 test test".to_string(), &server_1_name);
        assert_eq!(
            message_to_replicate,
            format!("rp {} replicate test 1 test test", id)
        );

        let op_lod_state = dbs.get_oplog_state();
        assert_eq!(
            op_lod_state,
            format!("pending_ops: 1, op_log_file_size: 0, op_log_count: 0")
        );

        // Same message register as pending counts as single pendding ops for a diferent server
        let message_to_replicate =
            dbs.register_pending_opp(id, "replicate test 1 test test".to_string(), &server_2_name);
        assert_eq!(
            message_to_replicate,
            format!("rp {} replicate test 1 test test", id)
        );

        let op_lod_state = dbs.get_oplog_state();
        assert_eq!(
            op_lod_state,
            format!("pending_ops: 1, op_log_file_size: 0, op_log_count: 0")
        );

        let id2 = Databases::next_op_log_id();
        let message_to_replicate = dbs.register_pending_opp(
            id2,
            "replicate test 2 test test".to_string(),
            &server_1_name,
        );
        assert_eq!(
            message_to_replicate,
            format!("rp {} replicate test 2 test test", id2)
        );

        let op_lod_state = dbs.get_oplog_state();
        assert_eq!(
            op_lod_state,
            format!("pending_ops: 2, op_log_file_size: 0, op_log_count: 0")
        );

        dbs.acknowledge_pending_opp(id, &server_1_name);
        let op_lod_state = dbs.get_oplog_state();
        assert_eq!(
            op_lod_state,
            format!("pending_ops: 2, op_log_file_size: 0, op_log_count: 0"),
            "pending_ops should still 2, message was replicated twice"
        );

        assert_eq!(
            dbs.acknowledge_pending_opp(id, &server_1_name),
            false,
            "Should not ack message from a server aleady ack"
        );
        let op_lod_state = dbs.get_oplog_state();
        assert_eq!(
            op_lod_state,
            format!("pending_ops: 2, op_log_file_size: 0, op_log_count: 0"),
            "pending_ops should still 2, message was replicated twice"
        );
        let op_lod_state = dbs.get_oplog_state();
        assert_eq!(
            op_lod_state,
            format!("pending_ops: 2, op_log_file_size: 0, op_log_count: 0"),
            "pending_ops should still 2, message was replicated twice"
        );

        dbs.acknowledge_pending_opp(id, &server_2_name);
        let op_lod_state = dbs.get_oplog_state();
        assert_eq!(
            op_lod_state,
            format!("pending_ops: 1, op_log_file_size: 0, op_log_count: 0"),
            "pending_ops should reduce to 1, message was replicated twice and acknowledged twice"
        );

        assert_eq!(
            dbs.acknowledge_pending_opp(id2, &server_1_name),
            true,
            "Should return true acknowledged"
        );

        let op_lod_state = dbs.get_oplog_state();
        assert_eq!(
            op_lod_state,
            format!("pending_ops: 0, op_log_file_size: 0, op_log_count: 0"),
            "pending_ops should reduce to 0, message was replicated once and acknowledged once"
        );
        assert_eq!(
            dbs.acknowledge_pending_opp(id2, &server_2_name),
            false,
            "Should return false if not acknowledged"
        );

        let op_lod_state = dbs.get_oplog_state();
        assert_eq!(
            op_lod_state,
            format!("pending_ops: 0, op_log_file_size: 0, op_log_count: 0"),
            "pending_ops should stay 0, message already fully acknowledged"
        );
    }
}
