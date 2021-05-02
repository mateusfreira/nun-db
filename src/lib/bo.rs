use futures::channel::mpsc::Sender;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

pub const TOKEN_KEY: &'static str = "$$token";
pub const ADMIN_DB: &'static str = "$admin";

pub struct Client {
    pub auth: Arc<AtomicBool>,
    pub cluster_member: Mutex<Option<ClusterMember>>,
}

impl Client {
    pub fn new_empty() -> Client {
        Client {
            auth: Arc::new(AtomicBool::new(false)),
            cluster_member: Mutex::new(None),
        }
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ClusterRole::Primary => write!(f, "Primary"),
            ClusterRole::Secoundary => write!(f, "Secoundary"),
            ClusterRole::StartingUp => write!(f, "StartingUp"),
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
        return DatabaseMataData { id: id };
    }
}

pub struct Database {
    pub map: std::sync::RwLock<HashMap<String, String>>,
    pub name: String,
    pub watchers: Watchers,
    pub connections: RwLock<AtomicUsize>,
    pub metadata: DatabaseMataData,
}

pub struct Databases {
    pub map: std::sync::RwLock<HashMap<String, Database>>,
    pub id_name_db_map: std::sync::RwLock<HashMap<u64, String>>,
    pub keys_map: std::sync::RwLock<HashMap<String, u64>>,
    pub id_keys_map: std::sync::RwLock<HashMap<u64, String>>,
    pub to_snapshot: RwLock<Vec<String>>,
    pub cluster_state: Mutex<ClusterState>,
    pub start_replication_sender: Sender<String>,
    pub replication_sender: Sender<String>,
    pub node_state: Arc<AtomicUsize>,
    pub tcp_address: String,
    pub process_id: u128,
    pub user: String,
    pub pwd: String,
}

impl Database {
    pub fn new(name: String, metadata: DatabaseMataData) -> Database {
        return Database {
            metadata: metadata,
            map: std::sync::RwLock::new(HashMap::new()),
            connections: RwLock::new(AtomicUsize::new(0)),
            name: name,
            watchers: Watchers {
                map: RwLock::new(HashMap::new()),
            },
        };
    }

    pub fn create_db_from_hash(
        name: String,
        data: HashMap<String, String>,
        metadata: DatabaseMataData,
    ) -> Database {
        return Database {
            metadata: metadata,
            map: RwLock::new(data),
            name: name,
            connections: RwLock::new(AtomicUsize::new(0)),
            watchers: Watchers {
                map: RwLock::new(HashMap::new()),
            },
        };
    }

    pub fn inc_connections(&self) {
        let mut connections = self
            .connections
            .write()
            .expect("Error getting the db.connections.lock to increment");
        *connections.get_mut() = *connections.get_mut() + 1;
    }

    pub fn dec_connections(&self) {
        let mut connections = self
            .connections
            .write()
            .expect("Error getting the db.connections.lock to decrement");
        *connections.get_mut() = *connections.get_mut() - 1;
    }

    pub fn connections_count(&self) -> usize {
        let connections = self
            .connections
            .read()
            .expect("Error getting the db.connections.lock to decrement");
        return connections.load(Ordering::Relaxed);
    }

    pub fn set_value(&self, key: String, value: String) {
        let mut db = self.map.write().unwrap();
        db.insert(key.clone(), value.clone());
        let mut watchers = self.watchers.map.write().unwrap();
        match watchers.get_mut(&key) {
            Some(senders) => {
                for sender in senders {
                    println!("Sending to another client");
                    match sender.try_send(
                        format_args!("changed {} {}\n", key.to_string(), value.to_string())
                            .to_string(),
                    ) {
                        Ok(_n) => (),
                        Err(e) => println!("Request::Set sender.send Error: {}", e),
                    }
                }
            }
            _ => {}
        }
    }

    pub fn remove_value(&self, key: String) -> Response {
        let mut watchers = self.watchers.map.write().unwrap();
        let mut db = self.map.write().unwrap();
        db.remove(&key.to_string());
        match watchers.get_mut(&key) {
            Some(senders) => {
                for sender in senders {
                    println!("Sending to another client");
                    match sender
                        .try_send(format_args!("changed {} <Empty>\n", key.to_string()).to_string())
                    {
                        Ok(_n) => (),
                        Err(e) => println!("Request::Remove sender.send Error: {}", e),
                    }
                }
            }
            _ => {}
        }
        Response::Ok {}
    }
}
impl Databases {
    pub fn new(
        user: String,
        pwd: String,
        tcp_address: String,
        start_replication_sender: Sender<String>,
        replication_sender: Sender<String>,
        keys_map: HashMap<String, u64>,
        process_id: u128,
    ) -> Databases {
        let mut id_keys_map: HashMap<u64, String> = HashMap::new();
        let initial_dbs = HashMap::new();
        let id_name_db_map = HashMap::new();
        for (key, val) in keys_map.iter() {
            id_keys_map.insert(*val, (*key).to_string());
        }
        let dbs = Databases {
            map: std::sync::RwLock::new(initial_dbs),
            id_name_db_map: std::sync::RwLock::new(id_name_db_map),
            keys_map: std::sync::RwLock::new(keys_map),
            id_keys_map: std::sync::RwLock::new(id_keys_map),
            to_snapshot: RwLock::new(Vec::new()),
            cluster_state: Mutex::new(ClusterState {
                members: Mutex::new(HashMap::new()),
            }),
            start_replication_sender: start_replication_sender,
            replication_sender: replication_sender,
            user: user,
            pwd: pwd.to_string(),
            tcp_address: tcp_address,
            node_state: Arc::new(AtomicUsize::new(ClusterRole::StartingUp as usize)),
            process_id: process_id,
        };

        let admin_db_name = String::from(ADMIN_DB);
        let admin_db = Database::new(admin_db_name.to_string(), DatabaseMataData::new(0)); // id 0 adnmin db
        admin_db.set_value(String::from(TOKEN_KEY), pwd.to_string());
        dbs.add_database(&admin_db_name.to_string(), admin_db);

        dbs
    }

    pub fn add_database(&self, name: &String, database: Database) {
        println!("add_database {}", name.to_string());
        let mut dbs = self.map.write().unwrap();
        let mut id_name_db_map = self.id_name_db_map.write().unwrap();
        id_name_db_map.insert(database.metadata.id as u64, name.to_string());
        dbs.insert(name.to_string(), database);
        dbs.get(&String::from(ADMIN_DB))
            .unwrap()
            .set_value(name.to_string(), String::from("{}"));
    }

    pub fn get_role(&self) -> ClusterRole {
        let role_int = (*self.node_state).load(Ordering::SeqCst);
        return ClusterRole::from(role_int);
    }

    pub fn is_primary(&self) -> bool {
        return self.get_role() == ClusterRole::Primary;
    }

    pub fn is_eligible(&self) -> bool {
        return self.get_role() == ClusterRole::StartingUp;
    }

    pub fn add_cluster_member(&self, member: ClusterMember) {
        //todo receive the data separated!!!
        let cluster_state = (*self).cluster_state.lock().unwrap();
        let mut members = cluster_state.members.lock().unwrap();
        if member.role == ClusterRole::Primary {
            println!("New primary added channging all old to secundary");
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

    pub fn remove_cluster_member(&self, name: &String) {
        let cluster_state = (*self).cluster_state.lock().unwrap();
        let mut members = cluster_state.members.lock().unwrap();
        members.remove(&name.to_string());
    }

    pub fn has_cluster_memeber(&self, name: &String) -> bool {
        let cluster_state = (*self).cluster_state.lock().unwrap();
        let members = cluster_state.members.lock().unwrap();
        return members.contains_key(name);
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
    pub opp: ReplicateOpp,
}

impl OpLogRecord {
    pub fn new(db: u64, key: u64, timestamp: u64, opp: ReplicateOpp) -> OpLogRecord {
        OpLogRecord {
            db: db,
            key: key,
            opp: opp,
            timestamp: timestamp,
        }
    }

    pub fn to_key(&self) -> String {
        format!("{}_{}", self.db, self.key)
    }

    pub fn to_string(&self) -> String {
        format!("{}_{}_{}", self.db, self.key, self.timestamp)
    }
}

#[derive(Clone)]
pub enum Request {
    Get {
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
    },
    ReplicateSet {
        db: String,
        key: String,
        value: String,
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
    Snapshot {},
    ReplicateSnapshot {
        db: String,
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
    ElectionWin {},
    Election {
        id: u128,
    },
    ElectionActive {},
    Keys {},
}

#[derive(PartialEq)]
pub enum Response {
    Value { key: String, value: String },
    Ok {},
    Set { key: String, value: String },
    Error { msg: String },
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
    fn add_database_should_add_a_database() {
        let (sender, _receiver): (Sender<String>, Receiver<String>) = channel(100);
        let (sender1, _receiver): (Sender<String>, Receiver<String>) = channel(100);
        let keys_map = HashMap::new();
        let dbs = Databases::new(
            String::from(""),
            String::from(""),
            String::from(""),
            sender,
            sender1,
            keys_map,
            1 as u128,
        );
        assert_eq!(dbs.map.read().expect("error to lock").keys().len(), 1); //Admin db

        let db = Database::new(String::from("some"), DatabaseMataData::new(1));

        dbs.add_database(&String::from("jose"), db);
        assert_eq!(dbs.map.read().expect("error to lock").keys().len(), 2); // Admin db and the db
    }
}
