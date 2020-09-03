use futures::channel::mpsc::Sender;
use std::fmt;
use std::sync::Mutex;

use std::collections::HashMap;

#[derive(Clone)]
pub struct ClusterMember {
    pub name: String,
    pub role: ClusterRole,
    pub sender: Option<Sender<String>>,
}

#[derive(Clone)]
pub enum ClusterRole {
    Primary,
    Secoundary,
}

impl fmt::Display for ClusterRole {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ClusterRole::Primary => write!(f, "Primary"),
            ClusterRole::Secoundary => write!(f, "Secoundary"),
        }
    }
}

pub struct ClusterState {
    pub members: Mutex<Vec<ClusterMember>>,
}

pub struct SelectedDatabase {
    pub name: Mutex<String>,
}

pub struct Database {
    pub map: Mutex<HashMap<String, String>>,
    pub name: Mutex<String>,
    pub watchers: Watchers,
}

pub struct Databases {
    pub map: Mutex<HashMap<String, Database>>,
    pub to_snapshot: Mutex<Vec<String>>,
    pub cluster_state: Mutex<ClusterState>,
    pub start_replication_sender: Sender<String>,
    pub replication_sender: Sender<String>,
    pub user: String,
    pub pwd: String,
}

pub struct Watchers {
    pub map: Mutex<HashMap<String, Vec<Sender<String>>>>,
}

#[derive(Clone)]
pub enum Request {
    Get {
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

    Join {
        name: String,
    },

    ReplicateJoin {
        name: String,
    },
    SetPrimary {
        name: String,
    },
    ClusterState {},
    ElectionWin {},
}

#[derive(PartialEq)]
pub enum Response {
    Value { key: String, value: String },
    Ok {},
    Set { key: String, value: String },
    Error { msg: String },
}
