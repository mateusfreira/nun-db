use futures::channel::mpsc::Sender;

use std::sync::Mutex;

use std::collections::HashMap;

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
    pub user: String,
    pub pwd: String,
}

pub struct Watchers {
    pub map: Mutex<HashMap<String, Vec<Sender<String>>>>,
}

pub enum Request {
    Get { key: String },
    Set { key: String, value: String },
    Watch { key: String },
    UnWatch { key: String },
    UnWatchAll {},
    Auth { user: String, password: String },
    CreateDb { token: String, name: String },
    UseDb { token: String, name: String },
    Snapshot {},
}

pub enum Response {
    Value { key: String, value: String },
    Ok {},
    Set { key: String, value: String },
    Error { msg: String },
}
