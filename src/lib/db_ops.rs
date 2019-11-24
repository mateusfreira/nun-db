use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

use bo::*;
use disk_ops::*;

pub fn apply_to_database(
    dbs: &Arc<Databases>,
    selected_db: &Arc<SelectedDatabase>,
    sender: &Sender<String>,
    opp: &dyn Fn(&Database) -> Response,
) -> Response {
    let db_name = selected_db.name.lock().unwrap();
    let dbs = dbs.map.lock().unwrap();
    let result: Response = match dbs.get(&db_name.to_string()) {
        Some(db) => opp(db),
        None => {
         sender.send(String::from("error no-db-selected\n")).unwrap();
         return    Response::Error {
            msg: "No database found!".to_string(),
            }
        },
    };
    return result;
}

pub fn apply_if_auth(auth: &Arc<AtomicBool>, opp: &dyn Fn() -> Response) -> Response {
    if auth.load(Ordering::SeqCst) {
        opp()
    } else {
        Response::Error {
            msg: "Not auth".to_string(),
        }
    }
}

pub fn get_key_value(key: String, sender: &Sender<String>, db: &Database) -> Response {
    let db = db.map.lock().unwrap();
    let value = match db.get(&key.to_string()) {
        Some(value) => value,
        None => "<Empty>",
    };
    match sender.send(format_args!("value {}\n", value.to_string()).to_string()) {
        Ok(_n) => (),
        Err(e) => println!("Request::Get sender.send Error: {}", e),
    }
    Response::Value {
        key: key.clone(),
        value: value.to_string(),
    }
}

pub fn set_key_value(
    key: String,
    value: String,
    watchers: &Arc<Watchers>,
    db: &Database,
) -> Response {
    let mut db = db.map.lock().unwrap();
    db.insert(key.clone(), value.clone());
    match watchers.map.lock().unwrap().get(&key) {
        Some(senders) => {
            for sender in senders {
                println!("Sinding to another client");
                match sender.send(
                    format_args!("changed {} {}\n", key.to_string(), value.to_string()).to_string(),
                ) {
                    Ok(_n) => (),
                    Err(e) => println!("Request::Set sender.send Error: {}", e),
                }
            }
        }
        _ => {}
    }
    Response::Set {
        key: key.clone(),
        value: value.to_string(),
    }
}

pub fn create_temp_db(name: String) -> Arc<Database> {
    let mut initial_db = HashMap::new();
    let db_file_name = file_name_from_db_name(name.clone());
    if Path::new(&db_file_name).exists() {
        // May I should move this out of here
        let mut file = File::open(db_file_name).unwrap();
        initial_db = bincode::deserialize_from(&mut file).unwrap();
    }
    return Arc::new(create_db_from_hash(name, initial_db));
}

pub fn create_db_from_hash(name: String, data: HashMap<String, String>) -> Database {
    return Database {
        map: Mutex::new(data),
        name: Mutex::new(name),
    };
}

pub fn create_temp_selected_db(name: String) -> Arc<SelectedDatabase> {
    let tmpdb = Arc::new(SelectedDatabase {
        name: Mutex::new(name),
    });
    return tmpdb;
}
