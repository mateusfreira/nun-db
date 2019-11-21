use std::sync::mpsc::Sender;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use bo::*;

pub fn apply_to_database(
    dbs: Arc<Databases>,
    selected_db: Arc<SelectedDatabase>,
    opp: &dyn Fn(&Database) -> Response,
) -> Response {
    let db_name = selected_db.name.lock().unwrap();
    let dbs = dbs.map.lock().unwrap();
    let result: Response = match dbs.get(&db_name.to_string()) {
        Some(db) => opp(db),
        None => Response::Error {
            msg: "No database found!".to_string(),
        },
    };
    return result;
}

pub fn apply_if_auth(auth: Arc<AtomicBool>, opp: &dyn Fn() -> Response) -> Response {
    if auth.load(Ordering::SeqCst) {
        opp()
    } else {
        Response::Error {
            msg: "Not auth".to_string(),
        }
    }
}

pub fn get_key_value(key: String, sender: Sender<String>, db: &Database) -> Response {
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

pub fn set_key_value(key: String, value: String, watchers: Arc<Watchers>, db: &Database) -> Response {
    let mut db = db.map.lock().unwrap();
    db.insert(key.clone().to_string(), value.clone().to_string());
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
    let initial_db = HashMap::new();
    let tmpdb = Arc::new(Database {
        map: Mutex::new(initial_db),
        name: Mutex::new(name),
    });
    return tmpdb;
}

pub fn create_temp_selected_db(name: String) -> Arc<SelectedDatabase> {
    let tmpdb = Arc::new(SelectedDatabase {
        name: Mutex::new(name),
    });
    return tmpdb;
}
