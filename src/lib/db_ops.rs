use futures::channel::mpsc::Sender;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bo::*;
use disk_ops::*;

pub const TOKEN_KEY: &'static str = "$$token";

pub fn apply_to_database(
    dbs: &Arc<Databases>,
    selected_db: &Arc<SelectedDatabase>,
    sender: &Sender<String>,
    opp: &dyn Fn(&Database) -> Response,
) -> Response {
    let db_name = selected_db
        .name
        .lock()
        .expect("Error getting the selected_db.name.lock");
    let dbs = dbs.map.lock().expect("Error getting the dbs.map.lock");
    let result: Response = match dbs.get(&db_name.to_string()) {
        Some(db) => opp(db),
        None => {
            match sender
                .clone()
                .try_send(String::from("error no-db-selected\n"))
            {
                Ok(_) => {}
                Err(e) => println!("apply_to_database::try_send {}", e),
            }
            return Response::Error {
                msg: "No database found!".to_string(),
            };
        }
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
pub fn snapshot_db(db: &Database, dbs: &Databases) -> Response {
    let name = db.name.lock().unwrap().clone();
    dbs.to_snapshot.lock().unwrap().push(name);
    Response::Ok {}
}

pub fn election_win(dbs: Arc<Databases>) -> Response {
    println!("Setting this server as a primary!");
    match dbs
        .start_replication_sender
        .clone()
        .try_send(format!("election-win self"))
    {
        Ok(_n) => (),
        Err(e) => println!("Request::ElectionWin sender.send Error: {}", e),
    }

    dbs.node_state
        .swap(ClusterRole::Primary as usize, Ordering::Relaxed);
    Response::Ok {}
}

pub fn get_key_value(key: &String, sender: &Sender<String>, db: &Database) -> Response {
    let db = db.map.lock().unwrap();
    let value = match db.get(&key.to_string()) {
        Some(value) => value,
        None => "<Empty>",
    };
    match sender
        .clone()
        .try_send(format_args!("value {}\n", value.to_string()).to_string())
    {
        Ok(_n) => (),
        Err(e) => println!("Request::Get sender.send Error: {}", e),
    }
    Response::Value {
        key: key.clone(),
        value: value.to_string(),
    }
}

pub fn is_valid_token(token: &String, db: &Database) -> bool {
    let db = db.map.lock().unwrap();
    match db.get(&TOKEN_KEY.to_string()) {
        Some(value) => {
            println!("[is_valid_token] Token {} value {}", value, token);
            value == token
        }
        None => false,
    }
}

pub fn set_key_value(key: String, value: String, db: &Database) -> Response {
    let mut watchers = db.watchers.map.lock().unwrap();
    let mut db = db.map.lock().unwrap();
    db.insert(key.clone(), value.clone());
    match watchers.get_mut(&key) {
        Some(senders) => {
            for sender in senders {
                println!("Sending to another client");
                match sender.try_send(
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

pub fn unwatch_key(key: &String, sender: &Sender<String>, db: &Database) -> Response {
    let mut senders = get_senders(&key, &db.watchers);
    println!("Senders before unwatch {:?}", senders.len());
    senders.retain(|x| !x.same_receiver(&sender));
    println!("Senders after unwatch {:?}", senders.len());
    let mut watchers = db.watchers.map.lock().expect("db.watchers.map.lock");
    watchers.insert(key.clone(), senders);
    Response::Ok {}
}

pub fn watch_key(key: &String, sender: &Sender<String>, db: &Database) -> Response {
    let mut watchers = db.watchers.map.lock().unwrap();
    let mut senders: Vec<Sender<String>> = match watchers.get(key) {
        Some(watchers_vec) => watchers_vec.clone(),
        _ => Vec::new(),
    };
    senders.push(sender.clone());
    watchers.insert(key.clone(), senders);
    Response::Ok {}
}

pub fn unwatch_all(sender: &Sender<String>, db: &Database) -> Response {
    println!("Will unwatch_all");
    let watchers = db
        .watchers
        .map
        .lock()
        .expect("Error on db.watchers.map.lock")
        .clone();
    for (key, _val) in watchers.iter() {
        unwatch_key(&key, &sender, &db);
    }
    println!("Done unwatch_all");
    Response::Ok {}
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
        watchers: Watchers {
            map: Mutex::new(HashMap::new()),
        },
    };
}

pub fn create_temp_selected_db(name: String) -> Arc<SelectedDatabase> {
    let tmpdb = Arc::new(SelectedDatabase {
        name: Mutex::new(name),
    });
    return tmpdb;
}

pub fn create_init_dbs(
    user: String,
    pwd: String,
    start_replication_sender: Sender<String>,
    replication_sender: Sender<String>,
) -> Arc<Databases> {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    let initial_dbs = HashMap::new();
    return Arc::new(Databases {
        map: Mutex::new(initial_dbs),
        to_snapshot: Mutex::new(Vec::new()),
        cluster_state: Mutex::new(ClusterState {
            members: Mutex::new(HashMap::new()),
        }),
        start_replication_sender: start_replication_sender,
        replication_sender: replication_sender,
        user: user,
        pwd: pwd,
        node_state: Arc::new(AtomicUsize::new(ClusterRole::StartingUp as usize)),
        process_id: since_the_epoch.as_millis(),
    });
}

pub fn get_senders(key: &String, watchers: &Watchers) -> Vec<Sender<String>> {
    let watchers = watchers
        .map
        .lock()
        .expect("Error on get_senders watchers.map.lock")
        .clone();
    return match watchers.get(key) {
        Some(watchers_vec) => watchers_vec.clone(),
        _ => Vec::new(),
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::mpsc::{channel, Receiver, Sender};
    #[test]
    fn should_set_a_value() {
        let key = String::from("key");
        let value = String::from("This is the value");
        let hash = HashMap::new();
        let db = create_db_from_hash(String::from("test"), hash);
        set_key_value(key.clone(), value.clone(), &db);

        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);

        let _value_in_hash = get_key_value(&key, &sender, &db);
        let message = receiver.try_next().unwrap().unwrap();
        assert_eq!(
            message.to_string(),
            format_args!("value {}\n", value.to_string()).to_string()
        );
    }

    #[test]
    fn should_unwatch_a_value() {
        let key = String::from("key");
        let hash = HashMap::new();
        let db = create_db_from_hash(String::from("test"), hash);
        let (sender, _receiver): (Sender<String>, Receiver<String>) = channel(100);
        watch_key(&key, &sender, &db);
        let senders = get_senders(&key, &db.watchers);
        assert_eq!(senders.len(), 1);
        unwatch_key(&key, &sender, &db);
        let senders = get_senders(&key, &db.watchers);
        assert_eq!(senders.len(), 0);
    }

    #[test]
    fn should_unwatch_all() {
        let key = String::from("key");
        let key1 = String::from("key1");
        let hash = HashMap::new();
        let db = create_db_from_hash(String::from("test"), hash);
        let (sender, _receiver): (Sender<String>, Receiver<String>) = channel(100);
        watch_key(&key, &sender, &db);
        watch_key(&key1, &sender, &db);
        let senders = get_senders(&key, &db.watchers);
        assert_eq!(senders.len(), 1);

        let senders = get_senders(&key1, &db.watchers);
        assert_eq!(senders.len(), 1);
        unwatch_all(&sender, &db);

        let senders = get_senders(&key1, &db.watchers);
        assert_eq!(senders.len(), 0);

        let senders = get_senders(&key, &db.watchers);
        assert_eq!(senders.len(), 0);
    }

    #[test]
    fn should_validate_token() {
        let token = String::from("key");
        let token_invalid = String::from("invalid");
        let hash = HashMap::new();
        let db = create_db_from_hash(String::from("test"), hash);
        set_key_value(TOKEN_KEY.to_string(), token.clone(), &db);

        assert_eq!(is_valid_token(&token, &db), true);
        assert_eq!(is_valid_token(&token_invalid, &db), false);
    }
}
