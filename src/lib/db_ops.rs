use futures::channel::mpsc::Sender;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::bo::*;

pub const CONNECTIONS_KEY: &'static str = "$connections";

pub fn apply_to_database(
    dbs: &Arc<Databases>,
    client: &Client,
    opp: &dyn Fn(&Database) -> Response,
) -> Response {
    let db_name = client.selected_db_name();
    let dbs = dbs.map.read().expect("Error getting the dbs.map.lock");
    let result: Response = match dbs.get(&db_name.to_string()) {
        Some(db) => opp(db),
        None => {
            match client
                .sender
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

pub fn create_db(name: &String, token: &String, dbs: &Arc<Databases>, client: &Client) -> Response {
    if dbs.is_primary() || client.is_primary() {
        // If this node is the primary or the primary is asking to create it
        let empty_db_box = create_temp_db(name.clone(), dbs);
        let empty_db = Arc::try_unwrap(empty_db_box);
        match empty_db {
            Ok(db) => {
                set_key_value(TOKEN_KEY.to_string(), token.clone(), &db);
                match dbs.add_database(&name.to_string(), db) {
                    Response::Ok {} => {
                        match client
                            .sender
                            .clone()
                            .try_send("create-db success\n".to_string())
                        {
                            Ok(_n) => (),
                            Err(e) => eprintln!("Request::CreateDb  Error: {}", e),
                        }
                        Response::Ok {}
                    }
                    r => r,
                }
            }
            _ => {
                println!("Could not create the database");
                match client
                    .sender
                    .clone()
                    .try_send("error create-db-error\n".to_string())
                {
                    Ok(_n) => Response::Error {
                        msg: String::from("Error clreating the DB"),
                    },
                    Err(e) => {
                        println!("Request::Set sender.send Error: {}", e);
                        Response::Error {
                            msg: String::from("Error to send client response"),
                        }
                    }
                }
            }
        }
    } else {
        Response::Error {
            msg: String::from("Create database only allow from primary!"),
        }
    }
}
pub fn snapshot_db(db: &Database, dbs: &Databases) -> Response {
    let name = db.name.clone();
    {
        dbs.to_snapshot.write().unwrap().push(name);
    };
    Response::Ok {}
}

pub fn get_key_value(key: &String, sender: &Sender<String>, db: &Database) -> Response {
    let db = db.map.read().unwrap();
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

pub fn get_key_value_new(key: &String, db: &Database) -> Response {
    let db = db.map.read().unwrap();
    let value = match db.get(&key.to_string()) {
        Some(value) => value,
        None => "<Empty>",
    };
    Response::Value {
        key: key.clone(),
        value: value.to_string(),
    }
}

pub fn remove_key(key: &String, db: &Database) -> Response {
    db.remove_value(key.to_string())
}

pub fn is_valid_token(token: &String, db: &Database) -> bool {
    let db = db.map.read().unwrap();
    match db.get(&TOKEN_KEY.to_string()) {
        Some(value) => {
            println!("[is_valid_token] Token {} value {}", value, token);
            value == token
        }
        None => false,
    }
}

pub fn set_connection_counter(db: &Database) -> Response {
    let value = db.connections_count().to_string();
    return set_key_value(CONNECTIONS_KEY.to_string(), value, db);
}
pub fn set_key_value(key: String, value: String, db: &Database) -> Response {
    db.set_value(key.to_string(), value.to_string());
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
    let mut watchers = db.watchers.map.write().expect("db.watchers.map.lock");
    watchers.insert(key.clone(), senders);
    Response::Ok {}
}

pub fn watch_key(key: &String, sender: &Sender<String>, db: &Database) -> Response {
    let mut watchers = db.watchers.map.write().unwrap();
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
        .write()
        .expect("Error on db.watchers.map.lock")
        .clone();
    for (key, _val) in watchers.iter() {
        unwatch_key(&key, &sender, &db);
    }
    println!("Done unwatch_all");
    Response::Ok {}
}

pub fn create_temp_db(name: String, dbs: &Arc<Databases>) -> Arc<Database> {
    let initial_db = HashMap::new();
    return Arc::new(Database::create_db_from_hash(
        name,
        initial_db,
        DatabaseMataData::new(dbs.map.read().expect("could not get lock").len()),
    ));
}

pub fn create_init_dbs(
    user: String,
    pwd: String,
    tcp_address: String,
    start_replication_sender: Sender<String>,
    replication_sender: Sender<String>,
    keys_map: HashMap<String, u64>,
) -> Arc<Databases> {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    return Arc::new(Databases::new(
        user,
        pwd,
        tcp_address,
        start_replication_sender,
        replication_sender,
        keys_map,
        since_the_epoch.as_millis(),
    ));
}

pub fn get_senders(key: &String, watchers: &Watchers) -> Vec<Sender<String>> {
    let watchers = watchers
        .map
        .read()
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
    fn should_unset_a_value() {
        let key = String::from("key");
        let value = String::from("This is the value");
        let hash = HashMap::new();
        let db =
            Database::create_db_from_hash(String::from("test"), hash, DatabaseMataData::new(0));
        set_key_value(key.clone(), value.clone(), &db);

        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);

        let _value_in_hash = get_key_value(&key, &sender, &db);
        let message = receiver.try_next().unwrap().unwrap();
        assert_eq!(
            message.to_string(),
            format_args!("value {}\n", value.to_string()).to_string()
        );

        remove_key(&key, &db);

        let _value_in_hash = get_key_value(&key, &sender, &db);
        let message = receiver.try_next().unwrap().unwrap();
        assert_eq!(message.to_string(), "value <Empty>\n".to_string());
    }
    #[test]
    fn should_set_a_value() {
        let key = String::from("key");
        let value = String::from("This is the value");
        let hash = HashMap::new();
        let db =
            Database::create_db_from_hash(String::from("test"), hash, DatabaseMataData::new(0));
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
        let db =
            Database::create_db_from_hash(String::from("test"), hash, DatabaseMataData::new(0));
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
        let db =
            Database::create_db_from_hash(String::from("test"), hash, DatabaseMataData::new(0));
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
        let db =
            Database::create_db_from_hash(String::from("test"), hash, DatabaseMataData::new(0));
        set_key_value(TOKEN_KEY.to_string(), token.clone(), &db);

        assert_eq!(is_valid_token(&token, &db), true);
        assert_eq!(is_valid_token(&token_invalid, &db), false);
    }
}
