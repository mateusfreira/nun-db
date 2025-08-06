use futures::channel::mpsc::Sender;
use log;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::bo::*;
use crate::disk_ops::*;

pub const CONNECTIONS_KEY: &'static str = "$connections";

pub fn create_db(
    name: &String,
    token: &String,
    dbs: &Arc<Databases>,
    client: &Client,
    strategy: ConsensuStrategy,
) -> Response {
    if dbs.is_primary() || client.is_primary() {
        log::debug!(
            "Request::CreateDb - Creating database {} with strategy {:?}",
            name,
            strategy
        );
        // If this node is the primary or the primary is asking to create it
        let empty_db_box = create_temp_db(name.clone(), strategy, dbs);
        let empty_db = Arc::try_unwrap(empty_db_box);
        match empty_db {
            Ok(db) => {
                set_key_value(TOKEN_KEY.to_string(), token.clone(), -1, &db, &dbs);
                match dbs.add_database(db) {
                    Response::Ok {} => {
                        match client
                            .sender
                            .clone()
                            .try_send("create-db success\n".to_string())
                        {
                            Ok(_n) => (),
                            Err(e) => log::warn!("Request::CreateDb  Error: {}", e),
                        }
                        Response::Ok {}
                    }
                    r => r,
                }
            }
            _ => {
                log::debug!("Could not create the database");
                match client
                    .sender
                    .clone()
                    .try_send("error create-db-error\n".to_string())
                {
                    Ok(_n) => Response::Error {
                        msg: String::from("Error clreating the DB"),
                    },
                    Err(e) => {
                        log::warn!("Request::Set sender.send Error: {}", e);
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

pub fn snapshot_db_by_name(name: &String, dbs: &Databases, reclaim_space: bool) -> Response {
    match dbs.add_db_to_snapshot_by_name(name, reclaim_space) {
        Ok(_) => Response::Ok {},
        Err(e) => Response::Error {
            msg: format!("Error trying to snapshot database: {}", e),
        },
    }
}

pub fn get_key_value(key: &String, sender: &Sender<String>, db: &Database) -> Response {
    let result = get_key_value_new(key, &db);
    if let Response::Value {
        key: _,
        value,
        version: _,
    } = result.clone()
    {
        match sender
            .clone()
            .try_send(format_args!("value {}\n", value.to_string()).to_string())
        {
            Err(e) => log::warn!("Request::Get sender.send Error: {}", e),
            _ => (),
        }
    }

    result
}

pub fn get_key_value_safe(key: &String, sender: &Sender<String>, db: &Database) -> Response {
    let result = get_key_value_new(key, &db);
    if let Response::Value {
        key: _,
        version,
        value,
    } = result.clone()
    {
        match sender
            .clone()
            .try_send(format_args!("value-version {} {}\n", version, value).to_string())
        {
            Ok(_n) => (),
            Err(e) => log::warn!("Request::Get sender.send Error: {}", e),
        }
    }
    result
}

/**
 * All get keys functions must call this function and parse the result from it
 */
pub fn get_key_value_new(key: &String, db: &Database) -> Response {
    let db = db.map.read().unwrap();
    let (value, version) = match db.get(&key.to_string()) {
        Some(value) => (value.to_string(), value.version),
        None => (String::from("<Empty>"), 1 as i32),
    };
    Response::Value {
        key: key.clone(),
        value: value.to_string(),
        version,
    }
}

pub fn remove_key(key: &String, db: &Database) -> Response {
    db.remove_value(key.to_string())
}

pub fn is_valid_token(token: &String, db: &Database) -> bool {
    let db = db.map.read().unwrap();
    match db.get(&TOKEN_KEY.to_string()) {
        Some(value) => {
            log::debug!("[is_valid_token] Token {} value {}", value, token);
            value == token
        }
        None => false,
    }
}

pub fn is_valid_user_token(token: &String, user_name: &String, db: &Database) -> bool {
    let db = db.map.read().unwrap();
    match db.get(&format!("$$user_{}", user_name)) {
        Some(value) => {
            log::debug!("[is_valid_token] Token {} value {}", value, token);
            value == token
        }
        None => false,
    }
}

pub fn set_connection_counter(db: &Database, dbs: &Arc<Databases>) -> Response {
    let value = db.connections_count().to_string();
    return set_key_value(CONNECTIONS_KEY.to_string(), value, -1, db, &dbs);
}

pub fn set_key_value(
    key: String,
    value: String,
    version: i32,
    db: &Database,
    dbs: &Arc<Databases>,
) -> Response {
    apply_change_to_db_try_fix_conflicts(
        &Change::new(key.to_string(), value.to_string(), version),
        &db,
        &dbs,
    )
}

pub fn apply_change_to_db_try_fix_conflicts(
    change: &Change,
    db: &Database,
    dbs: &Arc<Databases>,
) -> Response {
    let response = db.set_value(change);
    if let Response::VersionError {
        msg: _,
        key: _,
        old_version: _,
        version: _,
        old_value: _,
        state: _,
        change: _,
        db: _,
    } = response
    {
        log::debug!("VersionError in the key {}", change.key.clone());
        db.try_resolve_conflict_response(response.clone(), &dbs)
    } else {
        response
    }
}

pub fn unwatch_key(key: &String, sender: &Sender<String>, db: &Database) -> Response {
    let mut senders = get_senders(&key, &db.watchers);
    log::debug!("Senders before unwatch {:?}", senders.len());
    senders.retain(|x| !x.same_receiver(&sender));
    log::debug!("Senders after unwatch {:?}", senders.len());
    let mut watchers = db.watchers.map.write().expect("db.watchers.map.lock");
    watchers.insert(key.clone(), senders);
    Response::Ok {}
}

pub fn watch_key(key: &String, sender: &Sender<String>, db: &Database) -> Response {
    db.watch_key(&key, &sender)
}

pub fn unwatch_all(sender: &Sender<String>, db: &Database) -> Response {
    log::debug!("Will unwatch_all");
    let watchers = db
        .watchers
        .map
        .write()
        .expect("Error on db.watchers.map.lock")
        .clone();
    for (key, _val) in watchers.iter() {
        unwatch_key(&key, &sender, &db);
    }
    log::debug!("Done unwatch_all");
    Response::Ok {}
}

pub fn create_temp_db(
    name: String,
    strategy: ConsensuStrategy,
    dbs: &Arc<Databases>,
) -> Arc<Database> {
    let initial_db = HashMap::new();
    return Arc::new(Database::create_db_from_hash(
        name,
        initial_db,
        DatabaseMataData::new(dbs.map.read().expect("could not get lock").len(), strategy),
    ));
}

pub fn create_init_dbs(
    user: String,
    pwd: String,
    tcp_address: String,
    external_tcp_address: String,
    replication_supervisor_sender: Sender<String>,
    replication_sender: Sender<String>,
    keys_map: HashMap<String, u64>,
    is_oplog_valid: bool,
) -> Arc<Databases> {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    return Arc::new(Databases::new(
        user,
        pwd,
        tcp_address,
        external_tcp_address,
        replication_supervisor_sender,
        replication_sender,
        keys_map,
        since_the_epoch.as_millis(),
        is_oplog_valid,
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

pub fn safe_shutdown(dbs: &Arc<Databases>) {
    snapshot_keys(&dbs); // This is more important than the not snapshot_dbs
    snapshot_all_pendding_dbs(&dbs);
}

pub fn get_function_by_pattern(
    pattern: &String,
) -> for<'r, 's> fn(&'r std::string::String, &'s std::string::String) -> bool {
    let query_function = if pattern.ends_with('*') {
        starts_with
    } else if pattern.starts_with('*') {
        ends_with
    } else {
        contains
    };
    query_function
}

fn starts_with(key: &String, pattern: &String) -> bool {
    key.starts_with(&pattern.replace("*", ""))
}

fn ends_with(key: &String, pattern: &String) -> bool {
    key.ends_with(&pattern.replace("*", ""))
}

fn contains(key: &String, pattern: &String) -> bool {
    key.contains(pattern)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::mpsc::{channel, Receiver, Sender};
    use std::sync::atomic::Ordering;

    pub const SAMPLE_NAME: &'static str = "sample";

    fn get_dbs() -> Arc<Databases> {
        let (sender, _replication_receiver): (Sender<String>, Receiver<String>) = channel(100);
        let keys_map = HashMap::new();
        let dbs = Arc::new(Databases::new(
            String::from(""),
            String::from(""),
            String::from(""),
            String::from(""),
            sender.clone(),
            sender.clone(),
            keys_map,
            1 as u128,
            true,
        ));

        dbs.node_state
            .swap(ClusterRole::Primary as usize, Ordering::Relaxed);

        let name = String::from(SAMPLE_NAME);
        let token = String::from(SAMPLE_NAME);
        let (client, _) = Client::new_empty_and_receiver();
        create_db(&name, &token, &dbs, &client, ConsensuStrategy::Newer);
        dbs
    }

    #[test]
    fn should_unset_a_value() {
        let dbs: Arc<Databases> = get_dbs();
        let key = String::from("key");
        let value = String::from("This is the value");
        let hash = HashMap::new();
        let db = Database::create_db_from_hash(
            String::from("test"),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Newer),
        );
        set_key_value(key.clone(), value.clone(), -1, &db, &dbs);

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
    fn should_fail_set_value_if_version_is_not_valid() {
        let dbs = get_dbs();
        let key = String::from("key");
        let value = String::from("This is the value");
        let value_new = String::from("This is the new value");
        let hash = HashMap::new();
        let db = Database::create_db_from_hash(
            String::from("test"),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::None),
        );
        set_key_value(key.clone(), value.clone(), -1, &db, &dbs); // Version up to 0
        set_key_value(key.clone(), value.clone(), -1, &db, &dbs); // Version up to 1
        set_key_value(key.clone(), value.clone(), -1, &db, &dbs); // Version up to 2
        match set_key_value(key.clone(), value_new.clone(), 1, &db, &dbs) {
            Response::VersionError {
                msg,
                old_version: _,
                version: _,
                old_value: _,
                change: _,
                key: _,
                db: _,
                state: _,
            } => {
                assert_eq!(msg, "Invalid version!");
            }
            _ => {
                assert_eq!(1, 2);
            }
        }
    }

    #[test]
    fn should_set_value_if_version_is_valid() {
        let dbs = get_dbs();
        let key = String::from("key");
        let value = String::from("This is the value");
        let value_new = String::from("This is the new value");
        let hash = HashMap::new();
        let db = Database::create_db_from_hash(
            String::from("test"),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Newer),
        );
        set_key_value(key.clone(), value.clone(), 1, &db, &dbs);
        set_key_value(key.clone(), value_new.clone(), 2, &db, &dbs);

        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);

        let _value_in_hash = get_key_value(&key, &sender, &db);
        let message = receiver.try_next().unwrap().unwrap();
        assert_eq!(
            message.to_string(),
            format_args!("value {}\n", value_new.to_string()).to_string()
        );
    }

    #[test]
    fn should_resolve_conflict() {
        let dbs = get_dbs();
        let key = String::from("some");
        let db = Database::new(
            String::from("some"),
            DatabaseMataData::new(1, ConsensuStrategy::Newer),
        );
        let change1 = Change::new(key.clone(), String::from("some1"), 0);
        apply_change_to_db_try_fix_conflicts(&change1, &db, &dbs);
        let change2 = Change::new(String::from("some"), String::from("some2"), 0);

        assert_eq!(
            apply_change_to_db_try_fix_conflicts(&change2, &db, &dbs),
            Response::Set {
                key: String::from("some"),
                value: String::from("some2")
            }
        );
    }

    #[test]
    fn should_set_a_value() {
        let dbs = get_dbs();
        let key = String::from("key");
        let value = String::from("This is the value");
        let hash = HashMap::new();
        let db = Database::create_db_from_hash(
            String::from("test"),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Newer),
        );
        set_key_value(key.clone(), value.clone(), -1, &db, &dbs);

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
        let _dbs = get_dbs();
        let key = String::from("key");
        let hash = HashMap::new();
        let db = Database::create_db_from_hash(
            String::from("test"),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Newer),
        );
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
        let _dbs = get_dbs();
        let key = String::from("key");
        let key1 = String::from("key1");
        let hash = HashMap::new();
        let db = Database::create_db_from_hash(
            String::from("test"),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Newer),
        );
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
        let dbs = get_dbs();
        let token = String::from("key");
        let token_invalid = String::from("invalid");
        let hash = HashMap::new();
        let db = Database::create_db_from_hash(
            String::from("test"),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Newer),
        );
        set_key_value(TOKEN_KEY.to_string(), token.clone(), -1, &db, &dbs);

        assert_eq!(is_valid_token(&token, &db), true);
        assert_eq!(is_valid_token(&token_invalid, &db), false);
    }
}
