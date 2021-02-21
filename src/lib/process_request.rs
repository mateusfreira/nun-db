use futures::channel::mpsc::Sender;
use std::mem;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use bo::*;
use db_ops::*;
use election_ops::*;
use replication_ops::*;
use security::*;

pub fn process_request(
    input: &str,
    sender: &mut Sender<String>,
    db: &Arc<SelectedDatabase>,
    dbs: &Arc<Databases>,
    client: &mut Client,
) -> Response {
    let input_to_log = clean_string_to_log(input, &dbs);
    println!(
        "[{}] process_request got message '{}'. ",
        thread_id::get(),
        input_to_log
    );
    let db_name_state = db.name.lock().expect("Could not lock name mutex").clone();
    let is_primary = dbs.is_primary();
    let start = Instant::now();
    let request = match Request::parse(String::from(input).trim_matches('\n')) {
        Ok(req) => req,
        Err(e) => return Response::Error { msg: e },
    };

    println!(
        "[{}] process_request parsed message '{}'. ",
        thread_id::get(),
        input_to_log
    );
    let result = match request.clone() {
        Request::Auth { user, password } => {
            let valid_user = dbs.user.clone();
            let valid_pwd = dbs.pwd.clone();

            if user == valid_user && password == valid_pwd {
                client.auth.swap(true, Ordering::Relaxed);
            };
            let message = if client.auth.load(Ordering::SeqCst) {
                "valid auth\n".to_string()
            } else {
                "invalid auth\n".to_string()
            };
            match sender.clone().try_send(message) {
                Ok(_n) => (),
                Err(e) => println!("Request::Auth sender.send Error: {}", e),
            };
            Response::Ok {}
        }

        Request::Get { key } => {
            apply_to_database(&dbs, &db, &sender, &|_db| get_key_value(&key, &sender, _db))
        }

        Request::Set { key, value } => apply_to_database(&dbs, &db, &sender, &|_db| {
            if dbs.is_primary() {
                set_key_value(key.clone(), value.clone(), _db)
            } else {
                let db_name_state = _db.name.lock().expect("Could not lock name mutex");
                send_message_to_primary(
                    get_replicate_message(db_name_state.to_string(), key.clone(), value.clone()),
                    dbs,
                );
                Response::Ok {}
            }
        }),

        Request::ReplicateSet {
            db: name,
            key,
            value,
        } => apply_if_auth(&client.auth, &|| {
            let dbs = dbs.map.lock().expect("Could not lock the dbs mutex");
            let respose: Response = match dbs.get(&name.to_string()) {
                Some(db) => set_key_value(key.clone(), value.clone(), db),
                _ => {
                    println!("Not a valid database name");
                    Response::Error {
                        msg: "Not a valid database name".to_string(),
                    }
                }
            };
            respose
        }),

        Request::Snapshot {} => apply_if_auth(&client.auth, &|| {
            apply_to_database(&dbs, &db, &sender, &|_db| snapshot_db(_db, &dbs))
        }),

        Request::ReplicateSnapshot {
            db: db_to_snap_shot,
        } => apply_if_auth(&client.auth, &|| {
            let db = create_temp_selected_db(db_to_snap_shot.clone());
            apply_to_database(&dbs, &db, &sender, &|_db| snapshot_db(_db, &dbs))
        }),

        Request::UnWatch { key } => apply_to_database(&dbs, &db, &sender, &|_db| {
            unwatch_key(&key, &sender, _db);
            Response::Ok {}
        }),

        Request::UnWatchAll {} => apply_to_database(&dbs, &db, &sender, &|_db| {
            unwatch_all(&sender, _db);
            _db.dec_connections();//todo put it in a better methods
            set_connection_counter(_db);
            Response::Ok {}
        }),

        Request::Watch { key } => apply_to_database(&dbs, &db, &sender, &|_db| {
            watch_key(&key, &sender, _db);
            Response::Ok {}
        }),

        Request::UseDb { name, token } => {
            let mut db_name_state = db.name.lock().expect("Could not lock name mutex");
            let dbs = dbs.map.lock().expect("Could not lock the mao mutex");
            let respose: Response = match dbs.get(&name.to_string()) {
                Some(db) => {
                    if is_valid_token(&token, db) {
                        let _ = mem::replace(&mut *db_name_state, name.clone());
                        db.inc_connections();//Increment the number of connections 
                        set_connection_counter(db);
                        Response::Ok {}
                    } else {
                        Response::Error {
                            msg: "Invalid token".to_string(),
                        }
                    }
                }
                _ => {
                    println!("Not a valid database name");
                    Response::Error {
                        msg: "Not a valid database name".to_string(),
                    }
                }
            };
            respose
        }

        Request::CreateDb { name, token } => apply_if_auth(&client.auth, &|| {
            let mut dbs = dbs.map.lock().unwrap();
            let empty_db_box = create_temp_db(name.clone());
            let empty_db = Arc::try_unwrap(empty_db_box);
            match empty_db {
                Ok(db) => {
                    set_key_value(TOKEN_KEY.to_string(), token.clone(), &db);
                    dbs.insert(name.to_string(), db);
                    match sender.clone().try_send("create-db success\n".to_string()) {
                        Ok(_n) => (),
                        Err(e) => println!("Request::CreateDb  Error: {}", e),
                    }
                }
                _ => {
                    println!("Could not create the database");
                    match sender
                        .clone()
                        .try_send("error create-db-error\n".to_string())
                    {
                        Ok(_n) => (),
                        Err(e) => println!("Request::Set sender.send Error: {}", e),
                    }
                }
            }
            Response::Ok {}
        }),

        Request::ElectionActive {} => Response::Ok {}, //Nothing need to be done here now
        Request::ElectionWin {} => apply_if_auth(&client.auth, &|| election_win(dbs.clone())),
        Request::Election { id } => apply_if_auth(&client.auth, &|| {
            println!("Election received");
            if id == dbs.process_id {
                println!("Ignoring same node election");
            } else if id < dbs.process_id {
                println!("Will run the start_election");
                start_election(dbs);
            } else {
                println!("Won't run the start_election");
                match dbs
                    .replication_sender
                    .clone()
                    .try_send(format!("election alive"))
                {
                    Ok(_) => (),
                    Err(_) => println!("Error election alive"),
                }
                dbs.node_state
                    .swap(ClusterRole::Secoundary as usize, Ordering::Relaxed);
            }
            Response::Ok {}
        }),

        Request::SetPrimary { name } => apply_if_auth(&client.auth, &|| {
            println!("Setting {} as primary!", name);
            match dbs
                .start_replication_sender
                .clone()
                .try_send(format!("primary {}", name))
            {
                Ok(_n) => (),
                Err(e) => println!("Request::SetPrimary sender.send Error: {}", e),
            }
            dbs.node_state
                .swap(ClusterRole::Secoundary as usize, Ordering::Relaxed);
            let member = Some(ClusterMember {
                name: name.clone(),
                role: ClusterRole::Primary,
                sender: None,
            });
            let mut member_lock = client.cluster_member.lock().unwrap();
            *member_lock = member;
            Response::Ok {}
        }),

        Request::SetScoundary { name } => apply_if_auth(&client.auth, &|| {
            println!("Setting {} as primary!", name);
            let member = Some(ClusterMember {
                name: name.clone(),
                role: ClusterRole::Secoundary,
                sender: None,
            });
            let mut member_lock = client.cluster_member.lock().unwrap();
            *member_lock = member;
            Response::Ok {}
        }),

        Request::Join { name } => apply_if_auth(&client.auth, &|| {
            match dbs
                .start_replication_sender
                .clone()
                .try_send(format!("secoundary {}", name))
            {
                Ok(_n) => (),
                Err(e) => println!("Request::Join sender.send Error: {}", e),
            }
            start_election(dbs);
            Response::Ok {}
        }),

        Request::Leave { name } => apply_if_auth(&client.auth, &|| {
            match dbs
                .start_replication_sender
                .clone()
                .try_send(format!("leave {}", name))
            {
                Ok(_n) => (),
                Err(e) => println!("Request::leave sender.send Error: {}", e),
            }
            start_new_election(dbs);
            Response::Ok {}
        }),

        Request::ReplicateLeave { name } => apply_if_auth(&client.auth, &|| {
            match dbs
                .start_replication_sender
                .clone()
                .try_send(format!("leave {}", name))
            {
                Ok(_n) => (),
                Err(e) => println!("Request::replicateLeave sender.send Error: {}", e),
            }
            start_election(dbs);
            Response::Ok {}
        }),

        Request::ReplicateJoin { name } => apply_if_auth(&client.auth, &|| {
            match dbs
                .start_replication_sender
                .clone()
                .try_send(format!("new-secoundary {}", name))
            {
                Ok(_n) => (),
                Err(e) => println!("Request::ReplicateJoin sender.send Error: {}", e),
            }
            Response::Ok {}
        }),

        Request::ClusterState {} => apply_if_auth(&client.auth, &|| {
            let mut members: Vec<String> = dbs
                .cluster_state
                .lock()
                .unwrap()
                .members
                .lock()
                .unwrap()
                .iter()
                .map(|(_name, member)| format!("{}:{}", member.name, member.role))
                .collect();
            members.sort(); //OMG try not to use this
            let cluster_state_str = members.iter().fold(String::from(""), |current, acc| {
                format!("{} {},", current, acc)
            });
            match sender
                .clone()
                .try_send(format_args!("cluster-state {}\n", cluster_state_str).to_string())
            {
                Err(e) => println!("Request::ClusterState sender.send Error: {}", e),
                _ => (),
            }

            println!("ClusterState {}", cluster_state_str);
            Response::Value {
                key: String::from("cluster-state"),
                value: String::from(cluster_state_str),
            }
        }),

        Request::Keys {} => apply_to_database(&dbs, &db, &sender, &|db| {
            let keys: Vec<String> = db
                .map
                .lock()
                .unwrap()
                .keys()
                .filter(|key| !key.starts_with("$$")) // filter the secret keys
                .map(|key| format!("{}", key))
                .collect();
            let keys = keys.iter().fold(String::from(""), |current, acc| {
                format!("{},{}", current, acc)
            });
            match sender
                .clone()
                .try_send(format_args!("keys {}\n", keys).to_string())
            {
                Err(e) => println!("Request::ClusterState sender.send Error: {}", e),
                _ => (),
            }

            println!("DBKeys {}", keys);
            Response::Value {
                key: String::from("keys"),
                value: String::from(keys),
            }
        }),
    };

    let elapsed = start.elapsed();
    println!(
        "[{}] Server processed message '{}' in {:?}",
        thread_id::get(),
        input_to_log,
        elapsed
    );
    let replication_result = replicate_request(
        request,
        &db_name_state,
        result,
        &dbs.replication_sender.clone(),
        is_primary,
    );
    replication_result
}
