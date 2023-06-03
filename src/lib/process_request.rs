use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use crate::bo::*;
use crate::db_ops::*;
use crate::election_ops::*;
use crate::replication_ops::*;
use crate::security::*;
//use crate::consensus_ops::*;
use log;

fn process_request_obj(request: &Request, dbs: &Arc<Databases>, client: &mut Client) -> Response {
    match request.clone() {
        Request::ReplicateIncrement { db: name, key, inc } => apply_if_auth(&client.auth, &|| {
            let dbs = dbs.map.read().expect("Could not lock the dbs mutex");
            let respose: Response = match dbs.get(&name.to_string()) {
                Some(db) => {
                    db.inc_value(key.to_string(), inc);
                    Response::Ok {}
                }
                _ => {
                    log::debug!("Not a valid database name");
                    Response::Error {
                        msg: "Not a valid database name".to_string(),
                    }
                }
            };
            respose
        }),

        Request::Increment { key, inc } => apply_if_safe_access(&dbs, &client, &key, &|_db| {
            if dbs.is_primary() {
                _db.inc_value(key.to_string(), inc);
            } else {
                let db_name_state = _db.name.clone();
                // This is wrong
                send_message_to_primary(
                    get_replicate_increment_message(
                        db_name_state.to_string(),
                        key.clone(),
                        inc.to_string(),
                    ),
                    dbs,
                );
            }
            Response::Ok {}
        }),
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
            match client.sender.clone().try_send(message) {
                Ok(_n) => (),
                Err(e) => log::debug!("Request::Auth sender.send Error: {}", e),
            };
            Response::Ok {}
        }

        Request::Get { key } => apply_if_safe_access(&dbs, &client, &key, &|_db| {
            get_key_value(&key, &client.sender, _db)
        }),

        Request::GetSafe { key } => apply_if_safe_access(&dbs, &client, &key, &|_db| {
            get_key_value_safe(&key, &client.sender, _db)
        }),

        Request::Remove { key } => {
            apply_if_safe_access(&dbs, &client, &key, &|_db| remove_key(&key, _db))
        }

        Request::Set {
            key,
            value,
            version,
        } => apply_if_safe_access(&dbs, &client, &key, &|_db| {
            let respose = set_key_value(key.clone(), value.clone(), version, _db, &dbs);
            if !dbs.is_primary() {
                let db_name_state = _db.name.clone();
                send_message_to_primary(
                    get_replicate_message(
                        db_name_state.to_string(),
                        key.clone(),
                        value.clone(),
                        version,
                    ),
                    dbs,
                );
            }
            respose
        }),

        Request::ReplicateRemove { db: name, key } => apply_if_auth(&client.auth, &|| {
            let dbs = dbs.map.read().expect("Could not lock the dbs mutex");
            let respose: Response = match dbs.get(&name.to_string()) {
                Some(db) => remove_key(&key, db),
                _ => {
                    log::debug!("Not a valid database name");
                    Response::Error {
                        msg: "Not a valid database name".to_string(),
                    }
                }
            };
            respose
        }),

        Request::ReplicateSet {
            db: name,
            key,
            value,
            version,
        } => apply_if_auth(&client.auth, &|| {
            let dbs_map = dbs.map.read().expect("Could not lock the dbs mutex");
            let respose: Response = match dbs_map.get(&name.to_string()) {
                Some(db) => set_key_value(key.clone(), value.clone(), version, db, &dbs),
                _ => {
                    log::debug!("Not a valid database name");
                    Response::Error {
                        msg: "Not a valid database name".to_string(),
                    }
                }
            };
            respose
        }),

        Request::Snapshot { reclaim_space } => apply_if_auth(&client.auth, &|| {
            apply_to_database(&dbs, &client, &|_db| {
                if dbs.is_primary() {
                    snapshot_db(_db, &dbs, reclaim_space)
                } else {
                    send_message_to_primary(
                        format!(
                            "replicate-snapshot {} {}",
                            _db.name.to_string(),
                            reclaim_space
                        ),
                        &dbs,
                    );
                    Response::Ok {}
                }
            })
        }),

        Request::ReplicateSnapshot {
            reclaim_space,
            db: db_to_snap_shot,
        } => apply_if_auth(&client.auth, &|| {
            let dbs_map = dbs.map.read().expect("Error getting the dbs.map.lock");
            match dbs_map.get(&db_to_snap_shot.to_string()) {
                Some(db) => snapshot_db(db, &dbs, reclaim_space),
                _ => Response::Error {
                    msg: "Invalid db name".to_string(),
                },
            }
        }),

        Request::UnWatch { key } => apply_to_database(&dbs, &client, &|_db| {
            unwatch_key(&key, &client.sender, _db);
            Response::Ok {}
        }),

        Request::UnWatchAll {} => apply_to_database(&dbs, &client, &|_db| {
            unwatch_all(&client.sender, _db);
            Response::Ok {}
        }),

        Request::Watch { key } => apply_if_safe_access(&dbs, &client, &key, &|_db| {
            watch_key(&key, &client.sender, _db);
            Response::Ok {}
        }),

        Request::UseDb {
            name,
            token,
            user_name,
        } => {
            let dbs_map = dbs.map.read().expect("Could not lock the mao mutex");
            let respose: Response = match dbs_map.get(&name.to_string()) {
                Some(db) => {
                    match user_name {
                        Some(user_name) => {
                            let mut db_name_state = client.selected_db.name.write().unwrap();
                            let mut user_name_state = client.selected_db.user_name.write().unwrap();

                            if is_valid_user_token(&token, &user_name, db) {
                                let _ = std::mem::replace(&mut *db_name_state, name.clone());
                                let _ = std::mem::replace(
                                    &mut *user_name_state,
                                    Some(user_name.clone()),
                                );
                                db.inc_connections(); //Increment the number of connections
                                set_connection_counter(db, &dbs);
                                Response::Ok {}
                            } else {
                                Response::Error {
                                    msg: "Invalid token".to_string(),
                                }
                            }
                        }
                        None => {
                            if is_valid_token(&token, db) {
                                let mut db_name_state = client.selected_db.name.write().unwrap();
                                let _ = std::mem::replace(&mut *db_name_state, name.clone());
                                db.inc_connections(); //Increment the number of connections
                                set_connection_counter(db, &dbs);
                                Response::Ok {}
                            } else {
                                Response::Error {
                                    msg: "Invalid token".to_string(),
                                }
                            }
                        }
                    }
                }
                _ => {
                    log::debug!("Not a valid database name");
                    Response::Error {
                        msg: "Not a valid database name".to_string(),
                    }
                }
            };
            respose
        }
        Request::CreateUser { token, user_name } => {
            apply_if_safe_access(&dbs, &client, &String::from("$$user_"), &|_db| {
                let key = String::from(format!("$$user_{}", user_name));
                let respose = set_key_value(key.to_string(), token.to_string(), -1, _db, &dbs);
                match respose {
                    Response::Set { .. } => {
                        if !dbs.is_primary() {
                            let db_name_state = _db.name.clone();
                            send_message_to_primary(
                                get_replicate_message(
                                    db_name_state.to_string(),
                                    key,
                                    token.to_string(),
                                    -1,
                                ),
                                dbs,
                            );
                        }
                        Response::Ok {}
                    }
                    _ => respose,
                }
            })
        }

        Request::CreateDb {
            name,
            token,
            strategy,
        } => apply_if_auth(&client.auth, &|| {
            create_db(&name, &token, &dbs, &client, strategy)
        }),

        Request::ElectionActive {} => Response::Ok {}, //Nothing need to be done here now
        Request::ElectionWin {} => apply_if_auth(&client.auth, &|| election_win(&dbs)),
        Request::Election { id } => apply_if_auth(&client.auth, &|| election_eval(&dbs, id)),

        Request::SetPrimary { name } => apply_if_auth(&client.auth, &|| {
            if !dbs.is_primary() {
                log::info!("Setting {} as primary!", name);
            } else {
                log::warn!("Got a set primary from {} but already is a primary... There is going to be war!!", name);
            }
            match dbs
                .replication_supervisor_sender
                .clone()
                .try_send(format!("primary {}", name))
            {
                Ok(_n) => (),
                Err(e) => log::error!("Request::SetPrimary sender.send Error: {}", e),
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
            log::info!("Setting {} as secoundary!", name);
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
            if dbs.is_primary() {
                add_as_secoundary(&dbs, &name);
            } else {
                log::debug!("Ignoring join on secoundary!")
            }
            Response::Ok {}
        }),

        Request::Leave { name } => apply_if_auth(&client.auth, &|| {
            match dbs
                .replication_supervisor_sender
                .clone()
                .try_send(format!("leave {}", name))
            {
                Ok(_n) => (),
                Err(e) => log::debug!("Request::leave sender.send Error: {}", e),
            }
            start_new_election(&dbs); //Slow operation here
            Response::Ok {}
        }),

        Request::ReplicateLeave { name } => apply_if_auth(&client.auth, &|| {
            match dbs
                .replication_supervisor_sender
                .clone()
                .try_send(format!("leave {}", name))
            {
                Ok(_n) => (),
                Err(e) => log::debug!("Request::replicateLeave sender.send Error: {}", e),
            }
            Response::Ok {}
        }),

        Request::ReplicateJoin { name } => apply_if_auth(&client.auth, &|| {
            match dbs
                .replication_supervisor_sender
                .clone()
                .try_send(format!("new-secoundary {}", name))
            {
                Ok(_n) => (),
                Err(e) => log::debug!("Request::ReplicateJoin sender.send Error: {}", e),
            }
            Response::Ok {}
        }),

        Request::ReplicateSince {
            node_name,
            start_at,
        } => apply_if_auth(&client.auth, &|| {
            match dbs
                .replication_supervisor_sender
                .clone()
                .try_send(format!("replicate-since-to {} {}", node_name, start_at))
            {
                Ok(_n) => (),
                Err(e) => log::warn!("Request::ReplicateJoin sender.send Error: {}", e),
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
            log::debug!("ClusterMember {}", members.len());
            let cluster_state_str = members.iter().fold(String::from(""), |current, acc| {
                format!("{} {},", current, acc)
            });
            match client
                .sender
                .clone()
                .try_send(format_args!("cluster-state {}\n", cluster_state_str).to_string())
            {
                Err(e) => log::warn!("Request::ClusterState sender.send Error: {}", e),
                _ => (),
            }

            log::debug!("ClusterState {}", cluster_state_str);
            Response::Value {
                key: String::from("cluster-state"),
                value: String::from(cluster_state_str),
                version: -1,
            }
        }),

        Request::MetricsState {} => apply_if_auth(&client.auth, &|| {
            let oplog_state = dbs.get_oplog_state();
            log::debug!("MetricsState {}", oplog_state);
            let monitoring_state = dbs.get_monitoring_state();
            log::debug!("MonitoringState {}", monitoring_state);
            let metrics_state = format!("{},{}\n", oplog_state, monitoring_state);

            match client
                .sender
                .clone()
                .try_send(format_args!("metrics-state {}\n", metrics_state).to_string())
            {
                Err(e) => log::warn!("Request::ClusterState sender.send Error: {}", e),
                _ => (),
            }
            Response::Value {
                key: String::from("oplog-state"),
                value: String::from(metrics_state),
                version: -1,
            }
        }),

        Request::Keys { pattern } => apply_to_database(&dbs, &client, &|db| {
            let keys = db
                .list_keys(&pattern, client.is_admin_auth())
                .iter()
                .fold(String::from(""), |current, acc| {
                    format!("{},{}", current, acc)
                });
            match client
                .sender
                .clone()
                .try_send(format_args!("keys {}\n", keys).to_string())
            {
                Err(e) => log::warn!("Request::ClusterState sender.send Error: {}", e),
                _ => (),
            }

            Response::Value {
                key: String::from("keys"),
                value: String::from(keys),
                version: -1,
            }
        }),
        Request::Acknowledge {
            opp_id,
            server_name,
        } => apply_if_auth(&client.auth, &|| {
            dbs.acknowledge_pending_opp(opp_id, &server_name);
            Response::Ok {}
        }),
        Request::ReplicateRequest {
            request_str,
            opp_id,
        } => {
            send_message_to_primary(format!("ack {} {}", opp_id, dbs.tcp_address), dbs); // Todo validate auth
            match process_request(&request_str, &dbs, client) {
                Response::Error { msg } => {
                    log::warn!("Error to process message {}, error: {}", opp_id, msg);
                    Response::Error { msg }
                }
                r => r,
            }
        }
        /*
         * This command should only return get opperations
         * No change must be made as part of a Debug command
         */
        Request::Debug { command } => apply_if_auth(&client.auth, &|| {
            match command.as_str() {
                "pending-ops" => {
                    let pendin_msgs = dbs.get_pending_messages_debug().join("\n");
                    log::info!("Peding messages on the server {}", pendin_msgs);
                    match client
                        .sender
                        .clone()
                        .try_send(format_args!("pending-ops {}\n", pendin_msgs).to_string())
                    {
                        Err(e) => log::warn!("Request::pending-ops sender.send Error: {}", e),
                        _ => (),
                    }
                }
                "pendding-conflitcts" => {
                    apply_to_database(&dbs, &client, &|db| {
                        let keys: Vec<String> = {
                            db.map
                                .read()
                                .unwrap()
                                .iter()
                                .filter(|&(_k, v)| v.state != ValueStatus::Deleted)
                                .filter(|(key, _v)| key.starts_with("$$conflitcts"))
                                .map(|(key, _v)| format!("{}", key))
                                .collect()
                        };
                        let keys = keys.iter().fold(String::from(""), |current, acc| {
                            format!("{},{}", current, acc)
                        });
                        log::info!("Peding conflitcts on the server {}", keys);
                        match client
                            .sender
                            .clone()
                            .try_send(format_args!("conflitcts-list {}\n", keys).to_string())
                        {
                            Err(e) => {
                                log::warn!("Request::pending-ops sender.send Error: {}", e);
                            }
                            _ => (),
                        };
                        Response::Ok {}
                    });
                }
                "list-dbs" => {
                    let dbs_name_and_strategy = dbs.get_dbs_name_strategy().join("\n");
                    match client.sender.clone().try_send(
                        format_args!("dbs-list \n{}\n", dbs_name_and_strategy).to_string(),
                    ) {
                        Err(e) => log::warn!("Request::dbs-list sender.send Error: {}", e),
                        _ => (),
                    }
                }

                _ => log::info!("Invalid debug command"),
            };
            Response::Ok {}
        }),
        Request::Arbiter {} => apply_to_database(&dbs, &client, &|db| db.register_arbiter(&client)),
        Request::Resolve {
            opp_id,
            db_name,
            key,
            value,
            version,
        } => {
            log::info!("Processing resolve for {} to {} ", key, value);
            // Replica set or admin auth resolving
            if client.auth.load(Ordering::SeqCst) {
                apply_to_database_name(dbs, client, &db_name, &|db| {
                    if dbs.is_primary() {
                        db.resolve_conflit(
                            Change {
                                key: key.clone(),
                                value: value.clone(),
                                version,
                                opp_id,
                                resolve_conflict: true,
                            },
                            &dbs,
                        )
                    } else {
                        send_message_to_primary(
                            get_resolve_message(
                                opp_id,
                                db_name.to_string(),
                                key.clone(),
                                value.clone(),
                                version,
                            ),
                            dbs,
                        );
                        Response::Ok {}
                    }
                });
            } else {
                apply_to_database(&dbs, &client, &|db| {
                    if dbs.is_primary() {
                        db.resolve_conflit(
                            Change {
                                key: key.clone(),
                                value: value.clone(),
                                version,
                                opp_id,
                                resolve_conflict: true,
                            },
                            &dbs,
                        )
                    } else {
                        send_message_to_primary(
                            get_resolve_message(
                                opp_id,
                                db_name.to_string(),
                                key.clone(),
                                value.clone(),
                                version,
                            ),
                            dbs,
                        );
                        Response::Ok {}
                    }
                });
            };
            return Response::Ok {};
        }
        Request::ListCommands {} => apply_if_auth(&client.auth, &|| {
            let commands = Request::command_list();
            let commands = commands.iter().fold(String::from(""), |current, acc| {
                format!("{},{}", current, acc)
            });
            match client
                .sender
                .clone()
                .try_send(format_args!("commands-list {}\n", commands).to_string())
            {
                Err(e) => log::warn!("Request::commands-list sender.send Error: {}", e),
                _ => (),
            };
            Response::Ok {}
        }),
        Request::SetPermissions { user, kind, keys } => {
            apply_if_safe_access(&dbs, &client, &String::from("$$permission_"), &|_db| {
                let key = String::from(format!("$$permission_${}", user));
                let value = String::from(format!("{} {}", kind, keys.join(",")));
                let respose = set_key_value(key.to_string(), value.to_string(), -1, _db, &dbs);
                match respose {
                    Response::Set { .. } => {
                        if !dbs.is_primary() {
                            let db_name_state = _db.name.clone();
                            send_message_to_primary(
                                get_replicate_message(
                                    db_name_state.to_string(),
                                    key,
                                    value.to_string(),
                                    -1,
                                ),
                                dbs,
                            );
                        }
                        Response::Ok {}
                    }
                    _ => respose,
                }
            })
        }
    }
}

pub fn process_request(input: &str, dbs: &Arc<Databases>, client: &mut Client) -> Response {
    let input_to_log = clean_string_to_log(input, &dbs);
    log::debug!(
        "[{}] process_request got message '{}'. ",
        thread_id::get(),
        input_to_log
    );
    let db_name_state = client.selected_db_name();
    let start = Instant::now();
    let request = match Request::parse(String::from(input).trim_matches('\n')) {
        Ok(req) => req,
        Err(e) => return Response::Error { msg: e },
    };

    log::debug!(
        "[{}] process_request parsed message '{}'. ",
        thread_id::get(),
        input_to_log
    );

    let result = process_request_obj(&request, &dbs, client);

    let elapsed = start.elapsed();
    log::info!(
        "[{}] Server processed message '{}' in {:?}",
        thread_id::get(),
        input_to_log,
        elapsed
    );
    dbs.update_query_time_moving_avg(elapsed.as_millis());
    let replication_result = replicate_request(
        request,
        &db_name_state,
        result,
        &dbs.replication_sender.clone(),
    );
    replication_result
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::mpsc::{channel, Receiver, Sender};
    use std::collections::HashMap;

    fn create_default_args() -> (Receiver<String>, Arc<Databases>, Client) {
        let (sender1, _receiver): (Sender<String>, Receiver<String>) = channel(100);
        let (sender2, _receiver): (Sender<String>, Receiver<String>) = channel(100);
        let keys_map = HashMap::new();
        let dbs = Arc::new(Databases::new(
            String::from("user"),
            String::from("token"),
            String::from(""),
            sender1,
            sender2,
            keys_map,
            1 as u128,
            true,
        ));

        dbs.node_state
            .swap(ClusterRole::Primary as usize, Ordering::Relaxed);

        let (client, receiver) = Client::new_empty_and_receiver();

        return (receiver, dbs, client);
    }

    fn assert_received(receiver: &mut Receiver<String>, expected: &str) {
        match receiver.try_next() {
            Ok(Some(message)) => assert_eq!(message, expected),
            _ => assert!(false, "Receiver doesnt have any message"),
        };
    }

    fn assert_valid_request(request: Response) {
        assert_eq!(Response::Ok {}, request);
    }

    fn assert_invalid_request(request: Response) {
        match request {
            Response::Error { msg: _ } => assert!(true, "Request is invalid"),
            _ => assert!(false, "Request should be invalid"),
        };
    }

    #[test]
    fn should_auth_correctly() {
        let (mut receiver, dbs, mut client) = create_default_args();

        assert_eq!(client.auth.load(Ordering::SeqCst), false);

        process_request("auth user wrong_token", &dbs, &mut client);
        assert_eq!(client.auth.load(Ordering::SeqCst), false);
        assert_received(&mut receiver, "invalid auth\n");

        process_request("auth user token", &dbs, &mut client);
        assert_eq!(client.auth.load(Ordering::SeqCst), true);
        assert_received(&mut receiver, "valid auth\n");
    }

    #[test]
    fn should_return_only_not_deleted_keys() {
        let (mut _receiver, dbs, mut _client) = create_test_db();
        // New client connected without admin auth
        let (mut receiver, _, mut client) = create_default_args();
        process_request("use-db test test-1", &dbs, &mut client);
        process_request("set name jose", &dbs, &mut client);
        process_request("set name1 jose", &dbs, &mut client);
        process_request("keys", &dbs, &mut client);
        assert_received(&mut receiver, "keys ,$connections,name,name1\n");
        process_request("remove name1 jose", &dbs, &mut client);
        process_request("keys", &dbs, &mut client);
        assert_received(&mut receiver, "keys ,$connections,name\n");
    }

    #[test]
    fn should_return_secret_keys_if_admin_auth() {
        let (mut receiver, dbs, mut client) = create_test_db();
        process_request("set name jose", &dbs, &mut client);
        process_request("set name1 jose", &dbs, &mut client);
        process_request("keys", &dbs, &mut client);
        assert_received(&mut receiver, "keys ,$$token,$connections,name,name1\n");
        process_request("remove name1 jose", &dbs, &mut client);
        process_request("keys", &dbs, &mut client);
        assert_received(&mut receiver, "keys ,$$token,$connections,name\n");
    }

    #[test]
    fn should_return_keys_starting_with() {
        let (mut receiver, dbs, mut client) = create_test_db();
        process_request("set name jose", &dbs, &mut client);
        process_request("set name1 jose", &dbs, &mut client);
        process_request("keys name*", &dbs, &mut client);
        assert_received(&mut receiver, "keys ,name,name1\n");
    }

    #[test]
    fn should_return_keys_ending_with() {
        let (mut receiver, dbs, mut client) = create_test_db();
        process_request("set name jose", &dbs, &mut client);
        process_request("set name1 jose", &dbs, &mut client);
        process_request("keys *1", &dbs, &mut client);
        assert_received(&mut receiver, "keys ,name1\n");
    }

    #[test]
    fn should_return_keys_contains_with() {
        let (mut receiver, dbs, mut client) = create_test_db();
        process_request("set name jose", &dbs, &mut client);
        process_request("set name1 jose", &dbs, &mut client);
        process_request("keys a", &dbs, &mut client);
        assert_received(&mut receiver, "keys ,name,name1\n");
    }
    #[test]
    fn should_return_keys_contains_with_using_alias() {
        let (mut receiver, dbs, mut client) = create_test_db();
        process_request("set name jose", &dbs, &mut client);
        process_request("set name1 jose", &dbs, &mut client);
        process_request("ls a", &dbs, &mut client);
        assert_received(&mut receiver, "keys ,name,name1\n");
    }

    #[test]
    fn should_not_allow_to_remove_token_key() {
        let (mut receiver, dbs, mut client) = create_test_db();
        process_request("get $$token", &dbs, &mut client);
        assert_received(&mut receiver, "value test-1\n");
        let r = process_request("remove $$token", &dbs, &mut client);
        if let Response::Error { msg: e } = r {
            assert_eq!(e, "$$token key cannot be removed");
        } else {
            panic!("Should return error");
        }
    }

    #[test]
    fn should_not_allow_non_admins_to_remove_secure_keys() {
        let (mut receiver, dbs, mut client) = create_test_db();
        process_request("get $$token", &dbs, &mut client);
        process_request("set $$jose 1", &dbs, &mut client);
        assert_received(&mut receiver, "value test-1\n");
        client.auth.store(false, Ordering::Relaxed); // Unauth
        let r = process_request("remove $$jose", &dbs, &mut client);
        if let Response::Error { msg: e } = r {
            assert_eq!(e, "To read security keys you must auth as an admin!");
        } else {
            panic!("Should return error");
        }
    }

    #[test]
    fn should_not_allow_non_admins_to_write_secure_keys() {
        let (mut receiver, dbs, mut client) = create_test_db();
        process_request("get $$token", &dbs, &mut client);
        assert_received(&mut receiver, "value test-1\n");
        client.auth.store(false, Ordering::Relaxed); // Unauth
        let r = process_request("set $$token", &dbs, &mut client);
        if let Response::Error { msg: e } = r {
            assert_eq!(e, "To read security keys you must auth as an admin!");
        } else {
            panic!("Should return error");
        }
    }

    #[test]
    fn should_increment() {
        let (mut receiver, dbs, mut client) = create_test_db();
        process_request("increment some", &dbs, &mut client);
        process_request("get some", &dbs, &mut client);
        assert_received(&mut receiver, "value 1\n");
        process_request("increment some", &dbs, &mut client);
        process_request("get some", &dbs, &mut client);
        assert_received(&mut receiver, "value 2\n");
        process_request("increment some -1", &dbs, &mut client);
        process_request("get some", &dbs, &mut client);
        assert_received(&mut receiver, "value 1\n");

        process_request("increment some 2", &dbs, &mut client);
        process_request("get some", &dbs, &mut client);
        assert_received(&mut receiver, "value 3\n");
    }

    #[test]
    fn should_not_increment_private_key_if_not_admin() {
        let (mut receiver, dbs, mut client) = create_test_db();
        process_request("increment $$jose", &dbs, &mut client);
        process_request("get $$jose", &dbs, &mut client);
        assert_received(&mut receiver, "value 1\n");
        client.auth.store(false, Ordering::Relaxed); // Unauth
        let r = process_request("increment $$jose", &dbs, &mut client);
        if let Response::Error { msg: e } = r {
            assert_eq!(e, "To read security keys you must auth as an admin!");
        } else {
            panic!("Should return error");
        }
    }

    #[test]
    fn should_not_watch_private_key_if_not_admin() {
        let (mut _receiver, dbs, mut client) = create_test_db();
        process_request("increment $$jose", &dbs, &mut client);
        process_request("watch $$jose", &dbs, &mut client);
        client.auth.store(false, Ordering::Relaxed); // Unauth
        let r = process_request("watch $$jose", &dbs, &mut client);
        if let Response::Error { msg: e } = r {
            assert_eq!(e, "To read security keys you must auth as an admin!");
        } else {
            panic!("Should return error");
        }
    }

    fn create_test_db() -> (Receiver<String>, Arc<Databases>, Client) {
        let (mut receiver, dbs, mut client) = create_default_args();
        assert_eq!(client.auth.load(Ordering::SeqCst), false);
        process_request("auth user token", &dbs, &mut client);
        assert_received(&mut receiver, "valid auth\n");
        process_request("create-db test test-1", &dbs, &mut client);
        assert_received(&mut receiver, "create-db success\n");
        process_request("use-db test test-1", &dbs, &mut client);
        (receiver, dbs, client)
    }

    #[test]
    fn should_not_allow_non_admins_to_read_secure_keys() {
        let (mut receiver, dbs, mut client) = create_default_args();
        assert_eq!(client.auth.load(Ordering::SeqCst), false);
        process_request("auth user token", &dbs, &mut client);
        assert_received(&mut receiver, "valid auth\n");
        process_request("create-db test test-1", &dbs, &mut client);
        assert_received(&mut receiver, "create-db success\n");
        process_request("use-db test test-1", &dbs, &mut client);
        process_request("get $$token", &dbs, &mut client);
        assert_received(&mut receiver, "value test-1\n");
        client.auth.store(false, Ordering::Relaxed); // Unauth
        let r = process_request("get $$token", &dbs, &mut client);
        if let Response::Error { msg: e } = r {
            assert_eq!(e, "To read security keys you must auth as an admin!");
        } else {
            panic!("Should return error");
        }
    }

    #[test]
    fn should_create_db() {
        let (mut receiver, dbs, mut client) = create_default_args();
        client.auth.store(true, Ordering::Relaxed);
        assert_valid_request(process_request(
            "create-db my-db my-token",
            &dbs,
            &mut client,
        ));

        assert_received(&mut receiver, "create-db success\n");
        (*dbs.map.read().unwrap())
            .get("my-db")
            .expect("my-db should exists");
    }

    #[test]
    fn should_create_user_in_a_db() {
        let (mut receiver, dbs, mut client) = create_default_args();
        client.auth.store(true, Ordering::Relaxed);
        assert_valid_request(process_request(
            "create-db my-db my-token",
            &dbs,
            &mut client,
        ));
        assert_received(&mut receiver, "create-db success\n");

        process_request("use-db my-db my-token", &dbs, &mut client);
        assert_valid_request(process_request(
            "create-user my-user my-token",
            &dbs,
            &mut client,
        ));

        // No longer an admin
        client.auth.store(false, Ordering::Relaxed);
        assert_valid_request(process_request(
            "use my-db my-user my-token",
            &dbs,
            &mut client,
        ));
        process_request("set some value", &dbs, &mut client);
        process_request("get some", &dbs, &mut client);
        assert_received(&mut receiver, "value value\n");
    }

    #[test]
    fn should_not_create_db_if_already_exist() {
        let (_, dbs, mut client) = create_default_args();
        client.auth.store(true, Ordering::Relaxed);

        // @todo start dbs args with db already created, instead of relying on
        // the correct function of create-db command
        process_request("create-db my-db my-token", &dbs, &mut client);

        assert_invalid_request(process_request(
            "create-db my-db my-token",
            &dbs,
            &mut client,
        ));
    }

    #[test]
    fn should_list_commands_avaliable() {
        let (mut r, dbs, mut client) = create_default_args();
        client.auth.store(true, Ordering::Relaxed);
        process_request("list-commands", &dbs, &mut client);
        let result = r.try_next().unwrap().unwrap();
        assert!(result.contains("create-db"));
    }

    #[test]
    fn shoud_set_permission_to_user_correctly() {
        let (mut receiver, dbs, mut client) = create_default_args();
        client.auth.store(true, Ordering::Relaxed);
        assert_valid_request(process_request(
            "create-db my-db my-token",
            &dbs,
            &mut client,
        ));
        assert_received(&mut receiver, "create-db success\n");
        process_request("use-db my-db my-token", &dbs, &mut client);
        assert_valid_request(process_request(
            "create-user my-user my-token",
            &dbs,
            &mut client,
        ));
        // Connect to db as admin
        assert_valid_request(process_request("use my-db my-token", &dbs, &mut client));

        // Set permission
        assert_valid_request(process_request(
            "set-permissions my-user deny test*",
            &dbs,
            &mut client,
        ));

        // No longer an admin
        client.auth.store(false, Ordering::Relaxed);
        // Connect as user
        assert_valid_request(process_request(
            "use my-db my-user my-token",
            &dbs,
            &mut client,
        ));
        process_request("set some value", &dbs, &mut client);
        process_request("get some", &dbs, &mut client);
        assert_received(&mut receiver, "value value\n");

        process_request("set test-jose jose", &dbs, &mut client);
        assert_received(&mut receiver, "permission denied\n");
    }
}
