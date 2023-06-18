use crate::bo::*;
use crate::replication_ops::*;
use std::sync::Arc;

pub const CONFLICTS_KEY: &'static str = "$conflicts";
pub const RESOLVED_KEY_PREFIX: &'static str = "resolved";
pub const RESOLVE_KEY_PREFIX: &'static str = "resolve";

pub fn get_conflict_watch_key(change: &Change) -> String {
    String::from(format!(
        "{prefix}_{key}_{opp_id}",
        opp_id = change.opp_id,
        key = change.key,
        prefix = CONFLICTS_KEY
    ))
}
impl Database {
    pub fn list_conflicts_keys(&self, key: &String) -> Vec<String> {
        let pendding_conflict = self.list_keys(
            &String::from(format!("{prefix}_{key}", key = key, prefix = CONFLICTS_KEY)),
            true,
        );
        pendding_conflict
    }
    // Separate local conflict with replication conflict
    pub fn try_resolve_conflict_response(
        &self,
        conflitct_error: Response,
        dbs: &Arc<Databases>,
    ) -> Response {
        match conflitct_error {
            Response::VersionError {
                msg,
                key,
                old_version,
                version,
                old_value,
                change,
                db,
                state,
            } => {
                match self.metadata.consensus_strategy {
                    ConsensuStrategy::Newer => {
                        log::info!("Will resolve the conflitct in the key {} using Newer", key);
                        if change.opp_id > old_value.opp_id {
                            // New value is older
                            self.set_value(
                                &Change::new(key.clone(), change.value.clone(), old_version)
                                    .to_resolve_change(),
                            )
                        } else {
                            Response::Set {
                                key: key.clone(),
                                value: old_value.value.to_string(),
                            }
                        }
                    }
                    ConsensuStrategy::Arbiter => {
                        log::info!(
                            "Will resolve the conflitct in the key {} using Arbiter",
                            key
                        );
                        if !self.has_arbiter_connected() {
                            log::info!("Has no arbiter");
                            Response::Error {
                             msg: String::from("An conflitct happend and there is no arbiter client not connected"),
                            }
                        } else {
                            /*
                             * Sets the version to IN_CONFLICT_RESOLUTION_KEY_VERSION meaning all new changes
                             * to the same key must be also considered an conflict until the conflict is fully
                             * solved
                             */
                            self.set_value_version(
                                &change.key,
                                &old_value.value,
                                IN_CONFLICT_RESOLUTION_KEY_VERSION,
                                state,
                                old_value.value_disk_addr,
                                old_value.key_disk_addr,
                                old_value.opp_id,
                            );
                            /*
                             * Return the old value or  the key of the pending conflict.
                             * 1. @todo to make sure it is not resolved
                             * 2. @todo chaing multiple changes to test behavior to
                             *
                             */
                            let (old_value_or_conflict_key, change_version): (String, i32) =
                                if old_version == IN_CONFLICT_RESOLUTION_KEY_VERSION {
                                    let pendding_conflict = self.list_conflicts_keys(&change.key);
                                    log::debug!(
                                        "Conflict queue size for the key {} : {}",
                                        change.key,
                                        pendding_conflict.len()
                                    );
                                    (
                                        pendding_conflict.last().unwrap().to_string(),
                                        version + pendding_conflict.len() as i32,
                                    )
                                } else {
                                    (old_value.to_string(), old_version)
                                };
                            log::info!("Sending conflict to the arbiter {}", key);
                            let resolve_message = format!(
                                "{prefix} {opp_id} {db} {old_version} {key} {old_value} {value}",
                                prefix = RESOLVE_KEY_PREFIX,
                                opp_id = change.opp_id,
                                db = db,
                                old_version = change_version,
                                key = key,
                                old_value = old_value_or_conflict_key,
                                value = change.value
                            )
                            .to_string();
                            self.send_message_to_arbiter_client(resolve_message.clone());
                            let conflitct_key = get_conflict_watch_key(&change);
                            let conflict_register_change =
                                Change::new(conflitct_key.clone(), resolve_message, -1);
                            self.set_value(&conflict_register_change);

                            replicate_change(&conflict_register_change, &self, &dbs);
                            // Replicate
                            Response::Error {
                                msg: String::from(format!(
                                    "$$conflitct unresolved {}",
                                    conflitct_key.clone()
                                )),
                            }
                        }
                    }
                    ConsensuStrategy::None => {
                        log::info!("Will resolve the conflict in the key {} using None", key);
                        Response::VersionError {
                            msg,
                            key,
                            old_version,
                            version,
                            old_value,
                            change,
                            db,
                            state,
                        }
                    }
                }
            }
            r => r,
        }
    }

    fn send_message_to_arbiter_client(&self, message: String) {
        let watchers = self.watchers.map.read().unwrap();
        match watchers.get(&String::from(CONFLICTS_KEY)) {
            Some(senders) => {
                for sender in senders {
                    log::debug!("Sending to another client");
                    match sender.clone().try_send(message.clone()) {
                        Ok(_) => {
                            log::debug!("Send successfully!");
                        }
                        Err(_e) => {
                            log::warn!(
                                "Error to send message to arbiter, conflict may stay unresolved"
                            );
                        }
                    }
                }
                {}
            }
            _ => {}
        }
    }

    pub fn has_arbiter_connected(&self) -> bool {
        let watchers = self.watchers.map.read().unwrap();
        watchers.contains_key(CONFLICTS_KEY)
    }

    pub fn register_arbiter(&self, client: &Client) -> Response {
        let key = String::from(CONFLICTS_KEY);
        let response = self.watch_key(&key, &client.sender);
        let pendding_conflict = self.list_conflicts_keys(&String::from("")); // List all
        log::debug!(
            "Will send {} conflicts to arbiger to resolve",
            pendding_conflict.len()
        );
        for conflict in pendding_conflict {
            let conflict_command = self.get_value(conflict.clone()).unwrap().value;
            if conflict_command.starts_with(RESOLVED_KEY_PREFIX) {
                log::debug!("Conflict {} already resolved, remove the key", conflict);
                self.remove_value(conflict.clone());
            } else {
                self.send_message_to_arbiter_client(conflict_command);
            }
        }
        response
    }

    fn has_pendding_conflict(&self, key: &String) -> bool {
        let pendding_conflict = self.list_conflicts_keys(key);
        let values = pendding_conflict
            .iter()
            .map(|key| self.get_value(key.clone()).unwrap().value)
            .collect::<Vec<_>>();
        values
            .iter()
            .any(|value| !value.starts_with(RESOLVED_KEY_PREFIX))
    }

    pub fn resolve_conflit(&self, change: Change, dbs: &Arc<Databases>) -> Response {
        log::debug!(
            "resolving conflict change key: {} version : {}",
            change.key,
            change.version
        );
        // Will update the clients waiting for that conflict resolution update
        let conflict_register_change = Change::new(
            get_conflict_watch_key(&change),
            format!("{} {}", RESOLVED_KEY_PREFIX, change.value),
            -1,
        );
        self.set_value(&conflict_register_change);
        // Replicate conflict keys to other replicas
        replicate_change(&conflict_register_change, &self, &dbs);
        if self.has_pendding_conflict(&change.key) {
            let pendding_conflict = self.list_conflicts_keys(&change.key);
            let values = pendding_conflict
                .iter()
                .map(|key| self.get_value(key.clone()).unwrap().value)
                .collect::<Vec<_>>();
            log::debug!(
                "has_pendding_conflict conflict change key: {} version : {}, list: {}",
                change.key,
                change.version,
                values.join("-")
            );
            // If there is still open conflicts set version to conflicted
            self.set_value(
                &change
                    .to_resolve_change()
                    .to_different_version(IN_CONFLICT_RESOLUTION_KEY_VERSION),
            )
            //self.set_value(&change.to_resolve_change())
        } else {
            self.set_value(&change.to_resolve_change())
        }
    }
}

impl Value {
    pub fn is_in_conflict_resolution(&self) -> bool {
        self.version == IN_CONFLICT_RESOLUTION_KEY_VERSION
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::channel::mpsc::{channel, Receiver, Sender};
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;

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

    #[test]
    fn should_resolve_conflict() {
        let (_, dbs, _) = create_default_args();
        let key = String::from("some");
        let db = Database::new(
            String::from("some"),
            DatabaseMataData::new(1, ConsensuStrategy::Newer),
        );
        let change1 = Change::new(key.clone(), String::from("some1"), 0);
        let response = db.set_value(&change1);
        db.try_resolve_conflict_response(response, &dbs);
        let change2 = Change::new(String::from("some"), String::from("some2"), 0);
        let response = db.set_value(&change2);

        assert_eq!(
            db.try_resolve_conflict_response(response, &dbs),
            Response::Set {
                key: String::from("some"),
                value: String::from("some2")
            }
        );
    }

    #[test]
    fn should_resolve_conflict_with_newer() {
        let key = String::from("some");
        let db = Database::new(
            String::from("some"),
            DatabaseMataData::new(1, ConsensuStrategy::Newer),
        );
        let change1 = Change::new(key.clone(), String::from("some1"), 0); // m1
        let change2 = Change::new(String::from("some"), String::from("some2"), 0); //m2
        let (_, dbs, _) = create_default_args();
        db.set_value(&change2);
        assert_eq!(
            db.try_resolve_conflict_response(db.set_value(&change1), &dbs),
            Response::Set {
                key: String::from("some"),
                value: String::from("some2")
            }
        );
        assert_eq!(
            db.get_value(key.clone()).unwrap().value,
            String::from("some2")
        );
    }

    #[test]
    fn should_resolve_conflict_with_arbiter() {
        let (_, dbs, _) = create_default_args();
        let key = String::from("some");
        let db = Database::new(
            String::from("db_name"),
            DatabaseMataData::new(1, ConsensuStrategy::Arbiter),
        );

        let change1 = Change::new(key.clone(), String::from("some1"), 0); // m1
        let change2 = Change::new(String::from("some"), String::from("some2"), 0); //m2
        db.try_resolve_conflict_response(db.set_value(&change2), &dbs);
        assert_eq!(
            db.try_resolve_conflict_response(db.set_value(&change1), &dbs),
            Response::Error {
                msg: String::from(
                    "An conflitct happend and there is no arbiter client not connected"
                )
            }
        );
        let (client, mut receiver) = Client::new_empty_and_receiver();
        db.register_arbiter(&client);
        db.try_resolve_conflict_response(db.set_value(&change1), &dbs);
        let v = receiver.try_next().unwrap();
        assert_eq!(
            v.unwrap(),
            String::from(format!(
                "resolve {} db_name 1 some some2 some1",
                change1.opp_id
            )) // Not sure what to put here yet
        );
        let resolve_change = Change::new(String::from("some"), String::from("some1"), 2);
        db.resolve_conflit(resolve_change.clone(), &dbs);
        //process_request(&format!("resolved {} db_name some some1", change1.opp_id), &dbs, &mut client);
        assert_eq!(
            db.get_value(key.clone()).unwrap().value,
            String::from("some1")
        );
        // Once the conflict is resolved it should change the value to resolved value
        // clients will watch for that
        // queue of conflicts will also use resolve vs resolved to check conflicts statuses
        assert_eq!(
            db.get_value(get_conflict_watch_key(&resolve_change))
                .unwrap()
                .value,
            String::from("resolved some1")
        );
    }

    #[test]
    fn should_put_new_set_as_conflitct_if_key_is_already_conflicted_if_using_arbiter() {
        let (_, dbs, _) = create_default_args();
        //init_logger();
        let key = String::from("some");
        let db = Database::new(
            String::from("db_name"),
            DatabaseMataData::new(1, ConsensuStrategy::Arbiter),
        );

        let change1 = Change::new(key.clone(), String::from("some1"), 0); // m1
        let change2 = Change::new(String::from("some"), String::from("some2"), 0); //m2
                                                                                   //
        let change3 = Change::new(String::from("some"), String::from("some3"), 2); //m3
                                                                                   // Change all set_value to set_key_value
        db.set_value(&change2);
        let value = db.get_value(String::from("some")).unwrap();
        assert_eq!(value.version, 1);
        //set_key_value(key.clone(), value.clone(), -1, &db, &dbs);
        let (arbiter_client, mut receiver) = Client::new_empty_and_receiver();
        db.register_arbiter(&arbiter_client);
        let response = db.set_value(&change1);

        db.try_resolve_conflict_response(response, &dbs);
        // This is a valid change coming to a key that has a pending conflict
        let response = db.set_value(&change3);

        let value = db.get_value(String::from("some")).unwrap();
        assert_eq!(value.version, -2);

        db.try_resolve_conflict_response(response, &dbs);
        let v = receiver.try_next().unwrap();
        assert_eq!(
            v.unwrap(),
            String::from(format!(
                "resolve {} db_name 1 some some2 some1", // Conflict 1
                change1.opp_id
            ))
        );

        let resolve_change = Change {
            opp_id: change1.opp_id,
            key: String::from("some"),
            value: String::from("new_value"),
            version: 2,
            resolve_conflict: false,
        };
        let _resolved = db.resolve_conflit(resolve_change.clone(), &dbs);

        /*
         * Change one and 2  conflicted resolved to new_value
         * So next conflict resolution needs to be from new_value to some3
         * There should be some kind of chain
         */
        let v = receiver.try_next().unwrap();
        assert_eq!(
            v.unwrap(),
            String::from(format!(
                "resolve {} db_name 3 some {} some3", // Conflict 2
                change3.opp_id,
                get_conflict_watch_key(&change1),
            )) // Not sure what to put here yet
        );

        /* Even with the first resolution the version should still presents -2 since there is still
         * one pedding conflict!
         */
        let value = db.get_value(String::from("some")).unwrap();
        assert_eq!(value.version, IN_CONFLICT_RESOLUTION_KEY_VERSION);

        let resolve_change_2 = Change {
            opp_id: change3.opp_id,
            key: String::from("some"),
            value: String::from("new_value2"),
            version: 2,
            resolve_conflict: false,
        };
        let e = db.resolve_conflit(resolve_change_2.clone(), &dbs);
        println!("{:?}", e);
        let value = db.get_value(String::from("some")).unwrap();

        assert_eq!(value.version, 3);

        assert_eq!(
            db.get_value(key.clone()).unwrap().value,
            String::from("new_value2")
        );
        // Once the conflict is resolved it should change the value to resolved value
        // clients will watch for that
        // queue of conflicts will also use resolve vs resolved to check conflicts statuses
        assert_eq!(
            db.get_value(get_conflict_watch_key(&resolve_change))
                .unwrap()
                .value,
            String::from("resolved new_value")
        );

        assert_eq!(
            db.get_value(get_conflict_watch_key(&resolve_change_2))
                .unwrap()
                .value,
            String::from("resolved new_value2")
        );
    }

    #[test]
    fn should_not_allow_force_change_if_key_is_in_conflict_resolution() {
        let (_, dbs, _) = create_default_args();
        let key = String::from("some");
        let db = Database::new(
            String::from("db_name"),
            DatabaseMataData::new(1, ConsensuStrategy::Arbiter),
        );

        let change1 = Change::new(key.clone(), String::from("some1"), 0); // m1
        let change2 = Change::new(String::from("some"), String::from("some2"), 0); //m2
                                                                                   //
        let change3 = Change::new(String::from("some"), String::from("some3"), -1); //m3
        db.set_value(&change2);
        let (arbiter_client, mut receiver) = Client::new_empty_and_receiver();
        db.register_arbiter(&arbiter_client);
        db.try_resolve_conflict_response(db.set_value(&change1), &dbs);
        // This is a valid change coming to a key that has a pending conflict
        db.try_resolve_conflict_response(db.set_value(&change3), &dbs);
        let v = receiver.try_next().unwrap();
        assert_eq!(
            v.unwrap(),
            String::from(format!(
                "resolve {} db_name 1 some some2 some1", // Conflict 1
                change1.opp_id
            ))
        );

        let resolve_change = Change::new(String::from("some"), String::from("new_value"), 2);
        let _resolved = db.resolve_conflit(resolve_change.clone(), &dbs);

        /*
         * Change one and 2  conflicted resolved to new_value
         * So next conflict resolution needs to be from new_value to some3
         * There should be some kind of chain
         */
        let v = receiver.try_next().unwrap();
        assert_eq!(
            v.unwrap(),
            String::from(format!(
                "resolve {} db_name 0 some {} some3", // Conflict 2
                change3.opp_id,
                get_conflict_watch_key(&change1),
            )) // Not sure what to put here yet
        );

        let resolve_change_2 = Change::new(String::from("some"), String::from("new_value2"), 30000);
        let e = db.resolve_conflit(resolve_change_2.clone(), &dbs);
        println!("{:?}", e);

        assert_eq!(
            db.get_value(key.clone()).unwrap().value,
            String::from("new_value2")
        );
        // Once the conflict is resolved it should change the value to resolved value
        // clients will watch for that
        // queue of conflicts will also use resolve vs resolved to check conflicts statuses
        assert_eq!(
            db.get_value(get_conflict_watch_key(&resolve_change))
                .unwrap()
                .value,
            String::from("resolved new_value")
        );

        assert_eq!(
            db.get_value(get_conflict_watch_key(&resolve_change_2))
                .unwrap()
                .value,
            String::from("resolved new_value2")
        );
    }
}
