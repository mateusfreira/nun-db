use crate::bo::*;

pub const CONFLICTS_KEY: &'static str = "$$conflicts";

pub fn get_conflict_watch_key(change: &Change) -> String {
    String::from(format!(
        "{key}_{opp_id}",
        opp_id = change.opp_id,
        key = CONFLICTS_KEY
    ))
}
impl Database {
    // Separate local conflitct with replication confilct
    pub fn resolve(&self, conflitct_error: Response) -> Response {
        match conflitct_error {
            Response::VersionError {
                msg,
                key,
                old_version,
                version,
                old_value,
                change,
                db,
            } => match self.metadata.consensus_strategy {
                ConsensuStrategy::Newer => {
                    log::info!("Will resolve the conflitct in the key {} using Newer", key);
                    if change.opp_id > old_value.opp_id {
                        // New value is older
                        self.set_value(&Change::new(key.clone(), change.value.clone(), old_version))
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
                            msg: String::from("An conflitct happend and there is no arbiter client not connected!"),
                        }
                    } else {
                        log::info!("Sending conflict to the arbiter {}", key);
                        // Need may fail to send
                        // May fail to reply to the clietn
                        // May disconect beffore getting a response
                        // May be a good ideia to treat at the client
                        let resolve_message = format!(
                            "resolve {opp_id} {db} {old_version} {key} {old_value} {value}",
                            opp_id = change.opp_id,
                            db = db,
                            old_version = old_version,
                            key = key,
                            old_value = old_value,
                            value = change.value
                        ).to_string();
                        self.send_message_to_arbiter_client(resolve_message.clone());
                        let conflitct_key = get_conflict_watch_key(&change);
                        self.set_value(&Change::new(
                            conflitct_key.clone(),
                            resolve_message,
                            -1,
                        ));
                        Response::Error {
                            msg: String::from(format!(
                                "$$conflitct unresolved {}",
                                conflitct_key.clone()
                            )),
                        }
                    }
                }
                ConsensuStrategy::None => Response::VersionError {
                    msg,
                    key,
                    old_version,
                    version,
                    old_value,
                    change,
                    db,
                },
            },
            r => r,
        }
    }

    fn send_message_to_arbiter_client(&self, message: String) {
        let watchers = self.watchers.map.read().unwrap();
        match watchers.get(&String::from(CONFLICTS_KEY)) {
            Some(senders) => {
                for sender in senders {
                    log::debug!("Sending to another client");
                    sender.clone().try_send(message.clone()).unwrap();
                }
                {}
            }
            _ => {}
        }
    }

    pub fn is_arbitered(&self) -> bool {
        self.metadata.consensus_strategy == ConsensuStrategy::Arbiter
    }
    pub fn has_arbiter_connected(&self) -> bool {
        let watchers = self.watchers.map.read().unwrap();
        watchers.contains_key(CONFLICTS_KEY)
    }

    pub fn register_arbiter(&self, client: &Client) -> Response {
        let key = String::from(CONFLICTS_KEY);
        self.watch_key(&key, &client.sender)
    }

    pub fn resolve_conflit(&self, change: Change) -> Response {
        // Will update the clients waiting for that conflict resolution update
        self.set_value(&Change::new(
            get_conflict_watch_key(&change),
            change.value.clone(),
            -1,
        ));
        self.set_value(&change)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_resolve_conflict() {
        let key = String::from("some");
        let db = Database::new(
            String::from("some"),
            DatabaseMataData::new(1, ConsensuStrategy::Newer),
        );
        let change1 = Change::new(key.clone(), String::from("some1"), 0);
        db.set_value(&change1);
        let change2 = Change::new(String::from("some"), String::from("some2"), 0);
        assert_eq!(
            db.set_value(&change2),
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
        db.set_value(&change2);
        assert_eq!(
            db.set_value(&change1),
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
        let key = String::from("some");
        let db = Database::new(
            String::from("db_name"),
            DatabaseMataData::new(1, ConsensuStrategy::Arbiter),
        );

        let change1 = Change::new(key.clone(), String::from("some1"), 0); // m1
        let change2 = Change::new(String::from("some"), String::from("some2"), 0); //m2
        db.set_value(&change2);
        assert_eq!(
            db.set_value(&change1),
            Response::Error {
                msg: String::from(
                    "An conflitct happend and there is no arbiter client not connected!"
                )
            }
        );
        let (client, mut receiver) = Client::new_empty_and_receiver();
        db.register_arbiter(&client);
        let e = db.set_value(&change1);
        db.resolve(e);
        let v = receiver.try_next().unwrap();
        assert_eq!(
            v.unwrap(),
            String::from(format!(
                "resolve {} db_name 1 some some2 some1",
                change1.opp_id
            )) // Not sure what to put here yet
        );
        let resolve_change = Change::new(String::from("some"), String::from("some1"), 2);
        db.resolve_conflit(resolve_change.clone());
        //process_request(&format!("resolved {} db_name some some1", change1.opp_id), &dbs, &mut client);
        assert_eq!(
            db.get_value(key.clone()).unwrap().value,
            String::from("some1")
        );
        // Once the conflict is resolved it should change the value to resolved value
        // clients will watch for that
        // queue of conflicts will also use resolve vs resolved to check conflicts statuses
        assert_eq!(
            db.get_value(get_conflict_watch_key(&resolve_change)).unwrap().value,
            String::from("resolved some1")
        );
    }
}
