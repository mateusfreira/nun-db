use crate::bo::*;

pub const CONFLICTS_KEY: &'static str = "$$conflicts";

impl Database {
    // Separate local conflitct with replication confilct
    pub fn resolve(&self, conflitct_error: Response) -> Response {
        match conflitct_error {
            Response::VersionError {
                msg: _,
                key,
                old_version,
                version: _,
                old_value,
                change,
                db,
            } => match self.metadata.consensus_strategy {
                ConsensuStrategy::Newer => {
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
                    if !self.has_arbiter_connected() {
                        Response::Error {
                            msg: String::from("Arbiter client not connected!"),
                        }
                    } else {
                        // Need thread error to send
                        self.send_message_to_arbiter_client(String::from(format!("resolve {opp_id} {db} {old_version} {key} {old_value} {value}", opp_id = change.opp_id,db = db, old_version = old_version, key = key, old_value = old_value, value = change.value)));
                        Response::Error {
                            msg: String::from("Todo"),
                        }
                    }
                }
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
    pub fn has_arbiter_connected(&self) -> bool {
        let watchers = self.watchers.map.read().unwrap();
        watchers.contains_key(CONFLICTS_KEY)
    }

    pub fn register_arbiter(&self, client: &Client) -> Response {
        let key = String::from(CONFLICTS_KEY);
        self.watch_key(&key, &client.sender)
    }

    pub fn resolve_conflit(&self, change: Change) -> Response {
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
        let e = db.set_value(&change2);
        assert_eq!(
            e,
            Response::VersionError {
                msg: String::from("Invalid version!"),
                old_version: 1,
                version: 0,
                key: key.clone(),
                old_value: Value {
                    value: String::from("some1"),
                    version: 22,
                    opp_id: change1.opp_id,
                    state: ValueStatus::New,
                    value_disk_addr: 0,
                    key_disk_addr: 0
                },
                change: change2,
                db: String::from("some")
            }
        );
        assert_eq!(
            db.resolve(e),
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
    fn should_resolve_conflict_with_newer() {
        let key = String::from("some");
        let db = Database::new(
            String::from("some"),
            DatabaseMataData::new(1, ConsensuStrategy::Newer),
        );
        let change1 = Change::new(key.clone(), String::from("some1"), 0); // m1
        let change2 = Change::new(String::from("some"), String::from("some2"), 0); //m2
        db.set_value(&change2);
        let e = db.set_value(&change1);
        assert_eq!(
            e,
            Response::VersionError {
                msg: String::from("Invalid version!"),
                old_version: 1,
                version: 0,
                key: key.clone(),
                old_value: Value {
                    value: String::from("some2"),
                    version: 1,
                    opp_id: change1.opp_id,
                    state: ValueStatus::New,
                    value_disk_addr: 0,
                    key_disk_addr: 0
                },
                change: change1,
                db: String::from("some")
            }
        );
        assert_eq!(
            db.resolve(e),
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
        let e = db.set_value(&change1);
        assert_eq!(
            e,
            Response::VersionError {
                msg: String::from("Invalid version!"),
                old_version: 1,
                version: 0,
                key: key.clone(),
                old_value: Value {
                    value: String::from("some2"),
                    version: 1,
                    opp_id: change1.opp_id,
                    state: ValueStatus::New,
                    value_disk_addr: 0,
                    key_disk_addr: 0
                },
                change: change1.clone(),
                db: String::from("db_name")
            }
        );
        assert_eq!(
            db.resolve(e),
            Response::Error {
                msg: String::from("Arbiter client not connected!")
            }
        );
        let (client, mut receiver) = Client::new_empty_and_receiver();
        db.register_arbiter(&client);
        let e = db.set_value(&change1);
        db.resolve(e);
        let v = receiver.try_next().unwrap();
        assert_eq!(
            v.unwrap(),
            String::from(format!("resolve {} db_name 1 some some2 some1", change1.opp_id)) // Not sure what to put here yet
        );
        db.resolve_conflit(Change::new(String::from("some"), String::from("some1"), 2));
        //process_request(&format!("resolved {} db_name some some1", change1.opp_id), &dbs, &mut client);
        assert_eq!(
            db.get_value(key.clone()).unwrap().value,
            String::from("some1")
        );
        assert_eq!(
            db.get_value(key.clone()).unwrap().value,
            String::from("some1")
        );
    }
}
