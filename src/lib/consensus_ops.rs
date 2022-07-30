use crate::bo::*;

impl Database {
    // Separate local conflitct with replication confilct
    pub fn resolve(&self, conflitct_error: Response) -> Response {
        match conflitct_error {
            Response::VersionError {
                msg: _,
                key,
                old_version,
                version,
                old_value,
                new_value,
                db,
            } => match self.metadata.consensus_strategy {
                ConsensuStrategy::Newer => {
                    if new_value.opp_id > old_value.opp_id  {
                        // New value is older
                        self.set_value(Change::new(
                            key.clone(),
                            new_value.value.clone(),
                            old_version,
                        ))
                    } else {
                        Response::Set {
                            key: key.clone(),
                            value: old_value.value.to_string(),
                        }
                    }
                }
                ConsensuStrategy::Arbiter => Response::Error {
                    msg: String::from("Todo"),
                },
            },
            r => r,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::mpsc::{channel, Receiver, Sender};
    use std::collections::HashMap;

    fn get_empty_dbs() -> Databases {
        let (sender, _): (Sender<String>, Receiver<String>) = channel(100);
        let (sender1, _): (Sender<String>, Receiver<String>) = channel(100);
        let keys_map = HashMap::new();
        Databases::new(
            String::from(""),
            String::from(""),
            String::from(""),
            sender,
            sender1,
            keys_map,
            1 as u128,
            true,
        )
    }

    #[test]
    fn should_resolve_conflict() {
        let key = String::from("some");
        let db = Database::new(String::from("some"), DatabaseMataData::new(1));
        let change1 = Change::new(key.clone(), String::from("some1"), 0);
        db.set_value(change1.clone());
        let change2 = Change::new(String::from("some"), String::from("some2"), 0);
        let e = db.set_value(change2.clone());
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
                new_value: change2,
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
        let db = Database::new(String::from("some"), DatabaseMataData::new(1));
        let change1 = Change::new(key.clone(), String::from("some1"), 0); // m1
        let change2 = Change::new(String::from("some"), String::from("some2"), 0); //m2
        db.set_value(change2);
        let e = db.set_value(change1.clone());
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
                new_value: change1,
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
}
