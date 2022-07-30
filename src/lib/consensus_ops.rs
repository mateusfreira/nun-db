use crate::bo::*;

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
                new_value,
                db,
            } => match self.metadata.consensus_strategy {
                ConsensuStrategy::Newer => {
                    self.set_value(key.clone(), new_value.clone(), old_version)
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
        db.set_value(key.clone(), String::from("some1"), 0);
        let e = db.set_value(String::from("some"), String::from("some2"), 0);
        assert_eq!(
            e,
            Response::VersionError {
                msg: String::from("Invalid version!"),
                old_version: 1,
                version: 0,
                key: key.clone(),
                old_value: String::from("some1"),
                new_value: String::from("some2"),
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
