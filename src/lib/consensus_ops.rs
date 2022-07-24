use crate::bo::*;

impl Database {
    pub fn resolve(self, conflitct_error: Response) -> Response {
        Response::Error {
            msg: String::from("Todo"),
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
        let db = Database::new(String::from("some"), DatabaseMataData::new(1));
        db.set_value(String::from("some"), String::from("some1"), 0);

        let e = Response::VersionError {
            msg: String::from("some"),
            old_version: 1,
            version: 0,
            old_value: String::from("jose"),
            new_value: String::from("maria"),
            db: db.name.clone(),
        };
        assert_eq!(db.resolve(e), Response::Ok {});
    }
}
