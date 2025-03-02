use crate::bo::{Database, Value, ValueStatus};

pub fn get_keys_to_update(db: &Database, reclame_space: bool) -> Vec<(String, Value)> {
    let mut keys_to_update = vec![];
    {
        let data = db.map.read().expect("Error getting the db.map.read");
        data.iter()
            .filter(|&(_k, v)| v.state != ValueStatus::Ok || reclame_space)
            .for_each(|(k, v)| keys_to_update.push((k.clone(), v.clone())))
    };
    // Release the locker
    keys_to_update
}
