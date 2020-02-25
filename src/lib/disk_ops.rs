use std::collections::HashMap;
use std::env;
use std::fs::{create_dir_all, read_dir, File};
use std::path::Path;
use std::sync::Arc;

use bo::*;
use db_ops::*;

const SNAPSHOT_TIME: i64 = 120000; // 2 minutes
const FILE_NAME: &'static str = "-nun.data";
const DIR_NAME: &'static str = "dbs";

pub fn get_dir_name() -> String {
    match env::var_os("NUN_DBS_DIR") {
        Some(dir_name) => dir_name.into_string().unwrap(),
        None => DIR_NAME.to_string(),
    }
}

pub fn load_db_from_disck_or_empty(name: String) -> HashMap<String, String> {
    let mut initial_db = HashMap::new();
    let db_file_name = file_name_from_db_name(name.clone());
    println!("Will read the database {} from disk", db_file_name);
    if Path::new(&db_file_name).exists() {
        // May I should move this out of here
        let mut file = File::open(db_file_name).unwrap();
        initial_db = bincode::deserialize_from(&mut file).unwrap();
    }
    return initial_db;
}

fn load_one_db_from_disk(dbs: &Arc<Databases>, entry: std::io::Result<std::fs::DirEntry>) {
    let mut dbs = dbs.map.lock().unwrap();
    if let Ok(entry) = entry {
        let full_name = entry.file_name().into_string().unwrap();
        let db_name = db_name_from_file_name(full_nam);
        let db_data = load_db_from_disck_or_empty(db_name.to_string());
        dbs.insert(
            db_name.to_string(),
            create_db_from_hash(db_name.to_string(), db_data),
        );
    }
}
fn load_all_dbs_from_disk(dbs: &Arc<Databases>) {
    if let Ok(entries) = read_dir(get_dir_name()) {
        for entry in entries {
            load_one_db_from_disk(dbs, entry);
        }
    }
}
// send the given database to the disc
pub fn file_name_from_db_name(db_name: String) -> String {
    format!(
        "{dir}/{db_name}{sufix}",
        dir = get_dir_name(),
        db_name = db_name,
        sufix = FILE_NAME
    )
}

pub fn db_name_from_file_name(full_name: String) -> String {
        let partial_name = full_name.replace(FILE_NAME, "");
        let splited_name: Vec<&str> = partial_name.split("/").collect();
        let db_name = splited_name.last().unwrap();
        return db_name.to_string();
}

fn storage_data_disk(db: &Database, db_name: String) {
    let db = db.map.lock().unwrap();
    let mut file = File::create(file_name_from_db_name(db_name)).unwrap();
    bincode::serialize_into(&mut file, &db.clone()).unwrap();
}

// calls storage_data_disk each $SNAPSHOT_TIME seconds
pub fn start_snap_shot_timer(timer: timer::Timer, dbs: Arc<Databases>) {
    println!("Will start_snap_shot_timer");
    load_all_dbs_from_disk(&dbs);
    match create_dir_all(get_dir_name()) {
        Ok(_) => {}
        Err(e) => {
            println!("Error creating the data dirs {}", e);
            panic!("Error creating the data dirs");
        }
    };
    let (_tx, rx): (
        std::sync::mpsc::Sender<String>,
        std::sync::mpsc::Receiver<String>,
    ) = std::sync::mpsc::channel(); // Visit this again
    let _guard = {
        timer.schedule_repeating(chrono::Duration::milliseconds(SNAPSHOT_TIME), move || {
            let dbs = dbs.map.lock().unwrap();
            for (database_name, db) in dbs.iter() {
                println!("Will snapshot the database {}", database_name);
                storage_data_disk(db, database_name.clone());
            }
        })
    };
    rx.recv().unwrap(); // Thread will run for ever
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_return_the_db_name() {
        assert_eq!(db_name_from_file_name(String::from("dbs/org-1-nun.data")), "org-1");
    }

}
