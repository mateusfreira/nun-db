use std::collections::HashMap;
use std::env;
use std::fs::OpenOptions;
use std::fs::{create_dir_all, read_dir, File};
use std::io::prelude::*;
use std::io::BufWriter;
use std::io::Read;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bo::*;

const SNAPSHOT_TIME: i64 = 3000; // 30 secounds
const FILE_NAME: &'static str = "-nun.data";
const META_FILE_NAME: &'static str = "-nun.madadata";
const DIR_NAME: &'static str = "dbs";

const KEYS_FILE: &'static str = "keys-nun.keys";

const OP_LOG_FILE: &'static str = "oplog-nun.op";
const OP_KEY_SIZE: usize = 8;
const OP_DB_ID_SIZE: usize = 8;
const OP_TIME_SIZE: usize = 8;
const OP_OP_SIZE: usize = 1;
const OP_RECORD_SIZE: usize = OP_TIME_SIZE + OP_DB_ID_SIZE + OP_KEY_SIZE + OP_OP_SIZE;

pub fn get_dir_name() -> String {
    match env::var_os("NUN_DBS_DIR") {
        Some(dir_name) => dir_name.into_string().unwrap(),
        None => DIR_NAME.to_string(),
    }
}

pub fn load_keys_map_from_disk() -> HashMap<String, u64> {
    let mut initial_db = HashMap::new();
    let db_file_name = get_keys_map_file_name();
    println!("Will read the keys {} from disk", db_file_name);
    if Path::new(&db_file_name).exists() {
        // May I should move this out of here
        println!("Will read from disck");
        let mut file = File::open(db_file_name).unwrap();
        initial_db = bincode::deserialize_from(&mut file).unwrap();
    }
    return initial_db;
}

pub fn write_keys_map_to_disk(keys: HashMap<String, u64>) {
    let db_file_name = get_keys_map_file_name();
    println!("Will read the keys {} from disk", db_file_name);

    let mut keys_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(db_file_name)
        .unwrap();
    bincode::serialize_into(&mut keys_file, &keys.clone()).unwrap();
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

pub fn load_db_metadata_from_disk_or_empty(name: String, dbs: &Arc<Databases>) -> DatabaseMataData {
    let db_file_name = meta_file_name_from_db_name(name.clone());
    println!("Will read the metadata {} from disk", db_file_name);
    if Path::new(&db_file_name).exists() {
        // May I should move this out of here
        let mut file = File::open(db_file_name).unwrap();
        let mut buffer = [0; 8];
        file.read(&mut buffer).unwrap();
        let id: usize = usize::from_le_bytes(buffer);

        DatabaseMataData::new(id)
    } else {
        DatabaseMataData::new(dbs.map.read().unwrap().len())
    }
}

fn load_one_db_from_disk(dbs: &Arc<Databases>, entry: std::io::Result<std::fs::DirEntry>) {
    if let Ok(entry) = entry {
        let full_name = entry.file_name().into_string().unwrap();
        if full_name.ends_with(FILE_NAME) {
            let db_name = db_name_from_file_name(full_name);
            let db_data = load_db_from_disck_or_empty(db_name.to_string());
            let meta = load_db_metadata_from_disk_or_empty(db_name.to_string(), dbs);
            dbs.add_database(
                &db_name.to_string(),
                Database::create_db_from_hash(db_name.to_string(), db_data, meta),
            );
        }
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

pub fn get_op_log_file_name() -> String {
    format!("{dir}/{sufix}", dir = get_dir_name(), sufix = OP_LOG_FILE)
}

pub fn get_keys_map_file_name() -> String {
    format!("{dir}/{sufix}", dir = get_dir_name(), sufix = KEYS_FILE)
}

pub fn meta_file_name_from_db_name(db_name: String) -> String {
    format!(
        "{dir}/{db_name}{sufix}",
        dir = get_dir_name(),
        db_name = db_name,
        sufix = META_FILE_NAME
    )
}

pub fn db_name_from_file_name(full_name: String) -> String {
    let partial_name = full_name.replace(FILE_NAME, "");
    let splited_name: Vec<&str> = partial_name.split("/").collect();
    let db_name = splited_name.last().unwrap();
    return db_name.to_string();
}

fn storage_data_disk(db: &Database, db_name: String) {
    let data = db.map.read().unwrap();
    // store data
    let mut file = File::create(file_name_from_db_name(db_name.to_string())).unwrap();
    bincode::serialize_into(&mut file, &data.clone()).unwrap();

    let mut meta_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(meta_file_name_from_db_name(db_name))
        .unwrap();
    meta_file.write(&db.metadata.id.to_le_bytes()).unwrap(); //8 bytes
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
            let mut dbs_to_snapshot = {
                let dbs = dbs.to_snapshot.write().unwrap();
                dbs
                //.clone()
            };
            dbs_to_snapshot.dedup();
            while let Some(database_name) = dbs_to_snapshot.pop() {
                println!("Will snapshot the database {}", database_name);
                let dbs = dbs.clone();
                let dbs_map = dbs.map.read().unwrap();
                let db_opt = dbs_map.get(&database_name);
                match db_opt {
                    Some(db) => {
                        storage_data_disk(db, database_name.clone());
                        if database_name == ADMIN_DB {
                            // if saving the admin db save the keys
                            let keys_map = {
                                let keys_map = dbs.keys_map.read().unwrap();
                                keys_map.clone()
                            };
                            println!("Will snapshot the keys {}", keys_map.len());
                            write_keys_map_to_disk(keys_map);
                        }
                    }
                    _ => println!("Database not found {}", database_name),
                }
            }
        })
    };
    rx.recv().unwrap(); // Thread will run for ever
}

pub fn get_log_file_append_mode() -> BufWriter<File> {
    BufWriter::with_capacity(
        OP_RECORD_SIZE,
        OpenOptions::new()
            .append(true)
            .create(true)
            .open(get_op_log_file_name())
            .unwrap(),
    )
}

pub fn get_log_file_read_mode() -> File {
    match OpenOptions::new()
        .read(true)
        .open(get_op_log_file_name())
    {
        Err(e) => {
            eprint!("{:?}", e);
            OpenOptions::new()
                .read(true)
                .create(true)
                .open(get_op_log_file_name())
                .unwrap()
        }
        Ok(f) => f,
    }
}

pub fn write_op_log(stream: &mut BufWriter<File>, db_id: u64, key: u64, opp: ReplicateOpp) {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let in_ms =
        since_the_epoch.as_secs() * 1000 + since_the_epoch.subsec_nanos() as u64 / 1_000_000;
    let opp_to_write = opp as u8;

    stream.write(&in_ms.to_le_bytes()).unwrap(); //8
    stream.write(&key.to_le_bytes()).unwrap(); // 8
    stream.write(&db_id.to_le_bytes()).unwrap(); // 8
    stream.write(&[opp_to_write]).unwrap(); // 1
}

pub fn read_operations_since(since: u64) -> HashMap<u64, ReplicateOpp> {
    let mut opps_since = HashMap::new();
    let mut f = get_log_file_read_mode();
    let total_size = f.metadata().unwrap().len();
    let size_as_u64 = OP_RECORD_SIZE as u64;
    let mut max = total_size;
    let min = 0;
    let mut seek_point = (total_size / size_as_u64 / 2) * size_as_u64;
    let records = total_size / size_as_u64;

    println!("Total size {}, Recoreds {} in opfile", total_size, records);
    let mut time_buffer = [0; OP_TIME_SIZE];
    let mut key_buffer = [0; OP_KEY_SIZE];
    let mut db_id_buffer = [0; OP_DB_ID_SIZE];
    let mut oop_buffer = [0; 1];
    f.seek(SeekFrom::Start(seek_point)).unwrap();
    let now = Instant::now();
    while let Ok(i) = f.read(&mut time_buffer) {
        //Read key from disk
        let possible_records = (max - min) / size_as_u64;
        let mut n = u64::from_le_bytes(time_buffer);
        println!("n: {} since{}", n, since);
        if n == since || n < since {
            let e = now.elapsed();
            while let Ok(byte_read) = f.read(&mut key_buffer) {
                if byte_read == 0 { break; }
                f.read(&mut db_id_buffer);
                f.read(&mut oop_buffer).unwrap();
                let key: u64 = u64::from_le_bytes(key_buffer);
                let db_id: u64 = u64::from_le_bytes(key_buffer);
                let opp = ReplicateOpp::from(oop_buffer[0]);
                opps_since.insert(key, opp);//Needs to be one by database

                if let Err(_) = f.read(&mut time_buffer) {//Next time
                    //To skip time key
                    break;
                }
                n = u64::from_le_bytes(time_buffer);
                println!("Here key:{} db:{} n:{} opp:{:?}", key, db_id, n, oop_buffer);

            }
            break;
        }

        if n > since {
            max = seek_point;
            //println!( "seach smaller {} in {:?} {} {}", n, now.elapsed(), seek_point, possible_records );
            let n_recors: u64 = ((seek_point - min) / 2) / size_as_u64;
            seek_point = seek_point - (n_recors * size_as_u64);
        }

        if possible_records <= 1 {
            println!(" Did not found {:?} {}, {}", now.elapsed(), max, seek_point);
            break;
        }

        if i != OP_TIME_SIZE as usize {
            println!(" End of the file{:?} ms", now.elapsed());
            break;
        }

        f.seek(SeekFrom::Start(seek_point)).unwrap(); // Repoint disk seek
    }
    opps_since
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_return_the_db_name() {
        assert_eq!(
            db_name_from_file_name(String::from("dbs/org-1-nun.data")),
            "org-1"
        );
    }

    #[test]
    fn should_return_the_correct_op_log_name() {
        assert_eq!(get_op_log_file_name(), "dbs/oplog-nun.op");
    }
}
