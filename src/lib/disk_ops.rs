use log;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::fs::{create_dir_all, read_dir, File};
use std::io::prelude::*;
use std::io::BufWriter;
use std::io::Error;
use std::io::Read;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use crate::bo::*;

const SNAPSHOT_TIME: i64 = 3000; // 30 secounds
const FILE_NAME: &'static str = "-nun.data";
const META_FILE_NAME: &'static str = "-nun.madadata";
const DIR_NAME: &'static str = "dbs";

const KEYS_FILE: &'static str = "keys-nun.keys";

const OP_LOG_FILE: &'static str = "oplog-nun.op";
const INVALIDATE_OP_LOG_FILE: &'static str = "is-oplog.valid";

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
    log::debug!("Will read the keys {} from disk", db_file_name);
    if Path::new(&db_file_name).exists() {
        // May I should move this out of here
        log::debug!("Will read from disck");
        let mut file = File::open(db_file_name).unwrap();
        initial_db = bincode::deserialize_from(&mut file).unwrap();
    }
    return initial_db;
}

fn remove_invalidate_oplog_file() {
    let file_name = get_invalidate_file_name();
    log::debug!("Will delete {}", file_name);
    if Path::new(&file_name).exists() {
        fs::remove_file(file_name).unwrap();
    }
}

// @todo speed up saving
pub fn write_keys_map_to_disk(keys: HashMap<String, u64>) {
    let db_file_name = get_keys_map_file_name();
    log::debug!("Will write the keys {} from disk", db_file_name);

    let mut keys_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(db_file_name)
        .unwrap();
    bincode::serialize_into(&mut keys_file, &keys.clone()).unwrap();
}

pub fn load_db_from_disck_or_empty(name: String) -> HashMap<String, String> {
    let mut initial_db = HashMap::new();
    let db_file_name = file_name_from_db_name(&name);
    log::debug!("Will read the database {} from disk", db_file_name);
    if Path::new(&db_file_name).exists() {
        // May I should move this out of here
        let mut file = File::open(db_file_name).unwrap();
        initial_db = bincode::deserialize_from(&mut file).unwrap();
    }
    return initial_db;
}

pub fn load_db_metadata_from_disk_or_empty(name: String, dbs: &Arc<Databases>) -> DatabaseMataData {
    let db_file_name = meta_file_name_from_db_name(name.clone());
    log::debug!("Will read the metadata {} from disk", db_file_name);
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
            let db = create_db_from_file_name(&full_name, &dbs);
            let db_name = db_name_from_file_name(&full_name);
            dbs.add_database(&db_name.to_string(), db);
        }
    }
}

fn create_db_from_file_name(full_name: &String, dbs: &Arc<Databases>) -> Database {
    let db_name = db_name_from_file_name(&full_name);
    let db_data = load_db_from_disck_or_empty(db_name.to_string());
    let meta = load_db_metadata_from_disk_or_empty(db_name.to_string(), dbs);
    Database::create_db_from_hash(db_name.to_string(), db_data, meta)
}

pub fn load_all_dbs_from_disk(dbs: &Arc<Databases>) {
    log::debug!("Will load dbs from disck");
    match create_dir_all(get_dir_name()) {
        Ok(_) => {}
        Err(e) => {
            log::error!("Error creating the data dirs {}", e);
            panic!("Error creating the data dirs");
        }
    };
    if let Ok(entries) = read_dir(get_dir_name()) {
        for entry in entries {
            load_one_db_from_disk(dbs, entry);
        }
    }
}
// send the given database to the disc
pub fn file_name_from_db_name(db_name: &String) -> String {
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

fn get_invalidate_file_name() -> String {
    format!(
        "{dir}/{sufix}",
        dir = get_dir_name(),
        sufix = INVALIDATE_OP_LOG_FILE
    )
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

pub fn db_name_from_file_name(full_name: &String) -> String {
    let partial_name = full_name.replace(FILE_NAME, "");
    let splited_name: Vec<&str> = partial_name.split("/").collect();
    let db_name = splited_name.last().unwrap();
    return db_name.to_string();
}

fn storage_data_disk(db: &Database, db_name: String) {
    let data = db.to_string_hash();
    // store data
    let mut file = File::create(file_name_from_db_name(&db_name)).unwrap();
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
    log::info!("Will start_snap_shot_timer");
    let (_tx, rx): (
        std::sync::mpsc::Sender<String>,
        std::sync::mpsc::Receiver<String>,
    ) = std::sync::mpsc::channel(); // Visit this again
    let _guard = {
        timer.schedule_repeating(chrono::Duration::milliseconds(SNAPSHOT_TIME), move || {
            snapshot_all_pendding_dbs(&dbs);
        })
    };
    rx.recv().unwrap(); // Thread will run for ever
}

pub fn snapshot_all_pendding_dbs(dbs: &Arc<Databases>) {
    let queue_len = { dbs.to_snapshot.read().unwrap().len() };
    if queue_len > 0 {
        snapshot_keys(&dbs);
        let mut dbs_to_snapshot = {
            let dbs = dbs.to_snapshot.write().unwrap();
            dbs
        };
        dbs_to_snapshot.dedup();
        while let Some(database_name) = dbs_to_snapshot.pop() {
            log::debug!("Will snapshot the database {}", database_name);
            let dbs = dbs.clone();
            let dbs_map = dbs.map.read().unwrap();
            let db_opt = dbs_map.get(&database_name);
            if let Some(db) = db_opt {
                storage_data_disk(db, database_name.clone());
            } else {
                log::warn!("Database not found {}", database_name)
            }
        }
    }
}

pub fn snapshot_keys(dbs: &Arc<Databases>) {
    if !dbs.is_oplog_valid.load(Ordering::Relaxed) {
        let keys_map = {
            let keys_map = dbs.keys_map.read().unwrap();
            keys_map.clone()
        };
        log::debug!("Will snapshot the keys {}", keys_map.len());
        write_keys_map_to_disk(keys_map);
        mark_op_log_as_valid(dbs).unwrap();
    } else {
        log::debug!("keys already save, not saving keys file! Metadata already saved!")
    }
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

fn remove_op_log_file() {
    let file_name = get_op_log_file_name();
    if Path::new(&file_name).exists() {
        match fs::remove_file(file_name.clone()) {
            Err(e) => log::error!("Could not delete the {}, {}", file_name, e),
            _ => (),
        }; //clean file
    }
}

pub fn clean_op_log_metadata_files() {
    remove_invalidate_oplog_file();
    remove_op_log_file();
}

pub fn get_log_file_read_mode() -> File {
    match OpenOptions::new().read(true).open(get_op_log_file_name()) {
        Err(e) => {
            log::error!("{:?}", e);
            {
                // Force create the fail
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(get_op_log_file_name())
                    .unwrap();
            } // For createing the op log file, I don't return it here to avoind read processes holding the file in write more for too long
            OpenOptions::new()
                .read(true)
                .open(get_op_log_file_name())
                .unwrap()
        }
        Ok(f) => f,
    }
}

pub fn write_op_log(stream: &mut BufWriter<File>, db_id: u64, key: u64, opp: ReplicateOpp) -> u64 {
    let opp_id: u64 = Databases::next_op_log_id();
    let opp_to_write = opp as u8;

    stream.write(&opp_id.to_le_bytes()).unwrap(); //8
    stream.write(&key.to_le_bytes()).unwrap(); // 8
    stream.write(&db_id.to_le_bytes()).unwrap(); // 8
    stream.write(&[opp_to_write]).unwrap(); // 1
    stream.flush().unwrap();
    log::debug!("will write :  db: {db}", db = db_id);
    opp_id
}

pub fn last_op_time() -> u64 {
    let mut f = get_log_file_read_mode();
    let total_size = f.metadata().unwrap().len();
    let size_as_u64 = OP_RECORD_SIZE as u64;
    // if the file is empty return 0 to avoid  attempt to subtract with overflow error
    if total_size < size_as_u64 {
        return 0 as u64;
    }
    let last_record_position = total_size - size_as_u64;
    let mut time_buffer = [0; OP_TIME_SIZE];
    f.seek(SeekFrom::Start(last_record_position)).unwrap();
    if let Ok(_) = f.read(&mut time_buffer) {
        u64::from_le_bytes(time_buffer)
    } else {
        0
    }
}

/// Returns operations log file size and count
pub fn get_op_log_size() -> (u64, u64) {
    let f = get_log_file_read_mode();
    let size: u64 = match f.metadata() {
        Ok(f) => f.len(),
        Err(message) => {
            log::debug!("Faile to get the op file size, returning 0, {}", message);
            0
        }
    };
    (size, size / OP_RECORD_SIZE as u64)
}

pub fn read_operations_since(since: u64) -> HashMap<String, OpLogRecord> {
    let mut opps_since = HashMap::new();
    let mut f = get_log_file_read_mode();
    let total_size = f.metadata().unwrap().len();
    let size_as_u64 = OP_RECORD_SIZE as u64;
    let mut max = total_size;
    let mut min = 0;
    let mut seek_point = (total_size / size_as_u64 / 2) * size_as_u64;
    let mut time_buffer = [0; OP_TIME_SIZE];
    let mut key_buffer = [0; OP_KEY_SIZE];
    let mut db_id_buffer = [0; OP_DB_ID_SIZE];
    let mut oop_buffer = [0; 1];
    f.seek(SeekFrom::Start(seek_point)).unwrap();
    let now = Instant::now();
    let mut opp_count: u64 = 0;
    while let Ok(i) = f.read(&mut time_buffer) {
        //Read key from disk
        let possible_records = (max - min) / size_as_u64;
        let mut opp_time = u64::from_le_bytes(time_buffer);
        let read_all = possible_records == 1 && seek_point == size_as_u64;
        let no_more_smaller = possible_records <= 1 && (opp_time > since);
        if opp_time == since || no_more_smaller || read_all {
            log::debug!(
                "found! {} {} {} {}",
                possible_records,
                seek_point,
                total_size,
                no_more_smaller
            );
            while let Ok(byte_read) = f.read(&mut key_buffer) {
                if byte_read == 0 {
                    break;
                }
                f.read(&mut db_id_buffer).unwrap();
                f.read(&mut oop_buffer).unwrap();
                let key_id: u64 = u64::from_le_bytes(key_buffer);
                let db_id: u64 = u64::from_le_bytes(db_id_buffer);
                let opp = ReplicateOpp::from(oop_buffer[0]);
                opp_count = opp_count + 1; //opps_since.len() don't work
                let op_log = OpLogRecord::new(db_id, key_id, opp_time, opp_count, opp);
                opps_since.insert(op_log.to_key(), op_log); //Needs to be one by database

                if let Err(_) = f.read(&mut time_buffer) {
                    //Next time
                    //To skip time value
                    break;
                }
                opp_time = u64::from_le_bytes(time_buffer);
            }
            break;
        }

        if opp_time < since {
            min = seek_point;
            let n_recors: u64 = std::cmp::max(
                ((std::cmp::max(max, seek_point) - seek_point) / 2) / size_as_u64,
                1,
            );
            log::debug!(
                "search bigger {} in {:?} {} {} {}",
                opp_time,
                now.elapsed(),
                seek_point,
                possible_records,
                n_recors
            );
            seek_point = (n_recors * size_as_u64) + seek_point;
        }

        if opp_time > since {
            max = seek_point;
            log::debug!(
                "seach smaller op: {} since: {} in {:?} {} possible_records: {}, no_more_smaller: {}",
                opp_time,
                since,
                now.elapsed(),
                seek_point,
                possible_records,
                no_more_smaller
            );
            let n_recors: u64 = std::cmp::max(((seek_point - min) / 2) / size_as_u64, 1);
            seek_point = seek_point - (n_recors * size_as_u64);
        }

        if possible_records <= 1 {
            log::debug!(
                " Did not found {:?} {}, {} any record!",
                now.elapsed(),
                max,
                seek_point
            );
            //break;
        }

        if i != OP_TIME_SIZE as usize {
            log::debug!(" End of the file{:?} ms", now.elapsed());
            break;
        }

        f.seek(SeekFrom::Start(seek_point)).unwrap(); // Repoint disk seek
    }
    opps_since
}

pub fn get_invalidate_file_write_mode() -> BufWriter<File> {
    BufWriter::with_capacity(
        1,
        OpenOptions::new()
            .create(true)
            .write(true)
            .open(get_invalidate_file_name())
            .unwrap(),
    )
}
pub fn get_invalidate_file_read_mode() -> File {
    match OpenOptions::new()
        .read(true)
        .open(get_invalidate_file_name())
    {
        Err(e) => {
            log::debug!("{:?} will create the file", e);
            get_invalidate_file_write_mode(); // called here to create the file
            OpenOptions::new()
                .read(true)
                .open(get_invalidate_file_name())
                .unwrap()
        }
        Ok(f) => f,
    }
}

/// checks if the oplog files are valid
/// this is controlled by a file with a single byte, 0 means invalid, 1 valid
pub fn is_oplog_valid() -> bool {
    let mut f = get_invalidate_file_read_mode();
    let mut buffer = [1; 1];
    f.seek(SeekFrom::Start(0)).unwrap();
    f.read(&mut buffer).unwrap();
    buffer[0] == 1
}

pub fn invalidate_oplog(
    stream: &mut BufWriter<File>,
    dbs: &Arc<Databases>,
) -> Result<usize, Error> {
    let is_oplog_valid = { dbs.is_oplog_valid.load(Ordering::SeqCst) };
    if is_oplog_valid {
        dbs.is_oplog_valid.swap(false, Ordering::Relaxed);
        log::debug!("invalidating oplog");
        stream.seek(SeekFrom::Start(0)).unwrap();
        return stream.write(&[0]);
    } else {
        log::debug!("No need to invalidating oplog as it is already valid");
        Result::Ok(0)
    }
}

fn mark_op_log_as_valid(dbs: &Arc<Databases>) -> Result<usize, Error> {
    dbs.is_oplog_valid.swap(true, Ordering::Relaxed);
    log::debug!("marking op log as valid");
    let mut file_writer = get_invalidate_file_write_mode();
    file_writer.seek(SeekFrom::Start(0)).unwrap();
    file_writer.write(&[1])
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::mpsc::{channel, Receiver, Sender};

    fn create_dbs() -> std::sync::Arc<Databases> {
        clean_op_log_metadata_files();
        let (sender, _): (Sender<String>, Receiver<String>) = channel(100);
        let keys_map = HashMap::new();
        Arc::new(Databases::new(
            String::from(""),
            String::from(""),
            String::from(""),
            sender.clone(),
            sender.clone(),
            keys_map,
            1 as u128,
            true,
        ))
    }

    #[test]
    fn should_clean_all_metadata_files() {
        clean_op_log_metadata_files();

        let keys_file = get_keys_map_file_name();
        assert_eq!(Path::new(&keys_file).exists(), false);

        let op_log_file = get_op_log_file_name();
        assert_eq!(Path::new(&op_log_file).exists(), false);

        let invalidate_oplog_file = get_invalidate_file_name();
        assert_eq!(Path::new(&invalidate_oplog_file).exists(), false);
    }

    #[test]
    fn should_mark_the_keys_and_oplog_as_invalid() {
        let dbs = create_dbs();
        let mut file_writer = get_invalidate_file_write_mode();
        assert_eq!(is_oplog_valid(), true,);
        invalidate_oplog(&mut file_writer, &dbs).unwrap();
        assert_eq!(is_oplog_valid(), false,);

        mark_op_log_as_valid(&dbs).unwrap();
        assert_eq!(is_oplog_valid(), true,);
    }

    #[test]
    fn should_return_the_db_name() {
        assert_eq!(
            db_name_from_file_name(&String::from("dbs/org-1-nun.data")),
            "org-1"
        );
    }

    #[test]
    fn should_return_the_correct_op_log_name() {
        assert_eq!(get_op_log_file_name(), "dbs/oplog-nun.op");
    }

    #[test]
    fn should_return_0_if_the_op_log_is_empty() {
        let _f = get_log_file_append_mode(); //Get here to ensure the file exists
        clean_op_log_metadata_files();
        let _f = get_log_file_append_mode(); //Get here to ensure the file exists
        let last_op = last_op_time();
        assert_eq!(last_op, 0);
    }

    #[test]
    fn should_write_op_log_and_return_opp_id() {
        let mut f = get_log_file_append_mode();
        let opp_id = write_op_log(&mut f, 1, 1, ReplicateOpp::Update);
        let opp_id1 = write_op_log(&mut f, 1, 1, ReplicateOpp::Update);
        assert_ne!(opp_id, opp_id1);
    }

    fn remove_database_file(db_name: String) {
        let file_name = file_name_from_db_name(&db_name);
        if Path::new(&file_name).exists() {
            fs::remove_file(file_name).unwrap();
            fs::remove_file(meta_file_name_from_db_name(db_name)).unwrap();
        }
    }

    #[test]
    fn should_store_data_in_disk() {
        let dbs = create_test_dbs();
        let db_name = String::from("test-db");
        let mut hash = HashMap::new();
        hash.insert(String::from("some"), String::from("value"));
        hash.insert(String::from("some1"), String::from("value1"));
        let db = Database::create_db_from_hash(db_name.clone(), hash, DatabaseMataData::new(0));
        storage_data_disk(&db, db_name.clone());
        let db_file_name = file_name_from_db_name(&db_name);
        let loaded_db = create_db_from_file_name(&db_file_name, &dbs);

        assert_eq!(
            loaded_db.get_value(String::from("some1")).unwrap().value,
            String::from("value1")
        );
        remove_database_file(db_name);
    }

    #[test]
    fn should_restore_keys_with_same_version() {
        let dbs = create_test_dbs();
        let db_name = String::from("test-db");
        let mut hash = HashMap::new();
        let key = String::from("some");
        let value = String::from("value");
        let value_updated = String::from("value_updated");
        hash.insert(key.clone(), value.clone());
        hash.insert(String::from("some1"), String::from("value1"));
        let db = Database::create_db_from_hash(db_name.clone(), hash, DatabaseMataData::new(0));

        let key_value = db.get_value(String::from("some1")).unwrap();
        assert_eq!(key_value.value, String::from("value1"));
        db.set_value(key.clone(), value_updated.clone(), 2);
        let key_value_new = db.get_value(key.to_string()).unwrap();
        assert_eq!(key_value_new.version, 2);
        dbs.is_oplog_valid.store(false, Ordering::Relaxed);
        storage_data_disk(&db, db_name.clone());
        snapshot_keys(&dbs);

        let db_file_name = file_name_from_db_name(&db_name);
        let loaded_db = create_db_from_file_name(&db_file_name, &dbs);
        let key_value = loaded_db.get_value(key.to_string()).unwrap();
        assert_eq!(key_value.value, value_updated);

        assert_eq!(key_value.version, 2);
        remove_database_file(db_name);
    }

    fn create_test_dbs() -> Arc<Databases> {
        let (sender, _replication_receiver): (Sender<String>, Receiver<String>) = channel(100);
        let keys_map = HashMap::new();
        let dbs = Arc::new(Databases::new(
            String::from(""),
            String::from(""),
            String::from(""),
            sender.clone(),
            sender.clone(),
            keys_map,
            1 as u128,
            true,
        ));

        dbs.node_state
            .swap(ClusterRole::Primary as usize, Ordering::Relaxed);
        dbs
    }
}
