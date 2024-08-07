use log;
use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::fs::{create_dir_all, read_dir, File};
use std::io::prelude::*;
use std::io::BufWriter;
use std::io::Error;
use std::io::Read;
use std::io::SeekFrom;
use std::io::Write;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::str;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use crate::bo::*;
use crate::configuration::NUN_DBS_DIR;
use crate::configuration::NUN_DECLUTTER_INTERVAL;
use crate::configuration::NUN_MAX_OP_LOG_SIZE;

const BASE_FILE_NAME: &'static str = "-nun.data";
const DB_KEYS_FILE_NAME: &'static str = "-nun.data.keys";
const META_FILE_NAME: &'static str = "-nun.madadata";

const KEYS_FILE: &'static str = "keys-nun.keys";

const OP_LOG_FILE: &'static str = "oplog-nun.op";
const INVALIDATE_OP_LOG_FILE: &'static str = "is-oplog.valid";

const OP_KEY_SIZE: usize = 8;
const OP_DB_ID_SIZE: usize = 8;
const OP_TIME_SIZE: usize = 8;
const OP_OP_SIZE: usize = 1;
const OP_RECORD_SIZE: usize = OP_TIME_SIZE + OP_DB_ID_SIZE + OP_KEY_SIZE + OP_OP_SIZE;

// Record sizes
const VERSION_SIZE: usize = 4;
const ADDR_SIZE: usize = 8;
const U64_SIZE: usize = 8;
const U32_SIZE: usize = 4;

const VERSION_DELETED: i32 = -1;

impl Databases {
    pub fn add_db_to_snapshot_by_name(
        &self,
        name: &String,
        reclame_space: bool,
    ) -> Result<(), String> {
        match self.to_snapshot.write() {
            Ok(mut dbs) => {
                dbs.push((name.to_string(), reclame_space));
                Ok(())
            }
            Err(_) => return Err("Could not write to the snapshot".to_string()),
        }
    }
}

impl Database {
    pub fn data_disk_size(&self) -> u64 {
        let mut size = 0;
        log::debug!("Will get the size of the database {}", self.name);
        let db_file_name = file_name_from_db_name(&self.name);
        let (_keys_file_name, values_file_name) =
            get_key_value_files_name_from_file_name(db_file_name);
        let file = File::open(values_file_name).unwrap();
        size = size + file.metadata().unwrap().len();
        size
    }
}

pub fn get_dir_name() -> String {
    NUN_DBS_DIR.to_string()
}

pub fn get_op_log_dir_name() -> String {
    format!("{}/oplog", get_dir_name())
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
    let keys_file_name = get_keys_map_file_name();
    log::debug!("Will write the keys {} from disk", keys_file_name);

    let mut keys_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(keys_file_name)
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
        let mut buffer = [0; U64_SIZE];
        file.read(&mut buffer).unwrap();
        let id: usize = usize::from_le_bytes(buffer);
        let mut buffer = [0; U32_SIZE];
        file.read(&mut buffer).unwrap();
        let consensus_strategy: i32 = i32::from_le_bytes(buffer);

        DatabaseMataData::new(id, ConsensuStrategy::from(consensus_strategy))
    } else {
        DatabaseMataData::new(dbs.map.read().unwrap().len(), ConsensuStrategy::Newer)
    }
}

fn load_one_db_from_disk(dbs: &Arc<Databases>, entry: std::io::Result<std::fs::DirEntry>) {
    if let Ok(entry) = entry {
        let full_name = entry.file_name().into_string().unwrap();
        if full_name.ends_with(BASE_FILE_NAME) {
            log::warn!(
                "Will migrate the database {} before moving ahead!",
                full_name
            );
            let db = create_db_from_file_name_old(&full_name, &dbs);
            let db_name = db_name_from_file_name(&full_name);
            storage_data_disk(&db, &db_name, true);
            remove_old_db_file(&db_name);
            dbs.add_database(db);
        } else if full_name.ends_with(DB_KEYS_FILE_NAME) {
            let (db, _) = create_db_from_file_name(&full_name, &dbs);
            dbs.add_database(db);
        }
    }
}

fn create_db_from_file_name_old(full_name: &String, dbs: &Arc<Databases>) -> Database {
    let db_name = db_name_from_file_name(&full_name);
    let db_data = load_db_from_disck_or_empty(db_name.to_string());
    let meta = load_db_metadata_from_disk_or_empty(db_name.to_string(), dbs);
    Database::create_db_from_hash(db_name.to_string(), db_data, meta)
}

fn create_db_from_file_name(file_name: &String, dbs: &Arc<Databases>) -> (Database, String) {
    let db_name = db_name_from_file_name(&file_name.replace(".keys", ""));
    let full_name = file_name_from_db_name(&db_name);
    let meta = load_db_metadata_from_disk_or_empty(db_name.to_string(), dbs);
    let (keys_file_name, values_file_name) = get_key_value_files_name_from_file_name(full_name);

    let mut keys_file = OpenOptions::new().read(true).open(keys_file_name).unwrap();
    let mut values_file = OpenOptions::new()
        .read(true)
        .open(values_file_name)
        .unwrap();
    let mut value_data: HashMap<String, Value> = HashMap::new();

    let mut length_buffer = [0; U64_SIZE];
    let mut value_addr_buffer = [0; U64_SIZE];
    let mut version_buffer = [0; VERSION_SIZE];
    let mut key_disk_addr = 0;
    while let Ok(read) = keys_file.read(&mut length_buffer) {
        if read == 0 {
            //If could not read anything stop
            break;
        }

        //Read key
        let key_length: usize = usize::from_le_bytes(length_buffer);
        let mut key_buffer = vec![0; key_length];
        keys_file.read(&mut key_buffer).unwrap();
        let key = str::from_utf8(&key_buffer).unwrap();
        log::debug!("{}, key: {}", key_length, key);

        //Read version
        let _ = keys_file.read(&mut version_buffer).unwrap();
        let version = i32::from_le_bytes(version_buffer);

        //Read value addr
        let _ = keys_file.read(&mut value_addr_buffer).unwrap();
        let value_addr = u64::from_le_bytes(value_addr_buffer);

        //Read value value
        values_file
            .seek(SeekFrom::Start(value_addr as u64))
            .unwrap();
        let _ = values_file.read(&mut length_buffer).unwrap();
        let value_length = usize::from_le_bytes(length_buffer);
        let mut value_buffer = vec![0; value_length];
        let _ = values_file.read(&mut value_buffer);
        let value = str::from_utf8(&value_buffer).unwrap();
        if version != VERSION_DELETED {
            value_data.insert(
                key.to_string(),
                Value {
                    version,
                    value: value.to_string(),
                    state: ValueStatus::Ok,
                    value_disk_addr: value_addr,
                    key_disk_addr,
                    opp_id: Databases::next_op_log_id(),
                },
            );
        } else {
            log::debug!("Key {} not loaded becuase it is deleted", key);
        }
        key_disk_addr = key_disk_addr + get_key_disk_size(key_length);
    }
    (
        Database::create_db_from_value_hash(db_name.to_string(), value_data, meta),
        db_name.clone(),
    )
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

pub fn file_name_from_db_name(db_name: &String) -> String {
    format!(
        "{dir}/{db_name}{sufix}",
        dir = get_dir_name(),
        db_name = db_name,
        sufix = BASE_FILE_NAME
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
    let partial_name = full_name.replace(BASE_FILE_NAME, "");
    let splited_name: Vec<&str> = partial_name.split("/").collect();
    let db_name = splited_name.last().unwrap();
    return db_name.to_string();
}

fn remove_backup_key_file(db_name: &String) {
    let file_name = format!("{}.keys.old", file_name_from_db_name(&db_name));
    if Path::new(&file_name).exists() {
        fs::remove_file(file_name).unwrap();
    }
}

fn get_key_file_append_mode(db_name: &String, reclame_space: bool) -> BufWriter<File> {
    let file_name = format!("{}.keys", file_name_from_db_name(&db_name));

    if reclame_space && Path::new(&file_name).exists() {
        let backup_file = format!("{}.old", file_name);
        // Rename becuase remove may not be sync
        // Will be removed latter once the new file is safe see method remove_backup_key_file
        fs::rename(&file_name, &backup_file)
            .expect("Could not rename the data file to reclame space");
    }

    BufWriter::with_capacity(
        OP_RECORD_SIZE * 10,
        OpenOptions::new()
            .append(true)
            .create(true)
            .open(&file_name)
            .unwrap(),
    )
}

fn get_key_write_mode(db_name: &String) -> (File, u64) {
    let file_name = format!("{}.keys", file_name_from_db_name(&db_name));
    let size = match fs::metadata(&file_name) {
        Ok(metadata) => metadata.len(),
        _ => 0,
    };
    (
        OpenOptions::new().write(true).open(file_name).unwrap(),
        size,
    )
}

fn get_values_file_append_mode(db_name: &String, reclame_space: bool) -> (BufWriter<File>, u64) {
    let file_name = format!("{}.values", file_name_from_db_name(&db_name));
    if reclame_space && Path::new(&file_name).exists() {
        let backup_file = format!("{}.old", file_name);
        // Rename becuase remove may not be sync
        fs::rename(&file_name, &backup_file)
            .expect("Could not rename the data file to reclame space");
        fs::remove_file(&backup_file).expect("Could not delete the backup file to reclame  space");
    }

    let size = match fs::metadata(&file_name) {
        Ok(metadata) => metadata.len(),
        _ => 0,
    };
    (
        BufWriter::with_capacity(
            OP_RECORD_SIZE * 10,
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(file_name)
                .unwrap(),
        ),
        size,
    )
}

fn storage_data_disk(db: &Database, db_name: &String, reclame_space: bool) -> u32 {
    let keys_to_update = get_keys_to_save_to_disck(db, reclame_space);
    let mut keys_file = get_key_file_append_mode(&db_name, reclame_space);
    let (mut values_file, current_value_file_size) =
        get_values_file_append_mode(&db_name, reclame_space);
    // To inplace update
    let (mut keys_file_write, current_key_file_size) = get_key_write_mode(&db_name);
    log::debug!("current_key_file_size: {}", current_key_file_size);

    let mut value_addr = current_value_file_size;
    let mut next_key_addr = current_key_file_size;
    let mut changed_keys = 0;

    for (key, value) in keys_to_update {
        match value.state {
            ValueStatus::Ok => {
                if !reclame_space {
                    panic!("Values Ok should never get here")
                } else {
                    changed_keys = changed_keys + 1;
                    let (record_size, key_size) = write_new_key_value(
                        &mut values_file,
                        &value,
                        &key,
                        next_key_addr,
                        value_addr,
                        &mut keys_file,
                        db,
                    );
                    value_addr = value_addr + record_size;
                    next_key_addr = next_key_addr + key_size;
                }
            }
            ValueStatus::New => {
                changed_keys = changed_keys + 1;
                let (record_size, key_size) = write_new_key_value(
                    &mut values_file,
                    &value,
                    &key,
                    next_key_addr,
                    value_addr,
                    &mut keys_file,
                    db,
                );
                value_addr = value_addr + record_size;
                next_key_addr = next_key_addr + key_size;
            }

            ValueStatus::Updated => {
                changed_keys = changed_keys + 1;
                // Append value file
                let record_size = write_value(&mut values_file, &value, ValueStatus::Ok);

                if !reclame_space {
                    // In place upate in key file
                    update_key(
                        &mut keys_file_write,
                        &key,
                        value.version,
                        value_addr,
                        value.key_disk_addr,
                    );
                    db.set_value_as_ok(
                        &key,
                        &value,
                        value_addr,
                        value.key_disk_addr,
                        Databases::next_op_log_id(),
                    );
                    // Append key file
                } else {
                    let key_size = write_key(&mut keys_file, &key, &value, value_addr);
                    db.set_value_as_ok(
                        &key,
                        &value,
                        value_addr,
                        next_key_addr,
                        Databases::next_op_log_id(),
                    );
                    next_key_addr = next_key_addr + key_size;
                }
                value_addr = value_addr + record_size;
            }

            ValueStatus::Deleted => {
                if !reclame_space {
                    changed_keys = changed_keys + 1;
                    // In place upate in key file
                    update_key(
                        &mut keys_file_write,
                        &key,
                        VERSION_DELETED, // Version -1 means deleted
                        0,
                        value.key_disk_addr,
                    );
                } else {
                    log::debug!("To reclame_space nothing need to be done on delete");
                }
            }
        }
    }

    keys_file.flush().unwrap();
    keys_file_write.flush().unwrap();
    values_file.flush().unwrap();

    write_metadata_file(db_name, db);
    log::debug!("snapshoted {} keys", changed_keys);
    changed_keys
}

fn write_new_key_value(
    values_file: &mut BufWriter<File>,
    value: &Value,
    key: &String,
    next_key_addr: u64,
    value_addr: u64,
    keys_file: &mut BufWriter<File>,
    db: &Database,
) -> (u64, u64) {
    // Append value file
    let record_size = write_value(values_file, value, ValueStatus::Ok);
    log::debug!(
        "Write key: {}, addr: {} value_addr: {} ",
        key,
        next_key_addr,
        value_addr
    );
    // Append key file
    let key_size = write_key(keys_file, key, value, value_addr);
    db.set_value_as_ok(
        key,
        value,
        value_addr,
        next_key_addr,
        Databases::next_op_log_id(),
    );
    (record_size, key_size)
}

fn get_keys_to_save_to_disck(db: &Database, reclame_space: bool) -> Vec<(String, Value)> {
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

/// Writes a value to a giving file
///
fn write_value(values_file: &mut BufWriter<File>, value: &Value, status: ValueStatus) -> u64 {
    values_file.write(&value.value.len().to_le_bytes()).unwrap();
    //8bytes
    let value_as_bytes = value.value.as_bytes();
    //Nth bytes
    values_file.write(&value_as_bytes).unwrap();
    //4 bytes
    values_file.write(&status.to_le_bytes()).unwrap();
    let record_size = (U64_SIZE + value_as_bytes.len() + VERSION_SIZE) as u64;
    record_size
}

fn get_key_disk_size(key_size: usize) -> u64 {
    (U64_SIZE + key_size + ADDR_SIZE + VERSION_SIZE) as u64
}
/// Writes a key to a giving file
///
fn write_key(keys_file: &mut BufWriter<File>, key: &String, value: &Value, value_addr: u64) -> u64 {
    let len = key.len();
    //8bytes
    keys_file.write(&len.to_le_bytes()).unwrap();
    //Nth bytes
    keys_file.write(&key.as_bytes()).unwrap();
    //4 bytes
    keys_file.write(&value.version.to_le_bytes()).unwrap();
    //8 bytes
    keys_file.write(&value_addr.to_le_bytes()).unwrap();
    get_key_disk_size(key.len())
}

//Update key in place
fn update_key(
    keys_file: &mut File,
    key: &String,
    version: i32,
    value_addr: u64,
    key_disk_addr: u64,
) {
    let start_at = key_disk_addr + get_key_disk_size(key.len()) - (8 + VERSION_SIZE as u64);
    log::debug!(
        "Update key: {}, addr: {} value_addr: {} start_at: {}",
        key,
        key_disk_addr,
        value_addr,
        start_at
    );
    //4 bytes
    keys_file
        .write_at(&version.to_le_bytes(), start_at)
        .unwrap();
    //8 bytes
    keys_file
        .write_at(&value_addr.to_le_bytes(), start_at + VERSION_SIZE as u64)
        .unwrap();
}

fn write_metadata_file(db_name: &String, db: &Database) {
    let mut meta_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(meta_file_name_from_db_name(db_name.to_string()))
        .unwrap();
    //8 bytes
    meta_file.write(&db.metadata.id.to_le_bytes()).unwrap();
    //4 bytes
    meta_file
        .write(&db.metadata.consensus_strategy.to_le_bytes())
        .unwrap();
}

fn remove_old_db_files() {
    let op_log_files = get_op_log_entries_by_creation_date();
    if op_log_files.len() < 10 {
        log::debug!("No files to remove");
        return;
    }
    let files_to_remove = &op_log_files[9..];
    files_to_remove.iter().for_each(|entry| {
        let file_path = entry.path().to_str().unwrap().to_string();
        log::debug!("Will delete the file {}", file_path);
        fs::remove_file(file_path).unwrap();
    });
}
// calls storage_data_disk each $SNAPSHOT_TIME seconds
pub fn declutter_scheduler(timer: timer::Timer, dbs: Arc<Databases>) {
    log::info!("Will start_snap_shot_timer");
    let (_tx, rx): (
        std::sync::mpsc::Sender<String>,
        std::sync::mpsc::Receiver<String>,
    ) = std::sync::mpsc::channel(); // Visit this again
    let _guard = {
        timer.schedule_repeating(
            chrono::Duration::seconds(*NUN_DECLUTTER_INTERVAL),
            move || declutter(&dbs),
        )
    };
    rx.recv().unwrap(); // Thread will run for ever
}

pub fn declutter(dbs: &Arc<Databases>) {
    snapshot_all_pendding_dbs(&dbs);
    remove_old_db_files();
}

pub fn snapshot_all_pendding_dbs(dbs: &Arc<Databases>) {
    let queue_len = { dbs.to_snapshot.read().unwrap().len() };
    log::debug!("snapshot_all_pendding_dbs | queue_len == {}", queue_len);
    if queue_len > 0 {
        snapshot_keys(&dbs);
        let mut dbs_to_snapshot = {
            let dbs = dbs.to_snapshot.write().unwrap();
            dbs
        };
        dbs_to_snapshot.dedup();
        while let Some((database_name, reclaim_space)) = dbs_to_snapshot.pop() {
            log::debug!("Will snapshot the database {}", database_name);
            let dbs = dbs.clone();
            let dbs_map = dbs.map.read().unwrap();
            let db_opt = dbs_map.get(&database_name);
            if let Some(db) = db_opt {
                storage_data_disk(db, &database_name, reclaim_space);
                remove_backup_key_file(&database_name);
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

/*
*  if let Ok(entries) = read_dir(get_dir_name()) {
*      for entry in entries {
*          load_one_db_from_disk(dbs, entry);
*      }
*  }
*/
pub fn get_log_file_append_mode() -> BufWriter<File> {
    if get_file_size(&get_op_log_file_name()) >= single_op_log_file_size() {
        let op_log_dir = get_op_log_dir_name();
        if !Path::new(&op_log_dir).exists() {
            create_dir_all(&op_log_dir).unwrap();
        }
        let new_oplog_file_name = format!(
            "{dir}/{sufix}",
            dir = op_log_dir,
            sufix = format!("oplog-nun-{}.op", Databases::next_op_log_id())
        );
        fs::rename(&get_op_log_file_name(), &new_oplog_file_name)
            .expect("Could not rename the oplog file");
    }
    BufWriter::with_capacity(
        OP_RECORD_SIZE,
        OpenOptions::new()
            .append(true)
            .create(true)
            .open(get_op_log_file_name())
            .unwrap(),
    )
}

fn single_op_log_file_size() -> u64 {
    *NUN_MAX_OP_LOG_SIZE / 10
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
    if let Ok(entries) = read_dir(get_op_log_dir_name()) {
        for entry in entries {
            let file_name = entry.unwrap().file_name().into_string().unwrap();
            if file_name.ends_with(".op") {
                let full_path = format!("{}/{}", get_op_log_dir_name(), file_name);
                log::debug!("Will delete the file {}", full_path);
                fs::remove_file(full_path.clone()).unwrap();
            }
        }
    }
}

fn get_log_file_read_mode(file_name: &String) -> File {
    match OpenOptions::new().read(true).open(file_name) {
        Err(e) => {
            log::error!("{:?}", e);
            {
                // Force create the fail
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(file_name)
                    .unwrap();
            } // For creating the op log file, I don't return it here to avoid read processes holding the file in write more for too long
            OpenOptions::new().read(true).open(file_name).unwrap()
        }
        Ok(f) => f,
    }
}

pub fn try_write_op_log(
    op_log_stream: &mut BufWriter<File>,
    db_id: u64,
    key_id: u64,
    opp: &ReplicateOpp,
    op_log_id_in: u64,
) -> u64 {
    match write_op_log(op_log_stream, db_id, key_id, &opp, op_log_id_in) {
        Ok(id) => id,
        Err(_) => {
            *op_log_stream = get_log_file_append_mode();
            write_op_log(
                op_log_stream,
                db_id,
                key_id,
                &ReplicateOpp::Snapshot,
                op_log_id_in,
            )
            .unwrap()
        }
    }
}

pub fn write_op_log(
    stream: &mut BufWriter<File>,
    db_id: u64,
    key: u64,
    opp: &ReplicateOpp,
    opp_id: u64,
) -> Result<u64, String> {
    let opp_to_write = opp.to_u8();

    stream.write(&opp_id.to_le_bytes()).unwrap(); //8
    stream.write(&key.to_le_bytes()).unwrap(); // 8
    stream.write(&db_id.to_le_bytes()).unwrap(); // 8
    stream.write(&[opp_to_write]).unwrap(); // 1
    stream.flush().unwrap();
    log::debug!("will write :  db: {db}", db = db_id);
    if stream.stream_position().unwrap() > single_op_log_file_size() {
        Err(String::from("Oplog max size reached"))
    } else {
        Ok(opp_id)
    }
}

pub fn last_op_time() -> u64 {
    let mut f = get_log_file_read_mode(&get_op_log_file_name());
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
    let oplog_entries = get_op_log_entries_by_creation_date();
    let old_files_size = oplog_entries.iter().fold(0, |acc_size, entry| {
        let size = entry.metadata().unwrap().len();
        acc_size + size
    });
    let f = get_log_file_read_mode(&get_op_log_file_name());
    let size: u64 = match f.metadata() {
        Ok(f) => f.len(),
        Err(message) => {
            log::debug!("Faile to get the op file size, returning 0, {}", message);
            0
        }
    };
    let total_size = old_files_size + size;
    (total_size, total_size / OP_RECORD_SIZE as u64)
}

pub fn read_operations_since(since: u64) -> HashMap<String, OpLogRecord> {
    let mut opps_since = HashMap::new();
    let f = get_log_file_read_mode(&get_op_log_file_name());
    read_operations_since_from_file(f, since, &mut opps_since);

    let oplog_entries = get_op_log_entries_by_creation_date();
    for oplog_file_entry in oplog_entries {
        let file_name = oplog_file_entry.file_name().into_string().unwrap();
        if file_name.ends_with(".op") {
            let full_path = format!("{}/{}", get_op_log_dir_name(), file_name);
            let f = get_log_file_read_mode(&full_path);
            read_operations_since_from_file(f, since, &mut opps_since);
        }
    }

    opps_since
}

fn get_op_log_entries_by_creation_date() -> Vec<fs::DirEntry> {
    let oplog_dir_name = get_op_log_dir_name();
    if !Path::new(&oplog_dir_name).exists() {
        return vec![];
    }
    let mut oplog_entries: Vec<_> = match fs::read_dir(&oplog_dir_name) {
        Ok(entries) => entries.filter_map(Result::ok).collect(),
        Err(err) => {
            panic!("Could not read the oplog dir {}", err);
        }
    };
    oplog_entries.sort_by(|a, b| {
        let metadata_a = a.metadata().unwrap();
        let metadata_b = b.metadata().unwrap();
        metadata_b
            .created()
            .unwrap()
            .cmp(&metadata_a.created().unwrap())
    });
    oplog_entries
}

fn read_operations_since_from_file(
    mut f: File,
    since: u64,
    opps_since: &mut HashMap<String, OpLogRecord>,
) {
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
}

pub fn get_invalidate_file_write_mode() -> BufWriter<File> {
    match OpenOptions::new()
        .create(true)
        .write(true)
        .open(get_invalidate_file_name())
    {
        Ok(f) => BufWriter::with_capacity(1, f),
        Err(e) => {
            log::error!(
                "Could not open the file {}, Error: {}",
                get_invalidate_file_name(),
                e
            );
            panic!("Could not open the file {}", get_invalidate_file_name());
        }
    }
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

fn remove_old_db_file(db_name: &String) {
    let file_name = file_name_from_db_name(&db_name);
    if Path::new(&file_name).exists() {
        fs::remove_file(file_name.clone()).unwrap();
    }
}

/// # Examples
///
/// ```
/// let (k_name, v_name) = nundb::disk_ops::get_key_value_files_name_from_file_name(String::from("jose.nun.data"));
/// assert_eq!(k_name, "jose.nun.data.keys");
/// assert_eq!(v_name, "jose.nun.data.values");
/// ```
pub fn get_key_value_files_name_from_file_name(file_name: String) -> (String, String) {
    let keys_file_name = format!("{}.keys", file_name);
    let values_file_name = format!("{}.values", file_name);
    (keys_file_name, values_file_name)
}

fn get_file_size(file_name: &String) -> u64 {
    match fs::metadata(&file_name) {
        Ok(metadata) => metadata.len(),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::NUN_LOG_LEVEL;
    use env_logger::{Builder, Env, Target};
    use futures::channel::mpsc::{channel, Receiver, Sender};
    use std::env;
    fn init_logger() {
        let env = Env::default().filter_or("NUN_LOG_LEVEL", NUN_LOG_LEVEL.as_str());
        Builder::from_env(env)
            .format_level(false)
            .target(Target::Stdout)
            .format_timestamp_nanos()
            .init();
    }

    const OLD_FILE_NAME: &'static str = BASE_FILE_NAME;

    fn remove_database_file(db_name: &String) {
        remove_invalidate_oplog_file();
        let file_name = file_name_from_db_name(&db_name);
        let metada_data_file_name = meta_file_name_from_db_name(db_name.to_string());
        if Path::new(&metada_data_file_name).exists() {
            fs::remove_file(&metada_data_file_name).unwrap();
        }
        if Path::new(&file_name).exists() {
            fs::remove_file(file_name.clone()).unwrap();
        }

        let (keys_file_name, values_file_name) = get_key_value_files_name_from_file_name(file_name);
        if Path::new(&keys_file_name).exists() {
            fs::remove_file(keys_file_name.clone()).unwrap();
        }

        if Path::new(&values_file_name).exists() {
            fs::remove_file(values_file_name).unwrap();
        }
    }

    fn clean_all_db_files(db_name: &String) {
        remove_database_file(&db_name);
        clean_op_log_metadata_files();
        remove_keys_file();
    }

    fn storage_data_disk_old(db: &Database, db_name: String) {
        let data = db.to_string_hash();
        // store data
        let mut file = File::create(file_name_from_db_name(&db_name)).unwrap();
        bincode::serialize_into(&mut file, &data.clone()).unwrap();
        write_metadata_file(&db_name, db);
    }

    fn file_name_from_db_name_old(db_name: &String) -> String {
        format!(
            "{dir}/{db_name}{sufix}",
            dir = get_dir_name(),
            db_name = db_name,
            sufix = OLD_FILE_NAME
        )
    }
    fn create_dbs() -> std::sync::Arc<Databases> {
        clean_op_log_metadata_files();
        let (sender, _): (Sender<String>, Receiver<String>) = channel(100);
        let keys_map = HashMap::new();
        Arc::new(Databases::new(
            String::from(""),
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
    fn should_generate_new_file_when_old_file_is_full() {
        clean_op_log_metadata_files();
        env::set_var("NUN_MAX_OP_LOG_SIZE", "1000");
        {
            let mut oplog_file = get_log_file_append_mode();
            while let Ok(opp_id) = write_op_log(
                &mut oplog_file,
                1,
                1,
                &ReplicateOpp::Update,
                Databases::next_op_log_id(),
            ) {
                log::debug!("opp_id: {}", opp_id);
                let (size, _) = get_op_log_size();
                assert!(size <= 100);
            }
            // Will free f and close the resource ...
        } // oplog_file is closed here
        let mut f = get_log_file_append_mode();
        f.seek(SeekFrom::End(0)).unwrap();
        // Because I expect this file to be new and the old one is moved to the old value
        assert_eq!(f.stream_position().unwrap(), 0);
        env::set_var("NUN_MAX_OP_LOG_SIZE", "100000000");
    }

    #[test]
    fn should_cleanup_old_oplog_files() {
        clean_op_log_metadata_files();
        if let Ok(entries) = read_dir(get_dir_name()) {
            for entry in entries {
                let full_name = entry.unwrap().file_name().into_string().unwrap();
                if full_name.contains("oplog-nun-") {
                    // Should never happen
                    assert!(false);
                }
            }
        }
    }

    #[test]
    fn should_reject_write_if_oplog_is_max_size() {
        clean_op_log_metadata_files();
        env::set_var("NUN_MAX_OP_LOG_SIZE", "1000");
        let mut f = get_log_file_append_mode();
        while let Ok(opp_id) = write_op_log(
            &mut f,
            1,
            1,
            &ReplicateOpp::Update,
            Databases::next_op_log_id(),
        ) {
            log::debug!("opp_id: {}", opp_id);
            let (size, _) = get_op_log_size();
            assert!(size <= 100);
        }
        let (size, _) = get_op_log_size();
        assert!(size > 70);
        assert!(size <= 150);
        env::set_var("NUN_MAX_OP_LOG_SIZE", "100000000");
    }

    #[test]
    fn should_write_op_log_and_return_opp_id() {
        let mut f = get_log_file_append_mode();
        if let Ok(opp_id) = write_op_log(
            &mut f,
            1,
            1,
            &ReplicateOpp::Update,
            Databases::next_op_log_id(),
        ) {
            let opp_id1 = write_op_log(
                &mut f,
                1,
                1,
                &ReplicateOpp::Update,
                Databases::next_op_log_id(),
            )
            .unwrap();
            assert_ne!(opp_id, opp_id1);
        } else {
            panic!("Could not write to the op log file");
        }
    }

    #[test]
    fn should_store_data_in_disk() {
        let dbs = create_test_dbs();
        let db_name = String::from("test-db");
        let mut hash = HashMap::new();
        hash.insert(String::from("some"), String::from("value"));
        hash.insert(String::from("some1"), String::from("value1"));
        let db = Database::create_db_from_hash(
            db_name.clone(),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Arbiter),
        );
        storage_data_disk_old(&db, db_name.clone());
        let db_file_name = file_name_from_db_name(&db_name);
        let loaded_db = create_db_from_file_name_old(&db_file_name, &dbs);

        assert_eq!(
            loaded_db.get_value(String::from("some1")).unwrap().value,
            String::from("value1")
        );

        assert_eq!(
            loaded_db.metadata.consensus_strategy,
            ConsensuStrategy::Arbiter,
        );
        remove_database_file(&db_name);
    }

    #[test]
    fn should_restore_keys_with_same_version() {
        let dbs = create_test_dbs();
        let db_name = String::from("test-db");
        let db_file_name = file_name_from_db_name(&db_name);
        clean_all_db_files(&db_name);

        let mut hash = HashMap::new();
        let key = String::from("some");
        let value = String::from("value");
        let value_updated = String::from("value_updated");
        let key1 = String::from("some1");
        let value1 = String::from("value1");

        hash.insert(key.clone(), value.clone());
        hash.insert(key1.clone(), value1.clone());

        let db = Database::create_db_from_hash(
            db_name.clone(),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Newer),
        );
        db.set_value(&Change::new(key.clone(), value_updated.clone(), 2));

        let key_value_new = db.get_value(key.to_string()).unwrap();
        assert_eq!(key_value_new.version, 3);

        dbs.is_oplog_valid.store(false, Ordering::Relaxed);
        storage_data_disk(&db, &db_name, false);

        let (loaded_db, _) = create_db_from_file_name(&db_file_name, &dbs);
        assert_eq!(loaded_db.metadata.id, db.metadata.id);
        assert_eq!(
            loaded_db.metadata.consensus_strategy,
            db.metadata.consensus_strategy
        );

        let key_value = loaded_db.get_value(key.to_string()).unwrap();
        assert_eq!(key_value.value, value_updated);
        assert_eq!(key_value.version, 3);

        let key1_value = loaded_db.get_value(key1.to_string()).unwrap();
        assert_eq!(key1_value.value, value1);
        assert_eq!(key1_value.version, 1);

        let final_value = String::from("final_value");
        loaded_db.set_value(&Change::new(key.clone(), final_value.clone(), 3));

        // To test in place update
        storage_data_disk(&loaded_db, &db_name, false);
        let (loaded_db, _) = create_db_from_file_name(&db_file_name, &dbs);

        let key_value = loaded_db.get_value(key.to_string()).unwrap();
        assert_eq!(key_value.value, final_value);
        assert_eq!(key_value.version, 4);

        let key1_value = loaded_db.get_value(key1.to_string()).unwrap();
        assert_eq!(key1_value.value, value1);
        assert_eq!(key1_value.version, 1);

        let final_value = String::from("final_value1");
        loaded_db.set_value(&Change::new(key.clone(), final_value.clone(), 4));

        // To test in place update
        storage_data_disk(&loaded_db, &db_name, false);
        let (loaded_db, _) = create_db_from_file_name(&db_file_name, &dbs);

        let key_value = loaded_db.get_value(key.to_string()).unwrap();
        assert_eq!(key_value.value, final_value);
        assert_eq!(key_value.version, 5);

        let key1_value = loaded_db.get_value(key1.to_string()).unwrap();
        assert_eq!(key1_value.value, value1);
        assert_eq!(key1_value.version, 1);

        clean_all_db_files(&db_name);
    }

    #[test]
    fn shold_migrate_the_file_of_database_if_old_one_exists() {
        let dbs = create_test_dbs();
        let db_name = String::from("test-db");
        let mut hash = HashMap::new();
        let key = String::from("some");
        let value = String::from("value");
        let value_updated = String::from("value_updated");
        let key1 = String::from("some1");
        let value1 = String::from("value1");

        hash.insert(key.clone(), value.clone());
        hash.insert(key1.clone(), value1.clone());

        let db = Database::create_db_from_hash(
            db_name.clone(),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Newer),
        );
        db.set_value(&Change::new(key.clone(), value_updated.clone(), 2));

        let key_value_new = db.get_value(key.to_string()).unwrap();
        assert_eq!(key_value_new.version, 3);

        dbs.is_oplog_valid.store(false, Ordering::Relaxed);
        storage_data_disk_old(&db, db_name.clone()); //Store as old

        let dbs = create_test_dbs();
        load_all_dbs_from_disk(&dbs);
        let map = dbs.map.read().unwrap();
        let db_loaded = map.get(&db_name).unwrap();
        assert_eq!(db_loaded.get_value(key).unwrap(), value_updated);
        storage_data_disk(&db_loaded, &db_name, false); //Store as new
        let db_file_name = file_name_from_db_name_old(&db_name);
        assert!(!Path::new(&db_file_name).exists());
    }

    #[test]
    fn shold_load_the_database_from_disk_old() {
        let dbs = create_test_dbs();
        let db_name = String::from("test-db");
        let mut hash = HashMap::new();
        let key = String::from("some");
        let value = String::from("value");
        let value_updated = String::from("value_updated");
        let key1 = String::from("some1");
        let value1 = String::from("value1");

        hash.insert(key.clone(), value.clone());
        hash.insert(key1.clone(), value1.clone());

        let db = Database::create_db_from_hash(
            db_name.clone(),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Newer),
        );
        db.set_value(&Change::new(key.clone(), value_updated.clone(), 2));

        let key_value_new = db.get_value(key.to_string()).unwrap();
        assert_eq!(key_value_new.version, 3);

        dbs.is_oplog_valid.store(false, Ordering::Relaxed);
        storage_data_disk_old(&db, db_name.clone()); //Store as old

        let dbs = create_test_dbs();
        load_all_dbs_from_disk(&dbs);
        let map = dbs.map.read().unwrap();
        let db_loaded = map.get(&db_name).unwrap();
        assert_eq!(db_loaded.get_value(key).unwrap(), value_updated);

        remove_database_file(&db_name);
        clean_op_log_metadata_files();
        remove_keys_file();
    }

    #[test]
    fn shold_load_the_database_from_disk_new() {
        let dbs = create_test_dbs();
        let db_name = String::from("test-db");
        let mut hash = HashMap::new();
        let key = String::from("some");
        let value = String::from("value");
        let value_updated = String::from("value_updated");
        let key1 = String::from("some1");
        let value1 = String::from("value1");

        hash.insert(key.clone(), value.clone());
        hash.insert(key1.clone(), value1.clone());

        let db = Database::create_db_from_hash(
            db_name.clone(),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Newer),
        );
        db.set_value(&Change::new(key.clone(), value_updated.clone(), 2));

        let key_value_new = db.get_value(key.to_string()).unwrap();
        assert_eq!(key_value_new.version, 3);

        dbs.is_oplog_valid.store(false, Ordering::Relaxed);
        storage_data_disk(&db, &db_name, false);

        let dbs = create_test_dbs();
        load_all_dbs_from_disk(&dbs);
        let map = dbs.map.read().unwrap();
        let db_loaded = map.get(&db_name).unwrap();
        assert_eq!(db_loaded.get_value(key).unwrap(), value_updated);
        remove_database_file(&db_name);
        clean_op_log_metadata_files();
        remove_keys_file();
    }

    #[test]
    fn shold_remove_keys_from_disk_if_keys_were_excluded() {
        let dbs = create_test_dbs();
        let db_name = String::from("test-db");
        clean_all_db_files(&db_name);
        let mut hash = HashMap::new();
        let key = String::from("some");
        let value = String::from("value");
        let value_updated = String::from("value_updated");
        let key1 = String::from("some1");
        let value1 = String::from("value1");

        hash.insert(key.clone(), value.clone());
        hash.insert(key1.clone(), value1.clone());

        let db = Database::create_db_from_hash(
            db_name.clone(),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Newer),
        );
        db.set_value(&Change::new(key.clone(), value_updated.clone(), 2));

        let key_value_new = db.get_value(key.to_string()).unwrap();
        assert_eq!(key_value_new.version, 3);
        assert_eq!(db.count_keys(), 2);

        dbs.is_oplog_valid.store(false, Ordering::Relaxed);
        storage_data_disk(&db, &db_name, false);

        let dbs = create_test_dbs();
        load_all_dbs_from_disk(&dbs);
        let map = dbs.map.read().unwrap();
        let db_loaded = map.get(&db_name).unwrap();

        db_loaded.remove_value(key.to_string());
        //assert_eq!(db_loaded.count_keys(), 1);
        storage_data_disk(&db_loaded, &db_name, false);

        let dbs = create_test_dbs();
        load_all_dbs_from_disk(&dbs);
        let map = dbs.map.read().unwrap();
        let db_loaded_final = map.get(&db_name).unwrap();

        assert_eq!(db_loaded_final.count_keys(), 1);
        storage_data_disk(&db, &db_name, false);

        remove_database_file(&db_name);
        clean_op_log_metadata_files();
        remove_keys_file();
    }

    #[test]
    fn keys_file_size_should_not_change_if_no_new_keys_added() {
        let (_dbs, db_name, db) = create_db_with_10k_keys();
        remove_database_file(&db_name); // Clean old tests
        let full_name = file_name_from_db_name(&db_name);
        let (keys_file_name, _values_file_name) =
            get_key_value_files_name_from_file_name(full_name);
        storage_data_disk(&db, &db_name, false);
        let key_file_size_before = get_file_size(&keys_file_name);
        //remove_database_file(&db_name);
        storage_data_disk(&db, &db_name, false);
        let key_file_after = get_file_size(&keys_file_name);
        assert_eq!(key_file_size_before, key_file_after);

        remove_database_file(&db_name);
        clean_op_log_metadata_files();
        remove_keys_file();
    }

    #[test]
    fn keys_file_size_should_not_change_if_no_only_updates_happens() {
        let (_dbs, db_name, db) = create_db_with_10k_keys();
        remove_database_file(&db_name); // Clean old tests
        let full_name = file_name_from_db_name(&db_name);
        let (keys_file_name, _values_file_name) =
            get_key_value_files_name_from_file_name(full_name);
        storage_data_disk(&db, &db_name, false);
        let key_file_size_before = get_file_size(&keys_file_name);
        let key_1 = String::from("key_1");
        let value = db.get_value(key_1.clone()).unwrap();
        assert_eq!(value.state, ValueStatus::Ok);
        db.set_value(&Change::new(key_1.clone(), String::from("NewValue"), 2));

        let value = db.get_value(key_1.clone()).unwrap();
        assert_eq!(value.state, ValueStatus::Updated);

        storage_data_disk(&db, &db_name, false);
        let key_file_after = get_file_size(&keys_file_name);
        assert_eq!(key_file_size_before, key_file_after);

        remove_database_file(&db_name);
        clean_op_log_metadata_files();
        remove_keys_file();
    }

    #[test]
    fn remove_keys_should_work() {
        let (dbs, db_name, db) = create_empty_db();
        db.set_value(&Change::new("Jose".to_string(), "value".to_string(), 1));
        db.set_value(&Change::new(
            "Keyhshshshsh1".to_string(),
            "value".to_string(),
            1,
        ));
        let _ = db.remove_value(String::from("Keyhshshshsh1"));
        clean_all_db_files(&db_name);
        let _ = storage_data_disk(&db, &db_name, false);

        let db_file_name = file_name_from_db_name(&db_name);
        let (loaded_db, _) = create_db_from_file_name(&db_file_name, &dbs);

        let value = loaded_db.get_value(String::from("Keyhshshshsh1"));
        assert_eq!(value, None);

        let _ = loaded_db.remove_value(String::from("Keyhshshshsh1"));
        let _ = storage_data_disk(&loaded_db, &db_name, false);
        remove_database_file(&db_name);
        clean_op_log_metadata_files();
        remove_keys_file();
    }

    #[test]
    #[cfg(not(tarpaulin))]
    fn restore_should_be_fast() {
        let (dbs, db_name, db) = create_db_with_10k_keys();
        clean_all_db_files(&db_name);
        let start = Instant::now();
        let changed_keys = storage_data_disk(&db, &db_name, false);
        log::info!("TIme {:?}", start.elapsed());
        assert!(start.elapsed().as_millis() < 100);
        assert_eq!(changed_keys, 10000);

        let start_load = Instant::now();
        let db_file_name = file_name_from_db_name(&db_name);
        let (loaded_db, _) = create_db_from_file_name(&db_file_name, &dbs);
        let time_in_ms = start_load.elapsed().as_millis();
        log::info!("TIme to update {:?}ms", time_in_ms);
        assert!(start_load.elapsed().as_millis() < 100);

        let value = loaded_db.get_value(String::from("key_1")).unwrap();
        assert_eq!(value.state, ValueStatus::Ok);

        loaded_db.set_value(&Change::new(
            String::from("key_1"),
            String::from("New-value"),
            2,
        ));

        let value = loaded_db.get_value(String::from("key_1")).unwrap();
        let value_100 = loaded_db.get_value(String::from("key_100")).unwrap();
        let value_1000 = loaded_db.get_value(String::from("key_1000")).unwrap();
        assert_eq!(value.state, ValueStatus::Updated);
        assert_eq!(value_100.state, ValueStatus::Ok);
        assert_eq!(value_100.value, String::from("key_100"));
        assert_eq!(value_1000.value, String::from("key_1000"));
        let start_secount_storage = Instant::now();
        let changed_keys = storage_data_disk(&loaded_db, &db_name, false);
        assert_eq!(changed_keys, 1);

        let time_in_ms = start_secount_storage.elapsed().as_millis();
        log::debug!("TIme to update {:?}ms", time_in_ms);
        assert!(time_in_ms < 2);

        remove_database_file(&db_name);
        clean_op_log_metadata_files();
        remove_keys_file();
    }

    #[test]
    fn should_clean_up_all_files_that_are_more_10() {
        clean_op_log_metadata_files();
        let dbs = create_test_dbs();
        env::set_var("NUN_MAX_OP_LOG_SIZE", "1000");
        let mut oplog_file = get_log_file_append_mode();
        let mut i = 0;
        while i < 1000 {
            try_write_op_log(
                &mut oplog_file,
                1,
                1,
                &ReplicateOpp::Update,
                Databases::next_op_log_id(),
            ); // Will free f and close the resource ..
            i = i + 1;
        } // oplog_file is closed here;
          // make sure we can do 300k a secound
        let old_oplog_files = get_op_log_entries_by_creation_date();
        assert_eq!(old_oplog_files.len() > 10, true);
        declutter(&dbs);
        let old_oplog_files = get_op_log_entries_by_creation_date();
        assert_eq!(old_oplog_files.len(), 9);
        env::set_var("NUN_MAX_OP_LOG_SIZE", "100000000");
    }

    #[test]
    #[cfg(not(tarpaulin))]
    fn write_op_log_should_be_fast() {
        clean_op_log_metadata_files();
        env::set_var("NUN_MAX_OP_LOG_SIZE", "12500000");
        //env::set_var("NUN_LOG_LEVEL", "debug");
        init_logger();
        let mut oplog_file = get_log_file_append_mode();
        let mut i = 0;
        let start = Instant::now();
        //while i < 100_000 {
        while i < 10_000 {
            try_write_op_log(
                &mut oplog_file,
                1,
                1,
                &ReplicateOpp::Update,
                Databases::next_op_log_id(),
            ); // Will free f and close the resource ..
            i = i + 1;
        } // oplog_file is closed here;
        let duration = start.elapsed();
        println!("Time elapsed in total is: {:?} {}", duration, i);
        let time_in_ms = start.elapsed().as_millis();
        // make sure we can do 300k a secound
        assert!(time_in_ms < 300);
    }

    #[test]
    #[cfg(not(tarpaulin))]
    fn should_store_all_keys_if_reclame_space_mode() {
        let (dbs, db_name, db) = create_db_with_10k_keys();
        clean_all_db_files(&db_name);
        let full_name = file_name_from_db_name(&db_name);
        let (_keys_file_name, values_file_name) =
            get_key_value_files_name_from_file_name(full_name);
        let start = Instant::now();
        let changed_keys = storage_data_disk(&db, &db_name, false);
        log::info!("TIme {:?}", start.elapsed());
        assert!(start.elapsed().as_millis() < 100);
        assert_eq!(changed_keys, 10000);

        let start_load = Instant::now();
        let db_file_name = file_name_from_db_name(&db_name);
        let (loaded_db, _) = create_db_from_file_name(&db_file_name, &dbs);
        let time_in_ms = start_load.elapsed().as_millis();
        log::info!("Time to update {:?}ms", time_in_ms);
        assert!(start_load.elapsed().as_millis() < 100);

        let value = loaded_db.get_value(String::from("key_1")).unwrap();
        assert_eq!(value.state, ValueStatus::Ok);

        loaded_db.set_value(&Change::new(
            String::from("key_1"),
            String::from("New-value"),
            2,
        ));

        let value = loaded_db.get_value(String::from("key_1")).unwrap();
        let value_100 = loaded_db.get_value(String::from("key_100")).unwrap();
        let value_1000 = loaded_db.get_value(String::from("key_1000")).unwrap();
        assert_eq!(value.state, ValueStatus::Updated);
        assert_eq!(value_100.state, ValueStatus::Ok);
        assert_eq!(value_100.value, String::from("key_100"));
        assert_eq!(value_1000.value, String::from("key_1000"));

        let file_size_before = get_file_size(&values_file_name);

        let changed_keys = storage_data_disk(&loaded_db, &db_name, false);
        assert_eq!(changed_keys, 1);
        let file_size_after = get_file_size(&values_file_name);
        assert!(file_size_before < file_size_after);

        let changed_keys = storage_data_disk(&loaded_db, &db_name, false);
        assert_eq!(changed_keys, 0);

        let changed_keys = storage_data_disk(&loaded_db, &db_name, true);
        assert_eq!(changed_keys, 10000);
        let file_size_last = get_file_size(&values_file_name);

        assert!(file_size_last < file_size_after);

        remove_database_file(&db_name);
        clean_op_log_metadata_files();
        remove_keys_file();
    }

    fn create_empty_db() -> (Arc<Databases>, String, Database) {
        let dbs = create_test_dbs();
        let db_name = String::from("test-db");
        let hash = HashMap::new();
        let db = Database::create_db_from_hash(
            db_name.clone(),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Newer),
        );
        dbs.is_oplog_valid.store(false, Ordering::Relaxed);
        (dbs, db_name, db)
    }

    fn create_db_with_10k_keys() -> (Arc<Databases>, String, Database) {
        let dbs = create_test_dbs();
        let db_name = String::from("test-db");
        let mut hash = HashMap::new();
        for i in 0..10000 {
            hash.insert(format!("key_{}", i), format!("key_{}", i));
        }
        let db = Database::create_db_from_hash(
            db_name.clone(),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Newer),
        );
        dbs.is_oplog_valid.store(false, Ordering::Relaxed);
        (dbs, db_name, db)
    }

    fn create_test_dbs() -> Arc<Databases> {
        let (sender, _replication_receiver): (Sender<String>, Receiver<String>) = channel(100);
        let keys_map = HashMap::new();
        let dbs = Arc::new(Databases::new(
            String::from(""),
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

    fn remove_keys_file() {
        let key_file_name = get_keys_map_file_name();
        if Path::new(&key_file_name).exists() {
            fs::remove_file(key_file_name).unwrap();
        }
    }

    #[test]
    fn should_store_and_load_keys() {
        let mut keys = HashMap::new();
        keys.insert(String::from("key_1"), 1);
        keys.insert(String::from("key_2"), 2);
        write_keys_map_to_disk(keys);
        let keys_from_fisk = load_keys_map_from_disk();
        assert_eq!(keys_from_fisk.get("key_1"), Some(&1));
        assert_eq!(keys_from_fisk.get("key_2"), Some(&2));
    }

    #[test]
    fn should_create_a_new_file_if_invalidate_file_does_not_exists() {
        remove_invalidate_oplog_file();
        let ifrm = get_invalidate_file_read_mode();
        assert_eq!(ifrm.metadata().unwrap().len(), 0);
        invalidate_oplog(&mut get_invalidate_file_write_mode(), &create_test_dbs()).unwrap();
        let ifrm = get_invalidate_file_read_mode();
        assert_eq!(ifrm.metadata().unwrap().len(), 1);
    }

    #[test]
    fn should_store_all_dbs() {
        let dbs = create_test_dbs();
        dbs.is_oplog_valid.store(false, Ordering::Release);
        let db_name = String::from(format!("test-db_{}", Databases::next_op_log_id()));
        let mut hash = HashMap::new();
        hash.insert(String::from("some"), String::from("value"));
        hash.insert(String::from("some1"), String::from("value1"));
        let db = Database::create_db_from_hash(
            db_name.clone(),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Arbiter),
        );
        dbs.add_database(db);
        dbs.add_db_to_snapshot_by_name(&db_name, false).unwrap();
        let queue_size = { dbs.to_snapshot.read().unwrap().len() };
        assert_eq!(queue_size, 1);
        snapshot_all_pendding_dbs(&dbs);
        let queue_size = { dbs.to_snapshot.read().unwrap().len() };
        assert_eq!(queue_size, 0);

        let dbs_after_save = create_test_dbs();
        load_all_dbs_from_disk(&dbs_after_save);
        let dbs_map = dbs_after_save
            .map
            .read()
            .expect("Could not lock the dbs mutex");
        let loaded_db = dbs_map.get(&db_name).unwrap();
        assert_eq!(
            loaded_db.get_value(String::from("some")).unwrap().value,
            "value"
        );
    }

    #[test]
    fn should_store_all_dbs_and_reclaime_space() {
        let dbs = create_test_dbs();
        let db_name = String::from(format!("test-db_{}", Databases::next_op_log_id()));
        let mut hash = HashMap::new();
        hash.insert(String::from("some"), String::from("value"));
        hash.insert(String::from("some1"), String::from("value1"));
        let db = Database::create_db_from_hash(
            db_name.clone(),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Arbiter),
        );
        dbs.add_database(db);
        dbs.add_db_to_snapshot_by_name(&db_name, false).unwrap();
        let queue_size = { dbs.to_snapshot.read().unwrap().len() };
        assert_eq!(queue_size, 1);
        // Init save small space
        snapshot_all_pendding_dbs(&dbs);
        let queue_size = { dbs.to_snapshot.read().unwrap().len() };
        assert_eq!(queue_size, 0);
        let dbs_after_save = create_test_dbs();
        load_all_dbs_from_disk(&dbs_after_save);
        let dbs_map = dbs_after_save
            .map
            .read()
            .expect("Could not lock the dbs mutex");
        let loaded_db = dbs_map.get(&db_name).unwrap();
        assert_eq!(
            loaded_db.get_value(String::from("some")).unwrap().value,
            "value"
        );
        // ...

        let prev_size = loaded_db.data_disk_size();
        loaded_db.set_value(&Change::new(
            String::from("some"),
            String::from("value-jose"),
            2,
        ));
        dbs_after_save
            .add_db_to_snapshot_by_name(&db_name, false)
            .unwrap();
        // Increment space
        snapshot_all_pendding_dbs(&dbs_after_save);

        let size_after = loaded_db.data_disk_size();
        assert!(prev_size < size_after);

        loaded_db.set_value(&Change::new(
            String::from("some"),
            String::from("value-maria"),
            3,
        ));
        dbs_after_save
            .add_db_to_snapshot_by_name(&db_name, true)
            .unwrap();
        snapshot_all_pendding_dbs(&dbs_after_save);

        let final_size_after_reclame = loaded_db.data_disk_size();
        assert!(size_after > final_size_after_reclame);
    }

    #[test]
    fn should_remove_the_backfile_after_snapshot() {
        let dbs = create_test_dbs();
        let db_name = String::from(format!("test-db_{}", Databases::next_op_log_id()));
        let mut hash = HashMap::new();
        hash.insert(String::from("some"), String::from("value"));
        hash.insert(String::from("some1"), String::from("value1"));
        let db = Database::create_db_from_hash(
            db_name.clone(),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Arbiter),
        );
        dbs.add_database(db);
        dbs.add_db_to_snapshot_by_name(&db_name, false).unwrap();
        let queue_size = { dbs.to_snapshot.read().unwrap().len() };
        assert_eq!(queue_size, 1);
        snapshot_all_pendding_dbs(&dbs);

        let dbs_after_save = create_test_dbs();
        load_all_dbs_from_disk(&dbs_after_save);
        let dbs_map = dbs_after_save
            .map
            .read()
            .expect("Could not lock the dbs mutex");
        let loaded_db = dbs_map.get(&db_name).unwrap();
        loaded_db.set_value(&Change::new(
            String::from("some"),
            String::from("value-jose"),
            2,
        ));
        dbs.add_db_to_snapshot_by_name(&db_name, true).unwrap();
        snapshot_all_pendding_dbs(&dbs);

        let backup_file_name = format!("{}.keys.old", file_name_from_db_name(&db_name));
        assert_eq!(Path::new(&backup_file_name).exists(), false);
    }
}
