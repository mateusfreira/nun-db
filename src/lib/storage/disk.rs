use core::str;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::{create_dir_all, read_dir};
use std::io::prelude::*;
use std::io::BufWriter;
use std::io::SeekFrom;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use crate::bo::ConsensuStrategy;
use crate::bo::DatabaseMataData;
use crate::bo::Databases;
use crate::bo::{Database, Value, ValueStatus};

const DB_KEYS_FILE_NAME: &'static str = "-nun.data.keys";
const BASE_FILE_NAME: &'static str = "-nun.data";
const META_FILE_NAME: &'static str = "-nun.madadata";

const OP_TIME_SIZE: usize = 8;
const OP_KEY_SIZE: usize = 8;
const OP_DB_ID_SIZE: usize = 8;
const OP_OP_SIZE: usize = 1;
const OP_RECORD_SIZE: usize = OP_TIME_SIZE + OP_DB_ID_SIZE + OP_KEY_SIZE + OP_OP_SIZE;

const VERSION_SIZE: usize = 4;
const U64_SIZE: usize = 8;
const U32_SIZE: usize = 4;
const VERSION_DELETED: i32 = -1;
const ADDR_SIZE: usize = 8;

fn get_key_disk_size(key_size: usize) -> u64 {
    (U64_SIZE + key_size + ADDR_SIZE + VERSION_SIZE) as u64
}

pub struct NodeDrive {}
impl NodeDrive {
    fn load_one_db_from_disk(dbs: &Arc<Databases>, entry: std::io::Result<std::fs::DirEntry>) {
        if let Ok(entry) = entry {
            let full_name = entry.file_name().into_string().unwrap();
            if full_name.ends_with(DB_KEYS_FILE_NAME) {
                let (db, _) = create_db_from_file_name(&full_name, &dbs);
                dbs.add_database(db);
            } else {
                log::warn!(
                    "Files {} does not ends with {} will ignore",
                    full_name,
                    DB_KEYS_FILE_NAME
                );
            }
        }
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
                NodeDrive::load_one_db_from_disk(dbs, entry);
            }
        }
    }
    pub fn storage_data_disk(db: &Database, reclame_space: bool, db_name: &String) -> u32 {
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

pub fn meta_file_name_from_db_name(db_name: String) -> String {
    format!(
        "{dir}/{db_name}{sufix}",
        dir = get_dir_name(),
        db_name = db_name,
        sufix = META_FILE_NAME
    )
}

#[cfg(test)]
fn get_dir_name() -> String {
    let thread_id = thread_id::get();
    let unique_db_test_db_name = format!("/tmp/dbs-test-{}", thread_id);
    std::fs::create_dir_all(&unique_db_test_db_name).unwrap();
    return String::from(unique_db_test_db_name);
}

#[cfg(not(test))]
fn get_dir_name() -> String {
    use crate::configuration::NUN_DBS_DIR;
    NUN_DBS_DIR.to_string()
}

pub fn create_db_from_file_name(file_name: &String, dbs: &Arc<Databases>) -> (Database, String) {
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

pub fn db_name_from_file_name(full_name: &String) -> String {
    let partial_name = full_name.replace(BASE_FILE_NAME, "");
    let splited_name: Vec<&str> = partial_name.split("/").collect();
    let db_name = splited_name.last().unwrap();
    return db_name.to_string();
}

pub fn file_name_from_db_name(db_name: &String) -> String {
    format!(
        "{dir}/{db_name}{sufix}",
        dir = get_dir_name(),
        db_name = db_name,
        sufix = BASE_FILE_NAME
    )
}

fn load_db_metadata_from_disk_or_empty(name: String, dbs: &Arc<Databases>) -> DatabaseMataData {
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
