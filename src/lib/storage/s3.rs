use core::str;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

use aws_config::Region;
use aws_sdk_s3::config::Credentials;
use bytes::{BufMut, BytesMut};
use futures::io::Cursor;
use futures::{AsyncReadExt, AsyncSeekExt};
use std::collections::HashMap;
use tokio::runtime::Runtime;

use crate::bo::{ConsensuStrategy, Database, DatabaseMataData, Databases, Value, ValueStatus};
use crate::configuration::{
    NUN_S3_API_URL, NUN_S3_BUCKET, NUN_S3_KEY_ID, NUN_S3_PREFIX, NUN_S3_SECRET_KEY,
    NUN_S3_MAX_INFLIGHT_REQUESTS,
};

const VERSION_SIZE: usize = 4;
const ADDR_SIZE: usize = 8;
const U64_SIZE: usize = 8;
// const U32_SIZE: usize = 4;

const OP_KEY_SIZE: usize = 8;
const OP_DB_ID_SIZE: usize = 8;
const OP_TIME_SIZE: usize = 8;
const OP_OP_SIZE: usize = 1;
const OP_RECORD_SIZE: usize = OP_TIME_SIZE + OP_DB_ID_SIZE + OP_KEY_SIZE + OP_OP_SIZE;

fn get_key_disk_size(key_size: usize) -> u64 {
    (U64_SIZE + key_size + ADDR_SIZE + VERSION_SIZE) as u64
}

fn get_keys_to_push(db: &Database, reclame_space: bool) -> Vec<(String, Value)> {
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

pub struct S3Storage {}
impl S3Storage {
    pub fn storage_data_on_cloud(db: &Database, reclame_space: bool, db_name: &String) -> u32 {
        let mut changed_keys = 0;
        let rt = Runtime::new().unwrap();
        let keys_to_update = get_keys_to_push(db, reclame_space);

        let key_buffer = BytesMut::with_capacity(OP_RECORD_SIZE * 10);
        //@todo should this really be te buffer size of the values????
        let value_buffer = BytesMut::with_capacity(OP_RECORD_SIZE * 10);
        {
            let mut keys_file = key_buffer;
            let mut values_file = value_buffer;
            let current_key_file_size = 0;

            //let (mut values_file, current_value_file_size) = get_values_file_append_mode(&db_name, reclame_space);
            // To inplace update
            log::debug!("current_key_file_size: {}", current_key_file_size);

            let mut value_addr = u64::from(0 as u64); //current_value_file_size;
            let mut next_key_addr = current_key_file_size;

            for (key, value) in keys_to_update {
                changed_keys = changed_keys + 1;

                values_file.put_slice(&value.value.len().to_le_bytes());
                //8bytes
                let value_as_bytes = value.value.as_bytes();
                //Nth bytes
                values_file.put_slice(&value_as_bytes);
                //4 bytes
                values_file.put_slice(&value.state.to_le_bytes());
                let record_size = (U64_SIZE + value_as_bytes.len() + VERSION_SIZE) as u64;
                log::debug!(
                    "Write key: {}, addr: {} value_addr: {}, record_size: {}",
                    key,
                    next_key_addr,
                    value_addr,
                    record_size
                );
                // Append key file
                // Write key

                let len = key.len();

                //8bytes
                keys_file.put_slice(&len.to_le_bytes());
                //Nth bytes
                keys_file.put_slice(&key.as_bytes());
                //4 bytes
                keys_file.put_slice(&value.version.to_le_bytes());
                //8 bytes
                keys_file.put_slice(&value_addr.to_le_bytes());
                let key_size = get_key_disk_size(key.len());
                db.set_value_as_ok(
                    &key,
                    &value,
                    value_addr,
                    next_key_addr,
                    Databases::next_op_log_id(),
                );
                value_addr = value_addr + record_size;
                log::debug!("Next Value addr: {}", value_addr);
                next_key_addr = next_key_addr + key_size;
            }

            //keys_file.flush().unwrap();
            rt.block_on(S3Storage::store_buffer_to_s3(
                keys_file,
                &format!("{}/nun.keys", db_name),
            ));
            rt.block_on(S3Storage::store_buffer_to_s3(
                values_file,
                &format!("{}/nun.values", db_name),
            ));
        }
        //keys_file.
        //values_file.flush().unwrap();
        //write_metadata_file(db_name, db);
        //log::debug!("snapshoted {} keys", changed_keys);
        changed_keys
    }

    async fn store_buffer_to_s3(mut buff: BytesMut, db_name: &String) -> Option<bool> {
        let key = format!("{}/{}", NUN_S3_PREFIX.to_string(), db_name);

        let url = NUN_S3_API_URL.as_str();
        let bucket = NUN_S3_BUCKET.as_str();
        let key_id = NUN_S3_KEY_ID.as_str();
        let secret_key = NUN_S3_SECRET_KEY.as_str();
        log::debug!(
            "Reading from s3, buket: {}, key_id: {}, secret_key: {}, server: {}\n",
            bucket,
            key_id,
            secret_key,
            NUN_S3_API_URL.as_str()
        );

        let cred = Credentials::new(key_id, secret_key, None, None, "loaded-from-custom-env");
        let s3_config = aws_sdk_s3::config::Builder::new()
            .endpoint_url(url)
            .credentials_provider(cred)
            .region(Region::new("us-east"))
            .force_path_style(true)
            .build();

        let client = aws_sdk_s3::Client::from_conf(s3_config);
        let body = aws_sdk_s3::primitives::ByteStream::from(buff.split().freeze());

        match client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .send()
            .await
        {
            Ok(_) => Some(true),
            Err(_) => None,
        }
    }

    fn read_data_from_cloud(db_name: &String) -> Option<Database> {
        let keys_key_file = format!("{}/{}/nun.keys", NUN_S3_PREFIX.to_string(), db_name);
        let values_key_file = format!("{}/{}/nun.values", NUN_S3_PREFIX.to_string(), db_name);

        let url = NUN_S3_API_URL.as_str();
        let bucket = NUN_S3_BUCKET.as_str();
        let key_id = NUN_S3_KEY_ID.as_str();
        let secret_key = NUN_S3_SECRET_KEY.as_str();
        log::debug!("Reading from s3, key: {}, values: {}, buket: {}, key_id: {}, secret_key: {}, server: {}\n", keys_key_file, values_key_file, bucket, key_id, secret_key, url);

        let cred = Credentials::new(key_id, secret_key, None, None, "loaded-from-custom-env");
        let mut value_data: HashMap<String, Value> = HashMap::new();
        let s3_config = aws_sdk_s3::config::Builder::new()
            .endpoint_url(NUN_S3_API_URL.as_str())
            .credentials_provider(cred)
            .region(Region::new("us-east"))
            .force_path_style(true)
            .build();

        let client = aws_sdk_s3::Client::from_conf(s3_config);
        let rt = Runtime::new().unwrap();
        let mut values_cursor = rt.block_on(async {
            let r = client
                .get_object()
                .bucket(bucket)
                .key(values_key_file)
                .send()
                .await
                .unwrap();
            Cursor::new(r.body.collect().await.unwrap().into_bytes())
        });
        rt.block_on(async {
            match client
                .get_object()
                .bucket(bucket)
                .key(keys_key_file)
                .send()
                .await
            {
                Ok(r) => {
                    log::debug!("Reading the file");
                    let keys_file = r.body.collect().await.unwrap().into_bytes();
                    let mut keys_cursor = Cursor::new(keys_file);

                    //bytes.read(buf)
                    let mut length_buffer = [0; U64_SIZE];
                    let mut value_addr_buffer = [0; U64_SIZE];
                    let mut version_buffer = [0; VERSION_SIZE];

                    while let Ok(read) = keys_cursor.read(&mut length_buffer).await {
                        if read == 0 {
                            //If could not read anything stop
                            break;
                        }

                        //Read key
                        let key_length: usize = usize::from_le_bytes(length_buffer);
                        let mut key_buffer = vec![0; key_length];

                        keys_cursor.read(&mut key_buffer).await.unwrap();
                        let key = str::from_utf8(&key_buffer).unwrap();
                        log::debug!("{}, key: {}", key_length, key);

                        //Read version
                        let _ = keys_cursor.read(&mut version_buffer).await.unwrap();
                        let version = i32::from_le_bytes(version_buffer);

                        //Read value addr
                        let _ = keys_cursor.read(&mut value_addr_buffer).await.unwrap();
                        let value_addr = u64::from_le_bytes(value_addr_buffer);

                        log::debug!("Value addr {}", value_addr);
                        log::debug!("Value size {}", values_cursor.get_ref().len());
                        let before_cursor_position = values_cursor.position();
                        values_cursor.seek(std::io::SeekFrom::Start(value_addr)).await.unwrap();

                        //let mut length_buffer = [0; U64_SIZE];
                        values_cursor.read(&mut length_buffer).await.unwrap();
                        let value_length: usize = usize::from_le_bytes(length_buffer);
                        log::debug!("Value length {}", value_length);
                        let mut value_buffer = vec![0; value_length];
                        let value_cursor_position = values_cursor.position();
                        log::debug!("keys_Cursor position {}, before reading", value_cursor_position);
                        values_cursor.read(&mut value_buffer).await.unwrap();
                        let value = str::from_utf8(&value_buffer).unwrap();
                        log::debug!("Value: {} after readig", value);

                        let after_cursor_position = values_cursor.position();
                        log::debug!("Will add the value : {} to the hash, length: {}, cursor position, {}, before: {}, after: {}", value, value_length, value_cursor_position, before_cursor_position, after_cursor_position);

                        let value_object = Value {
                            version,
                            value: value.to_string(),
                            state: ValueStatus::Ok,
                            value_disk_addr: value_addr,
                            key_disk_addr: keys_cursor.position(),// todo Is this needed?
                            opp_id: Databases::next_op_log_id(),
                        };

                        //Read value value
                        log::debug!("Adding key: {}, value: {}", key, value);
                        value_data.insert(key.to_string(), value_object);
                    }
                    Some(Database::create_db_from_value_hash(
                        db_name.to_string(),
                        value_data,
                        DatabaseMataData::new(1, ConsensuStrategy::Arbiter),
                    ))
                }
                Err(e) => {
                    log::debug!("{}", e);
                    None
                }
            }
        })
    }

    pub fn load_all_dbs_from_cloud(dbs: &Arc<Databases>) {
        let start = std::time::Instant::now();
        let bucket = NUN_S3_BUCKET.as_str();
        let key_id = NUN_S3_KEY_ID.as_str();
        let secret_key = NUN_S3_SECRET_KEY.as_str();

        let cred = Credentials::new(key_id, secret_key, None, None, "loaded-from-custom-env");
        let s3_config = aws_sdk_s3::config::Builder::new()
            .endpoint_url(NUN_S3_API_URL.as_str())
            .credentials_provider(cred)
            .region(Region::new("us-east"))
            .force_path_style(true)
            .build();

        let client = aws_sdk_s3::Client::from_conf(s3_config);
        let rt = Runtime::new().unwrap();
        let objects = rt.block_on(async {
            client
                .list_objects_v2()
                .set_prefix(Some(NUN_S3_PREFIX.to_string()))
                .bucket(bucket)
                .send()
                .await
                .unwrap()
                .contents()
                .into_iter()
                .flat_map(|x| x.key())
                .map(ToString::to_string)
                .collect::<Vec<String>>()
        });
        log::debug!("Objects: {:?}", objects);
        let prefix_to_clean = format!("{}/", &NUN_S3_PREFIX.to_string());
        let db_names = objects
            .iter()
            .filter(|x| x.contains("nun.values")) // Filter only the values files
            .map(|x| x.to_string().replacen(&prefix_to_clean, "", 1))
            .map(|x| {
                let mut paths = x.split("/").collect::<Vec<&str>>();
                paths.truncate(paths.len() - 1);
                paths.join("/")
            })
            .collect::<Vec<String>>();
        log::debug!("DbsNames: {:?}", db_names);
        let running_threads = Arc::new(AtomicUsize::new(0));
        let dbs_threads: Vec<thread::JoinHandle<()>>= db_names.iter().map(move |db_name| {
            let running_threads = running_threads.clone();
            let dbs = dbs.clone();
            let db_name = db_name.clone();
            let db_thread = thread::spawn(move || {
                wait_and_lock(&running_threads);
                let start_db_load = std::time::Instant::now();
                log::info!("Thread running : {}", running_threads.load(Ordering::Acquire));
                let db = S3Storage::read_data_from_cloud(&db_name).unwrap();
                log::info!("Loaded db: {} in {:?}", db_name, start_db_load.elapsed());
                dbs.add_database(db);
                release_lock(&running_threads);
            });
            db_thread
        }).collect();
        dbs_threads.into_iter().for_each(|x| x.join().unwrap());
        log::info!("Loaded all dbs from cloud in {:?}", start.elapsed());
    }
}

fn release_lock(running_threads: &Arc<AtomicUsize>) {
    running_threads.fetch_sub(1, Ordering::SeqCst);
}
fn wait_and_lock(running_threads: &Arc<AtomicUsize>) {
    loop {
        let prev_val = running_threads.load(Ordering::Acquire);
        if prev_val > *NUN_S3_MAX_INFLIGHT_REQUESTS {
            log::debug!("Too many threads running, waiting for a slot to open");
            thread::sleep(std::time::Duration::from_millis(10));
            continue
        } else {
            log::debug!("Thread running await: {}", prev_val);
        }
        match running_threads.compare_exchange(prev_val, prev_val + 1, Ordering::Acquire, Ordering::Acquire) {
            Ok(_) => break,
            Err(_) => {
                log::debug!("Thread failed to update the counter : {}", prev_val);
                thread::sleep(std::time::Duration::from_millis(10));
                continue
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use core::time;
    use std::{collections::HashMap, sync::atomic::Ordering, thread};

    use super::*;
    //use env_logger::{Builder, Env, Target};
    use futures::channel::mpsc::{channel, Receiver, Sender};

    use crate::{
        bo::{Change, ClusterRole, ConsensuStrategy, Database, DatabaseMataData},
        disk_ops::Oplog,
    };

    /*
    fn init_logger() {
        let env = Env::default().filter_or("NUN_LOG_LEVEL", "debug");
        Builder::from_env(env)
            .format_level(false)
            .target(Target::Stdout)
            .format_timestamp_nanos()
            .init();
    }
    */

    #[test]
    fn should_store_data_in_s3() {
        //init_logger();
        let db = create_test_db();
        S3Storage::storage_data_on_cloud(&db, true, &String::from("test"));
        let db = S3Storage::read_data_from_cloud(&String::from("test")).unwrap();
        assert!(db.count_keys() == 5);
        log::debug!("{:?}", db.get_value("some".to_string()).unwrap());
        assert!(db.get_value("some".to_string()).unwrap() == String::from("value"));
    }

    #[test]
    fn should_read_all_dbs_from_s3() {
        //init_logger();
        let db = create_test_db();
        let db1 = create_test_db();

        let change = Change::new(
            String::from("this_is_totally_new"),
            String::from("jose"),
            -1,
        );
        db1.set_value(&change);
        S3Storage::storage_data_on_cloud(&db, true, &String::from("test"));
        S3Storage::storage_data_on_cloud(&db1, true, &String::from("test-new-test"));
        let db = S3Storage::read_data_from_cloud(&String::from("test")).unwrap();

        assert!(db.count_keys() == 5);
        println!("Value before assert in CI: {:?}", db.get_value("some".to_string()).unwrap());
        assert!(db.get_value("some".to_string()).unwrap() == String::from("value"));

        let dbs = prep_env();
        S3Storage::load_all_dbs_from_cloud(&dbs);
        let dbs_hash = dbs.acquire_dbs_read_lock();
        let db = dbs_hash.get("test").unwrap();
        let db1 = dbs_hash.get("test-new-test").unwrap();

        assert!(db.count_keys() == 5);
        assert!(db.get_value("some".to_string()).unwrap() == String::from("value"));
        assert!(db1.get_value("this_is_totally_new".to_string()).unwrap() == String::from("jose"));
    }

    fn create_test_db() -> Database {
        let db_name = String::from("test-db");
        let mut hash = HashMap::new();
        hash.insert(String::from("some"), String::from("value"));
        hash.insert(String::from("some1"), String::from("value1"));
        hash.insert(String::from("some2"), String::from("value1"));
        hash.insert(String::from("some3"), String::from("value1"));
        hash.insert(String::from("some4"), String::from("value1"));
        let db = Database::create_db_from_hash(
            db_name.clone(),
            hash,
            DatabaseMataData::new(0, ConsensuStrategy::Arbiter),
        );
        db
    }

    fn prep_env() -> Arc<Databases> {
        Oplog::clean_op_log_metadata_files();
        thread::sleep(time::Duration::from_millis(50)); //give it time to the opperation to happen
        let (sender, _): (Sender<String>, Receiver<String>) = channel(100);

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
}
