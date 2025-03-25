use core::str;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

use aws_config::Region;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::Client;
use bytes::{BufMut, BytesMut};
use futures::io::Cursor;
use futures::AsyncReadExt;
use std::collections::HashMap;
use tokio::runtime::Runtime;

use crate::bo::{ConsensuStrategy, Database, DatabaseMataData, Databases, Value, ValueStatus};
use crate::configuration::{
    NUN_S3_API_URL, 
    NUN_S3_BUCKET, 
    NUN_S3_KEY_ID, 
    NUN_S3_MAX_INFLIGHT_REQUESTS, 
    NUN_S3_PREFIX,
    NUN_S3_SECRET_KEY,
    NUN_S3_NUMBER_OF_PARTITIONS
};
use crate::storage::common::get_keys_by_filter;

use super::common::get_keys_to_update;

const VERSION_SIZE: usize = 4;
const U64_SIZE: usize = 8;
// const U32_SIZE: usize = 4;

const OP_KEY_SIZE: usize = 8;
const OP_DB_ID_SIZE: usize = 8;
const OP_TIME_SIZE: usize = 8;
const OP_OP_SIZE: usize = 1;
const OP_RECORD_SIZE: usize = OP_TIME_SIZE + OP_DB_ID_SIZE + OP_KEY_SIZE + OP_OP_SIZE;

pub struct S3Storage {}
impl S3Storage {
    pub fn hash(key: String) -> u64 {
        // Todo get this out of here
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
    pub fn storage_data_on_cloud(db: &Database, reclame_space: bool, db_name: &String) -> u32 {
        // Get this out of here
        let mut changed_keys = 0;
        let rt = Runtime::new().unwrap();
        let keys_to_update = get_keys_to_update(db, reclame_space);
        // Define what patitions needs update
        let mut partitions_to_update = keys_to_update
            .into_iter()
            .map(|(k, _v)| {
                get_patirion_from_key(&k)
            })
            .collect::<Vec<_>>();
        partitions_to_update.sort();
        partitions_to_update.dedup();
        // Find other keys in the same parition
        partitions_to_update
            .into_iter()
            .for_each(|partition| {
                // Slow uses less memory at a time but costs more in CPU
                // There will be a small lock in the DB object for each partition here.
                // I think this is better than a long lock
                let keys_in_patition = get_keys_by_filter(&db, &|key, _v| {
                    get_patirion_from_key(key) == partition
                });
                let mut file_buffer: BytesMut = BytesMut::with_capacity(OP_RECORD_SIZE * 10);
                for (key, value) in keys_in_patition {
                    log::debug!("Key: {} Value: {}", key, value.value);
                    changed_keys = changed_keys + 1;
                    let len = key.len();
                    //8bytes
                    file_buffer.put_slice(&len.to_le_bytes());
                    //Nth bytes
                    file_buffer.put_slice(&key.as_bytes());
                    // Writing the value
                    file_buffer.put_slice(&value.value.len().to_le_bytes());
                    //8bytes
                    let value_as_bytes = value.value.as_bytes();
                    //Nth bytes
                    file_buffer.put_slice(&value_as_bytes);
                    //4 bytes
                    //file_buffer.put_slice(&value.state.to_le_bytes());
                    file_buffer.put_slice(&ValueStatus::Ok.to_le_bytes());

                    //4 bytes
                    file_buffer.put_slice(&value.version.to_le_bytes());

                    db.set_value_as_ok(
                        &key,
                        &value,
                        partition, // Use partition id here to know where to store
                        partition, // Use partition id here to know where to store
                        Databases::next_op_log_id(),
                    );
                }
                log::debug!(
                    "Will store the database {} partition {}",
                    db_name,
                    partition
                );
                let store_result = rt.block_on(S3Storage::store_buffer_to_s3(
                    file_buffer,
                    &format!("{}/{}.nun", db_name, partition),
                ));
                match store_result { 
                    Ok(ok) => log::debug!("S3Storage::store_buffer_to_s3 {}", ok),
                    Err(err) => panic!("Error to store database {}", err),
                }
            });
        log::debug!("snapshoted {} keys", changed_keys);
        changed_keys
    }

    async fn store_buffer_to_s3(mut buff: BytesMut, db_name: &String) -> Result<bool, String> {
        let key = format!("{}/{}", NUN_S3_PREFIX.to_string(), db_name);

        let bucket = NUN_S3_BUCKET.as_str();

        log::debug!(
            "Reading from s3, bucket: {}\n",
            bucket,
        );

        let client = build_s3_client();
        let body = aws_sdk_s3::primitives::ByteStream::from(buff.split().freeze());

        match client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(msg) => Err(String::from(msg.to_string())),
        }
    }

    fn read_data_from_cloud(db_name: &String) -> Result<Database, String> {
        let rt = Runtime::new().unwrap();
        let mut value_data: HashMap<String, Value> = HashMap::new();
        let bucket = NUN_S3_BUCKET.as_str();
        let client = build_s3_client();
        // Read the list of partitions?
        let partition_list = get_patirion_list_form_s3(&rt, &client, &db_name, &bucket);
        let partitio_readin_results = partition_list.into_iter().map(|partition| {
            let partition_file = format!(
                "{}/{}/{}.nun",
                NUN_S3_PREFIX.to_string(),
                db_name,
                partition
            );

            let read_result: Result<(), String> = rt.block_on(async {
                match client
                    .get_object()
                    .bucket(bucket)
                    .key(&partition_file)
                    .send()
                    .await
                {
                    Ok(r) => {
                        let partition =
                            partition.split(".").next().unwrap().parse::<u64>().unwrap();
                        log::debug!("Reading the file {}", &partition_file);
                        let nun_file = r.body.collect().await.unwrap().into_bytes();

                        let mut file_cursor = Cursor::new(nun_file);
                        let mut key_length_buffer = [0; U64_SIZE];
                        while let Ok(read) = file_cursor.read(&mut key_length_buffer).await {
                            if read == 0 {
                                break;
                            }
                            // Reading key
                            let key = read_str_value(key_length_buffer, &mut file_cursor).await;
                            // Reading value
                            let mut value_length_buffer = [0; U64_SIZE];
                            file_cursor.read(&mut value_length_buffer).await.unwrap();

                            let value = read_str_value(value_length_buffer, &mut file_cursor).await;

                            let mut status_length_buffer = [0; VERSION_SIZE];
                            file_cursor.read(&mut status_length_buffer).await.unwrap();
                            let status = i32::from_le_bytes(status_length_buffer);
                            let mut version_length_buffer = [0; VERSION_SIZE];
                            file_cursor.read(&mut version_length_buffer).await.unwrap();
                            let version = i32::from_le_bytes(version_length_buffer);


                            let value_instance = Value {
                                value: value.to_string(),
                                version,
                                opp_id: Databases::next_op_log_id(),
                                state: ValueStatus::from(status),
                                value_disk_addr: partition,
                                key_disk_addr: 0,
                            };
                            value_data.insert(key.to_string(), value_instance);
                            log::debug!(
                                " Value {} added to key {} in the database {}",
                                value,
                                key,
                                &db_name
                            );
                        }
                    }
                    Err(e) => {
                        log::error!("{} trying to load the databse", e);
                        return Err(String::from(format!("Fail to load partition {} from s3.", partition)))
                    }
                };
                Ok(())
            });
            read_result
        });
        let has_any_partition_failed = partitio_readin_results.fold(Ok(()), error_if_error);
        if let Err(msg) = has_any_partition_failed {
            Err(String::from(format!("Fail to load files from s3: {}", msg)))
        } else {
            Ok(Database::create_db_from_value_hash(
                db_name.to_string(),
                value_data,
                DatabaseMataData::new(1, ConsensuStrategy::Arbiter),
            ))
        }
    }

    pub fn load_all_dbs_from_cloud<'a>(dbs: &'a Arc<Databases>) {
        let rt = Runtime::new().unwrap();
        let start = std::time::Instant::now();
        let bucket = NUN_S3_BUCKET.as_str();
        let client = build_s3_client();
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
        let mut db_names = objects
            .iter()
            .filter(|x| x.contains(".nun")) // Filter only the values files
            .map(|x| x.to_string().replacen(&prefix_to_clean, "", 1))
            .map(|x| {
                let mut paths = x.split("/").collect::<Vec<&str>>();
                paths.truncate(paths.len() - 1);
                paths.join("/")
            })
            .collect::<Vec<String>>();
        db_names.dedup();
        log::debug!("DbsNames: {:?}", db_names);
        let running_threads = Arc::new(AtomicUsize::new(0));
        let dbs_threads: Vec<thread::JoinHandle<()>> = db_names
            .iter()
            .map(move |db_name| {
                let running_threads = running_threads.clone();
                let dbs = dbs.clone();
                let db_name = db_name.clone();
                let db_thread = thread::spawn(move || {
                    await_thread_availability(&running_threads);
                    let start_db_load = std::time::Instant::now();
                    let db = match S3Storage::read_data_from_cloud(&db_name) {
                        Ok(db) => db,
                        Err(e) => panic!("Fail to load db from s3 {}, error: {}", db_name, e)
                    };
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

fn build_s3_client() -> Client {
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
    client
}

fn get_patirion_list_form_s3(rt: &Runtime, client: &Client, db_name: &String, bucket: &str) -> Vec<String> {
    let partition_list = rt.block_on(async {
        client
            .list_objects_v2()
            .set_prefix(Some(format!("{}/{}", NUN_S3_PREFIX.to_string(), db_name)))
            .bucket(bucket)
            .send()
            .await
            .unwrap()
            .contents()
            .into_iter()
            .flat_map(|x| x.key())
            .map(ToString::to_string)
            .map(|s| s.split("/").last().unwrap().to_string())
            .map(|s| s.split(".").next().unwrap().to_string())
            .collect::<Vec<String>>()
    });
    partition_list
}

async fn read_str_value(
    value_length_buffer: [u8; 8],
    file_cursor: &mut Cursor<bytes::Bytes>,
) -> String {
    let value_length = usize::from_le_bytes(value_length_buffer);

    let mut value_buffer = vec![0; value_length];
    file_cursor.read(&mut value_buffer).await.unwrap();
    let value = str::from_utf8(&value_buffer).unwrap();
    String::from(value)
}

fn release_lock(running_threads: &Arc<AtomicUsize>) {
    running_threads.fetch_sub(1, Ordering::SeqCst);
}

fn await_thread_availability(running_threads: &Arc<AtomicUsize>) {
    loop {
        let prev_val = running_threads.load(Ordering::Acquire);
        if prev_val > *NUN_S3_MAX_INFLIGHT_REQUESTS {
            log::debug!("Too many threads running, waiting for a slot to open");
            thread::sleep(std::time::Duration::from_millis(10));
            continue;
        } else {
            log::debug!("Thread running await: {}", prev_val);
        }
        match running_threads.compare_exchange(
            prev_val,
            prev_val + 1,
            Ordering::Acquire,
            Ordering::Acquire,
        ) {
            Ok(_) => break,
            Err(_) => {
                log::debug!("Thread failed to update the counter : {}", prev_val);
                thread::sleep(std::time::Duration::from_millis(10));
                continue;
            }
        }
    }
}

fn error_if_error<T, E>(a: Result<T, E>, b: Result<T, E>) -> Result<T, E> {
    if let Err(_) = a {
        return a;
    } else {
        b
    }
}

fn get_patirion_from_key(key: &String) ->  u64 {
    S3Storage::hash(key.to_string()) % *NUN_S3_NUMBER_OF_PARTITIONS
}

#[cfg(test)]
mod tests {
    use core::time;
    use std::{collections::HashMap, sync::atomic::Ordering, thread};

    use super::*;
    use env_logger::{Builder, Env, Target};
    use futures::channel::mpsc::{channel, Receiver, Sender};

    use crate::{
        bo::{Change, ClusterRole, ConsensuStrategy, Database, DatabaseMataData},
        disk_ops::Oplog,
    };

    fn init_logger() {
        if log::log_enabled!(log::Level::Info) {
            return;
        }
        let env = Env::default().filter_or("NUN_LOG_LEVEL", "debug");
        Builder::from_env(env)
            .format_level(false)
            .target(Target::Stdout)
            .format_timestamp_nanos()
            .init();
    }

    #[test]
    fn should_return_error_if_error() {
        let a = error_if_error::<(), String>(Ok(()), Ok(()));
        assert_eq!(a, Ok(()));

        let a = error_if_error(Ok(()), Err(String::from("Something whent wrong")));
        assert_eq!(a, Err(String::from("Something whent wrong")));
    }

    #[test]
    fn should_store_data_in_s3() {
        //init_logger();
        let db = create_test_db();
        let db_name_id = Databases::next_op_log_id();
        let final_db_name = String::from(format!("should_store_data_in_s3_test_{}", db_name_id));
        S3Storage::storage_data_on_cloud(&db, true, &final_db_name);
        let db = S3Storage::read_data_from_cloud(&final_db_name).unwrap();
        assert!(db.count_keys() == 5);
        log::debug!("{:?}", db.get_value("some".to_string()).unwrap());
        assert!(db.get_value("some".to_string()).unwrap() == String::from("value"));
        assert!(db.get_value("some".to_string()).unwrap().version == 1);

    }

    #[test]
    fn should_store_a_lot_data_in_s3() {
        init_logger();
        let db = create_test_db();
        let db_name_id = Databases::next_op_log_id();
        let final_db_name = String::from(format!("should_store_data_in_s3_test_{}", db_name_id));
        for n in 0..30_000 {
            let s_value = String::from(format!("{:?}", n));
            db.set_value(&Change::new(s_value.clone(), s_value, 0));
        }
        S3Storage::storage_data_on_cloud(&db, false, &final_db_name);

        let db_after = S3Storage::read_data_from_cloud(&final_db_name).unwrap();

        assert!(db_after.count_keys() == 30_005);
        assert!(db.get_value("some".to_string()).unwrap() == String::from("value"));
        db_after.set_value(&Change::new(
            String::from("some"),
            String::from("value_new"),
            2,
        ));
        let changed_keys = S3Storage::storage_data_on_cloud(&db_after, false, &final_db_name);

        assert!(changed_keys == 3068, "Changed keys: {}", changed_keys); // Tho only one key
                                                                         // changed we still have
                                                                         // to update +10% of the
                                                                         // data
        let db_after_update = S3Storage::read_data_from_cloud(&final_db_name).unwrap();
        let value_obj = db_after_update.get_value("some".to_string()).unwrap();
        assert!(
             value_obj.value == String::from("value_new")
        );

        log::debug!("Version {:?}", value_obj.version); 
        assert!(
             value_obj.version == 3
        );
    }

    #[test]
    fn should_read_all_dbs_from_s3() {
        init_logger();
        let data_base_prefix = Databases::next_op_log_id();
        let db1_name = String::from(format!(
            "test-should_read_all_dbs_from_s3_{}",
            data_base_prefix
        ));
        let db_name = String::from(format!("test-read_all_dbs_from_s3_{}", data_base_prefix));
        let db = create_test_db();
        let db1 = create_test_db();

        let change = Change::new(
            String::from("this_is_totally_new"),
            String::from("jose"),
            -1,
        );
        db1.set_value(&change);
        S3Storage::storage_data_on_cloud(&db, true, &db_name);
        S3Storage::storage_data_on_cloud(&db1, true, &db1_name);
        let db = S3Storage::read_data_from_cloud(&db1_name).unwrap();
        log::debug!(
            "{:?}, count, keys {:?}",
            db.count_keys(),
            db.list_keys(&String::from("*"), true)
        );
        assert!(db.count_keys() == 6);
        assert!(db.get_value("some".to_string()).unwrap() == String::from("value"));

        let dbs = prep_env();
        S3Storage::load_all_dbs_from_cloud(&dbs);
        let dbs_hash = dbs.acquire_dbs_read_lock();
        let db = dbs_hash.get(&db1_name).unwrap();
        let db1 = dbs_hash.get(&db1_name).unwrap();

        assert!(db.count_keys() == 6);
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
