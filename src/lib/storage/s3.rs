use std::io::Write;

use aws_config::Region;
use aws_sdk_s3::config::Credentials;
use bytes::{BufMut, BytesMut};

use crate::bo::{Database, Databases, Value, ValueStatus};

const VERSION_SIZE: usize = 4;
const ADDR_SIZE: usize = 8;
const U64_SIZE: usize = 8;
const U32_SIZE: usize = 4;

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
    pub fn storage_data_disk(db: &Database, reclame_space: bool, db_name: &String) -> u32 {
        let keys_to_update = get_keys_to_push(db, reclame_space);

        let key_buffer = BytesMut::with_capacity(OP_RECORD_SIZE * 10);
        //@todo should this really be te buffer size of the values????
        let value_buffer = BytesMut::with_capacity(OP_RECORD_SIZE * 10);

        let mut keys_file = key_buffer.writer();
        let mut values_file = value_buffer.writer();
        let current_key_file_size = 0;

        //let (mut values_file, current_value_file_size) = get_values_file_append_mode(&db_name, reclame_space);
        // To inplace update
        log::debug!("current_key_file_size: {}", current_key_file_size);

        let mut value_addr = u64::from(0 as u64); //current_value_file_size;
        let mut next_key_addr = current_key_file_size;
        let mut changed_keys = 0;

        for (key, value) in keys_to_update {
            match value.state {
                ValueStatus::Ok => {
                    if !reclame_space {
                        panic!("Values Ok should never get here")
                    } else {
                        changed_keys = changed_keys + 1;

                        values_file.write(&value.value.len().to_le_bytes()).unwrap();
                        //8bytes
                        let value_as_bytes = value.value.as_bytes();
                        //Nth bytes
                        values_file.write(&value_as_bytes).unwrap();
                        //4 bytes
                        values_file.write(&value.state.to_le_bytes()).unwrap();
                        let record_size = (U64_SIZE + value_as_bytes.len() + VERSION_SIZE) as u64;
                        log::debug!(
                            "Write key: {}, addr: {} value_addr: {} ",
                            key,
                            next_key_addr,
                            value_addr
                        );
                        // Append key file
                        // Write key

                        let len = key.len();
                        //8bytes
                        keys_file.write(&len.to_le_bytes()).unwrap();
                        //Nth bytes
                        keys_file.write(&key.as_bytes()).unwrap();
                        //4 bytes
                        keys_file.write(&value.version.to_le_bytes()).unwrap();
                        //8 bytes
                        keys_file.write(&value_addr.to_le_bytes()).unwrap();
                        let key_size = get_key_disk_size(key.len());
                        db.set_value_as_ok(
                            &key,
                            &value,
                            value_addr,
                            next_key_addr,
                            Databases::next_op_log_id(),
                        );
                        value_addr = value_addr + record_size;
                        next_key_addr = next_key_addr + key_size;
                    }
                }
                ValueStatus::New => {}

                ValueStatus::Updated => {}
                ValueStatus::Deleted => {}
            }
        }

        keys_file.flush().unwrap();
        //keys_file.
        S3Storage::store_buffer_to_s3(key_buffer);
        values_file.flush().unwrap();
        //write_metadata_file(db_name, db);
        log::debug!("snapshoted {} keys", changed_keys);
        changed_keys
    }

    async fn store_buffer_to_s3(mut buff: BytesMut) -> Option<bool> {
        let bucket = "nun-db";
        let key = "this-is-going-to-be-amazing/focking-key.text";
        let key_id = "B5ODvogu80CtAKna";
        let secret_key = "B5ODvogu80CtAKna-dush";
        let url = "http://127.0.0.1:9000";
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
            //.body(body)
            .body(body)
            .send()
            .await
        {
            Ok(_) => Some(true),
            Err(_) => None,
        }
    }
    //0
}


#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};
    use futures::channel::mpsc::{channel, Receiver, Sender};

    use super::*;

    use crate::bo::{ClusterRole, ConsensuStrategy, Database, DatabaseMataData};

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
            .swap(ClusterRole::Primary as usize, std::sync::atomic::Ordering::Relaxed);
        dbs
    }

    #[test]
    fn should_store_data_in_s3() {
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
        S3Storage::storage_data_disk(&db, true, &String::from("test"));
    }

}
