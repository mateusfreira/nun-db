use core::str;

use aws_config::Region;
use aws_sdk_s3::config::Credentials;
use bytes::{BufMut, BytesMut};
use futures::io::Cursor;
use futures::{AsyncReadExt, AsyncSeekExt};
use std::collections::HashMap;
use tokio::runtime::Runtime;

use crate::bo::{ConsensuStrategy, Database, DatabaseMataData, Databases, Value, ValueStatus};
use crate::configuration::{NUN_S3_API_URL, NUN_S3_BUCKET, NUN_S3_KEY_ID, NUN_S3_SECRET_KEY};

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
    pub fn storage_data_on_cloud(db: &Database, reclame_space: bool, db_name: &String) -> u32 {
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
            let mut changed_keys = 0;

            for (key, value) in keys_to_update {
                match value.state {
                    ValueStatus::New => {
                        if !reclame_space {
                            panic!("Values Ok should never get here")
                        } else {
                            changed_keys = changed_keys + 1;

                            values_file.put_slice(&value.value.len().to_le_bytes());
                            //8bytes
                            let value_as_bytes = value.value.as_bytes();
                            //Nth bytes
                            values_file.put_slice(&value_as_bytes);
                            //4 bytes
                            values_file.put_slice(&value.state.to_le_bytes());
                            let record_size =
                                (U64_SIZE + value_as_bytes.len() + VERSION_SIZE) as u64;
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
                    }
                    ValueStatus::Ok => {}
                    ValueStatus::Updated => {}
                    ValueStatus::Deleted => {}
                }
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
        //changed_keys
        0
    }

    async fn store_buffer_to_s3(mut buff: BytesMut, db_name: &String) -> Option<bool> {
        let bucket = "nun-db";
        let key = format!("this-is-going-to-be-amazing/{}", db_name);

        let url = NUN_S3_API_URL.as_str();
        let bucket = NUN_S3_BUCKET.as_str();
        let key_id = NUN_S3_KEY_ID.as_str();
        let secret_key = NUN_S3_SECRET_KEY.as_str();
        print!("Reading from s3, buket: {}, key_id: {}, secret_key: {}, server: {}\n", bucket, key_id, secret_key, NUN_S3_API_URL.as_str());

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

    fn read_data_from_cloud(db_name: &String, key: &String) -> Option<Database> {
        let keys_key_file = format!("this-is-going-to-be-amazing/{}/nun.keys", db_name);
        let values_key_file = format!("this-is-going-to-be-amazing/{}/nun.values", db_name);

        let url = NUN_S3_API_URL.as_str();
        let bucket = NUN_S3_BUCKET.as_str();
        let key_id = NUN_S3_KEY_ID.as_str();
        let secret_key = NUN_S3_SECRET_KEY.as_str();
        print!("Reading from s3, key: {}, values: {}, buket: {}, key_id: {}, secret_key: {}, server: {}\n", keys_key_file, values_key_file, bucket, key_id, secret_key, url);

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
                        let before_cursor_position = values_cursor.position();
                        values_cursor.seek(std::io::SeekFrom::Start(value_addr)).await.unwrap();

                        let mut length_buffer = [0; U64_SIZE];
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
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use super::*;
    use env_logger::{Builder, Env, Target};

    use crate::bo::{ConsensuStrategy, Database, DatabaseMataData};

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
        S3Storage::storage_data_on_cloud(&db, true, &String::from("test"));
        let db =
            S3Storage::read_data_from_cloud(&String::from("test"), &String::from("test")).unwrap();
        assert!(db.count_keys() == 5);
        println!("{:?}", db.get_value("some".to_string()).unwrap());
        assert!(db.get_value("some".to_string()).unwrap() == String::from("value"));
    }
}
