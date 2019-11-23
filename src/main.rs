extern crate bincode;
extern crate chrono;
extern crate env_logger;
extern crate futures;
extern crate rustc_serialize;
extern crate serde;
extern crate timer;
extern crate ws;

mod lib;

use std::fs::File;
use std::path::Path;

use std::sync::mpsc::channel;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use std::collections::HashMap;

use lib::bo::*;
use lib::*;

const FILE_NAME: &'static str = "freira-db.data";
const SNAPSHOT_TIME: i64 = 30000;

// send the given database to the disc
fn storage_data_disk(db: Arc<Database>) {
    let db = db.map.lock().unwrap();
    let mut file = File::create(FILE_NAME).unwrap();
    bincode::serialize_into(&mut file, &db.clone()).unwrap();
}

// calls storage_data_disk each $SNAPSHOT_TIME seconds
fn start_snap_shot_timer(timer: timer::Timer, db: Arc<Database>) {
    println!("Will start_snap_shot_timer");
    let (_tx, rx): (Sender<String>, Receiver<String>) = channel();
    let _guard = {
        timer.schedule_repeating(chrono::Duration::milliseconds(SNAPSHOT_TIME), move || {
            println!("Will snapshot the database");
            storage_data_disk(db.clone());
        })
    };
    rx.recv().unwrap(); // Thread will run for ever
}

fn main() -> Result<(), String> {
    env_logger::init();
    let timer = timer::Timer::new();
    let initial_dbs = HashMap::new();
    /*if Path::new(FILE_NAME).exists() {
        let mut file = File::open(FILE_NAME).unwrap();
        initial_db = bincode::deserialize_from(&mut file).unwrap();
    }*/

    let initial_watchers = HashMap::new();

    let dbs = Arc::new(Databases {
        map: Mutex::new(initial_dbs),
    });

    let watchers = Arc::new(Watchers {
        map: Mutex::new(initial_watchers),
    });
    let watchers_socket = watchers.clone();
    let db_socket = dbs.clone();
    let db_snap = dbs.clone();
    let _ws_thread =
        thread::spawn(|| lib::ws_ops::start_web_socket_client(watchers_socket, db_socket));
    //let _snapshot_thread = thread::spawn(|| start_snap_shot_timer(timer, db_snap));
    lib::tcp_ops::start_tcp_client(watchers.clone(), dbs.clone());
    Ok(())
}
