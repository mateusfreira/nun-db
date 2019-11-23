extern crate bincode;
extern crate chrono;
extern crate env_logger;
extern crate futures;
extern crate rustc_serialize;
extern crate serde;
extern crate timer;
extern crate ws;

mod lib;

use std::sync::{Arc, Mutex};
use std::thread;

use std::collections::HashMap;

use lib::bo::*;
use lib::*;

fn main() -> Result<(), String> {
    env_logger::init();
    let timer = timer::Timer::new();
    let initial_dbs = HashMap::new();

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
    let _snapshot_thread = thread::spawn(|| lib::disk_ops::start_snap_shot_timer(timer, db_snap));
    lib::tcp_ops::start_tcp_client(watchers.clone(), dbs.clone());
    Ok(())
}
