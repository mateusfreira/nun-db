extern crate bincode;
extern crate chrono;
extern crate env_logger;
extern crate futures;
extern crate rustc_serialize;
extern crate serde;
extern crate timer;
extern crate ws;

mod lib;

use std::thread;
use lib::*;

fn main() -> Result<(), String> {
    env_logger::init();
    let dbs = lib::db_ops::create_init_dbs();

    let timer = timer::Timer::new();
    let db_snap = dbs.clone();
    let _snapshot_thread = thread::spawn(|| lib::disk_ops::start_snap_shot_timer(timer, db_snap));

    let db_socket = dbs.clone();
    // Netwotk threds
    let _ws_thread = thread::spawn(|| lib::ws_ops::start_web_socket_client(db_socket));
    lib::tcp_ops::start_tcp_client(dbs.clone());
    // Disck thread
    Ok(())
}
