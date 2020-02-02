extern crate bincode;
extern crate chrono;
extern crate env_logger;
extern crate futures;
extern crate rustc_serialize;
extern crate serde;
extern crate thread_id;
extern crate timer;
extern crate ws;

extern crate tiny_http;

mod lib;

use lib::*;
use std::thread;

fn main() -> Result<(), String> {
    env_logger::init();
    let dbs = lib::db_ops::create_init_dbs();

    let timer = timer::Timer::new();
    let db_snap = dbs.clone();
    // Disck thread
    let _snapshot_thread = thread::spawn(|| lib::disk_ops::start_snap_shot_timer(timer, db_snap));

    let db_socket = dbs.clone();
    let db_http = dbs.clone();
    // Netwotk threds
    let _ws_thread = thread::spawn(|| lib::ws_ops::start_web_socket_client(db_socket));
    let _http_thread = thread::spawn(|| lib::http_ops::start_http_client(db_http));
    lib::tcp_ops::start_tcp_client(dbs.clone());
    Ok(())
}
