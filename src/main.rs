extern crate bincode;
extern crate chrono;
extern crate clap;
extern crate env_logger;
extern crate futures;
extern crate reqwest;
extern crate rustc_serialize;
extern crate serde;
extern crate thread_id;
extern crate timer;
extern crate tiny_http;
extern crate ws;

mod lib;

use lib::*;
use std::thread;

fn main() -> Result<(), String> {
    let matches = lib::commands::prepare_args();
    if let Some(_) = matches.subcommand_matches("start") {
        return start_db(
            matches.value_of("user").unwrap(),
            matches.value_of("pwd").unwrap(),
        );
    } else {
        return lib::commands::exec_command(&matches);
    }
}

fn start_db(user: &str, pwd: &str) -> Result<(), String> {
    env_logger::init();
    let dbs = lib::db_ops::create_init_dbs(user.to_string(), pwd.to_string());

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
