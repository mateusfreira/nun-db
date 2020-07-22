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
use futures::channel::mpsc::{channel, Receiver, Sender};

use std::sync::Arc;

use clap::ArgMatches;

fn main() -> Result<(), String> {
    let matches: ArgMatches = lib::commands::prepare_args();
    if let Some(start_match) = matches.subcommand_matches("start") {
        return start_db(
            matches.value_of("user").unwrap(),
            matches.value_of("pwd").unwrap(),
            start_match
                .value_of("tcp-address")
                .unwrap_or("0.0.0.0:3014"),
            start_match.value_of("ws-address").unwrap_or("0.0.0.0:3012"),
            start_match
                .value_of("http-address")
                .unwrap_or("0.0.0.0:3012"),
        );
    } else {
        return lib::commands::exec_command(&matches);
    }
}

fn start_db(
    user: &str,
    pwd: &str,
    tcp_address: &str,
    ws_address: &str,
    http_address: &str,
) -> Result<(), String> {
    let (mut replication_sender, mut replication_receiver): (Sender<String>, Receiver<String>) = channel(100);

    lib::replication_ops::start_replication("127.0.0.1:3014", replication_receiver);

    env_logger::init();
    let dbs = lib::db_ops::create_init_dbs(user.to_string(), pwd.to_string());

    let timer = timer::Timer::new();
    let db_snap = dbs.clone();
    // Disck thread
    let _snapshot_thread = thread::spawn(|| lib::disk_ops::start_snap_shot_timer(timer, db_snap));

    let db_socket = dbs.clone();
    let db_http = dbs.clone();
    let ws_address = Arc::new(ws_address.to_string());
    let http_address = Arc::new(http_address.to_string());

    // Netwotk threds
    let ws_thread =
        thread::spawn(move || lib::ws_ops::start_web_socket_client(db_socket, ws_address));

    let _http_thread = thread::spawn(|| lib::http_ops::start_http_client(db_http, http_address));
    lib::tcp_ops::start_tcp_client(dbs.clone(), replication_sender.clone(),tcp_address);
    ws_thread.join().expect("ws thread died");
    Ok(())
}
