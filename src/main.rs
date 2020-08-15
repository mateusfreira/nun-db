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

use futures::channel::mpsc::{channel, Receiver, Sender};
use lib::*;
use std::sync::atomic::AtomicBool;
use std::thread;

use std::sync::Arc;

use clap::ArgMatches;

fn main() -> Result<(), String> {
    env_logger::init();
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
            start_match.value_of("replicate-address").unwrap_or(""),
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
    replicate_address: &str,
) -> Result<(), String> {
    let should_replicate = replicate_address != "";
    let (replication_sender, replication_receiver): (Sender<String>, Receiver<String>) =
        channel(100);

    let replicate_address = Arc::new(replicate_address.to_string());
    let should_replicate_arc = Arc::new(AtomicBool::new(should_replicate));
    let replication_thread = thread::spawn(|| {
        lib::replication_ops::start_replication(
            replicate_address,
            replication_receiver,
            should_replicate_arc,
        );
    });

    let dbs = lib::db_ops::create_init_dbs(user.to_string(), pwd.to_string(), should_replicate);

    let timer = timer::Timer::new();
    let db_snap = dbs.clone();
    // Disck thread
    let _snapshot_thread = thread::spawn(|| lib::disk_ops::start_snap_shot_timer(timer, db_snap));

    let db_socket = dbs.clone();
    let db_http = dbs.clone();
    let http_address = Arc::new(http_address.to_string());

    let ws_address = Arc::new(ws_address.to_string());
    let replication_sender_ws = replication_sender.clone();
    let replication_sender_http = replication_sender.clone();

    // Netwotk threds
    let ws_thread = thread::spawn(move || {
        lib::ws_ops::start_web_socket_client(db_socket, ws_address, replication_sender_ws)
    });

    let _http_thread = thread::spawn(|| {
        lib::http_ops::start_http_client(db_http, http_address, replication_sender_http)
    });

    lib::tcp_ops::start_tcp_client(dbs.clone(), replication_sender.clone(), tcp_address);

    lib::replication_ops::auth_on_replication(
        user.to_string(),
        pwd.to_string(),
        replication_sender,
    );

    ws_thread.join().expect("ws thread died");
    replication_thread
        .join()
        .expect("replication_thread thread died");
    Ok(())
}
