mod lib;

use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::executor::block_on;
use futures::join;
use lib::*;
use std::thread;

use std::sync::Arc;

use clap::ArgMatches;

fn main() -> Result<(), String> {
    env_logger::init();
    let matches: ArgMatches<'_> = lib::commad_line::commands::prepare_args();
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
                .unwrap_or("0.0.0.0:3013"),
            start_match.value_of("replicate-address").unwrap_or(""),
        );
    } else {
        return lib::commad_line::commands::exec_command(&matches);
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
    let (replication_sender, replication_receiver): (Sender<String>, Receiver<String>) =
        channel(100);

    let (start_replication_sender, start_replication_receiver): (Sender<String>, Receiver<String>) =
        channel(100);
    let keys_map = disk_ops::load_keys_map_from_disk();
    println!("Keys {}", keys_map.len());

    let dbs = lib::db_ops::create_init_dbs(
        user.to_string(),
        pwd.to_string(),
        tcp_address.to_string(),
        start_replication_sender,
        replication_sender.clone(),
        keys_map,
    );

    let db_replication_start = dbs.clone();
    let tcp_address_to_relication = Arc::new(tcp_address.to_string());
    let replication_thread_creator = async {
        println!("lib::replication_ops::start_replication_creator_thread");
        lib::replication_ops::start_replication_creator_thread(
            start_replication_receiver,
            db_replication_start,
            tcp_address_to_relication,
        )
        .await
    };

    let db_replication = dbs.clone();
    let replication_thread = async {
        lib::replication_ops::start_replication_thread(replication_receiver, db_replication).await
    };

    let replicate_address_to_thread = Arc::new(replicate_address.to_string());

    let dbs_self_election = dbs.clone();
    let tcp_address_to_election = Arc::new(tcp_address.to_string());
    let joi_thread = thread::spawn(move || {
        lib::replication_ops::ask_to_join_all_replicas(&replicate_address_to_thread, &tcp_address_to_election.to_string(), &dbs_self_election.user.to_string(), &dbs_self_election.pwd.to_string())
    });

    let db_election = dbs.clone();
    let election_thread = thread::spawn(|| lib::election_ops::start_inital_election(db_election));

    let timer = timer::Timer::new();
    let db_snap = dbs.clone();
    // Disck thread
    let _snapshot_thread = thread::spawn(|| lib::disk_ops::start_snap_shot_timer(timer, db_snap));

    let db_socket = dbs.clone();
    let db_http = dbs.clone();
    let http_address = Arc::new(http_address.to_string());

    let ws_address = Arc::new(ws_address.to_string());

    // Netwotk threds
    let ws_thread =
        thread::spawn(move || lib::network::ws_ops::start_web_socket_client(db_socket, ws_address));

    let _http_thread =
        thread::spawn(|| lib::network::http_ops::start_http_client(db_http, http_address));

    let tcp_address = String::from(tcp_address.clone());
    let tcp_thread =
        thread::spawn(move || lib::network::tcp_ops::start_tcp_client(dbs.clone(), &tcp_address));
    let join_all_promises = async {
        join!(replication_thread_creator, replication_thread);
    };
    block_on(join_all_promises);
    tcp_thread.join().expect("Tcp thread died");
    ws_thread.join().expect("WS thread died");

    election_thread.join().expect("election_thread thread died");
    joi_thread.join().expect("joi_thread thread died");
    Ok(())
}
