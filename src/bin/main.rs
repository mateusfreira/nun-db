use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::executor::block_on;
use futures::join;
use log;
use nundb::*;
use signal_hook::{consts::SIGINT, iterator::Signals};
use std::thread;

use std::sync::Arc;

use clap::ArgMatches;
use env_logger::{Builder, Env, Target};

use nundb::configuration::{
    NUN_HTTP_ADDR, NUN_LOG_LEVEL, NUN_PWD, NUN_REPLICATE_ADDR, NUN_TCP_ADDR, NUN_USER, NUN_WS_ADDR,
};

fn init_logger() {
    let env = Env::default().filter_or("NUN_LOG_LEVEL", NUN_LOG_LEVEL.as_str());
    Builder::from_env(env)
        .format_level(false)
        .target(Target::Stdout)
        .format_timestamp_nanos()
        .init();
}

fn main() -> Result<(), String> {
    init_logger();
    log::info!("nundb starting!");
    let matches: ArgMatches<'_> = nundb::commad_line::commands::prepare_args();
    if let Some(start_match) = matches.subcommand_matches("start") {
        let tcp_address = start_match
            .value_of("tcp-address")
            .unwrap_or(NUN_TCP_ADDR.as_str());
        return start_db(
            matches.value_of("user").unwrap_or(NUN_USER.as_str()),
            matches.value_of("pwd").unwrap_or(NUN_PWD.as_str()),
            start_match
                .value_of("ws-address")
                .unwrap_or(NUN_WS_ADDR.as_str()),
            start_match
                .value_of("http-address")
                .unwrap_or(NUN_HTTP_ADDR.as_str()),
            tcp_address,
            start_match
                .value_of("replicate-address")
                .unwrap_or(NUN_REPLICATE_ADDR.as_str()),
            start_match
                .value_of("external-address")
                .unwrap_or(tcp_address),
        );
    } else {
        return nundb::commad_line::commands::exec_command(&matches);
    }
}

fn start_db(
    user: &str,
    pwd: &str,
    ws_address: &str,
    http_address: &str,
    tcp_address: &str,
    replicate_address: &str,
    external_tcpaddress: &str,
) -> Result<(), String> {
    if user == "" || pwd == "" {
        println!("NUN_USER and NUN_PWD must be provided via command line (nun-db -u $USER -p $PWD ...) or env var.");
        std::process::exit(1);
    }
    let (replication_sender, replication_receiver): (Sender<String>, Receiver<String>) =
        channel(100);

    let (replication_supervisor_sender, replication_supervisor_receiver): (
        Sender<String>,
        Receiver<String>,
    ) = channel(100);
    let keys_map = disk_ops::load_keys_map_from_disk();
    let is_oplog_valid = disk_ops::is_oplog_valid();

    if is_oplog_valid {
        log::debug!("All fine with op-log metadafiles");
    } else {
        log::warn!("Nun-db has restarted with op-log in a invalid state, oplog and keys metadafile will be deleted!");
        disk_ops::Oplog::clean_op_log_metadata_files();
    }

    let dbs = nundb::db_ops::create_init_dbs(
        user.to_string(),
        pwd.to_string(),
        tcp_address.to_string(),
        external_tcpaddress.to_string(),
        replication_supervisor_sender,
        replication_sender.clone(),
        keys_map,
        is_oplog_valid,
    );

    disk_ops::load_all_dbs(&dbs);
    let mut signals = Signals::new(&[SIGINT]).unwrap();
    let dbs_to_signal = dbs.clone();
    thread::spawn(move || {
        for sig in signals.forever() {
            println!("Received signal {:?}", sig);
            db_ops::safe_shutdown(&dbs_to_signal);
            std::process::exit(0);
        }
    });

    let tcp_address_to_thread = String::from(tcp_address);
    let dbs_tcp = dbs.clone();
    /*
     * This thread has to be the first one to start to avoid race conditions if the primary tries
     * to ping it too fast
     */
    let tcp_thread = thread::spawn(move || {
        nundb::network::tcp_ops::start_tcp_client(dbs_tcp, &tcp_address_to_thread)
    });

    let db_replication_start = dbs.clone();
    //let tcp_address_to_relication = Arc::new(tcp_address.to_string());
    let external_tcpaddress_to_relication = Arc::new(external_tcpaddress.to_string());
    let replication_thread_creator = async {
        log::debug!("nundb::replication_ops::start_replication_supervisor");
        nundb::replication_ops::start_replication_supervisor(
            replication_supervisor_receiver,
            db_replication_start,
            external_tcpaddress_to_relication,
        )
        .await
    };

    let db_replication = dbs.clone();
    let replication_thread = async {
        nundb::replication_ops::start_replication_thread(replication_receiver, db_replication).await
    };

    let replicate_address_to_thread = Arc::new(replicate_address.to_string());

    let dbs_self_election = dbs.clone();
    let tcp_address_to_election = Arc::new(tcp_address.to_string());
    let external_tcpaddress = Arc::new(external_tcpaddress.to_string());
    let join_thread = thread::spawn(move || {
        nundb::replication_ops::ask_to_join_all_replicas(
            &replicate_address_to_thread,
            &tcp_address_to_election.to_string(),
            &external_tcpaddress.to_string(),
            &dbs_self_election.user.to_string(),
            &dbs_self_election.pwd.to_string(),
        );
        nundb::election_ops::start_inital_election(dbs_self_election)
    });

    let timer = timer::Timer::new();
    let db_snap = dbs.clone();
    // Disck thread
    let _snapshot_thread = thread::spawn(|| nundb::disk_ops::declutter_scheduler(timer, db_snap));

    let db_socket = dbs.clone();
    let db_http = dbs.clone();
    let http_address = Arc::new(http_address.to_string());

    let ws_address = Arc::new(ws_address.to_string());

    // Netwotk threds
    let ws_thread = thread::spawn(move || {
        nundb::network::ws_ops::start_web_socket_client(db_socket, ws_address)
    });

    let _http_thread =
        thread::spawn(|| nundb::network::http_ops::start_http_client(db_http, http_address));

    let join_all_promises = async {
        join!(replication_thread_creator, replication_thread);
    };
    block_on(join_all_promises);
    tcp_thread.join().expect("Tcp thread died");
    ws_thread.join().expect("WS thread died");

    join_thread.join().expect("join_thread thread died");
    Ok(())
}
