use log;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::{thread, time};

use crate::bo::*;

pub fn start_inital_election(dbs: Arc<Databases>) {
    log::info!("will run start_inital_election");
    log::info!("calling start_election");
    start_election(&dbs);
}

pub fn start_election(dbs: &Arc<Databases>) {
    log::info!("Will start election");
    match dbs
        .replication_sender
        .clone()
        .try_send(format!("election candidate {}", dbs.process_id))
    {
        Ok(_) => (),
        Err(_) => log::warn!("Error election candidate"),
    }
    thread::sleep(time::Duration::from_millis(1000));
    if dbs.is_eligible() {
        log::info!("winning the election");
        election_win(&dbs);
    }
}

pub fn start_new_election(dbs: &Arc<Databases>) {
    log::info!("Will start new election from {}", dbs.tcp_address);
    dbs.node_state
        .swap(ClusterRole::StartingUp as usize, Ordering::Relaxed);
    start_election(&dbs);
}

pub fn election_eval(dbs: &Arc<Databases>, candidate_id: u128) -> Response {
    log::info!(
        "Election received candidate_id : {} ,dbs.process_id : {}",
        candidate_id,
        dbs.process_id
    );

    if candidate_id == dbs.process_id {
        log::debug!("Ignoring same node election");
    } else if candidate_id > dbs.process_id {
        log::info!("Will run the start_election");
        start_election(dbs);
    } else {
        log::info!("Won't run the start_election");
        match dbs
            .replication_sender
            .clone()
            .try_send(format!("election alive"))
        {
            Ok(_) => (),
            Err(_) => log::warn!("Error election alive"),
        }
        dbs.node_state
            .swap(ClusterRole::Secoundary as usize, Ordering::Relaxed);
    }
    Response::Ok {}
}

pub fn election_win(dbs: &Arc<Databases>) -> Response {
    log::info!(
        "Setting this server as a primary! tcp_address : {}",
        dbs.tcp_address
    );
    match dbs
        .replication_supervisor_sender
        .clone()
        .try_send(format!("election-win self"))
    {
        Ok(_n) => (),
        Err(e) => log::warn!("Request::ElectionWin sender.send Error: {}", e),
    }

    dbs.node_state
        .swap(ClusterRole::Primary as usize, Ordering::Relaxed);
    Response::Ok {}
}
