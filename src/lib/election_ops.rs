use log;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::{thread, time};

use crate::bo::*;

const ELECTION_TIMEOUT: u128 = 1000;

pub fn start_inital_election(dbs: Arc<Databases>) {
    log::info!("will run start_inital_election in 1s");
    thread::sleep(time::Duration::from_millis(1000));
    log::info!("calling start_election");
    if dbs.is_eligible() {
        start_election(&dbs);
    } else {
        log::info!("Will not run start_election at this node, because it is not eligible");
    }
}

pub fn start_election(dbs: &Arc<Databases>) {
    log::info!("Will start election");
    if dbs.count_cluster_members() <= 1 {
        log::info!("Only one node in the cluster, will set as primary");
        election_win(dbs);
        return;
    }
    match dbs.replicate_message(format!(
        "election candidate {} {}",
        dbs.process_id, dbs.external_tcp_address
    )) {
        Ok(id) => {
            let mut opp = dbs.get_pending_opp_copy(id);
            let mut start_time: u128 = 0;
            while opp.is_none() && start_time < ELECTION_TIMEOUT {
                log::debug!("Waiting for opp to be registered");
                thread::sleep(time::Duration::from_millis(2));
                start_time = start_time + 2;
                opp = dbs.get_pending_opp_copy(id);
            }
            if opp.is_none() {
                log::debug!("No opp registered, will set as primary");
                election_win(dbs);
                return;
            }
            start_time = 0;
            while opp.is_some() && !opp.unwrap().is_full_acknowledged() {
                if !dbs.is_eligible() {
                    log::info!("No longer eligible to be primary, will stop election");
                    return;
                }
                thread::sleep(time::Duration::from_millis(2));
                start_time = start_time + 2;
                opp = dbs.get_pending_opp_copy(id);
                match opp {
                    Some(opp) => log::debug!(
                        "Waiting election for Acks is_full_acknowledged: {:?} replication: {:?}, acknowledged: {:}", 
                        opp.is_full_acknowledged(),
                        opp.count_replication(),
                        opp.count_acknowledged()

                        ),
                    None => log::debug!("Waiting election for Acks is_nome: {:?}", opp.is_none()),
                }
                opp = dbs.get_pending_opp_copy(id);
                if start_time > ELECTION_TIMEOUT {
                    log::info!("Election timeout, will claim as primary");
                    election_win(&dbs);
                    return;
                }
            }

            log::info!("Election acks received");

            thread::sleep(time::Duration::from_millis(100)); // Will wait for the ack
            if dbs.is_eligible() {
                log::info!("winning the election");
                election_win(&dbs);
            }
        }
        Err(_) => log::warn!("Error election candidate"),
    }
}

pub fn start_new_election(dbs: &Arc<Databases>) {
    log::info!("Will start new election from {}", dbs.tcp_address);
    dbs.node_state
        .swap(ClusterRole::StartingUp as usize, Ordering::Relaxed);
    start_election(&dbs);
}

pub fn election_eval(dbs: &Arc<Databases>, candidate_id: u128, node_name: &String) -> Response {
    log::info!(
        "Election received [candidate_id : {}, node_name : {} ] , dbs : [process_id : {}, {} ]",
        candidate_id,
        node_name,
        dbs.process_id,
        dbs.external_tcp_address
    );

    if candidate_id == dbs.process_id {
        log::debug!("Ignoring same node election");
    } else if candidate_id > dbs.process_id {
        log::info!("Will run the start_election");
        start_election(dbs);
    } else {
        log::info!("Won't run the start_election");
        match dbs.replicate_message(format!("election alive {}", dbs.external_tcp_address)) {
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
