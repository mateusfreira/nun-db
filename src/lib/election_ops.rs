use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::{thread, time};

use crate::bo::*;

pub fn join_as_secoundary_and_start_election(dbs: &Arc<Databases>, name: &String) {
    match dbs
        .start_replication_sender
        .clone()
        .try_send(format!("secoundary {}", name))
    {
        Ok(_n) => (),
        Err(e) => println!("Request::Join sender.send Error: {}", e),
    }
    start_election(dbs);
}

pub fn start_inital_election(dbs: Arc<Databases>) {
    println!("will run start_inital_election");
    thread::sleep(time::Duration::from_millis(2000));
    println!("calling start_election");
    start_election(&dbs);
}

pub fn start_election(dbs: &Arc<Databases>) {
    println!("Will start election");
    match dbs
        .replication_sender
        .clone()
        .try_send(format!("election cadidate {}", dbs.process_id))
    {
        Ok(_) => (),
        Err(_) => println!("Error election cadidate"),
    }
    thread::sleep(time::Duration::from_millis(2000));
    if dbs.is_eligible() {
        println!("winning the election");
        election_win(&dbs);
    }
}

pub fn start_new_election(dbs: &Arc<Databases>) {
    println!("Will start new election");
    dbs.node_state
        .swap(ClusterRole::StartingUp as usize, Ordering::Relaxed);
    start_election(&dbs);
}

pub fn election_eval(dbs: &Arc<Databases>, candidate_id: u128) -> Response {
    println!(
        "Election received candidate_id : {} ,dbs.process_id : {}",
        candidate_id, dbs.process_id
    );

    if candidate_id == dbs.process_id {
        println!("Ignoring same node election");
    } else if candidate_id > dbs.process_id {
        println!("Will run the start_election");
        start_election(dbs);
    } else {
        println!("Won't run the start_election");
        match dbs
            .replication_sender
            .clone()
            .try_send(format!("election alive"))
        {
            Ok(_) => (),
            Err(_) => println!("Error election alive"),
        }
        dbs.node_state
            .swap(ClusterRole::Secoundary as usize, Ordering::Relaxed);
    }
    Response::Ok {}
}

pub fn election_win(dbs: &Arc<Databases>) -> Response {
    println!("Setting this server as a primary!");
    match dbs
        .start_replication_sender
        .clone()
        .try_send(format!("election-win self"))
    {
        Ok(_n) => (),
        Err(e) => println!("Request::ElectionWin sender.send Error: {}", e),
    }

    dbs.node_state
        .swap(ClusterRole::Primary as usize, Ordering::Relaxed);
    Response::Ok {}
}
