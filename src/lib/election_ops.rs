use std::sync::Arc;
use std::{thread, time};


use bo::*;
use db_ops::*;

pub fn start_inital_election(dbs: Arc<Databases>) {
    thread::sleep(time::Duration::from_millis(1));
    println!("calling start_election");
    start_election(&dbs);
}

pub fn start_election(dbs: &Arc<Databases>) {
    println!("Will start election");
    match dbs.replication_sender.clone().try_send(format!("election cadidate {}", dbs.process_id)) {
        Ok(_) => (),
        Err(_) => println!("Error election cadidate"),
    }
    thread::sleep(time::Duration::from_millis(2000));
    if dbs.is_eligible() {
        println!("winning the election");
        election_win(dbs.clone());
    }
}
