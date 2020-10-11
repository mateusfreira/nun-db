use std::net::TcpStream;
use std::thread;

use futures::channel::mpsc::{channel, Receiver, Sender};
use std::io::{BufWriter, Write};
use std::sync::Arc;

use bo::*;

use std::time;

pub fn replicate_web(replication_sender: &Sender<String>, message: String) {
    match replication_sender.clone().try_send(message.clone()) {
        Ok(_) => (),
        Err(_) => println!("Error replicating message {}", message),
    }
}

pub fn get_replicate_message(db_name: String, key: String, value: String) -> String {
    return format!("replicate {} {} {}", db_name, key, value);
}

pub fn replicate_request(
    input: Request,
    db_name: &String,
    reponse: Response,
    replication_sender: &Sender<String>,
    is_primary: bool,
) -> Response {
    match reponse {
        Response::Error { msg: _ } => reponse,
        _ => match input {
            Request::CreateDb { name, token } => {
                println!("Will replicate command a created database name {}", name);
                replicate_web(replication_sender, format!("create-db {} {}", name, token));
                Response::Ok {}
            }
            Request::Snapshot {} => {
                println!("Will replicate a snapshot to the database {}", db_name);
                replicate_web(
                    replication_sender,
                    format!("replicate-snapshot {}", db_name),
                );
                Response::Ok {}
            }
            Request::Set { value, key } => {
                println!("Will replicate the set of the key {} to {} ", key, value);
                replicate_web(
                    replication_sender,
                    get_replicate_message(db_name.to_string(), key, value),
                );
                Response::Ok {}
            }

            Request::Election { id } => {
                replicate_web(replication_sender, format!("election cadidate {}", id));
                Response::Ok {}
            }

            Request::ElectionActive {} => {
                replicate_web(replication_sender, format!("election active"));
                Response::Ok {}
            }

            Request::Leave { name } => {
                replicate_web(replication_sender, format!("replicate-leave {}", name));
                Response::Ok {}
            }

            Request::ReplicateSet { db, value, key } => {
                if is_primary {
                    println!("Will replicate the set of the key {} to {} ", key, value);
                    replicate_web(
                        replication_sender,
                        get_replicate_message(db.to_string(), key, value),
                    );
                }
                Response::Ok {}
            }
            _ => reponse,
        },
    }
}

/**
 * Will replicate the message if the sender is Some reference, if not will print and message
 *
 */
fn replicate_if_some(opt_sender: &Option<Sender<String>>, message: &String, name: &String) {
    match opt_sender {
        Some(member_sender) => {
            println!("Replicating {} to {}", message, name);
            match member_sender.clone().try_send(message.to_string()) {
                Ok(_) => (),
                Err(e) => println!("replicate_if_some sender.send Error: {}", e),
            }
        }
        None => println!(
            "start_replication_thread:: Not replicatin {}, None sender",
            message
        ),
    }
}
fn replicate_message_to_secoundary(message: String, dbs: &Arc<Databases>) {
    println!("Got the message {} to replicate ", message);
    let state = dbs.cluster_state.lock().unwrap();
    for (_name, member) in state.members.lock().unwrap().iter() {
        match member.role {
            ClusterRole::Secoundary => replicate_if_some(&member.sender, &message, &member.name),
            ClusterRole::Primary => (),
            ClusterRole::StartingUp => (),
        }
    }
}

pub fn send_message_to_primary(message: String, dbs: &Arc<Databases>) {
    println!("Got the message {} to send to primary", message);
    let state = dbs.cluster_state.lock().unwrap();
    for (_name, member) in state.members.lock().unwrap().iter() {
        match member.role {
            ClusterRole::Secoundary => (),
            ClusterRole::Primary => replicate_if_some(&member.sender, &message, &member.name),
            ClusterRole::StartingUp => (),
        }
    }
}
/**
 *
 * This function will wait for  requests to replicates the messages to the menbers receiver
 * Here I replicate the message the all the members in the cluster
 *
 */
pub fn start_replication_thread(mut replication_receiver: Receiver<String>, dbs: Arc<Databases>) {
    loop {
        match replication_receiver.try_next() {
            Ok(message_opt) => match message_opt {
                Some(message) => replicate_message_to_secoundary(message, &dbs),
                None => println!("replication::try_next::Empty message"),
            },
            _ => thread::sleep(time::Duration::from_millis(2)),
        }
    }
}

fn replicate_join(sender: Sender<String>, name: String) {
    match sender.clone().try_send(format!("replicate-join {}", name)) {
        Ok(_n) => (),
        Err(e) => println!("secoundary::replicate_join sender.send Error: {}", e),
    }
}

fn send_cluster_state_to_the_new_member(
    sender: &Sender<String>,
    dbs: &Arc<Databases>,
    name: &String,
) {
    let cluster_state = dbs.cluster_state.lock().unwrap();
    let members = cluster_state
        .members
        .lock()
        .expect("Could not lock members!")
        .clone();
    for (_name, member) in members.iter() {
        match member.role {
            ClusterRole::Secoundary => {
                //Notify the new secoundary about the already connected menbers.
                replicate_join(sender.clone(), member.name.to_string());
                //Notify the old secoundary about the new secoundary
                match &member.sender {
                    Some(member_sender) => {
                        replicate_join(member_sender.clone(), name.to_string());
                    }
                    None => {
                        println!("[start_replication_creator_thread] not replicate_join None found")
                    }
                }
            }
            ClusterRole::Primary => (),
            ClusterRole::StartingUp => (),
        }
    }
}

fn add_primary_to_secoundary(
    sender: &Sender<String>,
    name: String,
    tcp_addr: &String,
    dbs: Arc<Databases>,
    receiver: Receiver<String>,
) -> std::thread::JoinHandle<()> {
    let user = dbs.user.to_string();
    let pwd = dbs.pwd.to_string();
    dbs.add_cluster_member(ClusterMember {
        name: name.clone(),
        role: ClusterRole::Primary,
        sender: Some(sender.clone()),
    });
    let tcp_addr = tcp_addr.clone();
    let guard = thread::spawn(move || {
        start_replication(name, receiver, user, pwd, tcp_addr.to_string(), false);
    });
    guard
}

fn add_sencoundary_to_primary(
    sender: &Sender<String>,
    name: String,
    tcp_addr: &String,
    dbs: Arc<Databases>,
    receiver: Receiver<String>,
) -> std::thread::JoinHandle<()> {
    let user = dbs.user.to_string();
    let pwd = dbs.pwd.to_string();
    replicate_join(sender.clone(), name.to_string());
    dbs.add_cluster_member(ClusterMember {
        name: name.clone(),
        role: ClusterRole::Secoundary,
        sender: Some(sender.clone()),
    });

    let tcp_addr = tcp_addr.clone();
    let dbs = dbs.clone();
    let guard = thread::spawn(move || {
        start_replication(
            name.clone(),
            receiver,
            user,
            pwd,
            tcp_addr.to_string(),
            true,
        );

        println!("Removing member {} from cluster!", name);
        dbs.remove_cluster_member(&name);
    });
    (guard)
}

fn add_sencoundary_to_secoundary(
    sender: &Sender<String>,
    name: String,
    tcp_addr: &String,
    dbs: Arc<Databases>,
    receiver: Receiver<String>,
) -> () {
    let user = dbs.user.to_string();
    let pwd = dbs.pwd.to_string();
    dbs.add_cluster_member(ClusterMember {
        name: name.clone(),
        role: ClusterRole::Secoundary,
        sender: Some(sender.clone()),
    });

   /* let tcp_addr = tcp_addr.clone();
    let dbs = dbs.clone();
    let guard = thread::spawn(move || {
        start_replication(
            name.clone(),
            receiver,
            user,
            pwd,
            tcp_addr.to_string(),
            true,
        );

        println!("Removing member {} from cluster!", name);
        dbs.remove_cluster_member(&name);
    });
    (guard)*/
}

/**
 * This function goal is to start the theread to connecto to the other cluster members
 *
 * The command send to the replication_start_receiver are like
 *
 * commads e.g:
 * secoundary some_sever:1412
 * primary some_sever:1412
 * <kind> <name:port>
 *
 */

pub fn start_replication_creator_thread(
    mut replication_start_receiver: Receiver<String>,
    dbs: Arc<Databases>,
    tcp_addr: Arc<String>,
) {
    let mut guards = Vec::with_capacity(10); // Max 10 servers
    loop {
        match replication_start_receiver.try_next() {
            Ok(message_opt) => match message_opt {
                Some(start_replicate_message) => {
                    println!(
                        "[start_replication_creator_thread] got {}",
                        start_replicate_message
                    );
                    let mut command = start_replicate_message.splitn(2, " ");

                    let command_str = command.next();
                    let name = String::from(command.next().unwrap());
                    let (sender, receiver): (Sender<String>, Receiver<String>) = channel(100);

                    match command_str {
                        // Add secoundary to primary
                        Some("secoundary") => {
                            // Notify the members in the cluster about the new member
                            send_cluster_state_to_the_new_member(&sender, &dbs, &name);
                            let guard = add_sencoundary_to_primary(
                                &sender,
                                name.clone(),
                                &tcp_addr,
                                dbs.clone(),
                                receiver,
                            );
                            guards.push(guard);
                        }

                        Some("leave") => {
                            dbs.remove_cluster_member(&name);
                        }
                        // Add primary to secoundary
                        Some("primary") => {
                            // Notify the members in the cluster about the new member
                            send_cluster_state_to_the_new_member(&sender, &dbs, &name);
                            let guard = add_primary_to_secoundary(
                                &sender,
                                name.clone(),
                                &tcp_addr,
                                dbs.clone(),
                                receiver,
                            );
                            guards.push(guard);
                        }
                        // Add secoundary to secoundary
                        Some("new-secoundary") => {
                            // Notify the members in the cluster about the new member
                            send_cluster_state_to_the_new_member(&sender, &dbs, &name);
                            let _guard = add_sencoundary_to_secoundary(
                                &sender,
                                name.clone(),
                                &tcp_addr,
                                dbs.clone(),
                                receiver,
                            );
                            //guards.push(guard);
                        }
                        // Add primary to primary
                        Some("election-win") => {
                            dbs.add_cluster_member(ClusterMember {
                                name: tcp_addr.to_string(),
                                role: ClusterRole::Primary,
                                sender: None,
                            });
                            //noity others about primary
                            match dbs
                                .replication_sender
                                .clone()
                                .try_send(format!("set-primary {}", tcp_addr))
                            {
                                Ok(_) => (),
                                Err(_) => println!("Error election cadidate"),
                            }
                        }
                        _ => (),
                    }
                }
                None => println!("replication::try_next::Empty message"),
            },
            _ => thread::sleep(time::Duration::from_millis(2)),
        }
    }
}

fn start_replication(
    replicate_address: String,
    mut command_receiver: Receiver<String>,
    user: String,
    pwd: String,
    tcp_addr: String,
    is_primary: bool,
) {
    println!(
        "replicating to tcp client in the addr: {}",
        replicate_address
    );
    let socket = TcpStream::connect(replicate_address).unwrap();
    let writer = &mut BufWriter::new(&socket);
    auth_on_replication(user, pwd, tcp_addr, is_primary, writer);
    loop {
        match command_receiver.try_next() {
            Ok(message_opt) => match message_opt {
                Some(message) => {
                    println!("Will replicate {}", message);
                    writer.write_fmt(format_args!("{}\n", message)).unwrap();
                    match writer.flush() {
                        Err(e) => {
                            println!("replication error: {}", e);
                            break;
                        }
                        _ => (),
                    }
                }
                None => {
                    println!("replication::try_next::Empty message");
                    break;
                }
            },
            _ => thread::sleep(time::Duration::from_millis(2)),
        }
    }
}

pub fn auth_on_replication(
    user: String,
    pwd: String,
    tcp_addr: String,
    is_primary: bool,
    writer: &mut std::io::BufWriter<&std::net::TcpStream>,
) {
    writer
        .write_fmt(format_args!("auth {} {}\n", user, pwd))
        .unwrap();
    if is_primary {
        writer
            .write_fmt(format_args!("set-primary {}\n", tcp_addr))
            .unwrap();
    } else {
        writer
            .write_fmt(format_args!("set-secoundary {}\n", tcp_addr))
            .unwrap();
    }
    match writer.flush() {
        Err(e) => println!("auth replication error: {}", e),
        _ => (),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::mpsc::{channel, Receiver, Sender};

    #[test]
    fn should_not_replicate_error_messages() {
        let (sender, _): (Sender<String>, Receiver<String>) = channel(100);
        let resp_error = Response::Error {
            msg: "Any error".to_string(),
        };

        let db_name = "some".to_string();
        let result = match replicate_request(
            Request::Set {
                key: "any".to_string(),
                value: "any".to_string(),
            },
            &db_name,
            resp_error,
            &sender,
            false,
        ) {
            Response::Error { msg: _ } => true,
            _ => false,
        };
        assert!(result, "should have returned an error!")
    }

    #[test]
    fn should_replicate_if_the_command_is_a_set() {
        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);
        let resp_set = Response::Set {
            key: "any_key".to_string(),
            value: "any_value".to_string(),
        };

        let req_set = Request::Set {
            key: "any_key".to_string(),
            value: "any_value".to_string(),
        };
        let db_name = "some".to_string();
        let result = match replicate_request(req_set, &db_name, resp_set, &sender, false) {
            Response::Ok {} => true,
            _ => false,
        };
        assert!(result, "should have returned an ok response!");
        let replicate_command = receiver.try_next().unwrap().unwrap();
        assert_eq!(replicate_command, "replicate some any_key any_value")
    }

    #[test]
    fn should_not_replicate_if_the_command_is_a_get() {
        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);
        let resp_get = Response::Value {
            key: "any_key".to_string(),
            value: "any_value".to_string(),
        };

        let db_name = "some".to_string();
        let result = match replicate_request(
            Request::Get {
                key: "any_key".to_string(),
            },
            &db_name,
            resp_get,
            &sender,
            false,
        ) {
            Response::Value { key: _, value: _ } => true,
            _ => false,
        };
        assert!(result, "should have returned an value response!");
        let receiver_replicate_result = receiver.try_next().unwrap_or(None);
        assert_eq!(receiver_replicate_result, None);
    }

    #[test]
    fn should_replicate_if_the_command_is_a_snapshot() {
        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);
        let request = Request::Snapshot {};

        let resp_get = Response::Ok {};

        let db_name = "some_db_name".to_string();
        let result = match replicate_request(request, &db_name, resp_get, &sender, false) {
            Response::Ok {} => true,
            _ => false,
        };
        assert!(result, "should have returned an Ok response!");
        let receiver_replicate_result = receiver.try_next().unwrap().unwrap();
        assert_eq!(receiver_replicate_result, "replicate-snapshot some_db_name");
    }
    #[test]
    fn should_replicate_if_the_command_is_a_create_db() {
        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);
        let request = Request::CreateDb {
            name: "mateus_db".to_string(),
            token: "jose".to_string(),
        };

        let resp_get = Response::Ok {};

        let db_name = "some".to_string();
        let result = match replicate_request(request, &db_name, resp_get, &sender, false) {
            Response::Ok {} => true,
            _ => false,
        };
        assert!(result, "should have returned an Ok response!");
        let receiver_replicate_result = receiver.try_next().unwrap().unwrap();
        assert_eq!(receiver_replicate_result, "create-db mateus_db jose");
    }
}
