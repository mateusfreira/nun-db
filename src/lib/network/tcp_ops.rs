use futures::channel::mpsc::Receiver;
use log;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time;

use crate::bo::*;
use crate::process_request::*;
use crate::security::*;

pub fn start_tcp_client(dbs: Arc<Databases>, tcp_addressed: &str) {
    log::debug!("starting tcp client in the addr: {}", tcp_addressed);
    match TcpListener::bind(tcp_addressed) {
        Ok(listener) => {
            for stream in listener.incoming() {
                let dbs = dbs.clone();
                thread::spawn(move || match stream {
                    Ok(socket) => {
                        handle_client(socket, dbs);
                    }
                    _ => (),
                });
            }
        }
        _ => {
            log::error!("Bind error");
            panic!("TCP Bind error");
        }
    };
}

fn process_leave_request(leave_message: &String, dbs: &Arc<Databases>) {
    // Need this fake_client here because client is borrow in
    // `&*client.cluster_member.lock().unwrap()` as immutable
    // leave request does not use the client, therefore this is safe!
    // Double borrow here may leads to an dead lock
    // Fake client needs to be auth
    let (mut fake_client, _) = Client::new_empty_and_receiver();
    fake_client.auth.store(true, Ordering::Relaxed);
    match process_request(&leave_message, dbs, &mut fake_client) {
        Response::Error { msg } => {
            log::debug!("Error: {} trying to process {}", msg, leave_message);
        }
        _ => {
            log::debug!("{} Success processed", leave_message);
        }
    }
}

fn handle_client(stream: TcpStream, dbs: Arc<Databases>) {
    let mut reader = BufReader::new(&stream);
    let writer = &mut BufWriter::new(&stream);
    let (mut client, mut receiver) = Client::new_empty_and_receiver();
    writer.write_fmt(format_args!("ok \n")).unwrap();
    writer.flush().unwrap();
    loop {
        let mut buf = String::new();
        let read_line = reader.read_line(&mut buf);
        stream.set_nonblocking(true).unwrap();
        match read_line {
            Ok(_) => {
                log::debug!("Command print: {}", clean_string_to_log(&buf, &dbs));
                match buf.as_ref() {
                    "" => {
                        log::debug!("killing socket client, because of disconnected!!");
                        process_request("unwatch-all", &dbs, &mut client);
                        let member = &*client.cluster_member.lock().unwrap();
                        if let Some(m) = member {
                            match m.role {
                                ClusterRole::Primary => {
                                    log::debug!("Primary Cluster member disconnected: {}", m.name);
                                    process_leave_request(&format!("leave {}", m.name), &dbs);
                                }
                                ClusterRole::Secoundary => {
                                    log::debug!(
                                        "Secoundary Cluster member disconnected: {}",
                                        m.name
                                    );
                                    process_leave_request(
                                        &format!("replicate-leave {}", m.name),
                                        &dbs,
                                    ); // replicate-leave does not efornce election
                                }
                                ClusterRole::StartingUp => {
                                    log::debug!(
                                        "ClusterMember {} died while still in StartingUp mode",
                                        m.name
                                    );
                                    process_leave_request(
                                        &format!("replicate-leave {}", m.name),
                                        &dbs,
                                    ); // replicate-leave does not efornce election
                                }
                            }
                        }
                        client.left(&dbs);
                        break;
                    }
                    _ => match process_request(&buf, &dbs, &mut client) {
                        Response::Error { msg } => {
                            log::debug!("Error: {}", msg);
                            match client.sender.try_send(format!("error {} \n", msg)) {
                                Ok(_) => (),
                                _ => log::debug!("Error on sending and error request"),
                            }
                        }
                        _ => match client.sender.try_send(format!("ok \n")) {
                            Ok(_) => log::debug!("Success processed"),
                            _ => log::debug!("Success processed! error on sender"),
                        },
                    },
                }
            }
            _ => process_message(&mut receiver, writer),
        }
    }
}
fn process_message(receiver: &mut Receiver<String>, writer: &mut BufWriter<&TcpStream>) {
    match receiver.try_next() {
        Ok(message_opt) => match message_opt {
            Some(message) => {
                writer.write_fmt(format_args!("{}", message)).unwrap();
                match writer.flush() {
                    Ok(_n) => (),
                    Err(e) => log::warn!("process_message Error: {}", e),
                }
            }
            None => log::debug!("tcp_ops::process_message::Empty message"),
        },
        _ => thread::sleep(time::Duration::from_millis(2)),
    }
}
