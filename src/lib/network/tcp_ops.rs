use futures::channel::mpsc::{channel, Receiver, Sender};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time;

use bo::*;
use db_ops::*;
use process_request::*;
use security::*;

pub fn start_tcp_client(dbs: Arc<Databases>, tcp_addressed: &str) {
    println!("starting tcp client in the addr: {}", tcp_addressed);
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
            println!("Bind error");
        }
    };
}

fn handle_client(stream: TcpStream, dbs: Arc<Databases>) {
    let mut reader = BufReader::new(&stream);
    let writer = &mut BufWriter::new(&stream);
    let (mut sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);

    let mut client = Client::new_empty();
    let db = create_temp_selected_db("init".to_string());
    loop {
        let mut buf = String::new();
        let read_line = reader.read_line(&mut buf);
        stream.set_nonblocking(true).unwrap();
        match read_line {
            Ok(_) => {
                println!("Command print: {}", clean_string_to_log(&buf, &dbs));
                match buf.as_ref() {
                    "" => {
                        println!("killing socket client, because of disconnected!!");
                        let member = &*client.cluster_member.lock().unwrap();
                        match member {
                            Some(m) => {
                                match m.role {
                                    ClusterRole::Primary => {
                                        //New elections are only needed if the primary fails
                                        println!(
                                            "Cluster member disconnected role : {} name {} : ",
                                            m.role, m.name
                                        );
                                        // Need this fake_client here because client is borrow in
                                        // `&*client.cluster_member.lock().unwrap()` as immutable
                                        // leave request does not use the client, therefore this is safe!
                                        // Double borrow here may leads to an dead lock
                                        // Fake client needs to be auth
                                        let mut fake_client = Client::new_empty();
                                        fake_client.auth.store(true, Ordering::Relaxed);
                                        match process_request(
                                            &format!("leave {}", m.name),
                                            &mut sender,
                                            &db,
                                            &dbs,
                                            &mut fake_client,
                                        ) {
                                            Response::Error { msg } => {
                                                println!("Error: {}", msg);
                                                sender
                                                    .try_send(format!("error {} \n", msg))
                                                    .unwrap();
                                            }
                                            _ => {
                                                sender.try_send(format!("ok \n")).unwrap();
                                                println!("Success processed");
                                            }
                                        }
                                    }
                                    _ => (),
                                }
                            }
                            None => (),
                        };
                        //Implement leave
                        /*process_request(
                            "unwatch-all",
                            &mut sender,
                            &db,
                            &dbs,
                            &auth,
                            &mut replication_sender,
                        );*/
                        break;
                    }
                    _ => match process_request(&buf, &mut sender, &db, &dbs, &mut client) {
                        Response::Error { msg } => {
                            println!("Error: {}", msg);
                            match sender.try_send(format!("error {} \n", msg)) {
                                Ok(_)  => (),
                                _ => println!("Error on sending and error request")
                            }
                        }
                        _ => {
                            match sender.try_send(format!("ok \n")) {
                                Ok(_)  => println!("Success processed"),
                                _ => println!("Success processed! error on sender")
                            }
                        }
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
                    Err(e) => println!("process_message Error: {}", e),
                }
            }
            None => println!("tcp_ops::process_message::Empty message"),
        },
        _ => thread::sleep(time::Duration::from_millis(2)),
    }
}
