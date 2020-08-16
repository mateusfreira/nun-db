use futures::channel::mpsc::{channel, Receiver, Sender};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use tiny_http;

use bo::*;
use db_ops::*;
use process_request::*;

fn process_commands(
    commands: &Vec<&str>,
    sender: &mut Sender<String>,
    receiver: &mut Receiver<String>,
    db: &Arc<SelectedDatabase>,
    dbs: &Arc<Databases>,
    auth: &Arc<AtomicBool>,
    replication_sender: &mut Sender<String>,
) -> Vec<String> {
    let mut responses = Vec::new();
    for command in commands {
        let clean_command = command.trim();
        if clean_command != "" {
            match process_request(clean_command, sender, db, dbs, auth, replication_sender) {
                Response::Error { msg } => {
                    responses.push(msg.clone());
                    println!("Error: {}", msg);
                }
                _ => {
                    println!("[http] - success processed");
                    match receiver.try_next() {
                        Ok(message_opt) => match message_opt {
                            Some(message) => {
                                responses.push(message);
                            }
                            _ => {
                                responses.push("empty".to_string());
                                println!("http_ops::process_message::Empty message")
                            }
                        },
                        _ => {
                            responses.push("empty".to_string());
                            println!("http_ops::process_message::Error")
                        }
                    }
                }
            }
        }
    }
    return responses;
}
pub fn start_http_client(
    dbs: Arc<Databases>,
    http_address: Arc<String>,
    replication_sender: Sender<String>,
) {
    let http_address = http_address.to_string();
    println!(
        "Starting the http client with 4 threads in the addr: {}",
        http_address
    );
    let http_server = tiny_http::Server::http(http_address).unwrap();
    let http_server = Arc::new(http_server);
    let mut guards = Vec::with_capacity(10);
    for _ in 0..10 {
        let server = http_server.clone();
        let dbs = dbs.clone();
        let mut replication_sender = replication_sender.clone();
        let guard = thread::spawn(move || {
            loop {
                let (mut sender, mut receiver): (Sender<String>, Receiver<String>) = channel(10);
                let auth = Arc::new(AtomicBool::new(false)); //Fix this
                let db = create_temp_selected_db("init".to_string());
                let mut body = String::new();

                match server.recv() {
                    Ok(mut rq) => match rq.as_reader().read_to_string(&mut body) {
                        Ok(_) => {
                            println!("Body {}", body);
                            let commands: Vec<&str> = body.split(';').collect();
                            let responses = process_commands(
                                &commands,
                                &mut sender,
                                &mut receiver,
                                &db,
                                &dbs,
                                &auth,
                                &mut replication_sender,
                            );
                            let response = tiny_http::Response::from_string(responses.join(";"));
                            match rq.respond(response) {
                                Ok(_) => {}
                                Err(e) => println!("http_ops response error {}", e),
                            }
                            println!("[http] Processing the body{}", body);
                        }
                        Err(e) => println!("error {}", e),
                    },
                    Err(e) => println!("server.recv::error {}", e),
                }
            }
        });
        guards.push(guard);
    }
    for h in guards {
        h.join().unwrap();
    }
}
