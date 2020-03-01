use futures::channel::mpsc::{channel, Receiver, Sender};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use tiny_http;

use bo::*;
use core::*;
use db_ops::*;

fn process_commands(
    commands: &Vec<&str>,
    sender: &mut Sender<String>,
    receiver: &mut Receiver<String>,
    db: &Arc<SelectedDatabase>,
    dbs: &Arc<Databases>,
    auth: &Arc<AtomicBool>,
) -> Vec<String> {
    let mut responses = Vec::new();
    for command in commands {
        let clean_command = command.trim();
        if clean_command != "" {
            match process_request(clean_command, sender, db, dbs, auth) {
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
pub fn start_http_client(dbs: Arc<Databases>) {
    println!("String the http client with 30 threads");
    let http_server = tiny_http::Server::http("0.0.0.0:3013").unwrap();
    let http_server = Arc::new(http_server);
    let mut guards = Vec::with_capacity(4);
    for _ in 0..4 {
        let server = http_server.clone();
        let dbs = dbs.clone();
        let guard = thread::spawn(move || {
            loop {
                let (mut sender, mut receiver): (Sender<String>, Receiver<String>) = channel(10);
                let auth = Arc::new(AtomicBool::new(false)); //Fix this
                let db = create_temp_selected_db("init".to_string());
                let mut body = String::new();

                let mut rq = server.recv().unwrap();
                rq.as_reader().read_to_string(&mut body).unwrap();
                println!("[http] Processing the body{}", body);

                let commands: Vec<&str> = body.split(';').collect();
                let responses =
                    process_commands(&commands, &mut sender, &mut receiver, &db, &dbs, &auth);
                let response = tiny_http::Response::from_string(responses.join(";"));
                rq.respond(response).unwrap();
            }
        });
        guards.push(guard);
    }
    for h in guards {
        h.join().unwrap();
    }
}
