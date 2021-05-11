use futures::channel::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use tiny_http;

use bo::*;
use db_ops::*;
use process_request::*;
use security::*;

fn process_commands(
    commands: &Vec<&str>,
    sender: &mut Sender<String>,
    receiver: &mut Receiver<String>,
    db: &Arc<SelectedDatabase>,
    dbs: &Arc<Databases>,
    client: &mut Client,
) -> Vec<String> {
    let mut responses = Vec::new();
    for command in commands {
        let clean_command = command.trim();
        if clean_command != "" {
            match process_request(clean_command, sender, db, dbs, client) {
                Response::Error { msg } => {
                    responses.push(msg.clone());
                    println!("Http response Error: {}", msg);
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
                                println!("http_ops::process_message::Empty message");
                            }
                        },
                        Err(e) => {
                            responses.push("empty".to_string());
                            println!(
                                "http_ops::receiver.try_next empty for {}, message {}",
                                clean_command, e
                            )
                        }
                    }
                }
            }
        }
    }

    process_request("unwatch-all", sender, db, dbs, client); //To dicsconect

    return responses;
}
pub fn start_http_client(dbs: Arc<Databases>, http_address: Arc<String>) {
    let http_address = http_address.to_string();
    println!(
        "Starting the http client with 4 threads in the addr: {}",
        http_address
    );
    let http_server = tiny_http::Server::http(http_address).unwrap();
    let http_server = Arc::new(http_server);
    let mut guards = Vec::with_capacity(4);
    for _ in 0..4 {
        let server = http_server.clone();
        let dbs = dbs.clone();
        let guard = thread::spawn(move || loop {
            let (mut sender, mut receiver): (Sender<String>, Receiver<String>) = channel(10);

            let mut client = Client::new_empty();
            let db = create_temp_selected_db("init".to_string());
            let mut body = String::new();

            match server.recv() {
                Ok(mut rq) => match rq.as_reader().read_to_string(&mut body) {
                    Ok(_) => {
                        println!("[http] body {}", clean_string_to_log(&body, &dbs));
                        let commands: Vec<&str> = body.split(';').collect();
                        let responses = process_commands(
                            &commands,
                            &mut sender,
                            &mut receiver,
                            &db,
                            &dbs,
                            &mut client,
                        );
                        let response = tiny_http::Response::from_string(responses.join(";"));
                        match rq.respond(response) {
                            Ok(_) => {}
                            Err(e) => println!("http_ops response error {}", e),
                        }
                        println!(
                            "[http] Processing the body{}",
                            clean_string_to_log(&body, &dbs)
                        );
                    }
                    Err(e) => println!("error {}", e),
                },
                Err(e) => println!("server.recv::error {}", e),
            }
        });
        guards.push(guard);
    }
    for h in guards {
        h.join().unwrap();
    }
}
