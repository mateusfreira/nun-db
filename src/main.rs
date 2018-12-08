extern crate futures;

use std::net::{TcpListener, TcpStream};
use std::io::{BufReader, BufRead, BufWriter, Write};

use futures::Future;


use std::sync::mpsc::channel;
use std::sync::mpsc::{Sender, Receiver};

use std::thread;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

struct Database {
	map: Mutex<HashMap<String, String>>,
}

struct Watchers {
	map: Mutex<HashMap<String, Vec<Sender<String>>>>,
}

enum Request {
    Get { key: String },
    Set { key: String, value: String },
    Watch { key: String },
}

enum Response {
    Value { key: String, value: String },
    Ok {} ,
    Set { key: String, value: String},
    Error { msg: String },
}

fn process_request (input: &str, watchers:Arc<Watchers>, sender:Sender<String>, db:Arc<Database>) -> Response {
    let request = match Request::parse(input){
        Ok(req) => req,
        Err(e) => return Response::Error { msg: e },
    };
	match request {
        Request::Watch { key  } => {
            let mut watchers = watchers.map.lock().unwrap();
            let mut senders:Vec<Sender<String>> = match watchers.get(&key) {
                Some(mut watchers_vec) =>  {
                    watchers_vec.clone()
                }, 
                _ => Vec::new()
            };
            senders.push(sender.clone());
            watchers.insert(key.clone(), senders.clone());
            Response::Ok{}
        },
        Request::Get { key } => {
            let mut db = db.map.lock().unwrap();
            let value = match db.get(&key.to_string()) {
                Some(value) => value,
                None => "<Empty>",
            };
            sender.send(value.to_string());
            Response::Value{ key: key.clone(), value: value.to_string() }
        },
        Request::Set { key, value  } => {
            let mut db = db.map.lock().unwrap();
            db.insert(key.clone().to_string(), value.clone().to_string());
            println!("Will watch");
            match watchers.map.lock().unwrap().get(&key) {
            Some(senders) => {
                for sender in senders {
                    println!("Sinding to another client");
                    sender.send(value.clone());
                }
            }
             _ => {}
            }
            Response::Set{ key: key.clone(), value: value.to_string() }
        }
        _ => Response::Error { msg: "Not parser".to_string()}
    }
}

fn process_message(receiver: & Receiver<String>, writer: &mut BufWriter<& TcpStream>) {
    match receiver.try_recv() {
        Ok(message) => {
            writer.write_fmt(format_args!("{}", message)).unwrap();
            writer.flush();
        },
        _ => ()
    }
}

fn handle_client(stream: TcpStream, db: Arc<Database>, watchers:Arc<Watchers>) {
	let mut reader = BufReader::new(&stream);
	let  writer = &mut BufWriter::new(&stream);
    let (sender, receiver): (Sender<String>, Receiver<String>) = channel();

	loop {  
            let mut buf = String::new();
            let read_line = reader.read_line(&mut buf);
            stream.set_nonblocking(true).unwrap();
            match read_line {
                Ok(_) => {
                    let mut command  = buf.splitn(3, " ");
                    process_request(&buf, watchers.clone(), sender.clone(), db.clone());
                    println!("Command print: {}", buf);
                },
                _ => process_message(&receiver, writer)
            }
           
    }
}

fn main() -> Result<(), String>{
    let mut initial_db = HashMap::new();
    initial_db.insert("foo".to_string(), "bar".to_string());
    let mut initial_watchers = HashMap::new();

    let db = Arc::new(Database {
        map: Mutex::new(initial_db),
    });

    let watchers = Arc::new(Watchers {
        map: Mutex::new(initial_watchers),
    });
    
    match TcpListener::bind("127.0.0.1:9001") {
        Ok(listener) => {
            for stream in listener.incoming() {
                let db  = db.clone();
                let watchers = watchers.clone();
                thread::spawn(move || {
                    match stream {
                        Ok(socket) => {
                            handle_client(socket, db, watchers);
                        },
                        _ => ()
                    }
                });
            }
        },
        _ => {
            println!("Bind error");
        }
    };
    Ok(())
}



impl Request {
     fn parse(input: &str) -> Result<Request, String> {
        let mut command  = input.splitn(3, " ");
        let parsed_command  = match command.next() {
            Some("watch") => {
                let key = match command.next() {
                    Some(key) => key.replace("\n", ""),
                    None => {
                        return Err(format!("watch must contain a key")) 
                    }
                };
                Ok(Request::Watch{ key })
            },
            Some("get") => {
                let key = match command.next() {
                    Some(key) => key.replace("\n", ""),
                    None => {
                        return Err(format!("get must contain a key")) 
                    }
                };
                Ok(Request::Get{ key })
            },
            Some("set") =>  {
                let key = match command.next() {
                    Some(key) => key,
                    None => {
                        println!("SET must be followed by a key");
                        ""
                    },
                };
                let value = match command.next() {
                    Some(value) => value.replace("\n", ""),
                    None => {
                        println!("SET needs a value");
                        "".to_string()
                    },
                };
                Ok(Request::Set { key: key.to_string(), value: value.to_string() })
            },
            Some(cmd) => Err(format!("unknown command: {}", cmd)),
            _ => Err(format!("no command sent")),
        };
        parsed_command
     }
}
