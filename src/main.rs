extern crate futures;
mod lib;

use std::net::{TcpListener, TcpStream};
use std::io::{BufReader, BufRead, BufWriter, Write};

use futures::Future;


use std::sync::mpsc::channel;
use std::sync::mpsc::{Sender, Receiver};

use std::thread;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use lib::{*};

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
