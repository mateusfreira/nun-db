extern crate futures;

use std::net::{TcpListener, TcpStream, SocketAddr};
use std::io::Result;
use std::io::{BufReader, BufRead, BufWriter, Write};

use futures::Async;
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
	map: Mutex<HashMap<String, Sender<String>>>,
}



fn handle_client(stream: TcpStream, db: Arc<Database>, watchers:Arc<Watchers>) {
	let mut reader = BufReader::new(&stream);
	let mut writer = BufWriter::new(&stream);
    let (sender, receiver): (Sender<String>, Receiver<String>) = channel();

	loop {  
            let mut buf = String::new();
            let read_line = reader.read_line(&mut buf);
            stream.set_nonblocking(true).unwrap();
            match read_line {
                Ok(_) => {
                    let mut db = db.map.lock().unwrap();
                    let mut command  = buf.splitn(3, " ");
                        match command.next() {
                            Some("watch") => {
                                let key = match command.next() {
                                    Some(key) => key.replace("\n", ""),
                                    None => {
                                        println!("WATCH must be followed by a key");
                                        "Error".to_string()
                                    }
                                };
                                let value = match db.get(&key.to_string()) {
                                    Some(value) => value,
                                    None => "<Empty>",
                                };

                                watchers.map.lock().unwrap().insert(key.clone(), sender.clone());
                                println!("<WATCH> {}  {}", key, value)
                            }
                            Some("get") => {
                                let key = match command.next() {
                                    Some(key) => key.replace("\n", ""),
                                    None => {
                                        println!("GET must be followed by a key");
                                        "Error".to_string()
                                    }
                                };
                                if command.next().is_some() {
                                    //return Err(format!("GET's key must not be followed by anything"))
                                    println!("GET's key must not be followed by anything")
                                }
                                let value = match db.get(&key.to_string()) {
                                    Some(value) => value,
                                    None => "<Empty>",
                                };
                                sender.send(value.to_string());
                                println!("<GET> {}  {}", key, value)
                            }
                            Some("set") => {
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
                                db.insert(key.clone().to_string(), value.clone().to_string());
                                match watchers.map.lock().unwrap().get(key.clone()) {
                                Some(sender) => {
                                    sender.send(value.clone());
                                }
                                 _ => {}
                                }
                                println!("<SET> {}  {}", key, value);
                            }
                            Some(cmd) => println!("unknown command: {}", cmd),
                            None => println!("empty input"),
                    }
                    println!("Command print: {}", buf);
                },
                _ => {
                    match receiver.try_recv() {
                        Ok(message) => {
                            writer.write_fmt(format_args!("{}", message)).unwrap();
                            writer.flush();
                        },
                        _ => ()
                    }
                }
            }
    }
}

fn main() -> Result<()>{
    let mut initial_db = HashMap::new();
    initial_db.insert("foo".to_string(), "bar".to_string());
    let mut initial_watchers = HashMap::new();

    let db = Arc::new(Database {
        map: Mutex::new(initial_db),
    });

    let watchers = Arc::new(Watchers {
        map: Mutex::new(initial_watchers),
    });
    
    let listener = TcpListener::bind("127.0.0.1:9001")?;
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
    Ok(())
}
