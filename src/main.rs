extern crate bincode;
extern crate chrono;
extern crate env_logger;
extern crate futures;
extern crate rustc_serialize;
extern crate serde;
extern crate timer;
extern crate ws;

mod lib;

use std::env;
use std::fs::File;
use std::mem;
use std::path::Path;

use ws::{CloseCode, Handler, Message};

use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};

use std::thread;
use std::time;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

use std::collections::HashMap;

use lib::*;

const TO_CLOSE: &'static str = "##CLOSE##";
const FILE_NAME: &'static str = "freira-db.data";
const SNAPSHOT_TIME: i64 = 30000;

// send the given database to the disc
fn storage_data_disk(db: Arc<Database>) {
    let db = db.map.lock().unwrap();
    let mut file = File::create(FILE_NAME).unwrap();
    bincode::serialize_into(&mut file, &db.clone()).unwrap();
}

// calls storage_data_disk each $SNAPSHOT_TIME seconds
fn start_snap_shot_timer(timer: timer::Timer, db: Arc<Database>) {
    println!("Will start_snap_shot_timer");
    let (_tx, rx): (Sender<String>, Receiver<String>) = channel();
    let _guard = {
        timer.schedule_repeating(chrono::Duration::milliseconds(SNAPSHOT_TIME), move || {
            println!("Will snapshot the database");
            storage_data_disk(db.clone());
        })
    };
    rx.recv().unwrap(); // Thread will run for ever
}

fn apply_to_databae(
    dbs: Arc<Databases>,
    selected_db: Arc<SelectedDatabase>,
    opp: &dyn Fn(&Database) -> Response,
) -> Response {
    let db_name = selected_db.name.lock().unwrap();
    let dbs = dbs.map.lock().unwrap();
    let result: Response = match dbs.get(&db_name.to_string()) {
        Some(db) => opp(db),
        None => Response::Error {
            msg: "No database found!".to_string(),
        },
    };
    return result;
}

fn get_key_value(key: String, sender: Sender<String>, db: &Database) -> Response {
    let db = db.map.lock().unwrap();
    let value = match db.get(&key.to_string()) {
        Some(value) => value,
        None => "<Empty>",
    };
    match sender.send(format_args!("value {}\n", value.to_string()).to_string()) {
        Ok(_n) => (),
        Err(e) => println!("Request::Get sender.send Error: {}", e),
    }
    Response::Value {
        key: key.clone(),
        value: value.to_string(),
    }
}

fn set_key_value(key: String, value: String, watchers: Arc<Watchers>, db: &Database) -> Response {
    let mut db = db.map.lock().unwrap();
    db.insert(key.clone().to_string(), value.clone().to_string());
    match watchers.map.lock().unwrap().get(&key) {
        Some(senders) => {
            for sender in senders {
                println!("Sinding to another client");
                match sender.send(
                    format_args!("changed {} {}\n", key.to_string(), value.to_string()).to_string(),
                ) {
                    Ok(_n) => (),
                    Err(e) => println!("Request::Set sender.send Error: {}", e),
                }
            }
        }
        _ => {}
    }
    Response::Set {
        key: key.clone(),
        value: value.to_string(),
    }
}

fn apply_if_auth(auth: Arc<AtomicBool>, opp: &dyn Fn() -> Response) -> Response {
    if auth.load(Ordering::SeqCst) {
        opp()
    } else {
        Response::Error {
            msg: "Not auth".to_string(),
        }
    }
}

fn process_request(
    input: &str,
    watchers: Arc<Watchers>,
    sender: Sender<String>,
    db: Arc<SelectedDatabase>,
    dbs: Arc<Databases>,
    auth: Arc<AtomicBool>,
) -> Response {
    let request = match Request::parse(input) {
        Ok(req) => req,
        Err(e) => return Response::Error { msg: e },
    };
    match request {
        Request::Auth { user, password } => {
            let valid_user = match env::args().nth(1) {
                Some(user) => user.to_string(),
                _ => "mateus".to_string(),
            };

            let valid_pwd = match env::args().nth(2) {
                Some(pwd) => pwd.to_string(),
                _ => "mateus".to_string(),
            };

            if user == valid_user && password == valid_pwd {
                auth.swap(true, Ordering::Relaxed);
            };
            let message = if auth.load(Ordering::SeqCst) {
                "valid auth\n".to_string()
            } else {
                "invalid auth\n".to_string()
            };
            match sender.send(message) {
                Ok(_n) => (),
                Err(e) => println!("Request::Set sender.send Error: {}", e),
            }

            return Response::Ok {};
        }
        Request::Watch { key } => apply_if_auth(auth, &|| {
            let mut watchers = watchers.map.lock().unwrap();
            let mut senders: Vec<Sender<String>> = match watchers.get(&key) {
                Some(watchers_vec) => watchers_vec.clone(),
                _ => Vec::new(),
            };
            senders.push(sender.clone());
            watchers.insert(key.clone(), senders.clone());
            Response::Ok {}
        }),
        Request::Get { key } => apply_if_auth(auth, &|| {
            apply_to_databae(dbs.clone(), db.clone(), &|_db| {
                get_key_value(key.clone(), sender.clone(), _db)
            })
        }),
        Request::Set { key, value } => apply_if_auth(auth, &|| {
            apply_to_databae(dbs.clone(), db.clone(), &|_db| {
                set_key_value(key.clone(), value.clone(), watchers.clone(), _db)
            })
        }),
        Request::UseDb { name, token } => apply_if_auth(auth, &|| {
            let mut db_name_state = db.name.lock().expect("Could not lock name mutex");
            let dbs = dbs.map.lock().unwrap();
            let respose: Response = match dbs.get(&name.to_string()) {
                Some(_) => {
                    mem::replace(&mut *db_name_state, name.clone());
                    Response::Ok {}
                }
                _ => {
                    println!("Not a valid database name");
                    Response::Error {
                        msg: "Not a valid database name".to_string(),
                    }
                }
            };
            respose
        }),

        Request::CreateDb { name, token } => apply_if_auth(auth, &|| {
            let mut dbs = dbs.map.lock().unwrap();
            let empty_db_box = create_temp_db(name.clone());
            let empty_db = Arc::try_unwrap(empty_db_box);
            match empty_db {
                Ok(db) => {
                    dbs.insert(name.to_string(), db);
                    match sender.send("create-db success\n".to_string()) {
                        Ok(_n) => (),
                        Err(e) => println!("Request::CreateDb  Error: {}", e),
                    }
                }
                _ => {
                    println!("Could not create the database");
                    match sender.send("create-db error\n".to_string()) {
                        Ok(_n) => (),
                        Err(e) => println!("Request::Set sender.send Error: {}", e),
                    }
                }
            }
            Response::Ok {}
        }),
    }
}

fn process_message(receiver: &Receiver<String>, writer: &mut BufWriter<&TcpStream>) {
    match receiver.try_recv() {
        Ok(message) => {
            writer.write_fmt(format_args!("{}", message)).unwrap();
            match writer.flush() {
                Ok(_n) => (),
                Err(e) => println!("process_message Error: {}", e),
            }
        }
        _ => thread::sleep(time::Duration::from_millis(2)),
    }
}

fn handle_client(stream: TcpStream, dbs: Arc<Databases>, watchers: Arc<Watchers>) {
    let mut reader = BufReader::new(&stream);
    let writer = &mut BufWriter::new(&stream);
    let (sender, receiver): (Sender<String>, Receiver<String>) = channel();
    let auth = Arc::new(AtomicBool::new(false));
    let db = create_temp_selected_db("init".to_string());
    loop {
        let mut buf = String::new();
        let read_line = reader.read_line(&mut buf);
        stream.set_nonblocking(true).unwrap();
        match read_line {
            Ok(_) => {
                println!("Command print: {}", buf);
                match process_request(
                    &buf,
                    watchers.clone(),
                    sender.clone(),
                    db.clone(),
                    dbs.clone(),
                    auth.clone(),
                ) {
                    Response::Error { msg } => {
                        println!("Error: {}", msg);
                    },
                    _ => println!("Success processed")
                }
            }
            _ => process_message(&receiver, writer),
        }
    }
}
fn start_tcp_client(watchers: Arc<Watchers>, dbs: Arc<Databases>) {
    match TcpListener::bind("127.0.0.1:9001") {
        Ok(listener) => {
            for stream in listener.incoming() {
                let dbs = dbs.clone();
                let watchers = watchers.clone();
                thread::spawn(move || match stream {
                    Ok(socket) => {
                        handle_client(socket, dbs, watchers);
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
// Server WebSocket handler
struct Server {
    out: ws::Sender,
    sender: Sender<String>,
    watchers: Arc<Watchers>,
    dbs: Arc<Databases>,
    db: Arc<SelectedDatabase>,
    auth: Arc<AtomicBool>,
}

impl Handler for Server {
    fn on_open(&mut self, _: ws::Handshake) -> ws::Result<()> {
        let (sender, receiver): (Sender<String>, Receiver<String>) = channel();
        self.sender = sender;
        let sender = self.out.clone();
        let _read_thread = thread::spawn(move || loop {
            match receiver.recv() {
                Ok(message) => match message.as_ref() {
                    TO_CLOSE => {
                        println!("Closing server connection");
                        break;
                    }
                    _ => {
                        sender.send(message).unwrap();
                    }
                },
                _ => thread::sleep(time::Duration::from_millis(2)),
            }
        });
        Ok(())
    }

    fn on_message(&mut self, msg: Message) -> ws::Result<()> {
        println!("Server got message '{}'. ", msg);
        let message = msg.as_text().unwrap();
        println!("Server got message 1 '{}'. ", message);
        process_request(
            &message,
            self.watchers.clone(),
            self.sender.clone(),
            self.db.clone(),
            self.dbs.clone(),
            self.auth.clone(),
        );
        Ok(())
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {
        println!("WebSocket closing for ({:?}) {}", code, reason);
        self.sender.send(TO_CLOSE.to_string()).unwrap(); //Closes the read thread
    }
}

fn start_web_socket_client(watchers: Arc<Watchers>, dbs: Arc<Databases>) {
    let server = thread::spawn(move || {
        let (sender, _): (Sender<String>, Receiver<String>) = channel();
        ws::Builder::new()
            .with_settings(ws::Settings {
                max_connections: 10000,
                ..ws::Settings::default()
            })
            .build(move |out| Server {
                out,
                db: create_temp_selected_db("init".to_string()),
                dbs: dbs.clone(),
                watchers: watchers.clone(),
                sender: sender.clone(),
                auth: Arc::new(AtomicBool::new(false)),
            })
            .unwrap()
            .listen("0.0.0.0:3012")
            .unwrap()
    });

    println!("WebSocket started ");
    let _ = server.join();
}

fn create_temp_db(name: String) -> Arc<Database> {
    let initial_db = HashMap::new();
    let tmpdb = Arc::new(Database {
        map: Mutex::new(initial_db),
        name: Mutex::new(name),
    });
    return tmpdb;
}

fn create_temp_selected_db(name: String) -> Arc<SelectedDatabase> {
    let tmpdb = Arc::new(SelectedDatabase {
        name: Mutex::new(name),
    });
    return tmpdb;
}
fn main() -> Result<(), String> {
    env_logger::init();
    let timer = timer::Timer::new();
    let initial_dbs = HashMap::new();
    /*if Path::new(FILE_NAME).exists() {
        let mut file = File::open(FILE_NAME).unwrap();
        initial_db = bincode::deserialize_from(&mut file).unwrap();
    }*/

    let initial_watchers = HashMap::new();

    let dbs = Arc::new(Databases {
        map: Mutex::new(initial_dbs),
    });

    let watchers = Arc::new(Watchers {
        map: Mutex::new(initial_watchers),
    });
    let watchers_socket = watchers.clone();
    let db_socket = dbs.clone();
    let db_snap = dbs.clone();
    let _ws_thread = thread::spawn(|| start_web_socket_client(watchers_socket, db_socket));
    //let _snapshot_thread = thread::spawn(|| start_snap_shot_timer(timer, db_snap));
    start_tcp_client(watchers.clone(), dbs.clone());
    Ok(())
}
