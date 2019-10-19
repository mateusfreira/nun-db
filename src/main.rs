extern crate env_logger;
extern crate futures;
extern crate ws;

mod lib;

use std::env;
use ws::{listen, CloseCode, Handler, Message};

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

fn process_request(
    input: &str,
    watchers: Arc<Watchers>,
    sender: Sender<String>,
    db: Arc<Database>,
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
                _ => "".to_string()
            };

            let valid_pwd = match env::args().nth(2) {
                Some(pwd) => pwd.to_string(),
                _ => "".to_string()
            };
            if user == valid_user && password == valid_pwd {
                auth.swap(true, Ordering::Relaxed);
            };
            let message = if auth.load(Ordering::SeqCst) {
                "valid auth\n".to_string()
            } else {
                "invalid auth\n".to_string()
            };
            sender.send(message);
            return Response::Ok {};
        }
        Request::Watch { key } => {
            if auth.load(Ordering::SeqCst) {
                let mut watchers = watchers.map.lock().unwrap();
                let mut senders: Vec<Sender<String>> = match watchers.get(&key) {
                    Some(mut watchers_vec) => watchers_vec.clone(),
                    _ => Vec::new(),
                };
                senders.push(sender.clone());
                watchers.insert(key.clone(), senders.clone());
            }
            Response::Ok {}
        }
        Request::Get { key } => {
            if auth.load(Ordering::SeqCst) {
                let mut db = db.map.lock().unwrap();
                let value = match db.get(&key.to_string()) {
                    Some(value) => value,
                    None => "<Empty>",
                };
                sender.send(format_args!("value {}\n", value.to_string()).to_string());
                Response::Value {
                    key: key.clone(),
                    value: value.to_string(),
                }
            } else {
                Response::Ok {}
            }
        }
        Request::Set { key, value } => {
            if auth.load(Ordering::SeqCst) {
                let mut db = db.map.lock().unwrap();
                db.insert(key.clone().to_string(), value.clone().to_string());
                println!("Will watch");
                match watchers.map.lock().unwrap().get(&key) {
                    Some(senders) => {
                        for sender in senders {
                            println!("Sinding to another client");
                            sender.send(
                                format_args!("changed {} {}\n", key.to_string(), value.to_string())
                                    .to_string(),
                            );
                        }
                    }
                    _ => {}
                }
                Response::Set {
                    key: key.clone(),
                    value: value.to_string(),
                }
            } else {
                Response::Ok {}
            }
        }
    }
}

fn process_message(receiver: &Receiver<String>, writer: &mut BufWriter<&TcpStream>) {
    match receiver.try_recv() {
        Ok(message) => {
            writer.write_fmt(format_args!("{}", message)).unwrap();
            writer.flush();
        }
        _ => thread::sleep(time::Duration::from_millis(2)),
    }
}

fn handle_client(stream: TcpStream, db: Arc<Database>, watchers: Arc<Watchers>) {
    let mut reader = BufReader::new(&stream);
    let writer = &mut BufWriter::new(&stream);
    let (sender, receiver): (Sender<String>, Receiver<String>) = channel();
    let auth = Arc::new(AtomicBool::new(false));
    loop {
        let mut buf = String::new();
        let read_line = reader.read_line(&mut buf);
        stream.set_nonblocking(true).unwrap();
        match read_line {
            Ok(_) => {
                process_request(
                    &buf,
                    watchers.clone(),
                    sender.clone(),
                    db.clone(),
                    auth.clone(),
                );
                println!("Command print: {}", buf);
            }
            _ => process_message(&receiver, writer),
        }
    }
}
fn start_tcp_client(watchers: Arc<Watchers>, db: Arc<Database>) {
    match TcpListener::bind("127.0.0.1:9001") {
        Ok(listener) => {
            for stream in listener.incoming() {
                let db = db.clone();
                let watchers = watchers.clone();
                thread::spawn(move || match stream {
                    Ok(socket) => {
                        handle_client(socket, db, watchers);
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
    db: Arc<Database>,
    auth: Arc<AtomicBool>,
}

impl Handler for Server {
    fn on_open(&mut self, _: ws::Handshake) -> ws::Result<()> {
        let (sender, receiver): (Sender<String>, Receiver<String>) = channel();
        self.sender = sender;
        let sender = self.out.clone();
        let readTread = thread::spawn(move || loop {
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
        process_request(
            &message,
            self.watchers.clone(),
            self.sender.clone(),
            self.db.clone(),
            self.auth.clone(),
        );
        println!("Server got message 1 '{}'. ", message);
        Ok(())
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {
        println!("WebSocket closing for ({:?}) {}", code, reason);
        self.sender.send(TO_CLOSE.to_string()).unwrap(); //Closes the read thread
    }
}

fn start_web_socket_client(watchers: Arc<Watchers>, db: Arc<Database>) {
    let server = thread::spawn(move || {
        let (sender, _): (Sender<String>, Receiver<String>) = channel();
        ws::Builder::new()
            .with_settings(ws::Settings {
                max_connections: 10000,
                ..ws::Settings::default()
            })
            .build(move |out| Server {
                out,
                db: db.clone(),
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

fn main() -> Result<(), String> {
    env_logger::init();
    let mut initial_db = HashMap::new();
    initial_db.insert("foo".to_string(), "bar".to_string());
    let initial_watchers = HashMap::new();

    let db = Arc::new(Database {
        map: Mutex::new(initial_db),
    });

    let watchers = Arc::new(Watchers {
        map: Mutex::new(initial_watchers),
    });
    let watchers_socket = watchers.clone();
    let db_socket = db.clone();
    let wsThread = thread::spawn(|| start_web_socket_client(watchers_socket, db_socket));
    start_tcp_client(watchers.clone(), db.clone());
    Ok(())
}
