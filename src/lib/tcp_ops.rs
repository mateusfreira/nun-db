use futures::channel::mpsc::{channel, Receiver, Sender};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time;

use bo::*;
use core::*;
use db_ops::*;

pub fn start_tcp_client(dbs: Arc<Databases>) {
    println!("starting tcp client");
    match TcpListener::bind("0.0.0.0:3014") {
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
    let auth = Arc::new(AtomicBool::new(false));
    let db = create_temp_selected_db("init".to_string());
    loop {
        let mut buf = String::new();
        let read_line = reader.read_line(&mut buf);
        stream.set_nonblocking(true).unwrap();
        match read_line {
            Ok(_) => {
                println!("Command print: {}", buf);
                match buf.as_ref() {
                    "" => {
                        println!("killing socket client, because of disconection");
                        process_request("unwatch-all", &mut sender, &db, &dbs, &auth);
                        break;
                    }
                    _ => match process_request(&buf, &mut sender, &db, &dbs, &auth) {
                        Response::Error { msg } => {
                            println!("Error: {}", msg);
                            sender.try_send(format!("error {} \n", msg)).unwrap();
                        }
                        _ => {
                            sender.try_send(format!("ok \n")).unwrap();
                            println!("Success processed");
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
