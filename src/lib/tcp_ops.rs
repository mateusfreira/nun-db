use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::channel;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time;

use bo::*;
use core::*;
use db_ops::*;

pub fn start_tcp_client(dbs: Arc<Databases>) {
    match TcpListener::bind("127.0.0.1:9001") {
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
                println!("Command Size: {}", buf.len());
                match buf.as_ref() {
                    "" => {
                        println!("killing scket client, because of disconection");
                        break;
                    }
                    _ => match process_request(&buf, &sender, &db, &dbs, &auth) {
                        Response::Error { msg } => {
                            println!("Error: {}", msg);
                        }
                        _ => println!("Success processed"),
                    },
                }
            }
            _ => process_message(&receiver, writer),
        }
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
