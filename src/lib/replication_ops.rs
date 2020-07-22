use std::io::prelude::*;
use std::net::TcpStream;
use std::thread;

use std::io::{BufRead, BufReader, BufWriter, Write};
use futures::channel::mpsc::{channel, Receiver, Sender};
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use std::time;
use bo::*;
use db_ops::*;

pub fn replicate_request(input: &str, reponse: Response) -> Response {
    match reponse {
        Response::Ok {} => {
            println!("Will replicate : {}", input);
            return Response::Ok {};
        }
        _ => reponse,
    }
}

pub fn start_replication(tcp_addressed: &str, mut command_receiver:  Receiver<String>) {
    println!("starting tcp client in the addr: {}", tcp_addressed);
    let stream = TcpStream::connect(tcp_addressed);
    thread::spawn(move || match stream {
        Ok(socket) => {
            let writer = &mut BufWriter::new(&socket);
            match command_receiver.try_next() {
                Ok(message_opt) => match message_opt {
                    Some(message) => {
                        writer.write_fmt(format_args!("{}", message)).unwrap();
                        match writer.flush() {
                            Ok(_n) => (),
                            Err(e) => println!("replication error: {}", e),
                        }
                    }
                    None => println!("tcp_ops::process_message::Empty message"),
                },
                _ => thread::sleep(time::Duration::from_millis(2)),
            }
        }
        Err(e) => panic!("Could not start the replication socket!"),
    });
}
