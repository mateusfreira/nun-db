
use std::net::TcpStream;
use std::thread;

use futures::channel::mpsc::{Receiver, Sender};
use std::io::{BufWriter, Write};





use bo::*;

use std::time;

pub fn replicate_request(
    input: &str,
    reponse: Response,
    replication_sender: &Sender<String>,
) -> Response {
    match reponse {
        Response::Set { value: _, key: _ } => {
            println!("Will replicate : {}", input);
            replication_sender
                .clone()
                .try_send(input.to_string())
                .unwrap();
            return Response::Ok {};
        },
        Response::Ok {} => {
            println!("Will replicate : {}", input);
            replication_sender
                .clone()
                .try_send(input.to_string())
                .unwrap();
            return Response::Ok {};
        }
        _ => reponse,
    }
}

pub fn start_replication(tcp_addressed: &str, mut command_receiver: Receiver<String>) {
    println!("replicating to tcp client in the addr: {}", tcp_addressed);
    let stream = TcpStream::connect(tcp_addressed);
    let _ = thread::spawn(move || match stream {
        Ok(socket) => {
            let writer = &mut BufWriter::new(&socket);
            loop {
                match command_receiver.try_next() {
                    Ok(message_opt) => match message_opt {
                        Some(message) => {
                            println!("Will replicate {}", message);
                            writer.write_fmt(format_args!("{}\n", message)).unwrap();
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
        }
        Err(_e) => panic!("Could not start the replication socket!"),
    })
    .join();
}
