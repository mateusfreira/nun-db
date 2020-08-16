use std::net::TcpStream;
use std::thread;

use futures::channel::mpsc::{Receiver, Sender};
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bo::*;

use std::time;

pub fn replicate_request(
    input: Request,
    db_name: &String,
    reponse: Response,
    replication_sender: &Sender<String>,
) -> Response {
    match reponse {
        Response::Error { msg: _ } => reponse,
        _ => match input {
            Request::CreateDb { name, token } => {
                println!("Will replicate command a created database name {}", name);
                replication_sender
                    .clone()
                    .try_send(format!("create-db {} {}", name, token))
                    .unwrap();
                return Response::Ok {};
            }
            Request::Snapshot { } => {
                println!("Will replicate a snapshot to the database {}", db_name);
                replication_sender
                    .clone()
                    .try_send(format!("replicate-snapshot {}", db_name))
                    .unwrap();
                return Response::Ok {};
            }
            Request::Set { value, key } => {
                println!("Will replicate the set of the key {} to {} ", key, value);
                replication_sender
                    .clone()
                    .try_send(format!("replicate {} {} {}", db_name, key, value))
                    .unwrap();
                return Response::Ok {};
            }
            _ => reponse,
        },
    }
}

pub fn start_replication(
    replicate_address: Arc<String>,
    mut command_receiver: Receiver<String>,
    should_repliate: Arc<AtomicBool>,
) {
    if should_repliate.load(Ordering::SeqCst) {
        let replicate_address = replicate_address.to_string();
        println!(
            "replicating to tcp client in the addr: {}",
            replicate_address
        );
        let socket = TcpStream::connect(replicate_address).unwrap();
        let writer = &mut BufWriter::new(&socket);
        loop {
            match command_receiver.try_next() {
                Ok(message_opt) => match message_opt {
                    Some(message) => {
                        println!("Will replicate {}", message);
                        writer.write_fmt(format_args!("{}\n", message)).unwrap();
                        match writer.flush() {
                            Err(e) => println!("replication error: {}", e),
                            _ => (),
                        }
                    }
                    None => println!("replication::try_next::Empty message"),
                },
                _ => thread::sleep(time::Duration::from_millis(2)),
            }
        }
    } else {
        println!(
            "[replication_ops] no replication addr provided, will not start a replication thread"
        )
    }
}

pub fn auth_on_replication(user: String, pwd: String, mut replication_sender: Sender<String>) {
    replication_sender
        .try_send(format!("auth {} {}", user, pwd))
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::channel::mpsc::{channel, Receiver, Sender};

    #[test]
    fn should_not_replicate_error_messages() {
        let (sender, _): (Sender<String>, Receiver<String>) = channel(100);
        let resp_error = Response::Error {
            msg: "Any error".to_string(),
        };

        let db_name = "some".to_string();
        let result = match replicate_request(
            Request::Set {
                key: "any".to_string(),
                value: "any".to_string(),
            },
            &db_name,
            resp_error,
            &sender,
        ) {
            Response::Error { msg: _ } => true,
            _ => false,
        };
        assert!(result, "should have returned an error!")
    }

    #[test]
    fn should_replicate_if_the_command_is_a_set() {
        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);
        let resp_set = Response::Set {
            key: "any_key".to_string(),
            value: "any_value".to_string(),
        };

        let req_set = Request::Set {
            key: "any_key".to_string(),
            value: "any_value".to_string(),
        };
        let db_name = "some".to_string();
        let result = match replicate_request(req_set, &db_name, resp_set, &sender) {
            Response::Ok {} => true,
            _ => false,
        };
        assert!(result, "should have returned an ok response!");
        let replicate_command = receiver.try_next().unwrap().unwrap();
        assert_eq!(replicate_command, "replicate some any_key any_value")
    }

    #[test]
    fn should_not_replicate_if_the_command_is_a_get() {
        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);
        let resp_get = Response::Value {
            key: "any_key".to_string(),
            value: "any_value".to_string(),
        };

        let db_name = "some".to_string();
        let result = match replicate_request(
            Request::Get {
                key: "any_key".to_string(),
            },
            &db_name,
            resp_get,
            &sender,
        ) {
            Response::Value { key: _, value: _ } => true,
            _ => false,
        };
        assert!(result, "should have returned an value response!");
        let receiver_replicate_result = receiver.try_next().unwrap_or(None);
        assert_eq!(receiver_replicate_result, None);
    }

    #[test]
    fn should_replicate_if_the_command_is_a_snapshot() {
        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);
        let request = Request::Snapshot {};

        let resp_get = Response::Ok {};

        let db_name = "some_db_name".to_string();
        let result = match replicate_request(request, &db_name, resp_get, &sender) {
            Response::Ok {} => true,
            _ => false,
        };
        assert!(result, "should have returned an Ok response!");
        let receiver_replicate_result = receiver.try_next().unwrap().unwrap();
        assert_eq!(receiver_replicate_result, "replicate-snapshot some_db_name");
    }
    #[test]
    fn should_replicate_if_the_command_is_a_create_db() {
        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);
        let request = Request::CreateDb {
            name: "mateus_db".to_string(),
            token: "jose".to_string(),
        };

        let resp_get = Response::Ok {};

        let db_name = "some".to_string();
        let result = match replicate_request(request, &db_name, resp_get, &sender) {
            Response::Ok {} => true,
            _ => false,
        };
        assert!(result, "should have returned an Ok response!");
        let receiver_replicate_result = receiver.try_next().unwrap().unwrap();
        assert_eq!(receiver_replicate_result, "create-db mateus_db jose");
    }
}
