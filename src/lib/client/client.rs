use futures::channel::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use ws::Error;
use ws::{connect, CloseCode, Handler, Message};

use crate::bo::*;

pub struct NunDbClient {
    server_url: String,
    db_name: String,
    user: String,
    password: String,
    sender: Arc<Mutex<Sender<String>>>,
    receiver: Receiver<String>,
}

struct Tmp<'a> {
    socket_server: ws::Sender,
    extenal_sender: Arc<Mutex<Sender<String>>>,
    is_auth: bool,
    watch_fn: Option<&'a dyn Fn(&String) -> Result<(), String>>,
}
impl Handler for Tmp<'_> {
    /*
    fn on_request(&mut self, req: &ws::Request) -> Result<ws::Response, ws::Error> {
    }
    */
    fn on_error(&mut self, err: Error) {}
    fn on_message(&mut self, msg: Message) -> ws::Result<()> {
        println!("Got message: {}", msg);
        let message = msg.to_string().split("\n").collect::<Vec<&str>>()[0]
            .trim()
            .to_string();
        if message != "ok" {
            let command = message.splitn(2, " ").next();
            match command {
                Some("changed-version") => {
                    if let Some(watch_fn) = self.watch_fn {
                        let change_parse =
                            parse_change_response(&mut message.splitn(2, " ")).unwrap();
                        match change_parse {
                            Response::Value {
                                value,
                                key: _,
                                version: _,
                            } => {
                                //panic!("Watch the key {}", message);
                                match watch_fn(&value) {
                                    Ok(_) => {
                                        println!("Watch worked");
                                    }
                                    Err(_) => {
                                        self.socket_server.close(CloseCode::Away);
                                    }
                                }
                            }
                            _ => {
                                println!("Error this response is not handled {}", message);
                            }
                        }
                    }
                }
                Some("value") => {
                    let val = parse_value_response(&mut message.splitn(2, " ")).unwrap();
                    match val {
                        Response::Value {
                            value,
                            key: _,
                            version: _,
                        } => {
                            self.extenal_sender.lock().unwrap().try_send(value).unwrap();
                            self.socket_server.close(CloseCode::Away);
                        }
                        _ => {
                            println!("Error this response is not handled {}", message);
                        }
                    }
                }
                _ => {}
            }
        } else {
            // first message ok means auth is done
            if self.is_auth {
                self.extenal_sender
                    .lock()
                    .unwrap()
                    .try_send("ok".to_string())
                    .unwrap();
            } else {
                self.is_auth = true;
            }
        }
        Ok(())
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {
        println!(
            "Connection closed with code: {:?} and reason: {}",
            code, reason
        );
    }
}

impl NunDbClient {
    pub fn new(
        server_url: &str,
        db_name: &str,
        user: &str,
        password: &str,
    ) -> Result<NunDbClient, String> {
        let (send, receiver) = channel::<String>(100);
        Ok(NunDbClient {
            server_url: server_url.to_string(),
            db_name: db_name.to_string(),
            user: user.to_string(),
            password: password.to_string(),
            receiver,
            sender: Arc::new(Mutex::new(send)),
        })
    }

    pub async fn set(&self, key: &str, value: &str) {
        let command = format!("set {} {};get {}", key, value, key);
        println!("Command: {}", command);
        let _ = self.connect_and_wait_for_response(command.as_str());
    }

    pub async fn get(&self, key: &str) -> String {
        let command = format!("get {}", key);
        let result = self.connect_and_wait_for_response(command.as_str());
        result
    }

    pub fn watch(&self, key: &str, process_fn: &dyn Fn(&String) -> Result<(), String>) {
        let (user, password, db_name, server_url) = self.get_auth_data();
        let command = format!("watch {}", key);
        if let Err(e) = connect(server_url.clone(), |out| {
            auth_on_sender(&out, &db_name, &user, &password);
            out.send(command.clone()).unwrap();
            Tmp {
                socket_server: out,
                extenal_sender: self.sender.clone(),
                is_auth: false,
                watch_fn: Some(process_fn),
            }
        }) {
            println!("Failed to connect to server: {}", e);
        }
    }

    fn connect_and_wait_for_response(&self, command: &str) -> String {
        let command = command.to_string();
        let (user, password, db_name, server_url) = self.get_auth_data();

        let (send, mut receive) = channel::<String>(100);
        let send = Arc::new(Mutex::new(send));
        if let Err(e) = connect(server_url.clone(), |out| {
            auth_on_sender(&out, &db_name, &user, &password);
            out.send(command.clone()).unwrap();
            Tmp {
                socket_server: out,
                extenal_sender: send.clone(),
                is_auth: false,
                watch_fn: None,
            }
        }) {
            println!("Failed to connect to server: {}", e);
        }
        let result = receive.try_next();
        return result.unwrap().unwrap();
    }

    fn get_auth_data(&self) -> (String, String, String, String) {
        let user = self.user.clone();
        let password = self.password.clone();
        let db_name = self.db_name.clone();
        let server_url = self.server_url.clone();
        (user, password, db_name, server_url)
    }

    fn try_next(&mut self) -> Result<Option<String>, TryRecvError> {
        return self.receiver.try_next();
    }
}

fn auth_on_sender(out: &ws::Sender, db_name: &String, user: &String, password: &String) {
    out.send(format!("use-db {} {} {}", db_name, user, password))
        .unwrap();
}

fn parse_value_response(command: &mut std::str::SplitN<&str>) -> Result<Response, String> {
    let value_command = command.next().unwrap();
    let value = match command.next() {
        Some(value) => value.replace("\n", ""),
        None => return Err(format!("get must contain a key")),
    };
    Ok(Response::Value {
        value,
        key: String::from(""),
        version: 0,
    })
}

fn parse_change_response(command: &mut std::str::SplitN<&str>) -> Result<Response, String> {
    let _change_command = command.next().unwrap();
    let mut key_parts = match command.next() {
        Some(value) => value.splitn(3, " "),
        None => return Err(format!("get must contain a key")),
    };
    let key = key_parts.next().unwrap();
    let version = key_parts.next().unwrap();
    let value = key_parts.next().unwrap();
    Ok(Response::Value {
        value: String::from(value.replace("\n", "")),
        key: String::from(key),
        version: version.parse::<i32>().unwrap(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use std::{thread, time};

    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    #[test]
    fn should_connect_to_nun_db() {
        let nun_client = NunDbClient::new("ws://localhost:3012", "test", "user", "test-pwd");
        assert_eq!(nun_client.is_ok(), true);
    }

    #[test]
    fn should_get_a_value() {
        let rand_value = Databases::next_op_log_id().to_string();
        let nun_client = NunDbClient::new("ws://127.0.0.1:3058", "sample", "user", "sample-pwd");
        let con = nun_client.unwrap();
        //let a  = con.connect();
        let set = con.set("key1", rand_value.as_str());
        aw!(set);
        let value = con.get("key1");
        assert_eq!(aw!(value), rand_value);
        //a.join().unwrap();
    }

    #[test]
    fn should_watch_a_key() {
        let rand_value = Databases::next_op_log_id().to_string();
        let key_name = "key1";

        let rand_value_watch = rand_value.clone();
        let watch_thread = thread::spawn(move || {
            let nun_client =
                NunDbClient::new("ws://127.0.0.1:3058", "sample", "user", "sample-pwd");
            let con = nun_client.unwrap();
            con.watch(key_name, &|value| {
                assert_eq!(value.to_string(), rand_value_watch);
                Err(String::from("Done"))
            });
        });

        let rand_value_set = rand_value.clone();
        let set_thread = thread::spawn(move || async move {
            thread::sleep(time::Duration::from_millis(100));
            let nun_client =
                NunDbClient::new("ws://127.0.0.1:3058", "sample", "user", "sample-pwd");
            let con = nun_client.unwrap();
            // let rand_value_set = Databases::next_op_log_id().to_string();
            let _set = con.set("key1", rand_value_set.as_str()).await;
        });
        block_on(set_thread.join().unwrap());
        //set_thread.join().unwrap();
        match watch_thread.join() {
            Ok(_) => {}
            Err(e) => {
                println!("Error: {:?}", e);
                assert_eq!(true, false);
            }
        }
    }
}
