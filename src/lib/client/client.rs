use futures::channel::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use ws::{connect, CloseCode, Handler, Message};

use crate::bo::*;

pub struct NunDbClient {
    server_url: String,
    db_name: String,
    user: String,
    password: String,
    sender: Arc<Mutex<Sender<String>>>,
}

struct Tmp<'a> {
    socket_server: ws::Sender,
    extenal_sender: Arc<Mutex<Sender<String>>>,
    is_auth: bool,
    watch_fn: Option<&'a dyn Fn(&String) -> Result<(), String>>,
}
impl Handler for Tmp<'_> {
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
                                        self.socket_server.close(CloseCode::Away).unwrap();
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
                            self.socket_server.close(CloseCode::Away).unwrap();
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
        let (send, _receiver) = channel::<String>(100);
        Ok(NunDbClient {
            server_url: server_url.to_string(),
            db_name: db_name.to_string(),
            user: user.to_string(),
            password: password.to_string(),
            sender: Arc::new(Mutex::new(send)),
        })
    }

    pub async fn set(&self, key: &str, value: &str) -> Result<(), String> {
        let command = format!("set {} {};get {}", key, value, key);
        println!("Command: {}", command);
        match self.connect_and_wait_for_response(command.as_str()) {
            Some(_) => Ok(()),
            None => Ok(()),
        }
    }

    pub async fn get(&self, key: &str) -> Option<String> {
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

    fn connect_and_wait_for_response(&self, command: &str) -> Option<String> {
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
        return result.unwrap();
    }

    fn get_auth_data(&self) -> (String, String, String, String) {
        let user = self.user.clone();
        let password = self.password.clone();
        let db_name = self.db_name.clone();
        let server_url = self.server_url.clone();
        (user, password, db_name, server_url)
    }

    /*
    fn try_next(&mut self) -> Result<Option<String>, TryRecvError> {
        return self.receiver.try_next();
    }
    */
}

fn auth_on_sender(out: &ws::Sender, db_name: &String, user: &String, password: &String) {
    out.send(format!("use-db {} {} {}", db_name, user, password))
        .unwrap();
}

fn parse_value_response(command: &mut std::str::SplitN<&str>) -> Result<Response, String> {
    let _command_value = command.next().unwrap();
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
