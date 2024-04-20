use crate::configuration::NUN_LOG_LEVEL;
use env_logger::{Builder, Env, Target};
use futures::channel::mpsc::{channel, Sender};
use log;
use std::sync::Once;
use std::sync::{Arc, Mutex};
use ws::{connect, CloseCode, Handler, Message};

static INIT_LOG: Once = Once::new();

fn init_logger() {
    INIT_LOG.call_once(|| {
        let env = Env::default().filter_or("NUN_LOG_LEVEL", NUN_LOG_LEVEL.as_str());
        Builder::from_env(env)
            .format_level(false)
            .target(Target::Stdout)
            .format_timestamp_nanos()
            .init();
    });
}

use crate::bo::*;

pub struct NunDbClient {
    server_url: String,
    db_name: String,
    user: String,
    password: String,
    sender: Arc<Mutex<Sender<String>>>,
}

struct NunDbCLientWsHandler<'a> {
    socket_server: ws::Sender,
    extenal_sender: Arc<Mutex<Sender<String>>>,
    is_auth: bool,
    watch_fn: Option<&'a dyn Fn(&String) -> Result<(), String>>,
}
impl Handler for NunDbCLientWsHandler<'_> {
    fn on_message(&mut self, msg: Message) -> ws::Result<()> {
        log::debug!("Got message: {}", msg);
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
                            } => match watch_fn(&value) {
                                Ok(_) => {
                                    log::debug!("Watch worked");
                                }
                                Err(_) => {
                                    self.socket_server.close(CloseCode::Away).unwrap();
                                    log::debug!("watch fn filed will close our socket");
                                }
                            },
                            _ => {
                                log::error!("Error this response is not handled {}", message);
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
                            log::error!("Error this response is not handled {}", message);
                        }
                    }
                }
                Some("changed") => {
                    log::debug!(
                        "Changed event is ignored since it is only used in legacy versions"
                    );
                }
                Some(c) => {
                    log::warn!("Unhandle command {}", c);
                }
                None => {
                    panic!("Response without a command, PANIC!");
                }
            }
        } else {
            // first message ok means auth is done
            if self.is_auth {
                match self
                    .extenal_sender
                    .lock()
                    .unwrap()
                    .try_send("ok".to_string())
                {
                    Ok(_) => (),
                    Err(e) => {
                        log::error!("Error to send ok to external sender {:?}", e);
                        ()
                    }
                }
            } else {
                self.is_auth = true;
            }
        }
        Ok(())
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {
        log::debug!(
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
        init_logger();
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
        log::debug!("Command: {}", command);
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
            NunDbCLientWsHandler {
                socket_server: out,
                extenal_sender: self.sender.clone(),
                is_auth: false,
                watch_fn: Some(process_fn),
            }
        }) {
            log::error!("Failed to connect to server: {}", e);
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
            NunDbCLientWsHandler {
                socket_server: out,
                extenal_sender: send.clone(),
                is_auth: false,
                watch_fn: None,
            }
        }) {
            log::error!("Failed to connect to server: {}", e);
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
