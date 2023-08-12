use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::executor::block_on;
use futures::stream::StreamExt;
use log;
use std::sync::Arc;
use std::thread;

use thread_id;
use ws::{CloseCode, Handler, Message};

use crate::bo::*;
use crate::process_request::*;
use crate::security::*;

const TO_CLOSE: &'static str = "##CLOSE##";

// Server WebSocket handler
struct Server {
    out: ws::Sender,
    dbs: Arc<Databases>,
    client: Client,
}

impl Handler for Server {
    fn on_open(&mut self, _: ws::Handshake) -> ws::Result<()> {
        let ws_sender = self.out.clone();
        let (sender, mut receiver): (Sender<String>, Receiver<String>) = channel(100);
        self.client.sender = sender;
        let _read_thread = thread::spawn(move || {
            let read_promise = async {
                loop {
                    let message = receiver.next().await;
                    if let Some(message) = message {
                        match message.as_ref() {
                            TO_CLOSE => {
                                log::debug!("Closing server connection");
                                break;
                            }
                            message => {
                                log::debug!("ws_ops::_read_thread::message {}", message);
                                match ws_sender.send(message) {
                                    Err(e) => {
                                        log::warn!("ws_ops::_read_thread::send::Error {}", e)
                                    }
                                    Ok(_) => (),
                                };
                            }
                        }
                    } else {
                        log::warn!("ws_ops::_read_thread::error::None Will close the connection");
                        break;
                    }
                }
            };
            block_on(read_promise);
        });
        Ok(())
    }

    fn on_message(&mut self, msg: Message) -> ws::Result<()> {
        let message = msg.as_text().unwrap();
        log::debug!(
            "[{}] Server got message '{}'. ",
            thread_id::get(),
            clean_string_to_log(&message, &self.dbs)
        );
        match process_request(&message, &self.dbs, &mut self.client) {
            Response::Error { msg } => {
                log::debug!("Error: {}", msg);
                match self.client.sender.try_send(format!("error {} \n", msg)) {
                    Ok(_) => {}
                    Err(e) => log::warn!(
                        "ws_ops::_read_thread::process_request::try_send::Error {}",
                        e
                    ),
                }
            }
            Response::VersionError {
                msg,
                key: _,
                old_version: _,
                version: _,
                old_value: _,
                state: _,
                change: _,
                db: _,
            } => {
                log::debug!("Error: {}", msg);
                match self.client.sender.try_send(format!("error {} \n", msg)) {
                    Ok(_) => {}
                    Err(e) => log::warn!(
                        "ws_ops::_read_thread::process_request::try_send::Error {}",
                        e
                    ),
                }
            }
            Response::Set { key, value } => {
                log::debug!(
                    "[{}] Server responded message  {:?} to set",
                    thread_id::get(),
                    message
                );
                match self.client.sender.try_send(format!("set {} {} \n", key, value)) {
                    Ok(_) => {}
                    Err(e) => log::warn!(
                        "ws_ops::_read_thread::process_request::_::try_send::Error {}",
                        e
                    ),
                }
                log::debug!("ws::Success processed");

            },
            response => {
                log::debug!(
                    "[{}] Server responded message  {:?} to {}",
                    thread_id::get(),
                    response,
                    message
                );
                match self.client.sender.try_send(format!("ok \n")) {
                    Ok(_) => {}
                    Err(e) => log::warn!(
                        "ws_ops::_read_thread::process_request::_::try_send::Error {}",
                        e
                    ),
                }
                log::debug!("ws::Success processed");
            }
        }

        Ok(())
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {
        log::debug!("WebSocket closing for ({:?}) {}", code, reason);
        match self.client.sender.try_send(TO_CLOSE.to_string()) {
            //To close the read thread
            Ok(_) => {}
            Err(e) => log::warn!("on_close::Error {}", e),
        }
        process_request("unwatch-all", &self.dbs, &mut self.client);
        self.client.left(&self.dbs);
    }
}

pub fn start_web_socket_client(dbs: Arc<Databases>, ws_address: Arc<String>) {
    let ws_address = ws_address.to_string();
    log::debug!("Starting the web socket client with addr: {}", ws_address);
    let server = thread::spawn(move || {
        let (sender, _): (Sender<String>, Receiver<String>) = channel(100);
        ws::Builder::new()
            .with_settings(ws::Settings {
                max_connections: 100000,
                ..ws::Settings::default()
            })
            .build(move |out| Server {
                out,
                dbs: dbs.clone(),
                client: Client::new_empty(sender.clone()),
            })
            .unwrap()
            .listen(ws_address)
            .unwrap()
    });

    log::debug!("WebSocket started ");
    let _ = server.join();
}
