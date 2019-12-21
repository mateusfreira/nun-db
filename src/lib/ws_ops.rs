use std::sync::atomic::AtomicBool;
use std::sync::mpsc::channel;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::thread;
use thread_id;
use std::time;
use std::time::Instant;
use ws::{CloseCode, Handler, Message};

use bo::*;
use core::*;
use db_ops::*;

const TO_CLOSE: &'static str = "##CLOSE##";

// Server WebSocket handler
struct Server {
    out: ws::Sender,
    sender: Sender<String>,
    dbs: Arc<Databases>,
    db: Arc<SelectedDatabase>,
    auth: Arc<AtomicBool>,
}

impl Handler for Server {
    fn on_open(&mut self, _: ws::Handshake) -> ws::Result<()> {
        let (sender, receiver): (Sender<String>, Receiver<String>) = channel();
        self.sender = sender;
        let sender = self.out.clone();
        let _read_thread = thread::spawn(move || loop {
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
        let message = msg.as_text().unwrap();
        println!("[{}] Server got message '{}'. ", thread_id::get(), message);
        let start = Instant::now();
        process_request(&message, &self.sender, &self.db, &self.dbs, &self.auth);
        let elapsed = start.elapsed();
        println!("[{}] Server processed message '{}' in {:?}", thread_id::get(), message, elapsed);
        Ok(())
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {
        println!("WebSocket closing for ({:?}) {}", code, reason);
        self.sender.send(TO_CLOSE.to_string()).unwrap(); //Closes the read thread
    }
}

pub fn start_web_socket_client(dbs: Arc<Databases>) {
    let server = thread::spawn(move || {
        let (sender, _): (Sender<String>, Receiver<String>) = channel();
        ws::Builder::new()
            .with_settings(ws::Settings {
                max_connections: 10000,
                ..ws::Settings::default()
            })
            .build(move |out| Server {
                out,
                db: create_temp_selected_db("init".to_string()),
                dbs: dbs.clone(),
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
