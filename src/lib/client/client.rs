use std::sync::Arc;
use std::thread;
use ws::{connect, CloseCode, Message, Handler};

use crate::bo::*;


struct NunDbClient {
    server_url: String,
    db_name: String,
    user: String,
    password: String,
    receiver: std::sync::mpsc::Receiver<String>,
    sender: Arc<std::sync::mpsc::Sender<String>>,
}

struct Tmp {
    socket_server: ws::Sender,
    extenal_sender: Arc<std::sync::mpsc::Sender<String>>,
    is_auth: bool,
}
impl Handler for Tmp {

    fn on_message(&mut self, msg: Message) -> ws::Result<()> {
        println!("Got message: {}", msg);
        let message = msg.to_string().split("\n").collect::<Vec<&str>>()[0].trim().to_string();
        if message != "ok" {
            let val = parse_value_response(&mut message.splitn(2, " ")).unwrap();
            match val {
                Response::Value { value, key: _, version: _ } => {
                    self.extenal_sender.send(value).unwrap();
                }
                _ => {
                    println!("Error this response is not handled {}", message);
                }
            }
        } else {
            // first message ok means auth is done 
            if self.is_auth {
                self.extenal_sender.send("ok".to_string()).unwrap();
            } else {
                self.is_auth = true;
            }
        }
        Ok(())
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {
        println!("Connection closed with code: {:?} and reason: {}", code, reason);
    }

}

impl NunDbClient {
    pub fn new(
        server_url: &str,
        db_name: &str,
        user: &str,
        password: &str,
    ) -> Result<NunDbClient, String> {
        let (send, receive) = std::sync::mpsc::channel::<String>();
        Ok(NunDbClient {
            server_url: server_url.to_string(),
            db_name: db_name.to_string(),
            user: user.to_string(),
            password: password.to_string(),
            receiver: receive,
            sender: Arc::new(send),
        })
    }

    pub async fn set(&self, key: &str, value: &str) {
        let command = format!("set {} {}", key, value);
        let _ = self.connect_and_wait_for_response(command.as_str());
    }

    pub async fn get(&self, key: &str) -> String {
        let command = format!("get {}", key);
        let result = self.connect_and_wait_for_response(command.as_str());
        result
    }

    pub fn connect(&self) {
        let (user, password, db_name, server_url) = self.get_auth_data();
        let con = thread::spawn(move || {
            if let Err(e) = connect(server_url.clone(), |out| {
                auth_on_sender(&out, &db_name, &user, &password);
                //out.send(command.clone()).unwrap();
               Tmp { socket_server: out, extenal_sender: self.sender.clone(), is_auth: false }
            }) {
                println!("Failed to connect to server: {}", e);
            }
        });
        con.join().unwrap();
    }

    fn connect_and_wait_for_response(&self, command: &str) -> String {
        let command = command.to_string();
        let (user, password, db_name, server_url) = self.get_auth_data();

        let (send, receive) = std::sync::mpsc::channel::<String>();
        let _ = thread::spawn(move || {
            if let Err(e) = connect(server_url.clone(), |out| {
                auth_on_sender(&out, &db_name, &user, &password);
                out.send(command.clone()).unwrap();
               Tmp { socket_server: out, extenal_sender: send.clone(), is_auth: false }
            }) {
                println!("Failed to connect to server: {}", e);
            }
        });
        let result = receive.recv().unwrap();
        result
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
    out.send(format!("use-db {} {} {}", db_name, user, password)).unwrap();
}



fn parse_value_response(command: &mut std::str::SplitN<&str>) -> Result<Response, String> {
    let value_command  = command.next().unwrap();
    let value = match command.next() {
        Some(value) => value.replace("\n", ""),
        None => return Err(format!("get must contain a key")),
    };
    Ok(Response::Value { value, key: String::from(""), version: 0})
}



#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    #[test]
    fn should_connect_to_nun_db() {
        // Connect to the NunDb server with local host test db user user and password test-pwd
        let nun_client = NunDbClient::new("ws://localhost:3012", "test", "user", "test-pwd");
        assert_eq!(nun_client.is_ok(), true);
    }

    #[test]
    fn should_get_a_value() {
        let rand_value = Databases::next_op_log_id().to_string();
        let nun_client = NunDbClient::new("ws://localhost:3058", "sample", "user", "sample-pwd");
        let con = nun_client.unwrap();
        let a  = con.connect();
        let set  = con.set("key1", rand_value.as_str());
        aw!(set);
        let value = con.get("key1");
        assert_eq!(aw!(value), rand_value);
        //a.join().unwrap();
    }
}

