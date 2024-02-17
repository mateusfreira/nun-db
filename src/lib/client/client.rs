use std::thread;
use ws::{connect, CloseCode, Message, Handler};
use crate::bo::*;


struct NunDbClient {
    server_url: String,
    db_name: String,
    user: String,
    password: String,
}

struct Tmp {
    socket_server: ws::Sender,
    extenal_sender: std::sync::mpsc::Sender<String>,
}
impl Handler for Tmp {

    fn on_message(&mut self, msg: Message) -> ws::Result<()> {
        println!("Got message: {}", msg);
        //self.socket_server.close(CloseCode::Normal);
        //self.socket_server.shutdown()
        let message = msg.to_string().split("\n").collect::<Vec<&str>>()[0].trim().to_string();
        if message != "ok" {
            let val = parse_value_response(&mut message.splitn(2, " ")).unwrap();
            match val {
                Response::Value { value, key, version } => {
                    self.extenal_sender.send(value).unwrap();
                }
                _ => {
                    println!("Error this response is not handled {}", message);
                }
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
        Ok(NunDbClient {
            server_url: server_url.to_string(),
            db_name: db_name.to_string(),
            user: user.to_string(),
            password: password.to_string(),
        })
    }

    pub async fn set(&self, key: &str, value: &str) {

    }

    pub async fn get(&self, key: &str) -> String {
        let (send, receive) = std::sync::mpsc::channel::<String>();
        let key = key.to_string();
        let user = self.user.clone();
        let password = self.password.clone();
        let db_name = self.db_name.clone();
        let server_url = self.server_url.clone();

        let c_thread = thread::spawn(move || {
            if let Err(e) = connect(server_url.clone(), |out| {
                out.send(format!("use-db {} {} {}", db_name, user, password)).unwrap();
                out.send(format!("get {} {}", key.clone(), db_name)).unwrap();
               Tmp { socket_server: out, extenal_sender: send.clone()}
            }) {
                println!("Failed to connect to server: {}", e);
            }
        });
        let result = receive.recv().unwrap();
        //c_thread.join().unwrap();
        println!("Connected to server");
        result
    }
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
        let nun_client = NunDbClient::new("ws://localhost:3058", "sample", "user", "sample-pwd");
        let con = nun_client.unwrap();
        con.set("key1", "value1");
        let value = con.get("key1");
        assert_eq!(aw!(value), "value1");
    }
}

