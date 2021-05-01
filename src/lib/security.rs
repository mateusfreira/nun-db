use bo::*;
use std::sync::Arc;

pub fn clean_string_to_log(input: &str, dbs: &Arc<Databases>) -> String {
    let user_replacer: String = format!("{} ", &dbs.user.to_string());
    let pwd_replacer: String = format!("{}", &dbs.pwd.to_string());
    return input
        .replace(&user_replacer.to_string(), "**** ")
        .replace(&pwd_replacer.to_string(), "****");
}

#[cfg(test)]
mod tests {
    use super::*;
    use db_ops::*;
    use futures::channel::mpsc::{channel, Receiver, Sender};
    use std::collections::HashMap;

    #[test]
    fn should_clean_user_and_pwd() -> Result<(), String> {
        let (start_replication_sender, _receiver): (Sender<String>, Receiver<String>) =
            channel(100);
        let (replication_sender, _receiver): (Sender<String>, Receiver<String>) = channel(100);
        let tcp_addr = String::from("127.0.0.1");
        let keys_map = HashMap::new();
        let dbs = create_init_dbs(
            String::from("mateus"),
            String::from("mateus-123"),
            tcp_addr,
            start_replication_sender,
            replication_sender,
            keys_map,
        );

        let clean_input = clean_string_to_log("auth mateus mateus-123;", &dbs);

        assert_eq!(clean_input, String::from("auth **** ****;"));
        Ok(())
    }
}
