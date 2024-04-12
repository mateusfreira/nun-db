#[cfg(test)]
pub mod helpers;
mod tests {
    use std::sync::Once;
    use crate::helpers::*;
    use nundb::client::client::NunDbClient;
    use nundb::bo::Databases;
    use futures::executor::block_on;
    use std::{thread, time};
    use std::process::Child;

    static INIT: Once = Once::new();

    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }

    fn init() -> Child {
        helpers::clean_env();
        let primary = helpers::start_primary();
        helpers::nundb_exec_primary(
            &String::from("create-db sample sample-pwd;use sample sample-pwd;create-user user sample-pwd; set-permissions user rwix *"),
        );
        primary
    }

    fn kill_primary(mut child: Child) {
        child.kill().unwrap();
    }

    #[test]
    fn should_connect_to_nun_db() {
        let nun_client = NunDbClient::new("ws://localhost:3012", "test", "user", "test-pwd");
        assert_eq!(nun_client.is_ok(), true);
    }

    #[test]
    fn should_get_a_value() {
        init();
        helpers::run_test(|| {
            let rand_value = Databases::next_op_log_id().to_string();
            let nun_client = NunDbClient::new("ws://127.0.0.1:3058", "sample", "user", "sample-pwd");
            let con = nun_client.unwrap();
            let set = con.set("key1", rand_value.as_str());
            aw!(set);
            let value = con.get("key1");
            assert_eq!(aw!(value), rand_value);
        }, init, kill_primary);
    }

    #[test]
    fn should_watch_a_key() {
        helpers::run_test(|| {
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
        }, init, kill_primary);
    }
}

