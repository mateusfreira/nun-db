#[cfg(test)]
pub mod helpers {

    use assert_cmd::prelude::*; // Add methods on commands
    use predicates::prelude::*;
    use std::env;
    use std::panic;
    use std::process::Child;
    use std::process::Command;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use std::{thread, time}; // Used for writing assertions

    pub const USER_NAME: &'static str = "mateus";
    pub const PWD: &'static str = "mateus";

    pub const PRIMARY_TCP_ADDRESS: &'static str = "127.0.0.1:3017";
    pub const PRIMARY_HTTP_ADDRESS: &'static str = "127.0.0.1:9092";
    pub const PRIMARY_WS_ADDRESS: &'static str = "127.0.0.1:3058";
    pub const PRIMARY_HTTP_URI: &'static str = "http://127.0.0.1:9092";

    pub const SECOUNDARY_HTTP_ADDRESS: &'static str = "127.0.0.1:9093";
    pub const SECOUNDARY_TCP_ADDRESS: &'static str = "127.0.0.1:3018";
    pub const SECOUNDARY_WS_ADDRESS: &'static str = "127.0.0.1:3059";
    pub const SECOUNDAR_HTTP_URI: &'static str = "http://127.0.0.1:9093";

    pub const SECOUNDARY2_HTTP_ADDRESS: &'static str = "127.0.0.1:9094";
    pub const SECOUNDAR2_HTTP_URI: &'static str = "http://127.0.0.1:9094";
    pub const SECOUNDARY2_TCP_ADDRESS: &'static str = "127.0.0.1:3016";
    pub const SECOUNDARY2_TCP_ADDRESS_LOCAL: &'static str = "localhost:3016";
    pub const SECOUNDARY2_WS_ADDRESS: &'static str = "127.0.0.1:3060";

    pub const REPLICATE_SET_ADDRS: &'static str = "127.0.0.1:3016,127.0.0.1:3017,127.0.0.1:3018";

    pub fn time_to_start_replica() -> u64 {
        match env::var_os("TIME_TO_START") {
            Some(time_to_start) => time_to_start.into_string().unwrap().parse::<u64>().unwrap(),
            None => 1,
        }
    }

    pub fn start_primary() -> std::process::Child {
        let mut cmd = Command::cargo_bin("nun-db").unwrap();
        let db_process = cmd
            .args(["-p", PWD])
            .args(["--user", USER_NAME])
            .arg("start")
            .args(["--http-address", PRIMARY_HTTP_ADDRESS])
            .args(["--tcp-address", PRIMARY_TCP_ADDRESS])
            .args(["--ws-address", PRIMARY_WS_ADDRESS])
            .args(["--replicate-address", REPLICATE_SET_ADDRS])
            .env("NUN_DBS_DIR", "/tmp/dbs")
            .spawn()
            .unwrap();
        wait_seconds(time_to_start_replica()); // Need 1s here to run initial election
        db_process
    }

    pub fn start_secoundary() -> std::process::Child {
        let mut cmd = Command::cargo_bin("nun-db").unwrap();
        let db_process = cmd
            .args(["-p", USER_NAME])
            .args(["--user", PWD])
            .arg("start")
            .args(["--http-address", SECOUNDARY_HTTP_ADDRESS])
            .args(["--tcp-address", SECOUNDARY_TCP_ADDRESS])
            .args(["--ws-address", SECOUNDARY_WS_ADDRESS])
            .args(["--replicate-address", REPLICATE_SET_ADDRS])
            .env("NUN_DBS_DIR", "/tmp/dbs1")
            .spawn()
            .unwrap();
        wait_seconds(time_to_start_replica()); // Need 1s here to run initial election
        db_process
    }

    pub fn start_secoundary_2_external_addr() -> std::process::Child {
        let mut cmd = Command::cargo_bin("nun-db").unwrap();
        let db_process = cmd
            .args(["-p", USER_NAME])
            .args(["--user", PWD])
            .arg("start")
            .args(["--http-address", SECOUNDARY2_HTTP_ADDRESS])
            .args(["--tcp-address", SECOUNDARY2_TCP_ADDRESS])
            .args(["--external-address", SECOUNDARY2_TCP_ADDRESS_LOCAL])
            .args(["--ws-address", SECOUNDARY2_WS_ADDRESS])
            .args(["--replicate-address", REPLICATE_SET_ADDRS])
            .env("NUN_DBS_DIR", "/tmp/dbs2")
            .spawn()
            .unwrap();
        wait_seconds(time_to_start_replica()); // Need 1s here to run initial election
        db_process
    }
    pub fn start_secoundary_2() -> std::process::Child {
        let mut cmd = Command::cargo_bin("nun-db").unwrap();
        let db_process = cmd
            .args(["-p", USER_NAME])
            .args(["--user", PWD])
            .arg("start")
            .args(["--http-address", SECOUNDARY2_HTTP_ADDRESS])
            .args(["--tcp-address", SECOUNDARY2_TCP_ADDRESS])
            .args(["--ws-address", SECOUNDARY2_WS_ADDRESS])
            .args(["--replicate-address", REPLICATE_SET_ADDRS])
            .env("NUN_DBS_DIR", "/tmp/dbs2")
            .spawn()
            .unwrap();
        wait_seconds(time_to_start_replica()); // Need 1s here to run initial election
        db_process
    }

    pub fn nundb_exec(host: &str, command: &str) -> assert_cmd::assert::Assert {
        Command::cargo_bin("nun-db")
            .unwrap()
            .args(["-p", "mateus"])
            .args(["--user", "mateus"])
            .args(["--host", host])
            .arg("exec")
            .arg(command)
            .assert()
    }

    pub fn nundb_call(host: &str, command: &str) -> std::process::Output {
        Command::cargo_bin("nun-db")
            .unwrap()
            .args(["-p", "mateus"])
            .args(["--user", "mateus"])
            .args(["--host", host])
            .arg("exec")
            .arg(command)
            .output()
            .unwrap()
    }

    pub fn nundb_exec_primary(command: &str) -> assert_cmd::assert::Assert {
        nundb_exec(PRIMARY_HTTP_URI, command)
    }

    pub fn nundb_exec_secondary(command: &str) -> assert_cmd::assert::Assert {
        nundb_exec(SECOUNDAR_HTTP_URI, command)
    }

    pub fn wait_seconds(time: u64) {
        let time_to_start = time::Duration::from_secs(time);
        thread::sleep(time_to_start);
    }

    pub fn clean_env() {
        let mut cmd = Command::new("bash");
        let clen_cmd = cmd.args(["-c", "rm -Rf /tmp/dbs||true&&rm -Rf /tmp/dbs1||true&&rm -Rf /tmp/dbs2||true/&&mkdir  /tmp/dbs2||true/&&mkdir /tmp/dbs1||true&&mkdir /tmp/dbs||true&&killall nun-db|true"]);
        clen_cmd.assert().success();
    }

    pub fn start_3_replicas() -> (Child, Child, Child) {
        (start_primary(), start_secoundary(), start_secoundary_2())
    }

    pub fn start_3_replicas_with_external_addr() -> (Child, Child, Child) {
        (
            start_primary(),
            start_secoundary(),
            start_secoundary_2_external_addr(),
        )
    }

    pub fn create_test_db() {
        nundb_exec(
            &PRIMARY_HTTP_URI.to_string(),
            &String::from("create-db test test-pwd; use-db test test-pwd;"),
        )
        .success()
        .stdout(predicate::str::contains("empty"));
        wait_seconds(3); //Wait 3s to the replication
    }

    pub fn kill_replicas(
        mut replica_processes: (Child, Child, Child),
    ) -> Result<(), Box<dyn std::error::Error>> {
        log::info!("Killing replicas");
        replica_processes.0.kill()?;
        replica_processes.1.kill()?;
        replica_processes.2.kill()?;
        log::info!("Replicas killed");
        Ok(())
    }

    pub fn get_db_name_seed() -> String {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let id = since_the_epoch.as_nanos() as u64;
        return format!("{}", id);
    }
    pub fn start_db_with_docker() {
        let mut cmd = Command::new("make");
        let cmd = cmd.args(["restart-all-replicas"]);
        cmd.assert().success();
        wait_seconds(5);
    }

    pub fn stop_db_with_docker() {
        //let mut cmd = Command::new("make");
        //let cmd = cmd.args(["docker-down"]);
        //cmd.assert().success();
    }

    pub fn run_test<T>(test: T, setup: fn() -> Child, teardown: fn(Child) -> ()) -> ()
    where
        T: FnOnce() -> () + panic::UnwindSafe,
    {
        let child = setup();
        let result = panic::catch_unwind(|| test());
        teardown(child);
        assert!(result.is_ok())
    }
}
