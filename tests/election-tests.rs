#[cfg(test)]
pub mod helpers;
mod tests {
    use crate::helpers::*;
    // Add methods on commands
    use assert_cmd::prelude::*; // Add methods on commands
    use predicates::prelude::*; // Used for writing assertions
    use std::process::Command;

    #[test]
    fn should_start_the_primary() -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let (mut db_process, primary_uri) = helpers::start_primary_uri(3030);
        let mut cmd = Command::cargo_bin("nun-db")?;
        let get_cmd = cmd.args(["-p", "mateus"])
            .args(["--user", "mateus"])
            .args(["--host", &primary_uri])
            .arg("exec")
            .arg("create-db test test-pwd; use-db test test-pwd;set-safe name 0 mateus; get-safe name");

        get_cmd
            .assert()
            .success()
            .stdout(predicate::str::contains("value-version 1 mateus"));
        db_process.kill()?;
        Ok(())
    }

    #[test]
    fn older_process_should_be_the_primary() -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let replicas_processes = helpers::start_3_replicas_uri(3016);
        let primary_http = replicas_processes.3;
        let processes = (
            replicas_processes.0,
            replicas_processes.1,
            replicas_processes.2,
        );
        helpers::nundb_exec(&primary_http, &String::from("cluster-state"))
            .success()
            .stdout(predicate::str::contains(format!(
                "{}(self):Primary",
                helpers::PRIMARY_TCP_ADDRESS
            )));
        helpers::kill_replicas(processes)?;
        Ok(())
    }

    #[test]
    fn should_elect_a_new_primary_if_the_primary_die() -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let (test_env, mut primary, secoundary, secoundary2) =
            helpers::start_full_replica_set(1217);
        helpers::nundb_exec(
            &test_env.primary.get_http_uri(),
            &String::from("cluster-state"),
        )
        .success()
        .stdout(predicate::str::contains(format!(
            "{}(self):Primary",
            &test_env.primary.tcp_address
        )));
        primary.kill()?; //Kill Primary
        helpers::wait_seconds(5); //Give it 5 seconds to elect the new leader
                                  // revisit
        helpers::nundb_exec(
            &test_env.secoundary.get_http_uri(),
            &String::from("cluster-state"),
        )
        .success()
        .stdout(predicate::str::contains(format!(
            "{}(self):Primary",
            &test_env.secoundary.tcp_address
        )));
        helpers::nundb_exec(
            &test_env.secoundary2.get_http_uri(),
            &String::from("cluster-state"),
        )
        .success()
        .stdout(predicate::str::contains(format!(
            "{}(Connected):Primary",
            &test_env.secoundary.tcp_address
        )));

        let processes = (secoundary, secoundary2, primary);
        helpers::kill_replicas(processes)?;
        Ok(())
    }

    /*
     * Depends on docker needs to see a faster way to do the same thing
    fn before_each() {
        println!("before each");
        helpers::clean_env();
        //helpers::stop_db_with_docker();
        helpers::start_db_with_docker();
    }

    fn after_each() {
        println!("after each");
        helpers::stop_db_with_docker();
    }

    #[test]
    fn should_run_election_as_expected_with_latenct() {
        helpers::run_test(
            || {
                helpers::nundb_exec(
                    &helpers::PRIMARY_HTTP_URI.to_string(),
                    &String::from("cluster-state"),
                )
                .success()
                .stdout(predicate::str::contains("toxiproxy:3017:Secoundary"))
                .stdout(predicate::str::contains("toxiproxy:3018:Primary"))
                .stdout(predicate::str::contains("toxiproxy:3019:Secoundary"));
                ()
            },
            before_each,
            after_each,
        )
    }
    */
}
