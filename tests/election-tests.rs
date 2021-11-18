#[cfg(test)]
pub mod helpers;
mod tests {
    use crate::helpers::*;
    use assert_cmd::prelude::*; // Add methods on commands
    use predicates::prelude::*; // Used for writing assertions
    use std::process::Command;

    #[test]
    fn should_start_the_primary() -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let mut db_process = helpers::start_primary();
        let mut cmd = Command::cargo_bin("nun-db")?;
        let get_cmd = cmd.args(["-p", "mateus"])
            .args(["--user", "mateus"])
            .args(["--host", helpers::PRIMARY_HTTP_URI])
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
        let replicas_processes = helpers::start_3_replicas();
        helpers::nundb_exec(
            &helpers::PRIMARY_HTTP_URI.to_string(),
            &String::from("cluster-state"),
        )
        .success()
        .stdout(predicate::str::contains(format!(
            "{}:Primary",
            helpers::PRIMARY_TCP_ADDRESS
        )));
        helpers::kill_replicas(replicas_processes)?;
        Ok(())
    }

    #[test]
    fn should_elect_a_new_primary_if_the_primary_die() -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let mut replicas_processes = helpers::start_3_replicas();
        helpers::nundb_exec(
            &helpers::PRIMARY_HTTP_URI.to_string(),
            &String::from("cluster-state"),
        )
        .success()
        .stdout(predicate::str::contains(format!(
            "{}:Primary",
            helpers::PRIMARY_TCP_ADDRESS
        )));
        replicas_processes.0.kill()?; //Kill Primary
        helpers::wait_seconds(5); //Give it 5 seconds to elect the new leader
                                   // revisit
        helpers::nundb_exec(
            &helpers::SECOUNDAR_HTTP_URI.to_string(),
            &String::from("cluster-state"),
        )
        .success()
        .stdout(predicate::str::contains(format!(
            "{}:Primary",
            helpers::SECOUNDARY_TCP_ADDRESS
        )));
        helpers::nundb_exec(
            &helpers::SECOUNDAR_HTTP_URI.to_string(),
            &String::from("cluster-state"),
        )
        .success()
        .stdout(predicate::str::contains(format!(
            "{}:Primary",
            helpers::SECOUNDARY_TCP_ADDRESS
        )));

        helpers::kill_replicas(replicas_processes)?;
        Ok(())
    }
}
