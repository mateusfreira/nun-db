#[cfg(test)]
pub mod helpers;
mod tests {
    use crate::helpers::*;
    use assert_cmd::prelude::*; // Add methods on commands
    use predicates::prelude::*; // Used for writing assertions
    use std::process::Command;

    #[test]
    fn should_start_the_primary() -> Result<(), Box<dyn std::error::Error>> {
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
    fn should_replicate_as_expected() -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let mut primary_process = helpers::start_primary();
        let mut secoundary_process = helpers::start_secoundary();
        let mut set_primary = Command::cargo_bin("nun-db")?;
        let _ = set_primary
            .args(["-p", "mateus"])
            .args(["--user", "mateus"])
            .args(["--host", helpers::PRIMARY_HTTP_URI])
            .arg("exec")
            .arg("create-db test test-pwd; use-db test test-pwd;set-safe name 0 mateus;")
            .assert()
            .success()
            .stdout(predicate::str::contains("empty;empty"));
        helpers::wait_secounds(1); //Wait 1s to the replication

        let mut get_secoundary = Command::cargo_bin("nun-db")?;
        let get_cmd = get_secoundary
            .args(["-p", "mateus"])
            .args(["--user", "mateus"])
            .args(["--host", helpers::SECOUNDAR_HTTP_URI])
            .arg("exec")
            .arg("use-db test test-pwd;get-safe name");

        get_cmd
            .assert()
            .success()
            .stdout(predicate::str::contains("value-version 1 mateus"));
        primary_process.kill()?;
        secoundary_process.kill()?;
        Ok(())
    }
}
