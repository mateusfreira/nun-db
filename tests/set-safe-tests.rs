#[cfg(test)]
pub mod helpers;
mod tests {
    use crate::helpers::*;
    use assert_cmd::prelude::*; // Add methods on commands
    use predicates::prelude::*; // Used for writing assertions
    use std::process::Command;

    #[test]
    fn should_safe_sate_a_value() -> Result<(), Box<dyn std::error::Error>> {
        let mut db_process = helpers::start_db();
        let mut cmd = Command::cargo_bin("nun-db")?;
        let get_cmd = cmd.args(["-p", "mateus"])
            .args(["--user", "mateus"])
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
    fn should_not_safe_if_set_safe_is_invalid() -> Result<(), Box<dyn std::error::Error>> {
        let mut db_process = helpers::start_db();
        let mut cmd = Command::cargo_bin("nun-db")?;
        let get_cmd = cmd.args(["-p", "mateus"])
            .args(["--user", "mateus"])
            .arg("exec")
            .arg("create-db test test-pwd; use-db test test-pwd;set-safe name 0 mateus; set-safe name 0 maria ;get-safe name");

        get_cmd
            .assert()
            .success()
            .stdout(predicate::str::contains("value-version 1 mateus"));
        db_process.kill()?;
        Ok(())
    }

    #[test]
    fn should_safe_if_set_safe_is_valid() -> Result<(), Box<dyn std::error::Error>> {
        let mut db_process = helpers::start_db();
        let mut cmd = Command::cargo_bin("nun-db")?;
        let get_cmd = cmd.args(["-p", "mateus"])
            .args(["--user", "mateus"])
            .arg("exec")
            .arg("create-db test test-pwd; use-db test test-pwd;set-safe name 0 mateus; set-safe name 2 maria;get-safe name");

        get_cmd
            .assert()
            .success()
            .stdout(predicate::str::contains("value-version 2 maria"));
        db_process.kill()?;
        Ok(())
    }
}
