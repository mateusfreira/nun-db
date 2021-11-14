#[cfg(test)]
mod tests {
    use assert_cmd::prelude::*; // Add methods on commands
    use predicates::prelude::*; // Used for writing assertions
    use std::process::Command;
    use std::{thread, time}; // Run programs
    #[test]
    fn test_test() -> Result<(), Box<dyn std::error::Error>> {
        let time_to_start = time::Duration::from_secs(3);

        let mut cmd = Command::cargo_bin("nun-db")?;
        let mut db_process = cmd
            .args(["-p", "mateus"])
            .args(["--user", "mateus"])
            .arg("start")
            .spawn()?;
        thread::sleep(time_to_start);

        let mut cmd = Command::cargo_bin("nun-db")?;
        let get_cmd = cmd.args(["-p", "mateus"])
            .args(["--user", "mateus"])
            .arg("exec")
            .arg("create-db test test-pwd; use-db test test-pwd;set-safe name 0 mateus; get-safe name");

        get_cmd.assert()
            .success()
            .stdout(predicate::str::contains("value-version 1 mateus"));
        db_process.kill()?;
        Ok(())
    }
}
