#[cfg(test)]
pub mod helpers;
mod tests {
    use crate::helpers::*;
    use predicates::prelude::*; // Used for writing assertions

    #[test]
    fn should_replicate_as_expected() -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let replicas_processes = helpers::start_3_replicas();
        helpers::nundb_exec(
            &helpers::PRIMARY_HTTP_URI.to_string(),
            &String::from("create-db test test-pwd; use-db test test-pwd;set-safe name 0 mateus;"),
        )
        .success()
        .stdout(predicate::str::contains("empty")); // Empty is the expected response here
        helpers::wait_seconds(3); //Wait 1s to the replication
        helpers::nundb_exec(
            &helpers::SECOUNDAR_HTTP_URI.to_string(),
            &String::from("use-db test test-pwd;get name"),
        )
        .success()
        .stdout(predicate::str::contains("value mateus"));
        helpers::nundb_exec(
            &helpers::SECOUNDAR2_HTTP_URI.to_string(),
            &String::from("use-db test test-pwd;get name"),
        )
        .success()
        .stdout(predicate::str::contains("value mateus"));
        helpers::kill_replicas(replicas_processes)?;
        Ok(())
    }

    #[test]
    fn should_replicate_as_expected_set_safe() -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let replicas_processes = helpers::start_3_replicas();
        helpers::nundb_exec(
            &helpers::PRIMARY_HTTP_URI.to_string(),
            &String::from("create-db test test-pwd; use-db test test-pwd;set-safe name 0 mateus;"),
        )
        .success()
        .stdout(predicate::str::contains("empty;empty"));

        helpers::wait_seconds(1); //Wait 1s to the replication

        helpers::nundb_exec(
            &helpers::SECOUNDAR_HTTP_URI.to_string(),
            &String::from("use-db test test-pwd;get-safe name"),
        )
        .success()
        .stdout(predicate::str::contains("value-version 1 mateus"));

        helpers::nundb_exec(
            &helpers::SECOUNDAR2_HTTP_URI.to_string(),
            &String::from("use-db test test-pwd;get-safe name"),
        )
        .success()
        .stdout(predicate::str::contains("value-version 1 mateus"));

        //Save from secoundary 2 read from primary and secoundary 1
        helpers::nundb_exec(
            &helpers::SECOUNDAR2_HTTP_URI.to_string(),
            &String::from("use-db test test-pwd;set-safe name 1 maria;"),
        )
        .success()
        .stdout(predicate::str::contains("empty;empty"));

        helpers::nundb_exec(
            &helpers::PRIMARY_HTTP_URI.to_string(),
            &String::from("use-db test test-pwd;get name;get-safe name"),
        )
        .success()
        .stdout(predicate::str::contains("value maria"))
        .stdout(predicate::str::contains("value-version 2 maria"));

        helpers::nundb_exec(
            &helpers::SECOUNDAR_HTTP_URI.to_string(),
            &String::from("use-db test test-pwd;get name;get-safe name"),
        )
        .success()
        .stdout(predicate::str::contains("value maria"))
        .stdout(predicate::str::contains("value-version 2 maria"));
        helpers::kill_replicas(replicas_processes)?;
        Ok(())
    }
}
