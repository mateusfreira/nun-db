#[cfg(test)]
pub mod helpers;
mod tests {
    use crate::helpers::*;
    use predicates::prelude::*; // Used for writing assertions

    #[test]
    #[cfg(not(tarpaulin))]
    fn should_replicate_user_creation() -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let replicas_processes = helpers::start_3_replicas();
        helpers::nundb_exec(
            &helpers::PRIMARY_HTTP_URI.to_string(),
            &String::from("create-db test test-pwd;"),
        )
        .success()
        .stdout(predicate::str::contains("create-db success")); // Empty is the expected response here
        helpers::nundb_exec(
            &helpers::PRIMARY_HTTP_URI.to_string(),
            &String::from("use-db test test-pwd; create-user foo bar;set-permissions foo rw *; set-safe name 0 mateus;"),
        )
        .success()
        .stdout(predicate::str::contains("empty")); // Empty is the expected response here
        helpers::wait_seconds(3);
        helpers::nundb_exec(
            //&helpers::PRIMARY_HTTP_URI.to_string(),
            &helpers::SECOUNDAR_HTTP_URI.to_string(),
            &String::from("use-db test foo bar;get name"),
        )
        .success()
        .stdout(predicate::str::contains("value mateus"));
        helpers::nundb_exec(
            &helpers::SECOUNDAR2_HTTP_URI.to_string(),
            &String::from("use-db test foo bar;get name"),
        )
        .success()
        .stdout(predicate::str::contains("value mateus"));
        helpers::kill_replicas(replicas_processes)?;
        Ok(())
    }

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

        helpers::wait_seconds(helpers::time_to_start_replica() * 3); //Wait 3s to the replication

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

    #[test]
    #[cfg(not(tarpaulin))]
    fn should_use_exter_address_if_external_addr_is_provided(
    ) -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let replicas_processes = helpers::start_3_replicas_with_external_addr();
        helpers::wait_seconds(3); //Wait 3s to the replication

        helpers::nundb_exec(
            &helpers::PRIMARY_HTTP_URI.to_string(),
            &String::from("create-db test test-pwd; use-db test test-pwd;set-safe name 0 mateus;"),
        )
        .success()
        .stdout(predicate::str::contains("empty;empty"));

        helpers::wait_seconds(3); //Wait 3s to the replication

        helpers::nundb_exec(
            &helpers::PRIMARY_HTTP_URI.to_string(),
            &String::from("cluster-state"),
        )
        .success()
        .stdout(predicate::str::contains("localhost:3016"));
        helpers::wait_seconds(3); //Wait 3s to the replication
        helpers::nundb_exec(
            &helpers::PRIMARY_HTTP_URI.to_string(),
            &String::from("metrics-state"),
        )
        .success()
        .stdout(predicate::str::contains("pending_ops: 0"));

        helpers::kill_replicas(replicas_processes)?;
        Ok(())
    }

    /*
    // This tests require latancy beetwhen processes
    #[test]
    fn should_set_and_imediatly_read_from_secoundary() -> Result<(), Box<dyn std::error::Error>> {
        let db_name_seed = helpers::get_db_name_seed();
        helpers::clean_env();
        let replicas_processes = helpers::start_3_replicas();
        helpers::nundb_exec(
            &helpers::PRIMARY_HTTP_URI.to_string(),
            &String::from(format!("create-db {} test-pwd; use-db {} test-pwd;", db_name_seed, db_name_seed)),
        )
        .success()
        .stdout(predicate::str::contains("empty"));

        helpers::wait_seconds(3); //Wait 3s to the replication

        let db_name_seed_t1 = db_name_seed.clone();
        let secound_thread = thread::spawn(move || {
            helpers::nundb_exec(
                &helpers::SECOUNDAR_HTTP_URI.to_string(),
                &String::from(format!("use-db {} test-pwd;set-safe name 2 mateus; get-safe name", db_name_seed_t1)),
            )
            .success()
            .stdout(predicate::str::contains("value-version 3 mateus"));
        });

        let db_name_seed_t1 = db_name_seed.clone();
        let secound2_thread = thread::spawn(move || {
            helpers::nundb_exec(
                &helpers::SECOUNDAR2_HTTP_URI.to_string(),
                &String::from(format!("use-db {} test-pwd;set-safe name 0 maria; get-safe name", db_name_seed_t1.clone())),
            )
            .success()
            .stdout(predicate::str::contains("value-version 1 maria"));
        });
        secound_thread.join().unwrap();
        secound2_thread.join().unwrap();

        helpers::nundb_exec(
            &helpers::PRIMARY_HTTP_URI.to_string(),
            &String::from("use-db test test-pwd;get-safe name"),
        )
        .success()
        .stdout(predicate::str::contains("value-version 3 mateus"));
        helpers::kill_replicas(replicas_processes)?;
        Ok(())
    }

    #[test]
    fn should_not_replicate_processing_from_the_sending_replica(
    ) -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let replicas_processes = helpers::start_3_replicas();
        helpers::create_test_db();

        let secound_thread = thread::spawn(move || {
            helpers::nundb_exec(
                &helpers::SECOUNDAR_HTTP_URI.to_string(),
                &String::from("use-db test test-pwd;set-safe name 0 mateus; get-safe name"),
            )
            .success()
            .stdout(predicate::str::contains("value-version 1 mateus"));
        });

        let secound2_thread = thread::spawn(move || {
            helpers::nundb_exec(
                &helpers::SECOUNDAR2_HTTP_URI.to_string(),
                &String::from("use-db test test-pwd;set-safe name 2 maria; get-safe name"),
            )
            .success()
            .stdout(predicate::str::contains("value-version 3 maria"));
        });
        secound_thread.join().unwrap();
        secound2_thread.join().unwrap();

        helpers::nundb_exec(
            &helpers::SECOUNDAR2_HTTP_URI.to_string(),
            &String::from("use-db test test-pwd;get-safe name"),
        )
        .success()
        .stdout(predicate::str::contains("value-version 3 maria"));
        helpers::kill_replicas(replicas_processes)?;
        Ok(())
    }
    */
}
