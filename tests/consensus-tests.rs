#[cfg(test)]
pub mod helpers;
mod tests {
    use crate::helpers::*;
    use predicates::prelude::*; // Used for writing assertions
    /// Should create an chain of conflicts
    /// Force set key name 2 times with wrong version
    /// set value suceeds
    /// change 2 conflicts with change 1 therefore should point to the resolution command
    /// change 3 conflicts with change 2 therefore should point to the change2 conflict key
    #[test]
    fn should_chain_conflicts() -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let (test_env, primary, secoundary, secoundary2) = helpers::start_full_replica_set();

        helpers::nundb_exec(
            &test_env.primary.get_http_uri(),
            "create-db test test-pwd arbiter; use-db test test-pwd;set-safe name 1 mateus; get-safe name;",
        )
        .stdout(predicate::str::contains("value-version 2 mateus"));

        helpers::wait_seconds(3); //Give it 2 seconds to elect the new leader
                                  // revisit
        let output =  helpers::nundb_call(
            &test_env.secoundary.get_http_uri(),
            &String::from("use test test-pwd;arbiter;set-safe name 0 change-1; set-safe name 0 change-2;set-safe name 0 change-3;ls"),
        );

        let output_string = String::from_utf8(output.stdout).unwrap();
        let mut response_parts = output_string.split("Response ");
        response_parts.next(); // command
        let respose = response_parts.next().unwrap(); // response
        let mut parts = respose.split(";");
        parts.next(); // auth command
        parts.next(); // use command
        parts.next(); // arbiter command
        let conflict1 = parts
            .next()
            .unwrap()
            .split("unresolved")
            .last()
            .unwrap()
            .trim(); // set command
        let conflict2 = parts
            .next()
            .unwrap()
            .split("unresolved")
            .last()
            .unwrap()
            .trim(); // set command
        let conflict3 = parts
            .next()
            .unwrap()
            .split("unresolved")
            .last()
            .unwrap()
            .trim(); // set command
        helpers::wait_seconds(2);
        let get_command = format!("use test test-pwd;get {}", conflict1);
        helpers::nundb_exec(&test_env.secoundary.get_http_uri(), &get_command)
            // value resolve ${change_id} test 2 name mateus change-1
            .stdout(predicate::str::contains("test 2 name mateus change-1"));

        let get_command_conflict2 = format!("use test test-pwd;get {}", conflict2);
        helpers::nundb_exec( &test_env.secoundary.get_http_uri(), &get_command_conflict2)
            .stdout(predicate::str::contains(conflict1));

        let get_command_conflict3 = format!("use test test-pwd;get {}", conflict3);
        helpers::nundb_exec(&test_env.secoundary.get_http_uri(), &get_command_conflict3, )
            .stdout(predicate::str::contains(conflict2));

        let processes = (
            primary,
            secoundary,
            secoundary2,
        );
        helpers::kill_replicas(processes)?;

        Ok(())
    }


    #[test]
    fn should_replicate_conflicts_keys_to_all_replicas() -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let (test_env, primary, secoundary, secoundary2) = helpers::start_full_replica_set();
        helpers::nundb_exec(
            &test_env.primary.get_http_uri(),
            "create-db test test-pwd arbiter; use-db test test-pwd;set-safe name 1 mateus; get-safe name;",
        )
        .stdout(predicate::str::contains("value-version 2 mateus"));

        helpers::wait_seconds(2);
        let output =  helpers::nundb_call(
            &test_env.primary.get_http_uri(),
            &String::from("use test test-pwd;arbiter;set-safe name 0 change-1; set-safe name 0 change-2;set-safe name 0 change-3;ls"),
        );

        helpers::wait_seconds(2);
        let output_string = String::from_utf8(output.stdout).unwrap();
        let mut response_parts = output_string.split("Response ");
        response_parts.next(); // command
        let respose = response_parts.next().unwrap(); // response
        let mut parts = respose.split(";");
        parts.next(); // auth command
        parts.next(); // use command
        parts.next(); // arbiter command
        let conflict1 = parts
            .next()
            .unwrap()
            .split("unresolved")
            .last()
            .unwrap()
            .trim(); // set command
                     // Should be working, replication implemented, maybe timing?
        let get_command = format!("use test test-pwd;get {}", conflict1);
        helpers::nundb_exec(&test_env.primary.get_http_uri(),&get_command)
            // value resolve ${change_id} test 2 name mateus change-1
            .stdout(predicate::str::contains("test 2 name mateus change-1"));

        let replicas_processes = (
            primary,
            secoundary,
            secoundary2,
        );
        helpers::kill_replicas(replicas_processes)?;
        Ok(())
    }

    // @todo read from another replicate after replicate (value must not change in the other
    // replica if not resolved yet)
}
