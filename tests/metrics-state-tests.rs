#[cfg(test)]
pub mod helpers;
mod tests {
    use crate::helpers::*;
    use predicates::prelude::*; // Used for writing assertions

    #[test]
    fn should_return_the_metrics_state() -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let (mut db_process, uri) = helpers::start_primary_uri(3229);
        helpers::nundb_exec(&uri, "metrics-state")
            .stdout(predicate::str::contains("metrics-state pending_ops: 0, op_log_file_size: 0, op_log_count: 0,replication_time_moving_avg: 0.0, get_query_time_moving_avg: 0.0"));

        helpers::nundb_exec(&uri, "create-db test test-pwd; use-db test test-pwd;set-safe name 1 mateus; set-safe name 2 maria;set-safe name 3 mateus; set-safe name 4 maria;get-safe name");

        helpers::nundb_exec(&uri,"metrics-state").stdout(predicate::str::contains(
            "op_log_file_size: 125, op_log_count: 5",
        )); //3 transations

        db_process.kill()?;
        Ok(())
    }
}
