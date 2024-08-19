#[cfg(test)]
pub mod helpers;
mod tests {
    use crate::helpers::*;
    use predicates::prelude::*; // Used for writing assertions

    #[test]
    fn should_safe_sate_a_value() -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let mut db_process = helpers::start_primary();
        helpers::nundb_exec_primary(
            "create-db test test-pwd; use-db test test-pwd;set-safe name 0 mateus; get-safe name",
        )
        .stdout(predicate::str::contains("value-version 1 mateus"));
        db_process.kill()?;
        Ok(())
    }

    #[test]
    fn should_not_set_if_set_safe_is_invalid() -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let mut db_process = helpers::start_primary();
        helpers::nundb_exec_primary("create-db test test-pwd none; use-db test test-pwd;set-safe name 0 mateus; set-safe name 0 maria;get-safe name")
            .stdout(predicate::str::contains("value-version 1 mateus"));

        helpers::nundb_exec_primary("use-db test test-pwd;set-safe name 0 amanda;")
            .stdout(predicate::str::contains("Invalid version!"));

        helpers::nundb_exec_primary("use-db test test-pwd;set-safe name 1 amanda;")
            .stdout(predicate::str::contains("empty;empty"));
        db_process.kill()?;
        Ok(())
    }

    #[test]
    fn should_set_if_set_safe_is_valid() -> Result<(), Box<dyn std::error::Error>> {
        helpers::clean_env();
        let mut db_process = helpers::start_primary();
        helpers::nundb_exec_primary("create-db test test-pwd; use-db test test-pwd;set-safe name 0 mateus; set-safe name 2 maria;get-safe name")
            .stdout(predicate::str::contains("value-version 3 maria"));

        db_process.kill()?;
        Ok(())
    }
}
