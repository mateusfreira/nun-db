use std::sync::Arc;
use bo::*;

pub fn clean_string_to_log(
    input: &str, 
    dbs: &Arc<Databases>,
    ) -> String {
    return input.replace(&dbs.user.to_string(), "****").replace(&dbs.pwd.to_string(), "****");
}
