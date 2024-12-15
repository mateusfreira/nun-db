use lazy_static::lazy_static;
use std::env;

lazy_static! {
    pub static ref NUN_USER: String = expect_env_var("NUN_USER", "nun", false);// Can be overridden by command line
    pub static ref NUN_PWD: String = expect_env_var("NUN_PWD", "nun-pwd", false);// Can be overridden by command line
    pub static ref NUN_DBS_DIR: String = optional_env_var("NUN_DBS_DIR", "dbs");
    pub static ref NUN_WS_ADDR: String = optional_env_var("NUN_WS_ADDR", "0.0.0.0:3012");
    pub static ref NUN_HTTP_ADDR: String = optional_env_var("NUN_HTTP_ADDR", "0.0.0.0:3013");
    pub static ref NUN_TCP_ADDR: String = optional_env_var("NUN_TCP_ADDR", "0.0.0.0:3014");
    pub static ref NUN_REPLICATE_ADDR: String = optional_env_var("NUN_REPLICATE_ADDR", "");
    pub static ref NUN_LOG_LEVEL: String = optional_env_var("NUN_LOG_LEVEL", "Info"); //(Off, Error, Warn, Info, Debug, Trace)
    pub static ref NUN_ELECTION_TIMEOUT: u128 = optional_env_var("NUN_ELECTION_TIMEOUT", "1000").to_string().parse::<u128>().unwrap();
    // 1GB
    pub static ref NUN_MAX_OP_LOG_SIZE: u64 = optional_env_var("NUN_MAX_OP_LOG_SIZE", "1073741824").to_string().parse::<u64>().unwrap();
    pub static ref NUN_DECLUTTER_INTERVAL: i64 = optional_env_var("NUN_DECLUTTER_INTERVAL", "300").to_string().parse::<i64>().unwrap();

    pub static ref NUN_S3_KEY_ID: String = optional_env_var("NUN_S3_KEY_ID", "");
    pub static ref NUN_S3_SECRET_KEY: String = optional_env_var("NUN_S3_SECRET_KEY", "");
    pub static ref NUN_S3_BUCKET: String = optional_env_var("NUN_S3_BUCKET", "nun-db");
    pub static ref NUN_S3_PREFIX: String = optional_env_var("NUN_S3_PREFIX", "/");
    pub static ref NUN_S3_API_URL: String = optional_env_var("NUN_S3_API_URL", "http://127.0.0.1:9000");

}

pub fn optional_env_var(name: &str, default: &str) -> String {
    return env::var(name).unwrap_or(String::from(default));
}

#[cfg(debug_assertions)]
fn expect_env_var(name: &str, default: &str, _required: bool) -> String {
    return env::var(name).unwrap_or(String::from(default));
}

#[cfg(not(debug_assertions))]
fn expect_env_var(name: &str, _default: &str, required: bool) -> String {
    if required {
        return env::var(name).expect(&format!(
            "Environment variable {name} is not defined",
            name = name
        ));
    } else {
        return env::var(name).unwrap_or("".to_string());
    }
}

#[cfg(test)]
mod tests {
    use crate::configuration::expect_env_var;
    use crate::configuration::optional_env_var;
    use std::env;

    #[test]
    fn run_mode_should_get_empty_but_debug_mode_got_value() {
        #[cfg(debug_assertions)]
        assert_eq!(expect_env_var("NUN_USER", "nun", false), "nun".to_string());

        #[cfg(not(debug_assertions))]
        assert_eq!(expect_env_var("NUN_USER", "", false), "".to_string())
    }

    #[test]
    fn optional_env_var_should_default_value_if_not_present() {
        env::remove_var("NUN_USER");
        assert_eq!(optional_env_var("NUN_USER", "jose"), "jose".to_string());
    }
}
