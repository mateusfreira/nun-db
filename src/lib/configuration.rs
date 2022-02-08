use std::env;

#[derive(Debug)]
pub struct Configuration {
    pub nun_user: String,
    pub nun_pwd: String,
    pub nun_dbs_dir: String,
    pub nun_ws_addr: String,
    pub nun_http_addr: String,
    pub nun_tcp_addr: String,
    pub nun_replicate_addr: String,
    pub nun_log_level: String,
    pub nun_log_dir: String,
    pub nun_log_to_file: String,
}

#[cfg(debug_assertions)]
fn expect_env_var(name: &str, default: &str) -> String {
    return env::var(name).unwrap_or(String::from(default));
}

#[cfg(not(debug_assertions))]
fn expect_env_var(name: &str, _default: &str) -> String {
    return env::var(name).expect(&format!(
        "Environment variable {name} is not defined",
        name = name
    ));
}

pub fn get_configuration() -> Configuration {
    Configuration {
        nun_user: expect_env_var("NUN_USER", "mateus"),
        nun_pwd: expect_env_var("NUN_PWD", "mateus"),
        nun_dbs_dir: expect_env_var("NUN_DBS_DIR", "/tmp/data/nun_db"),
        nun_ws_addr: expect_env_var("NUN_WS_ADDR", "0.0.0.0:3012"),
        nun_http_addr: expect_env_var("NUN_HTTP_ADDR", "0.0.0.0:3013"),
        nun_tcp_addr: expect_env_var("NUN_HTTP_ADDR", "0.0.0.0:3014"),
        nun_replicate_addr: expect_env_var("NUN_REPLICATE_ADDR", ""),
        nun_log_level: expect_env_var("NUN_LOG_LEVEL", "Info"), //(Off, Error, Warn, Info, Debug, Trace)
        nun_log_dir: expect_env_var("NUN_LOG_DIR", "/tmp/data/nun_db/log"),
        nun_log_to_file: expect_env_var("NUN_LOG_TO_FILE", "true"), //true or false
    }
}


#[cfg(test)]
mod tests {
    use crate::lib::configuration::{get_configuration};

    #[test]
    fn run_mode_should_get_empty_but_debug_mode_got_value() {
        let config = get_configuration();
        
        #[cfg(debug_assertions)]
        assert_eq!(config.nun_user, "mateus".to_string());

        #[cfg(not(debug_assertions))]
        assert_eq!(config.nun_user, "".to_string())
    }

}
