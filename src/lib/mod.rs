pub mod bo;
pub mod core;
pub mod db_ops;
pub mod disk_ops;
pub mod http_ops;
pub mod tcp_ops;
pub mod ws_ops;

use bo::*;

impl Request {
    pub fn parse(input: &str) -> Result<Request, String> {
        let mut command = input.splitn(3, " ");
        let parsed_command = match command.next() {
            Some("watch") => {
                let key = match command.next() {
                    Some(key) => key.replace("\n", ""),
                    None => return Err(format!("watch must contain a key")),
                };
                Ok(Request::Watch { key })
            }
            Some("unwatch-all") => Ok(Request::UnWatchAll {}),
            Some("unwatch") => {
                let key = match command.next() {
                    Some(key) => key.replace("\n", ""),
                    None => return Err(format!("unwatch must contain a key")),
                };
                Ok(Request::UnWatch { key })
            }
            Some("get") => {
                let key = match command.next() {
                    Some(key) => key.replace("\n", ""),
                    None => return Err(format!("get must contain a key")),
                };
                Ok(Request::Get { key })
            }
            Some("set") => {
                let key = match command.next() {
                    Some(key) => key,
                    None => {
                        println!("SET must be followed by a key");
                        ""
                    }
                };
                let value = match command.next() {
                    Some(value) => value.replace("\n", ""),
                    None => {
                        println!("SET needs a value");
                        "".to_string()
                    }
                };
                Ok(Request::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                })
            }
            Some("auth") => {
                let user = match command.next() {
                    Some(key) => key,
                    None => {
                        println!("Auth needs to provide an user");
                        ""
                    }
                };
                let pwd = match command.next() {
                    Some(pwd) => pwd.to_string(),
                    None => {
                        println!("Auth needs and password");
                        "".to_string()
                    }
                };
                Ok(Request::Auth {
                    user: user.to_string(),
                    password: pwd.to_string().replace("\n", ""),
                })
            }
            Some("use-db") => {
                let name = match command.next() {
                    Some(name) => name,
                    None => {
                        println!("UseDb needs to provide an db name");
                        ""
                    }
                };
                let token = match command.next() {
                    Some(key) => String::from(key).replace("\n", ""),
                    None => {
                        println!("UseDb needs and token");
                        "".to_string()
                    }
                };
                Ok(Request::UseDb {
                    name: name.to_string(),
                    token: token.to_string(),
                })
            }
            Some("create-db") => {
                let name = match command.next() {
                    Some(key) => key,
                    None => {
                        println!("CreateDb needs to provide an db name");
                        ""
                    }
                };
                let token = match command.next() {
                    Some(key) => String::from(key).replace("\n", ""),
                    None => {
                        println!("CreateDb needs and token");
                        "".to_string()
                    }
                };

                Ok(Request::CreateDb {
                    name: name.to_string(),
                    token: token.to_string(),
                })
            }
            Some(cmd) => Err(format!("unknown command: {}", cmd)),
            _ => Err(format!("no command sent")),
        };
        parsed_command
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_use_db() -> Result<(), String> {
        match Request::parse("use-db foo some-key") {
            Ok(Request::UseDb { token, name }) => {
                if name == "foo" && token == "some-key" {
                    Ok(())
                } else {
                    Err(String::from("user should be foo and password bar"))
                }
            }
            _ => Err(String::from("get foo sould be parsed to Get command")),
        }
    }

    #[test]
    fn should_parse_create_db() -> Result<(), String> {
        match Request::parse("create-db foo some-key") {
            Ok(Request::CreateDb { token, name }) => {
                if name == "foo" && token == "some-key" {
                    Ok(())
                } else {
                    Err(String::from("user should be foo and password bar"))
                }
            }
            _ => Err(String::from("get foo sould be parsed to Get command")),
        }
    }

    #[test]
    fn should_parse_auth() -> Result<(), String> {
        match Request::parse("auth foo bar") {
            Ok(Request::Auth { user, password }) => {
                if user == "foo" && password == "bar" {
                    Ok(())
                } else {
                    Err(String::from("user should be foo and password bar"))
                }
            }
            _ => Err(String::from("get foo sould be parsed to Get command")),
        }
    }
    #[test]
    fn should_parse_get() -> Result<(), String> {
        match Request::parse("get foo") {
            Ok(Request::Get { key }) => {
                if key == "foo" {
                    Ok(())
                } else {
                    Err(String::from("the key should be foo"))
                }
            }
            _ => Err(String::from("get foo sould be parsed to Get command")),
        }
    }

    #[test]
    fn should_parse_get_ending_with_end_line() -> Result<(), String> {
        match Request::parse("get foo\n") {
            Ok(Request::Get { key }) => {
                if key == "foo" {
                    Ok(())
                } else {
                    Err(String::from("the key should be foo"))
                }
            }
            _ => Err(String::from("get foo should be parsed to Get command")),
        }
    }

    #[test]
    fn should_parse_set_ending_with_end_line() -> Result<(), String> {
        match Request::parse("set foo 1\n") {
            Ok(Request::Set { key, value }) => {
                if key == "foo" && value == "1" {
                    Ok(())
                } else {
                    Err(String::from(
                        "the key should be foo and the value should be 1",
                    ))
                }
            }
            _ => Err(String::from("get foo should be parsed to Set command")),
        }
    }

    #[test]
    fn should_parse_watch_ending_with_end_line() -> Result<(), String> {
        match Request::parse("watch foo\n") {
            Ok(Request::Watch { key }) => {
                if key == "foo" {
                    Ok(())
                } else {
                    Err(String::from("the key should be foo"))
                }
            }
            _ => Err(String::from("get foo should be parsed to watch command")),
        }
    }

    #[test]
    fn should_return_error_if_no_command_is_send() -> Result<(), String> {
        match Request::parse("a\n") {
            Err(msg) => {
                if msg == "unknown command: a\n" {
                    Ok(())
                } else {
                    Err(String::from("message it wrong"))
                }
            }
            _ => Err(String::from("get foo should be parsed to watch command")),
        }
    }

    #[test]
    fn should_return_an_unwatch_all() -> Result<(), String> {
        match Request::parse("unwatch-all") {
            Ok(Request::UnWatchAll {}) => Ok(()),
            _ => Err(String::from("unwatch-all not parsed correct")),
        }
    }
}
