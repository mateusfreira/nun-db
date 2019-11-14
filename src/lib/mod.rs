use std::sync::mpsc::Sender;
use std::sync::{Mutex};

use std::collections::HashMap;

pub struct Database {
    pub map: Mutex<HashMap<String, String>>,
}

pub struct Watchers {
    pub map: Mutex<HashMap<String, Vec<Sender<String>>>>,
}

pub enum Request {
    Get { key: String },
    Set { key: String, value: String },
    Watch { key: String },
    Auth { user: String, password: String },
}

pub enum Response {
    Value { key: String, value: String },
    Ok {},
    Set { key: String, value: String },
    Error { msg: String },
}

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
                    password: pwd.to_string(),
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
}
