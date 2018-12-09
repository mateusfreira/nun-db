use std::sync::mpsc::{Sender, Receiver};
use std::sync::{Arc, Mutex};

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
}

pub enum Response {
    Value { key: String, value: String },
    Ok {} ,
    Set { key: String, value: String},
    Error { msg: String },
}


pub struct SenderBox {
	pub sender: Mutex<Sender<String>>,
}

impl Request {
     pub fn parse(input: &str) -> Result<Request, String> {
        let mut command  = input.splitn(3, " ");
        let parsed_command  = match command.next() {
            Some("watch") => {
                let key = match command.next() {
                    Some(key) => key.replace("\n", ""),
                    None => {
                        return Err(format!("watch must contain a key")) 
                    }
                };
                Ok(Request::Watch{ key })
            },
            Some("get") => {
                let key = match command.next() {
                    Some(key) => key.replace("\n", ""),
                    None => {
                        return Err(format!("get must contain a key")) 
                    }
                };
                Ok(Request::Get{ key })
            },
            Some("set") =>  {
                let key = match command.next() {
                    Some(key) => key,
                    None => {
                        println!("SET must be followed by a key");
                        ""
                    },
                };
                let value = match command.next() {
                    Some(value) => value.replace("\n", ""),
                    None => {
                        println!("SET needs a value");
                        "".to_string()
                    },
                };
                Ok(Request::Set { key: key.to_string(), value: value.to_string() })
            },
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
    fn should_parse_get() -> Result<(), String> {
		match Request::parse("get foo") {
            Ok(Request::Get{ key }) => {
                if(key == "foo") {
			        Ok(())
                } else {
                    Err(String::from("the key should be foo"))
                }
            },
            _ => {
                Err(String::from("get foo sould be parsed to Get command"))
            }
        } 
    }

    #[test]
    fn should_parse_get_ending_with_end_line() -> Result<(), String> {
		match Request::parse("get foo\n") {
            Ok(Request::Get{ key }) => {
                if(key == "foo") {
			        Ok(())
                } else {
                    Err(String::from("the key should be foo"))
                }
            },
            _ => {
                Err(String::from("get foo sould be parsed to Get command"))
            }
        } 
    }



    #[test]
    fn should_parse_set_ending_with_end_line() -> Result<(), String> {
		match Request::parse("set foo 1\n") {
            Ok(Request::Set{ key, value}) => {
                if(key == "foo" && value == "1") {
			        Ok(())
                } else {
                    Err(String::from("the key should be foo and the value should be 1"))
                }
            },
            _ => {
                Err(String::from("get foo sould be parsed to Set command"))
            }
        } 
    }
 
    #[test]
    fn should_parse_watch_ending_with_end_line() -> Result<(), String> {
		match Request::parse("watch foo\n") {
            Ok(Request::Watch{ key }) => {
                if(key == "foo") {
			        Ok(())
                } else {
                    Err(String::from("the key should be foo"))
                }
            },
            _ => {
                Err(String::from("get foo sould be parsed to watch command"))
            }
        } 
    }

    #[test]
    fn should_return_error_if_no_command_is_send() -> Result<(), String> {
		match Request::parse("a\n") {
            Err(msg) => {
                if( msg == "unknown command: a\n") {
                    Ok(())
                } else {
                    Err(String::from("message it wrong"))
                }
            },
            _ => {
                Err(String::from("get foo sould be parsed to watch command"))
            }
        } 
    }
}
