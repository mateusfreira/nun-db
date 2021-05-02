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
            Some("keys") => Ok(Request::Keys {}),
            Some("snapshot") => Ok(Request::Snapshot {}),
            Some("replicate-snapshot") => {
                let db_name = match command.next() {
                    Some(db_name) => db_name.replace("\n", ""),
                    None => return Err(format!("replicate-snapshot must contain a db name")),
                };
                Ok(Request::ReplicateSnapshot { db: db_name })
            }
            Some("replicate-since") => {
                let nome_name = match command.next() {
                    Some(db_name) => db_name.replace("\n", ""),
                    None => return Err(format!("replicate-since must contain a node name")),
                };

                let start_at_str = match command.next() {
                    Some(start_at_str) => start_at_str.replace("\n", ""),
                    None => return Err(format!("replicate-since must contain a start at")),
                };

                let start_at = match start_at_str.parse::<u64>() {
                    Ok(start_at) => start_at,
                    Err(_) => return Err(format!("replicate-since start_at must be a u64")),
                };

                Ok(Request::ReplicateSince {
                    node_name: nome_name,
                    start_at: start_at,
                })
            }
            Some("cluster-state") => Ok(Request::ClusterState {}),
            Some("election") => match command.next() {
                Some("win") => Ok(Request::ElectionWin {}),
                Some("cadidate") => {
                    let process_id = match command.next() {
                        Some(id) => id.parse::<u128>().unwrap(),
                        None => return Err(format!("replicate-snapshot must contain a db name")),
                    };
                    Ok(Request::Election { id: process_id })
                }
                _ => Ok(Request::ElectionActive {}),
            },
            Some("join") => {
                let name = match command.next() {
                    Some(name) => name.replace("\n", ""),
                    None => return Err(format!("join must contain a name")),
                };
                Ok(Request::Join { name: name })
            }

            Some("leave") => {
                let name = match command.next() {
                    Some(name) => name.replace("\n", ""),
                    None => return Err(format!("leave must contain a name")),
                };
                Ok(Request::Leave { name: name })
            }

            Some("replicate-leave") => {
                let name = match command.next() {
                    Some(name) => name.replace("\n", ""),
                    None => return Err(format!("leave must contain a name")),
                };
                Ok(Request::ReplicateLeave { name: name })
            }

            Some("replicate-join") => {
                let name = match command.next() {
                    Some(name) => name.replace("\n", ""),
                    None => return Err(format!("join must contain a name")),
                };
                Ok(Request::ReplicateJoin { name: name })
            }

            Some("set-primary") => {
                let name = match command.next() {
                    Some(name) => name.replace("\n", ""),
                    None => return Err(format!("set-primary must contain a name")),
                };
                Ok(Request::SetPrimary { name: name })
            }

            Some("set-secoundary") => {
                let name = match command.next() {
                    Some(name) => name.replace("\n", ""),
                    None => return Err(format!("set-primary must contain a name")),
                };
                Ok(Request::SetScoundary { name: name })
            }
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

            Some("remove") => {
                let key = match command.next() {
                    Some(key) => key,
                    None => {
                        println!("REMOVE must be followed by a key");
                        ""
                    }
                };
                Ok(Request::Remove {
                    key: key.to_string(),
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

            Some("replicate-remove") => {
                let db = match command.next() {
                    Some(key) => key,
                    None => {
                        println!("replicate-remove needs to provide an db name");
                        ""
                    }
                };

                let key = match command.next() {
                    Some(key) => String::from(key).replace("\n", ""),
                    None => {
                        println!("replicate remove needs key");
                        "".to_string()
                    }
                };
                Ok(Request::ReplicateRemove {
                    db: db.to_string(),
                    key: key,
                })
            }
            Some("replicate") => {
                let db = match command.next() {
                    Some(key) => key,
                    None => {
                        println!("replicate needs to provide an db name");
                        ""
                    }
                };
                return match command.next() {
                    Some(command_value) => {
                        let mut command = command_value.splitn(2, " ");
                        let name = match command.next() {
                            Some(key) => String::from(key).replace("\n", ""),
                            None => {
                                println!("ReplicateSet needs name");
                                "".to_string()
                            }
                        };

                        let value = match command.next() {
                            Some(key) => String::from(key).replace("\n", ""),
                            None => {
                                println!("ReplicateSet needs value");
                                "".to_string()
                            }
                        };
                        Ok(Request::ReplicateSet {
                            db: db.to_string(),
                            key: name.to_string(),
                            value: value.to_string(),
                        })
                    }
                    None => {
                        println!("replicate needs to provide an db name");
                        Err(format!("no command sent"))
                    }
                };
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

    #[test]
    fn should_parse_replicaion() -> Result<(), String> {
        match Request::parse("replicate vue jose 1") {
            Ok(Request::ReplicateSet { db, key, value }) => {
                if db == "vue" && key == "jose" && value == "1" {
                    Ok(())
                } else {
                    Err(String::from("db should vue key should be jose value 1"))
                }
            }
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_replicaion_snapshot() -> Result<(), String> {
        match Request::parse("replicate-snapshot vue") {
            Ok(Request::ReplicateSnapshot { db }) => {
                if db == "vue" {
                    Ok(())
                } else {
                    Err(String::from("db should vue key should be jose value 1"))
                }
            }
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_join() -> Result<(), String> {
        match Request::parse("join some:8071") {
            Ok(Request::Join { name }) => {
                if name == "some:8071" {
                    Ok(())
                } else {
                    Err(String::from("Name is wrong!"))
                }
            }
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_replicate_join() -> Result<(), String> {
        match Request::parse("replicate-join some:8071") {
            Ok(Request::ReplicateJoin { name }) => {
                if name == "some:8071" {
                    Ok(())
                } else {
                    Err(String::from("Name is wrong!"))
                }
            }
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_replicaion_set_primary() -> Result<(), String> {
        match Request::parse("set-primary some:3014") {
            Ok(Request::SetPrimary { name }) => {
                if name == "some:3014" {
                    Ok(())
                } else {
                    Err(String::from("set primary name value wrong parsed!!"))
                }
            }
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_cluster_state() -> Result<(), String> {
        match Request::parse("cluster-state") {
            Ok(Request::ClusterState {}) => Ok(()),
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_election_win() -> Result<(), String> {
        match Request::parse("election win") {
            Ok(Request::ElectionWin {}) => Ok(()),
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_keys() -> Result<(), String> {
        match Request::parse("keys") {
            Ok(Request::Keys {}) => Ok(()),
            _ => Err(String::from("wrong command parsed")),
        }
    }
}
