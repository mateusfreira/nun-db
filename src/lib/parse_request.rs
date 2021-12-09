use crate::bo::*;
use log;

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
            Some("oplog-state") => Ok(Request::OpLogState {}),
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
            Some("get-safe") => {
                let key = match command.next() {
                    Some(key) => key.replace("\n", ""),
                    None => return Err(format!("get-safe must contain a key")),
                };
                Ok(Request::GetSafe { key })
            }
            Some("set") => {
                let key = match command.next() {
                    Some(key) => key,
                    None => {
                        log::debug!("SET must be followed by a key");
                        ""
                    }
                };
                let value = match command.next() {
                    Some(value) => value.replace("\n", ""),
                    None => {
                        log::debug!("SET needs a value");
                        "".to_string()
                    }
                };
                Ok(Request::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                    version: -1,
                })
            }
            Some("set-safe") => {
                let key = match command.next() {
                    Some(key) => key,
                    None => {
                        log::debug!("SET must be followed by a key");
                        ""
                    }
                };
                let mut rest = match command.next() {
                    Some(rest) => rest.splitn(2, " "),
                    None => {
                        log::debug!("set-safe must be followed by a version and a key");
                        return Err(String::from(
                            "set-safe must be followed by a version and key",
                        ));
                    }
                };

                let version = match rest.next() {
                    Some(value) => match i32::from_str_radix(&value.replace("\n", ""), 10) {
                        Ok(n) => n,
                        _ => -1,
                    },
                    None => -1,
                };

                let value = match rest.next() {
                    Some(value) => value.replace("\n", ""),
                    None => return Err(String::from("set-safe must be followed by a key")),
                };

                Ok(Request::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                    version,
                })
            }
            Some("increment") => {
                let key = match command.next() {
                    Some(key) => key,
                    None => {
                        log::debug!("increment must be followed by a key");
                        ""
                    }
                };
                let inc = match command.next() {
                    Some(value) => match i32::from_str_radix(&value.replace("\n", ""), 10) {
                        Ok(n) => n,
                        _ => 1,
                    },
                    None => 1,
                };
                Ok(Request::Increment {
                    key: key.to_string(),
                    inc: inc,
                })
            }
            Some("replicate-increment") => {
                let db_name = match command.next() {
                    Some(db_name) => db_name.replace("\n", ""),
                    None => return Err(format!("replicate-snapshot must contain a db name")),
                };

                let mut rest = match command.next() {
                    Some(rest) => rest.splitn(2, " "),
                    None => {
                        log::debug!("increment must be followed by a key");
                        return Err(String::from(
                            "replicate-increment must be followed by a key",
                        ));
                    }
                };

                let key = match rest.next() {
                    Some(key) => key,
                    None => {
                        return Err(String::from(
                            "replicate-increment must be followed by a key",
                        ))
                    }
                };

                let inc = match rest.next() {
                    Some(value) => match i32::from_str_radix(&value.replace("\n", ""), 10) {
                        Ok(n) => n,
                        _ => 1,
                    },
                    None => 1,
                };
                Ok(Request::ReplicateIncrement {
                    db: db_name.to_string(),
                    key: key.to_string(),
                    inc: inc,
                })
            }

            Some("remove") => {
                let key = match command.next() {
                    Some(key) => key,
                    None => {
                        log::debug!("REMOVE must be followed by a key");
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
                        log::debug!("Auth needs to provide an user");
                        ""
                    }
                };
                let pwd = match command.next() {
                    Some(pwd) => pwd.to_string(),
                    None => {
                        log::debug!("Auth needs and password");
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
                        log::debug!("UseDb needs to provide an db name");
                        ""
                    }
                };
                let token = match command.next() {
                    Some(key) => String::from(key).replace("\n", ""),
                    None => {
                        log::debug!("UseDb needs and token");
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
                        log::debug!("CreateDb needs to provide an db name");
                        ""
                    }
                };
                let token = match command.next() {
                    Some(key) => String::from(key).replace("\n", ""),
                    None => {
                        log::debug!("CreateDb needs and token");
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
                        log::debug!("replicate-remove needs to provide an db name");
                        ""
                    }
                };

                let key = match command.next() {
                    Some(key) => String::from(key).replace("\n", ""),
                    None => {
                        log::debug!("replicate remove needs key");
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
                        log::debug!("replicate needs to provide an db name");
                        ""
                    }
                };
                return match command.next() {
                    Some(command_value) => {
                        let mut command = command_value.splitn(2, " ");
                        let name = match command.next() {
                            Some(key) => String::from(key).replace("\n", ""),
                            None => {
                                log::debug!("ReplicateSet needs name");
                                "".to_string()
                            }
                        };

                        let value = match command.next() {
                            Some(key) => String::from(key).replace("\n", ""),
                            None => {
                                log::debug!("ReplicateSet needs value");
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
                        log::debug!("replicate needs to provide an db name");
                        Err(format!("no command sent"))
                    }
                };
            }
            Some("rp") => {
                let opp_id: u64 = match command.next() {
                    Some(id_str) => match id_str.parse::<u64>() {
                        Ok(id) => id,
                        Err(_) => {
                            return Err(format!("Invalid request Id"));
                        }
                    },
                    None => {
                        return Err(format!("Invalid request Id"));
                    }
                };

                let request_str: String = String::from(match command.next() {
                    Some(request_str) => request_str,
                    None => "",
                });

                if request_str == "" {
                    return Err(format!("Invalid replication request str"));
                }

                Ok(Request::ReplicateRequest {
                    opp_id,
                    request_str,
                })
            }
            Some("ack") => {
                let opp_id: u64 = match command.next() {
                    Some(id_str) => match id_str.parse::<u64>() {
                        Ok(id) => id,
                        Err(_) => {
                            return Err(format!("Invalid request Id"));
                        }
                    },
                    None => {
                        return Err(format!("Invalid request Id"));
                    }
                };

                let server_name: String = String::from(match command.next() {
                    Some(request_str) => request_str,
                    None => "",
                });

                if server_name == "" {
                    return Err(format!("Invalid server name"));
                }

                Ok(Request::Acknowledge {
                    opp_id,
                    server_name,
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
    fn should_parse_get_safe() -> Result<(), String> {
        match Request::parse("get-safe name") {
            Ok(Request::GetSafe { key }) => {
                if key == "name" {
                    Ok(())
                } else {
                    Err(String::from("Key must be name"))
                }
            }
            _ => Err(String::from("get-safe was not parsed correctly!")),
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
    fn should_parse_set_with_version() -> Result<(), String> {
        match Request::parse("set-safe foo 2 1\n") {
            Ok(Request::Set {
                key,
                value,
                version,
            }) => {
                if key == "foo" && value == "1" && version == 2 {
                    Ok(())
                } else {
                    Err(String::from(
                        "the key should be foo and the value should be 1 and version 2",
                    ))
                }
            }
            _ => Err(String::from("get foo should be parsed to set command")),
        }
    }

    #[test]
    fn should_parse_set_safe_without_version() -> Result<(), String> {
        match Request::parse("set-safe foo 1\n") {
            Err(message) => {
                if message == "set-safe must be followed by a key" {
                    Ok(())
                } else {
                    Err(String::from("Wrong validation error"))
                }
            }
            _ => Err(String::from("Should not have parsed")),
        }
    }

    #[test]
    fn should_parse_set_ending_with_end_line() -> Result<(), String> {
        match Request::parse("set foo 1\n") {
            Ok(Request::Set {
                key,
                value,
                version,
            }) => {
                if key == "foo" && value == "1" && version == -1 {
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
    fn should_parse_oplogstate() -> Result<(), String> {
        match Request::parse("oplog-state") {
            Ok(Request::OpLogState {}) => Ok(()),
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_increment() -> Result<(), String> {
        match Request::parse("increment key") {
            Ok(Request::Increment { key, inc }) => {
                if key == "key" && inc == 1 {
                    Ok(())
                } else {
                    Err(String::from("wrong command parsed"))
                }
            }
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_increment_with_value() -> Result<(), String> {
        match Request::parse("increment key 10") {
            Ok(Request::Increment { key, inc }) => {
                if key == "key" && inc == 10 {
                    Ok(())
                } else {
                    Err(String::from("wrong command parsed"))
                }
            }
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_increment_negative_value() -> Result<(), String> {
        match Request::parse("increment key -10") {
            Ok(Request::Increment { key, inc }) => {
                if key == "key" && inc == -10 {
                    Ok(())
                } else {
                    Err(String::from("wrong command parsed"))
                }
            }
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_replicate_increment_with_value() -> Result<(), String> {
        match Request::parse("increment key 10") {
            Ok(Request::Increment { key, inc }) => {
                if key == "key" && inc == 10 {
                    Ok(())
                } else {
                    Err(String::from("wrong command parsed"))
                }
            }
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_replicate_increment_negative_value() -> Result<(), String> {
        match Request::parse("replicate-increment db-name key -10") {
            Ok(Request::ReplicateIncrement { db, key, inc }) => {
                log::debug!("{},{},{}", db, key, inc);
                if key == "key" && inc == -10 && db == "db-name" {
                    Ok(())
                } else {
                    Err(String::from("wrong command parsed"))
                }
            }
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

    #[test]
    fn should_parse_repliation_request_valid() -> Result<(), String> {
        match Request::parse("rp 1 replicate joao 1") {
            Ok(Request::ReplicateRequest {
                opp_id,
                request_str,
            }) => {
                if opp_id == 1 && request_str == "replicate joao 1" {
                    Ok(())
                } else {
                    Err(String::from("Invalid replication request"))
                }
            }
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_repliation_request_invalid_valid() -> Result<(), String> {
        match Request::parse("rp 1") {
            Err(message) => {
                if message == "Invalid replication request str" {
                    Ok(())
                } else {
                    Err(String::from("Invalid error message"))
                }
            }
            _ => Err(String::from("Should not have parsed!!")),
        }
    }

    #[test]
    fn should_parse_ack_command() -> Result<(), String> {
        match Request::parse("ack 1 serv1") {
            Ok(Request::Acknowledge {
                opp_id,
                server_name,
            }) => {
                if opp_id == 1 {
                    if server_name == "serv1" {
                        Ok(())
                    } else {
                        Err(String::from("Invalid server name"))
                    }
                } else {
                    Err(String::from("Invalid id"))
                }
            }
            _ => Err(String::from("Invalid parsing")),
        }
    }
}
