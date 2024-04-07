use crate::bo::*;
use lazy_static::lazy_static;
use log;
use std::collections::HashMap;

lazy_static! {
    static ref PARSER_HASH_TABLE: HashMap<&'static str, fn(&mut std::str::SplitN<&str>) -> Result<Request, String>> = {
        let mut map: HashMap<&str, fn(&mut std::str::SplitN<&str>) -> Result<Request, String>> =
            HashMap::new();
        map.insert("ack", parse_ack_command);
        map.insert("arbiter", parse_arbiter_command);
        map.insert("auth", parse_auth_command);
        map.insert("cluster-state", |_| Ok(Request::ClusterState {}));

        map.insert("create-db", parse_create_db_command);
        map.insert("create-user", parse_create_user_command);

        map.insert("debug", parse_debug_command);
        map.insert("election", parse_election_command);

        map.insert("get", parse_get_command);
        map.insert("get-safe", parse_get_safe_command);
        map.insert("increment", parse_increment_command);
        map.insert("join", parse_join_command);
        map.insert("keys", parse_keys_command);
        map.insert("leave", parse_leave_command);
        map.insert("ls", parse_keys_command);
        map.insert("metrics-state", |_| Ok(Request::MetricsState {}));
        map.insert("remove", parse_remove_command);
        map.insert("replicate", parse_replicate_command);
        map.insert("replicate-increment", parse_replicate_increment_command);
        map.insert("replicate-join", parse_replicate_join_command);
        map.insert("replicate-leave", parse_replicate_leave_command);
        map.insert("replicate-remove", parse_replicate_remove_command);
        map.insert("replicate-since", parse_replicate_since_command);
        map.insert("replicate-snapshot", parse_replicate_snapshot_command);
        map.insert("resolve", parse_resolve_command);
        map.insert("rp", parse_rp_command);
        map.insert("set", parse_set_command);
        map.insert("set-primary", parse_set_primary_command);
        map.insert("set-safe", parse_set_safe_command);
        map.insert("set-secoundary", parse_set_secoundary_command);
        map.insert("snapshot", parse_snapshot_command);
        map.insert("unwatch", parse_unwatch_command);
        map.insert("unwatch-all", |_| Ok(Request::UnWatchAll {}));
        map.insert("use", parse_use_command);
        map.insert("use-db", parse_use_command);
        map.insert("watch", parse_watch_command);
        map.insert("list-commands", parse_list_commands_command);
        map.insert("set-permissions", parse_set_permissions_command);

        map
    };
}

impl Request {
    pub fn command_list() -> Vec<String> {
        PARSER_HASH_TABLE.keys().map(|x| x.to_string()).collect()
    }
    pub fn parse(input: &str) -> Result<Request, String> {
        let mut command = input.splitn(3, " ");
        if let Some(cmd) = command.next() {
            match PARSER_HASH_TABLE.get(cmd) {
                Some(f) => f(&mut command),
                None => {
                    log::debug!("unknown command: {}", cmd);
                    Err(format!("unknown command: {}", cmd))
                }
            }
        } else {
            log::debug!("unknown command");
            Err(String::from("unknown command"))
        }
    }
}

fn parse_auth_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
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
fn parse_remove_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
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
fn parse_replicate_increment_command(
    command: &mut std::str::SplitN<&str>,
) -> Result<Request, String> {
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
        inc,
    })
}
fn parse_increment_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
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
        inc,
    })
}
fn parse_set_safe_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
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

fn parse_set_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
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
fn parse_get_safe_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let key = match command.next() {
        Some(key) => key.replace("\n", ""),
        None => return Err(format!("get-safe must contain a key")),
    };
    Ok(Request::GetSafe { key })
}

fn parse_get_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let key = match command.next() {
        Some(key) => key.replace("\n", ""),
        None => return Err(format!("get must contain a key")),
    };
    Ok(Request::Get { key })
}

fn parse_unwatch_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let key = match command.next() {
        Some(key) => key.replace("\n", ""),
        None => return Err(format!("unwatch must contain a key")),
    };
    Ok(Request::UnWatch { key })
}
fn parse_set_secoundary_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let name = match command.next() {
        Some(name) => name.replace("\n", ""),
        None => return Err(format!("set-secoundary must contain a name")),
    };
    Ok(Request::SetScoundary { name })
}
fn parse_set_primary_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let name = match command.next() {
        Some(name) => name.replace("\n", ""),
        None => return Err(format!("set-primary must contain a name")),
    };
    Ok(Request::SetPrimary { name })
}
fn parse_replicate_join_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let name = match command.next() {
        Some(name) => name.replace("\n", ""),
        None => return Err(format!("join must contain a name")),
    };
    Ok(Request::ReplicateJoin { name })
}

fn parse_replicate_leave_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let name = match command.next() {
        Some(name) => name.replace("\n", ""),
        None => return Err(format!("leave must contain a name")),
    };
    Ok(Request::ReplicateLeave { name })
}

fn parse_leave_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let name = match command.next() {
        Some(name) => name.replace("\n", ""),
        None => return Err(format!("leave must contain a name")),
    };
    Ok(Request::Leave { name })
}

fn parse_join_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let name = match command.next() {
        Some(name) => name.replace("\n", ""),
        None => return Err(format!("join must contain a name")),
    };
    Ok(Request::Join { name: name })
}

fn parse_election_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    match command.next() {
        Some("win") => Ok(Request::ElectionWin {}),
        Some("candidate") => {
            let rest = match command.next() {
                Some(rest) => rest,
                None => return Err(format!("candidate must contain process id and server name")),
            };
            let mut rest = rest.splitn(2, " ");
            let process_id = match rest.next() {
                Some(id) => id.parse::<u128>().unwrap(),
                None => return Err(format!("replicate-snapshot must contain a db name")),
            };
            let node_name = match rest.next() {
                Some(server_name) => server_name.replace("\n", ""),
                None => {
                    log::warn!("election missing server name there must be an un-updated node on the cluster");
                    "no-server".to_string()
                }
            };
            Ok(Request::Election {
                id: process_id,
                node_name,
            })
        }
        _ => Ok(Request::ElectionActive {
            node_name: command.next().unwrap_or("no-server").to_string(),
        }),
    }
}

fn parse_resolve_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let opp_id = match command.next() {
        Some(opp_id) => match opp_id.parse::<u64>() {
            Ok(opp_id) => opp_id,
            Err(_) => return Err(format!("Invalid opp_id")),
        },
        None => return Err(format!("opp id mandatory")),
    };
    let mut rest = match command.next() {
        Some(rest) => rest.splitn(4, " "),
        None => {
            log::debug!("resolve missing params");
            return Err(String::from(
                "resoved must be followed by db_name, key version and value",
            ));
        }
    };

    let db_name = match rest.next() {
        Some(db_name) => db_name.replace("\n", ""),
        None => return Err(String::from("db_name must be provided")),
    };

    let key = match rest.next() {
        Some(key) => key.replace("\n", ""),
        None => return Err(String::from("key must be provided")),
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

    Ok(Request::Resolve {
        key: key.to_string(),
        value: value.to_string(),
        version,
        db_name,
        opp_id,
    })
}

fn parse_debug_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let debug_command = match command.next() {
        Some(command) => command.to_string(),
        None => {
            return Err(format!("command is mandatory"));
        }
    };

    Ok(Request::Debug {
        command: debug_command,
    })
}

fn parse_arbiter_command(_: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    Ok(Request::Arbiter {})
}

fn parse_set_permissions_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let user = match command.next() {
        Some(user) => user.to_string(),
        None => {
            return Err(format!("user is mandatory"));
        }
    };
    let rest_str = match command.next() {
        Some(pl) => pl.to_string(),
        None => {
            return Err(format!("permission list is mandatory"));
        }
    };
    let permissions = Permission::permissions_from_str(rest_str.as_str());

    Ok(Request::SetPermissions { user, permissions })
}

fn parse_ack_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    log::debug!("Parsing ack command");
    let opp_id: u64 = match command.next() {
        Some(id_str) => match id_str.parse::<u64>() {
            Ok(id) => id,
            Err(_) => {
                log::debug!("Invalid request Id");
                return Err(format!("Invalid request Id"));
            }
        },
        None => {
            log::debug!("Invalid request Id");
            return Err(format!("Invalid request Id"));
        }
    };

    let server_name: String = String::from(match command.next() {
        Some(request_str) => request_str,
        None => "",
    });

    if server_name == "" {
        log::debug!("Invalid server name");
        return Err(format!("Invalid server name"));
    }

    Ok(Request::Acknowledge {
        opp_id,
        server_name,
    })
}
fn parse_rp_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
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

fn parse_replicate_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let db = match command.next() {
        Some(key) => key,
        None => {
            log::debug!("replicate needs to provide an db name");
            ""
        }
    };
    return match command.next() {
        Some(command_value) => {
            let mut command = command_value.splitn(3, " ");
            let name = match command.next() {
                Some(key) => String::from(key).replace("\n", ""),
                None => {
                    log::debug!("ReplicateSet needs name");
                    "".to_string()
                }
            };

            let version = match command.next() {
                Some(value) => match i32::from_str_radix(&value.replace("\n", ""), 10) {
                    Ok(n) => n,
                    _ => -1,
                },
                None => -1,
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
                version,
            })
        }
        None => {
            log::debug!("replicate needs to provide an db name");
            Err(format!("no command sent"))
        }
    };
}

fn parse_replicate_remove_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
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
        key,
    })
}

fn parse_replicate_since_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
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
        start_at,
    })
}

fn parse_replicate_snapshot_command(
    command: &mut std::str::SplitN<&str>,
) -> Result<Request, String> {
    let db_name = match command.next() {
        Some(db_name) => db_name.replace("\n", ""),
        None => return Err(format!("replicate-snapshot must contain a db name")),
    };
    let reclaim_space = match command.next() {
        Some(reclaim_space) => reclaim_space.replace("\n", "") == "true",
        None => false,
    };
    Ok(Request::ReplicateSnapshot {
        db_names: db_name.split("|").map(|s| s.to_string()).collect(),
        reclaim_space,
    })
}

fn parse_snapshot_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let reclaim_space = command.next().unwrap_or("false");
    let dbs = command
        .next()
        .unwrap_or("")
        .split("|")
        .map(|s| s.to_string())
        .filter(|a| a != "")
        .collect();

    Ok(Request::Snapshot {
        reclaim_space: reclaim_space == "true",
        db_names: dbs,
    })
}

fn parse_list_commands_command(_: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    Ok(Request::ListCommands {})
}
fn parse_watch_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let key = match command.next() {
        Some(key) => key.replace("\n", ""),
        None => return Err(format!("watch must contain a key")),
    };
    Ok(Request::Watch { key })
}

fn parse_keys_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let pattern = match command.next() {
        Some(pattren) => pattren.replace("\n", ""),
        None => "".to_string(),
    };
    Ok(Request::Keys { pattern })
}

fn parse_use_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let name = match command.next() {
        Some(name) => name,
        None => {
            log::debug!("UseDb needs to provide an db name");
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

    let token_or_user_name = match rest.next() {
        Some(key) => String::from(key).replace("\n", ""),
        None => {
            log::debug!("UseDb needs and token");
            "".to_string()
        }
    };

    match rest.next() {
        Some(token) => Ok(Request::UseDb {
            name: name.to_string(),
            token: token.to_string(),
            user_name: Some(token_or_user_name.to_string()),
        }),
        None => Ok(Request::UseDb {
            name: name.to_string(),
            token: token_or_user_name.to_string(),
            user_name: None,
        }),
    }
}

fn parse_create_db_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let name = match command.next() {
        Some(key) => key,
        None => {
            log::debug!("CreateDb needs to provide an db name");
            ""
        }
    };
    let mut rest = match command.next() {
        Some(rest) => rest.splitn(2, " "),
        None => {
            log::debug!("create-db needs token and strategy");
            return Err(String::from("create-db must be followed by a token"));
        }
    };

    let token = match rest.next() {
        Some(key) => String::from(key).replace("\n", ""),
        None => {
            log::debug!("CreateDb needs and token");
            "".to_string()
        }
    };

    let strategy = match rest.next() {
        Some(s) => String::from(s).replace("\n", ""),
        None => "none".to_string(),
    };

    Ok(Request::CreateDb {
        name: name.to_string(),
        token: token.to_string(),
        strategy: ConsensuStrategy::from(strategy),
    })
}

fn parse_create_user_command(command: &mut std::str::SplitN<&str>) -> Result<Request, String> {
    let user_name = match command.next() {
        Some(name) => name,
        None => {
            log::debug!("CreateUser needs to provide an user name");
            ""
        }
    };
    let token = match command.next() {
        Some(key) => String::from(key).replace("\n", ""),
        None => {
            log::debug!("CreateUser needs and token");
            "".to_string()
        }
    };
    Ok(Request::CreateUser {
        user_name: user_name.to_string(),
        token: token.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_use_db() -> Result<(), String> {
        match Request::parse("use-db foo some-key") {
            Ok(Request::UseDb {
                token,
                name,
                user_name: _,
            }) => {
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
    fn should_parse_use_db_with_user_name_and_token() -> Result<(), String> {
        match Request::parse("use foo user-dush some-key") {
            Ok(Request::UseDb {
                token,
                name,
                user_name,
            }) => {
                if name == "foo" && token == "some-key" && user_name == Some("user-dush".to_owned())
                {
                    Ok(())
                } else {
                    Err(String::from("user should be foo and password bar"))
                }
            }
            _ => Err(String::from("get foo sould be parsed to Get command")),
        }
    }

    #[test]
    fn should_parse_use_db_with_user_name_and_token_with_user_db() -> Result<(), String> {
        match Request::parse("use-db foo user-dush some-key") {
            Ok(Request::UseDb {
                token,
                name,
                user_name,
            }) => {
                if name == "foo" && token == "some-key" && user_name == Some("user-dush".to_owned())
                {
                    Ok(())
                } else {
                    Err(String::from("user should be foo and password bar"))
                }
            }
            _ => Err(String::from("get foo sould be parsed to Get command")),
        }
    }

    #[test]
    fn should_parse_use() -> Result<(), String> {
        match Request::parse("use foo some-key") {
            Ok(Request::UseDb {
                token,
                name,
                user_name: _,
            }) => {
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
    fn should_parse_create_user() -> Result<(), String> {
        match Request::parse("create-user foo bar") {
            Ok(Request::CreateUser { user_name, token }) => {
                if user_name == "foo" && token == "bar" {
                    Ok(())
                } else {
                    Err(String::from("user should be foo and password bar"))
                }
            }
            _ => Err(String::from(
                "create-user foo bar should be parsed to CreateUser command",
            )),
        }
    }

    #[test]
    fn should_parse_create_db() -> Result<(), String> {
        match Request::parse("create-db foo some-key") {
            Ok(Request::CreateDb {
                token,
                name,
                strategy,
            }) => {
                if name == "foo" && token == "some-key" && strategy == ConsensuStrategy::None {
                    Ok(())
                } else {
                    Err(String::from("user should be foo and password bar"))
                }
            }
            _ => Err(String::from("get foo sould be parsed to Get command")),
        }
    }

    #[test]
    fn should_parse_create_db_with_strategy() -> Result<(), String> {
        match Request::parse("create-db foo some-key arbiter") {
            Ok(Request::CreateDb {
                token,
                name,
                strategy,
            }) => {
                if name == "foo" && token == "some-key" && strategy == ConsensuStrategy::Arbiter {
                    Ok(())
                } else {
                    Err(String::from("user should be foo and password bar"))
                }
            }
            _ => Err(String::from("get foo should be parsed to Get command")),
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
    fn should_parse_set_with_version_with_big_version() -> Result<(), String> {
        match Request::parse("set-safe jose12389123849 1663536194 123\n") {
            Ok(Request::Set {
                key,
                value,
                version,
            }) => {
                if key == "jose12389123849" && value == "123" && version == 1663536194 {
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
    fn should_parse_set_with_version_and_json() -> Result<(), String> {
        match Request::parse(
            "set-safe obj_new 1673521342 {\"_id\":1673521342,\"value\":{\"name\":\"name^2\"}}\n",
        ) {
            Ok(Request::Set {
                key,
                value,
                version,
            }) => {
                if key == "obj_new"
                    && value == "{\"_id\":1673521342,\"value\":{\"name\":\"name^2\"}}"
                    && version == 1673521342
                {
                    Ok(())
                } else {
                    Err(String::from("Wrong parse"))
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
        match Request::parse("replicate vue jose 3 1") {
            Ok(Request::ReplicateSet {
                db,
                key,
                value,
                version,
            }) => {
                if db == "vue" && key == "jose" && value == "1" && version == 3 {
                    Ok(())
                } else {
                    Err(String::from("db should vue key should be jose value 1"))
                }
            }
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_replicaion_snapshot_with_reclaim_space() -> Result<(), String> {
        match Request::parse("replicate-snapshot vue true") {
            Ok(Request::ReplicateSnapshot {
                reclaim_space,
                db_names,
            }) => {
                if db_names[0] == "vue" && reclaim_space {
                    Ok(())
                } else {
                    Err(String::from("db should vue key should be jose value 1"))
                }
            }
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_snapshot() -> Result<(), String> {
        match Request::parse("snapshot") {
            Ok(Request::Snapshot {
                reclaim_space,
                db_names,
            }) => {
                if !reclaim_space && db_names.is_empty() {
                    Ok(())
                } else {
                    Err(String::from("Should not reclaim_space by default"))
                }
            }
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_replicaion_snapshot() -> Result<(), String> {
        match Request::parse("replicate-snapshot vue") {
            Ok(Request::ReplicateSnapshot {
                db_names,
                reclaim_space,
            }) => {
                if db_names[0] == "vue" && !reclaim_space {
                    Ok(())
                } else {
                    Err(String::from("db should vue key should be jose value 1"))
                }
            }
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_replicaion_snapshot_with_many_databases() -> Result<(), String> {
        match Request::parse("replicate-snapshot vue|jose") {
            Ok(Request::ReplicateSnapshot {
                db_names,
                reclaim_space,
            }) => {
                if db_names[0] == "vue" && !reclaim_space && db_names[1] == "jose" {
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
        match Request::parse("metrics-state") {
            Ok(Request::MetricsState {}) => Ok(()),
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
            Ok(Request::Keys { pattern: _ }) => Ok(()),
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_query_pattern() -> Result<(), String> {
        match Request::parse("keys jose*") {
            Ok(Request::Keys { pattern }) => {
                if pattern == "jose*" {
                    Ok(())
                } else {
                    Err(String::from("wrong pattern parsed"))
                }
            }
            _ => Err(String::from("wrong command parsed")),
        }
    }

    #[test]
    fn should_parse_keys_as_ls_command() -> Result<(), String> {
        match Request::parse("ls") {
            Ok(Request::Keys { pattern: _ }) => Ok(()),
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

    #[test]
    fn should_parse_debug_command() -> Result<(), String> {
        match Request::parse("debug pending_messages") {
            Ok(Request::Debug { command }) => {
                if command == "pending_messages" {
                    Ok(())
                } else {
                    Err(String::from("Invalid command"))
                }
            }
            _ => Err(String::from("Invalid parsing")),
        }
    }

    #[test]
    fn should_parse_debug_command_invalid() -> Result<(), String> {
        match Request::parse("debug") {
            Err(message) => {
                if message == "command is mandatory" {
                    Ok(())
                } else {
                    Err(String::from("Invalid error message"))
                }
            }
            _ => Err(String::from("Should not have parsed!!")),
        }
    }

    #[test]
    fn should_parse_regier_as_arbiter_command() -> Result<(), String> {
        match Request::parse("arbiter") {
            Ok(Request::Arbiter {}) => Ok(()),
            _ => Err(String::from("Invalid parsing")),
        }
    }

    #[test]
    fn should_parse_resolve_conflit_with_error() -> Result<(), String> {
        match Request::parse("resolve a db_name some 2 some1") {
            Err(m) => {
                if m != "Invalid opp_id" {
                    Err(String::from("Invalid opp_id"))
                } else {
                    Ok(())
                }
            }
            _ => Err(String::from("Invalid parsing")),
        }
    }

    #[test]
    fn should_parse_resolve_conflit() -> Result<(), String> {
        match Request::parse("resolve 1 db_name some 2 some1") {
            Ok(Request::Resolve {
                opp_id,
                db_name,
                key,
                value,
                version,
            }) => {
                if opp_id != 1 {
                    Err(String::from("Invalid opp_id"))
                } else if db_name != "db_name" {
                    Err(String::from("Invalid db_name"))
                } else if key != "some" {
                    Err(String::from("Invalid key"))
                } else if value != "some1" {
                    Err(String::from("Invalid value"))
                } else if version != 2 {
                    Err(String::from("Invalid version"))
                } else {
                    Ok(())
                }
            }
            _ => Err(String::from("Invalid parsing")),
        }
    }

    #[test]
    fn should_parse_set_permission_command() -> Result<(), String> {
        match Request::parse("set-permissions jose r test") {
            Ok(Request::SetPermissions { user, permissions }) => {
                let permision = permissions[0].clone();
                let kinds = permision.kinds;
                let keys = permision.keys;
                if user != "jose" {
                    Err(String::from("Invalid user"))
                } else if kinds[0] != PermissionKind::Read {
                    Err(String::from("Invalid kind"))
                } else if keys.first().unwrap() != "test" {
                    Err(String::from("Invalid keys"))
                } else {
                    Ok(())
                }
            }
            _ => Err(String::from("Invalid parsing")),
        }
    }

    #[test]
    fn should_parse_set_permission_command_as_write() -> Result<(), String> {
        match Request::parse("set-permissions jose rwix test") {
            Ok(Request::SetPermissions { user, permissions }) => {
                let permision = permissions[0].clone();
                let kinds = permision.kinds;
                let keys = permision.keys;

                if user != "jose" {
                    Err(String::from("Invalid user"))
                } else if kinds[0] != PermissionKind::Read
                    || kinds[1] != PermissionKind::Write
                    || kinds[2] != PermissionKind::Increment
                    || kinds[3] != PermissionKind::Remove
                {
                    Err(String::from("Invalid kind"))
                } else if keys.first().unwrap() != "test" {
                    Err(String::from("Invalid keys"))
                } else {
                    Ok(())
                }
            }
            _ => Err(String::from("Invalid parsing")),
        }
    }

    #[test]
    fn should_parse_set_permission_with_multiple_permissions_set() -> Result<(), String> {
        match Request::parse("set-permissions jose rwix test|r $connection|i date*") {
            Ok(Request::SetPermissions { user, permissions }) => {
                let permision = permissions[0].clone();
                let kinds = permision.kinds;
                let keys = permision.keys;

                if user != "jose" {
                    Err(String::from("Invalid user"))
                } else if kinds[0] != PermissionKind::Read
                    || kinds[1] != PermissionKind::Write
                    || kinds[2] != PermissionKind::Increment
                    || kinds[3] != PermissionKind::Remove
                {
                    Err(String::from("Invalid kind"))
                } else if keys.first().unwrap() != "test" {
                    Err(String::from("Invalid keys"))
                } else {
                    Ok(())
                }
            }
            _ => Err(String::from("Invalid parsing")),
        }
    }
}
