use criterion::{criterion_group, criterion_main, Criterion};
use nundb::bo::*;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("Parse use-db", |b| {
        b.iter(|| Request::parse("use-db jose jose"))
    });
    c.bench_function("Parse use-db old parse", |b| {
        b.iter(|| parse_old("use-db jose jose"))
    });

    c.bench_function("Parse invalid command", |b| {
        b.iter(|| Request::parse("none jose"))
    });
    c.bench_function("Parse invalid command old parse", |b| {
        b.iter(|| parse_old("none jose"))
    });
}

pub fn parse_old(input: &str) -> Result<Request, String> {
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
        Some("keys") => {
            let pattern = match command.next() {
                Some(pattren) => pattren.replace("\n", ""),
                None => "".to_string(),
            };
            Ok(Request::Keys { pattern })
        }
        Some("ls") => {
            let pattern = match command.next() {
                Some(pattren) => pattren.replace("\n", ""),
                None => "".to_string(),
            };
            Ok(Request::Keys { pattern })
        }
        Some("snapshot") => {
            let reclaim_space = command.next().unwrap_or("false");
            Ok(Request::Snapshot {
                reclaim_space: reclaim_space == "true",
            })
        }
        Some("replicate-snapshot") => {
            let db_name = match command.next() {
                Some(db_name) => db_name.replace("\n", ""),
                None => return Err(format!("replicate-snapshot must contain a db name")),
            };
            let reclaim_space = match command.next() {
                Some(reclaim_space) => reclaim_space.replace("\n", "") == "true",
                None => false,
            };
            Ok(Request::ReplicateSnapshot {
                db: db_name,
                reclaim_space,
            })
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
        Some("metrics-state") => Ok(Request::MetricsState {}),
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
        Some("use") => {
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
                user_name: None,
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
                user_name: None,
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
        Some("debug") => {
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
        Some("arbiter") => Ok(Request::Arbiter {}),

        Some("resolve") => {
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
        Some(cmd) => Err(format!("unknown command: {}", cmd)),
        _ => Err(format!("no command sent")),
    };
    parsed_command
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
