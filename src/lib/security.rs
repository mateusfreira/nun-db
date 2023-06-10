use crate::db_ops::get_function_by_pattern;
use core::sync::atomic::{AtomicBool, Ordering};

use crate::bo::*;
use std::sync::Arc;

pub const SECURY_KEYS_PREFIX: &'static str = "$$";

pub fn apply_if_auth(auth: &Arc<AtomicBool>, opp: &dyn Fn() -> Response) -> Response {
    if auth.load(Ordering::SeqCst) {
        opp()
    } else {
        Response::Error {
            msg: "Not auth".to_string(),
        }
    }
}

pub fn apply_if_safe_access(
    dbs: &Arc<Databases>,
    client: &Client,
    key: &String,
    opp: &dyn Fn(&Database) -> Response,
    permission_required: PermissionKind,
) -> Response {
    if key.starts_with(SECURY_KEYS_PREFIX) && !client.is_admin_auth() {
        Response::Error {
            msg: "To read security keys you must auth as an admin!".to_string(),
        }
    } else {
        let db_name = client.selected_db_name();
        apply_to_database_name_if_has_permission(
            &dbs,
            &client,
            &db_name,
            opp,
            Some(key),
            &permission_required,
        )
    }
}

fn parse_permission(permision: &str) -> Vec<Permission> {
    let permissions = permision.split("|").collect::<Vec<&str>>();
    permissions
        .iter()
        .map(|x| {
            let parts = x.splitn(2, " ").collect::<Vec<&str>>();
            Permission {
                kinds: parts[0]
                    .to_string()
                    .chars()
                    .map(|x| PermissionKind::from(x))
                    .collect(),
                keys: parts[1].split(",").map(|x| x.to_string()).collect(),
            }
        })
        .collect()
}
fn has_permission(
    client: &Client,
    key: &String,
    db: &Database,
    required_permission: &PermissionKind,
) -> bool {
    if key.starts_with(SECURY_KEYS_PREFIX) {
        client.is_admin_auth()
    } else {
        let selected_db_user_name = client.selected_db_user_name().unwrap_or("all".to_string());
        let permisions = db.get_value(String::from(format!(
            "$$permission_${}",
            selected_db_user_name
        )));
        log::debug!("permisions: {:?}", permisions);
        println!(
            "permisions: {:?} user: {:?}",
            permisions, selected_db_user_name
        );
        match permisions {
            Some(permisions) => {
                let permisions = parse_permission(&permisions.value);

                permisions.into_iter().any(|permision| {
                    println!("permisions_parsed: {:?}", permision.kinds);
                    let kinds = permision.kinds.clone();
                    if !kinds.contains(&required_permission) {
                        return false;
                    }
                    println!("Has kind: {:?}", permision.keys);
                    let is_allowed = permision
                        .keys
                        .into_iter()
                        .any(|x| get_function_by_pattern(&x)(key, &x));
                    is_allowed
                })
            }
            None => selected_db_user_name == "all",
        }
    }
}

pub fn apply_to_database_name(
    dbs: &Arc<Databases>,
    client: &Client,
    db_name: &String,
    opp: &dyn Fn(&Database) -> Response,
    permission_required: &PermissionKind,
) -> Response {
    apply_to_database_name_if_has_permission(
        &dbs,
        &client,
        &db_name,
        &opp,
        None,
        &permission_required,
    )
}

pub fn apply_to_database_name_if_has_permission(
    dbs: &Arc<Databases>,
    client: &Client,
    db_name: &String,
    opp: &dyn Fn(&Database) -> Response,
    key: Option<&String>,
    permission_required: &PermissionKind,
) -> Response {
    let dbs = dbs.map.read().expect("Error getting the dbs.map.lock");
    let result: Response = match dbs.get(&db_name.to_string()) {
        Some(db) => {
            if key == None || has_permission(client, key.unwrap(), db, &permission_required) {
                opp(db)
            } else {
                match client
                    .sender
                    .clone()
                    .try_send(String::from("permission denied\n"))
                {
                    Ok(_) => {}
                    Err(e) => log::warn!("apply_to_database::try_send {}", e),
                };
                Response::Error {
                    msg: "permission denied".to_string(),
                }
            }
        }
        None => {
            match client
                .sender
                .clone()
                .try_send(String::from("error no-db-selected\n"))
            {
                Ok(_) => {}
                Err(e) => log::warn!("apply_to_database::try_send {}", e),
            }
            return Response::Error {
                msg: "No database found!".to_string(),
            };
        }
    };
    return result;
}

pub fn apply_to_database(
    dbs: &Arc<Databases>,
    client: &Client,
    opp: &dyn Fn(&Database) -> Response,
) -> Response {
    let db_name = client.selected_db_name();
    apply_to_database_name(dbs, client, &db_name, opp, &PermissionKind::Read)
}

pub fn clean_string_to_log(input: &str, dbs: &Arc<Databases>) -> String {
    let user_replacer: String = format!("{} ", &dbs.user.to_string());
    let pwd_replacer: String = format!("{}", &dbs.pwd.to_string());
    return input
        .replace(&user_replacer.to_string(), "**** ")
        .replace(&pwd_replacer.to_string(), "****");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_ops::*;
    use futures::channel::mpsc::{channel, Receiver, Sender};
    use std::collections::HashMap;

    #[test]
    fn should_clean_user_and_pwd() -> Result<(), String> {
        let (replication_supervisor_sender, _receiver): (Sender<String>, Receiver<String>) =
            channel(100);
        let (replication_sender, _receiver): (Sender<String>, Receiver<String>) = channel(100);
        let tcp_addr = String::from("127.0.0.1");
        let keys_map = HashMap::new();
        let dbs = create_init_dbs(
            String::from("mateus"),
            String::from("mateus-123"),
            tcp_addr,
            replication_supervisor_sender,
            replication_sender,
            keys_map,
            true,
        );

        let clean_input = clean_string_to_log("auth mateus mateus-123;", &dbs);

        assert_eq!(clean_input, String::from("auth **** ****;"));
        Ok(())
    }
}
