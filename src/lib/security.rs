use crate::db_ops::get_function_by_pattern;
use core::sync::atomic::{AtomicBool, Ordering};

use crate::bo::*;
use std::sync::Arc;

pub const SECURY_KEYS_PREFIX: &'static str = "$$";
pub const USER_NAME_KEYS_PREFIX: &'static str = "$$user";
pub const PERMISSION_KEYS_PREFIX: &'static str = "$$permission_$";

const PERMISSION_DENIED_MESSAGE: &'static str = "permission denied\n";
const NO_DB_SELECTED_MESSAGE: &'static str = "error no-db-selected\n";

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
        match permisions {
            Some(permisions) => {
                let permisions = Permission::permissions_from_str(permisions.value.as_str());

                permisions.into_iter().any(|permision| {
                    log::debug!("permisions_parsed: {:?}", permision.kinds);
                    let kinds = permision.kinds.clone();
                    if !kinds.contains(&required_permission) {
                        return false;
                    }
                    log::debug!("Has kind: {:?}", permision.keys);
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
    let dbs = dbs.acquire_dbs_read_lock();
    let result: Response = match dbs.get(&db_name.to_string()) {
        Some(db) => {
            if key == None || has_permission(client, key.unwrap(), db, &permission_required) {
                opp(db)
            } else {
                let msg = String::from(PERMISSION_DENIED_MESSAGE);
                client.send_message(&msg);
                Response::Error {
                    msg,
                }
            }
        }
        None => {
            let msg = String::from(NO_DB_SELECTED_MESSAGE);
            client.send_message(&msg);
            return Response::Error {
                msg,
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

pub fn user_name_key_from_user_name(user_name: &String) -> String {
    String::from(format!("{}_{}", USER_NAME_KEYS_PREFIX, user_name))
}

pub fn permissions_key_from_user_name(user_name: &String) -> String {
    String::from(format!("{}{}", PERMISSION_KEYS_PREFIX, user_name))
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
            tcp_addr.to_string(),
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
