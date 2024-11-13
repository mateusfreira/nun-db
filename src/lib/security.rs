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
        return Response::Error {
            msg: "To read security keys you must auth as an admin!".to_string(),
        };
    }

    let db_name = client.selected_db_name();
    let user_name = client.selected_db_user_name();

    apply_to_database_name(
        dbs,
        client,
        &db_name,
        &|db| {
            if client.is_admin_auth() || user_name.is_none() {
                return opp(db);
            }

            match &user_name {
                Some(_) => {
                    if has_permission(client, key, db, &permission_required) {
                        opp(db)
                    } else {
                        client.send_message(&"permission denied\n".to_string());
                        Response::Error {
                            msg: format!("No permission to {} key: {}", permission_required, key),
                        }
                    }
                }
                None => {
                    if permission_required == PermissionKind::Read {
                        opp(db)
                    } else {
                        client.send_message(&"permission denied\n".to_string());
                        Response::Error {
                            msg: "permission denied".to_string(),
                        }
                    }
                }
            }
        },
        &permission_required,
    )
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
                Response::Error { msg }
            }
        }
        None => {
            let msg = String::from(NO_DB_SELECTED_MESSAGE);
            client.send_message(&msg);
            return Response::Error { msg };
        }
    };
    return result;
}

pub fn create_read_only_user(
    dbs: &Arc<Databases>,
    client: &Client,
    user: &str,
    pwd: &str,
    allowed_patterns: Vec<String>,
) -> Response {
    if !client.is_admin_auth() {
        return Response::Error {
            msg: "Only admin can create users".to_string(),
        };
    }

    let user_key = user_name_key_from_user_name(&user.to_string());

    let permission_key = permissions_key_from_user_name(&user.to_string());

    let read_only_permission = Permission {
        kinds: vec![PermissionKind::Read],
        keys: allowed_patterns,
    };

    apply_to_database_name(
        dbs,
        client,
        &ADMIN_DB.to_string(),
        &|db| {
            db.set_value(&Change::new(user_key.clone(), pwd.to_string(), -1));

            db.set_value(&Change::new(
                permission_key.clone(),
                Permission::permissions_to_str_value(&vec![read_only_permission.clone()]),
                -1,
            ));

            match client.sender.clone().try_send("user-created\n".to_string()) {
                Ok(_) => (),
                Err(e) => log::warn!("Error sending user-created message: {}", e),
            }

            Response::Ok {}
        },
        &PermissionKind::Write,
    )
}

pub fn verify_access(
    db: &Database,
    user: &str,
    key: &String,
    required_permission: &PermissionKind,
) -> bool {
    if user == "all" {
        return *required_permission == PermissionKind::Read;
    }

    let permission_key = permissions_key_from_user_name(&user.to_string());

    match db.get_value(permission_key) {
        Some(perms) => {
            let permissions = Permission::permissions_from_str(&perms.value);
            permissions.iter().any(|perm| {
                perm.kinds.contains(required_permission)
                    && perm
                        .keys
                        .iter()
                        .any(|pattern| get_function_by_pattern(pattern)(key, pattern))
            })
        }
        None => false,
    }
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
    use std::{
        collections::HashMap,
        sync::{Mutex, RwLock},
    };

    fn setup_test_env() -> (Arc<Databases>, Client) {
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

        let (client, _receiver) = Client::new_empty_and_receiver();
        client.auth.store(true, Ordering::SeqCst);

        (dbs, client)
    }

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

    #[test]
    fn test_readonly_user_creation() {
        let (dbs, client) = setup_test_env();

        let result = create_read_only_user(
            &dbs,
            &client,
            "readonly_user",
            "password123",
            vec!["data:*".to_string(), "public:*".to_string()],
        );

        assert!(matches!(result, Response::Ok {}));

        let binding = dbs.acquire_dbs_read_lock();
        let db = binding.get(ADMIN_DB).unwrap();

        assert!(verify_access(
            db,
            "readonly_user",
            &"data:test".to_string(),
            &PermissionKind::Read
        ));

        assert!(verify_access(
            db,
            "readonly_user",
            &"public:doc".to_string(),
            &PermissionKind::Read
        ));

        assert!(!verify_access(
            db,
            "readonly_user",
            &"private:doc".to_string(),
            &PermissionKind::Read
        ));

        assert!(!verify_access(
            db,
            "readonly_user",
            &"data:test".to_string(),
            &PermissionKind::Write
        ));

        assert!(!verify_access(
            db,
            "readonly_user",
            &"public:doc".to_string(),
            &PermissionKind::Increment
        ));
    }

    #[test]
    fn test_read_only_access_restrictions() {
        let (dbs, client) = setup_test_env();

        create_read_only_user(
            &dbs,
            &client,
            "readonly_user",
            "password123",
            vec!["data:*".to_string()],
        );

        let selected_db = SelectedDatabase {
            name: RwLock::new(ADMIN_DB.to_string()),
            user_name: RwLock::new(Some("readonly_user".to_string())),
        };

        let readonly_client = Client {
            auth: Arc::new(AtomicBool::new(false)),
            cluster_member: Mutex::new(None),
            selected_db: Arc::new(selected_db),
            sender: client.sender.clone(),
        };

        let result = apply_if_safe_access(
            &dbs,
            &readonly_client,
            &"data:test".to_string(),
            &|_| Response::Ok {},
            PermissionKind::Read,
        );
        assert!(matches!(result, Response::Ok {}));

        let result = apply_if_safe_access(
            &dbs,
            &readonly_client,
            &"data:test".to_string(),
            &|_| Response::Ok {},
            PermissionKind::Write,
        );
        assert!(matches!(result, Response::Error { .. }));
    }
}
