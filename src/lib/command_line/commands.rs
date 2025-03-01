use clap::{App, Arg, ArgMatches, SubCommand};
use log;

pub fn prepare_args<'a>() -> ArgMatches<'static> {
    return App::new("Nun-db")
        .version("0.1")
        .author("Mateus F. S <mateus.freira@gmail.com>")
        .about("The best real-time database open source!")
        .arg(
            Arg::with_name("user")
                .short("u")
                .long("user")
                .takes_value(true)
                .help("Admin username"),
        )
        .arg(
            Arg::with_name("pwd")
                .short("p")
                .takes_value(true)
                .help("Admin password"),
        )
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .takes_value(true)
                .help("Database host. e.g: http://localhost:3013"),
        )
        .subcommand(
            SubCommand::with_name("create-db")
                .about("Create a nun-db-database")
                .arg(
                    Arg::with_name("db-name")
                        .short("d")
                        .takes_value(true)
                        .help("Database name to be created"),
                )
                .arg(
                    Arg::with_name("db-token")
                        .short("t")
                        .long("db-token")
                        .takes_value(true)
                        .help("Database token"),
                ),
        )
        .subcommand(
            SubCommand::with_name("exec")
                .about("Execute one or more commands")
                .arg(
                    Arg::with_name("commands")
                        .takes_value(true)
                        .index(1)
                        .help("Execute a sequece of command separated commands in the dabtase"),
                ),
        )
        .subcommand(
            SubCommand::with_name("start")
                .arg(
                    Arg::with_name("tcp-address")
                        .short("t")
                        .long("tcp-address")
                        .takes_value(true)
                        .help("TCP address"),
                )
                .arg(
                    Arg::with_name("ws-address")
                        .short("w")
                        .long("ws-address")
                        .takes_value(true)
                        .help("Web socket address"),
                )
                .arg(
                    Arg::with_name("http-address")
                        .short("h")
                        .long("http-address")
                        .takes_value(true)
                        .help("Http address"),
                )
                .arg(
                    Arg::with_name("replicate-address")
                        .short("r")
                        .long("replicate-address")
                        .takes_value(true)
                        .help("Replicate address"),
                )
                .arg(
                    Arg::with_name("external-address")
                        .short("ea")
                        .long("external-address")
                        .takes_value(true)
                        .help("TCP address to use to join the cluster as a default address, useful when the node tcp address is not reachable from other nodes"),
                )
                .about("Start Nun-db service"),
        )
        .get_matches();
}

pub fn exec_command(matches: &ArgMatches<'_>) -> Result<(), String> {
    if let Some(create_db_matches) = matches.subcommand_matches("create-db") {
        log::debug!(
            "Will create the database {}, with the token {}",
            create_db_matches.value_of("db-name").unwrap(),
            create_db_matches.value_of("db-token").unwrap()
        );
        let body = format!(
            "auth {user} {pwd}; create-db {name} {token};",
            user = matches.value_of("user").unwrap(),
            pwd = matches.value_of("pwd").unwrap(),
            name = create_db_matches.value_of("db-name").unwrap(),
            token = create_db_matches.value_of("db-token").unwrap(),
        );
        let client = reqwest::blocking::Client::new();
        let res = client
            .post(matches.value_of("host").unwrap_or("http://127.0.0.1:3013"))
            .body(body)
            .send()
            .unwrap()
            .text()
            .unwrap();
        println!("Response {:?}", res,);
        return Ok(());
    }

    if let Some(exec_matches) = matches.subcommand_matches("exec") {
        log::debug!(
            "Will execute the commands database {}",
            exec_matches.value_of("commands").unwrap(),
        );
        let body = format!(
            "auth {user} {pwd}; {commands}",
            user = matches.value_of("user").unwrap(),
            pwd = matches.value_of("pwd").unwrap(),
            commands = exec_matches.value_of("commands").unwrap(),
        );
        let client = reqwest::blocking::Client::new();
        let res = client
            .post(matches.value_of("host").unwrap_or("http://localhost:3013"))
            .body(body)
            .send()
            .unwrap()
            .text()
            .unwrap();
        println!("Response {:?}", res,);
        return Ok(());
    }
    Ok(())
}
