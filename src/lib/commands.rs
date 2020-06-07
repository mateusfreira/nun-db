use clap::{ App, Arg, ArgMatches, SubCommand };

pub fn prepare_args<'a>() -> ArgMatches<'a> {
   return  App::new("Nun-db")
        .version("0.1")
        .author("Mateus F. S <mateus.freira@gmail.com>")
        .about("The best real-time database open source!")
        .arg(
            Arg::with_name("user")
                .short("u")
                .long("user")
                .required(true)
                .takes_value(true)
                .help("Admin username"),
        )
        .arg(
            Arg::with_name("pwd")
                .short("p")
                .required(true)
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
        .subcommand(SubCommand::with_name("start").about("Start Nun-db service"))
        .get_matches();

}

pub fn exec_command(matches: &ArgMatches) -> Result<(), String> {

    if let Some(create_db_matches) = matches.subcommand_matches("create-db") {
        println!(
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
            .post(
                matches
                    .value_of("host")
                    .unwrap_or("http://localhost:3013"),
            )
            .body(body)
            .send()
            .unwrap()
            .text()
            .unwrap();
        println!("Response {:?}", res,);
        return Ok(());
    }

    if let Some(exec_matches) = matches.subcommand_matches("exec") {
        println!(
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
            .post(
                matches
                    .value_of("host")
                    .unwrap_or("http://localhost:3013"),
            )
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
