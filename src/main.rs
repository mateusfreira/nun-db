use std::net::{TcpListener, TcpStream};
use std::io::Result;
use std::io::{BufReader, BufRead};


fn handle_client(stream: TcpStream) {
	let mut buf = String::new();
	let mut reader = BufReader::new(&stream);
	loop {
		let read_line = reader.read_line(&mut buf);
		match read_line { 
			Ok(_) => println!("{}", buf),
			_ => {}
		}
	}
}

fn main() -> Result<()>{
    let listener = TcpListener::bind("127.0.0.1:9001")?;
    for stream in listener.incoming() {
        handle_client(stream?);
    }
    Ok(())
}
