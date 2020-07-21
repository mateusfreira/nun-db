use futures::channel::mpsc::Sender;
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bo::*;
use db_ops::*;

pub fn replicate_request(input: &str, reponse: Response) -> Response {
    match reponse {
        Response::Ok {} => {
            return Response::Ok {};
        }
        _ => reponse,
    }
}
