use crate::bo::*;
use std::sync::Arc;

#[warn(dead_code)]
pub fn resolve(_input: &str, _dbs: &Arc<Databases>) -> Response {
    Response::Ok {}
}

#[cfg(test)]
mod tests {}
