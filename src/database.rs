use std::collections::HashMap;
use crate::resp::{Resp};
use std::sync::{ RwLock };
use std::time::{Instant, Duration};

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub struct Record {
    pub data: Vec<u8>,
    expiry: Option<(Instant, Duration)>,
}

impl Record {
    // converts from a parsed Resp to a Record for storage
    // client always sends a bulk string, so we can safely assume that
    pub fn from_resp(resp: Resp) -> Option<Record> {
        match resp {
            Resp::BulkString(b) => Some(Record { data: b, expiry: None }),
            _ => None,
        }
    }

    pub fn from_vec(v: Vec<u8>) -> Record {
        Record { data: v, expiry: None }
    }

    pub fn set_expiry(&mut self, duration: Duration) {
        self.expiry = Some((Instant::now(), duration));
    }

    pub fn has_expired(&self) -> bool {
        if let Some((start, duration)) = self.expiry {
            start.elapsed() >= duration
        } else {
            false
        }
    }
}


pub struct Database {
    // (key, value)
    store: RwLock<HashMap<Vec<u8>, Record>>,
}


impl Database {
    pub fn new() -> Self {
        Database {
            store: RwLock::new(HashMap::new()),
        }
    }

    pub fn set(&self, key: Vec<u8>, value: Record) -> Option<Record> {
        self.store.write().unwrap().insert(key, value)
    }

    pub fn get(&self, key: &[u8]) -> Option<Record> {
        self.store.read().unwrap().get(key).cloned()
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        self.store.read().unwrap().contains_key(key)
    }

    pub fn del(&self, key: &[u8]) -> bool {
        self.store.write().unwrap().remove(key).is_some()
    }
}