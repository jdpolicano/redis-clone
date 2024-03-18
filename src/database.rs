use std::collections::HashMap;
use crate::resp::{Resp};
use std::sync::{Arc, Mutex};

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub enum DbType {
    String(Vec<u8>),
    List,
    Set,
    Hash,
    ZSet,
    Stream,
    None,
}

impl DbType {
    // converts from a parsed Resp to a DbType for storage
    // client always sends a bulk string, so we can safely assume that
    pub fn from_resp(resp: Resp) -> Option<DbType> {
        match resp {
            Resp::BulkString(b) => Some(DbType::String(b)),
            _ => None,
        }
    }

    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        match self {
            DbType::String(b) => Some(b.clone()),
            _ => None,
        }
    }

    pub fn is_string(&self) -> bool {
        match self {
            DbType::String(b) => true,
            _ => false,
        }
    }

    pub fn is_none(&self) -> bool {
        match self {
            DbType::None => true,
            _ => false,
        }
    }
}


pub struct Database {
    store: Arc<Mutex<HashMap<DbType, DbType>>>,
    ttl: Arc<Mutex<HashMap<DbType, i64>>>,
}


impl Database {
    pub fn new() -> Self {
        Database {
            store: Arc::new(Mutex::new(HashMap::new())),
            ttl: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set(&self, key: DbType, value: DbType) {
        let mut store = self.store.lock().unwrap();
        store.insert(key, value);
    }

    pub fn get(&self, key: &DbType) -> Option<DbType> {
        self.store.lock().unwrap().get(&key).cloned()
    }

    pub fn del(&self, key: &DbType) -> bool {
        let mut store = self.store.lock().unwrap();
        store.remove(&key).is_some()
    }

    pub fn expery(&self, key: &DbType, seconds: i64) {
        let mut ttl = self.ttl.lock().unwrap();
        ttl.insert(key.clone(), seconds);
    }
}