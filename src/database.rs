use std::collections::HashMap;
use crate::resp::{Resp};
use std::sync::{Arc, Mutex};
use std::time::{Instant, Duration};

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub enum Record {
    String(Vec<u8>),
    List,
    Set,
    Hash,
    ZSet,
    Stream,
    None,
}

impl Record {
    // converts from a parsed Resp to a Record for storage
    // client always sends a bulk string, so we can safely assume that
    pub fn from_resp(resp: Resp) -> Option<Record> {
        match resp {
            Resp::BulkString(b) => Some(Record::String(b)),
            _ => None,
        }
    }

    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        match self {
            Record::String(b) => Some(b.clone()),
            _ => None,
        }
    }

    pub fn is_string(&self) -> bool {
        match self {
            Record::String(_) => true,
            _ => false,
        }
    }

    pub fn is_none(&self) -> bool {
        match self {
            Record::None => true,
            _ => false,
        }
    }
}


pub struct Database {
    // (key, value)
    store: Arc<Mutex<HashMap<Record, Record>>>,
    // (key, creation time, expiry)
    ttl: Arc<Mutex<HashMap<Record, (Instant, Duration)>>>,
}


impl Database {
    pub fn new() -> Self {
        Database {
            store: Arc::new(Mutex::new(HashMap::new())),
            ttl: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set(&self, key: Record, value: Record) -> Option<Record> {
        let mut store = self.lock().0;
        store.insert(key, value)
    }

    pub fn get(&self, key: &Record) -> Option<Record> {
        // check if the key has an expiry...
        let (mut store, mut ttl) = self.lock();

        if let Some((created, expiry)) = ttl.get(key) {
            if created.elapsed() > *expiry {
                store.remove(&key);
                ttl.remove(&key);
                return None;
            }
            return store.get(key).cloned()
        }

        store.get(key).cloned()
    }

    pub fn del(&self, key: &Record) -> bool {
        let (mut store, mut ttl) = self.lock();
        ttl.remove(&key);
        store.remove(&key).is_some()
    }

    pub fn expiry(&self, key: &Record, d: Duration) {
        let mut ttl = self.lock().1;
        ttl.insert(key.clone(), (Instant::now(), d));
    }

    // this was a bad choice, but to avoid a deadlock
    // each call should aquire both store and ttl locks
    // otherwise there may be two threads competing for the locks
    fn lock(&self) -> (
        std::sync::MutexGuard<HashMap<Record, Record>>, 
        std::sync::MutexGuard<HashMap<Record, (Instant, Duration)>>
    ) {
        (self.store.lock().unwrap(), self.ttl.lock().unwrap())
    }
}