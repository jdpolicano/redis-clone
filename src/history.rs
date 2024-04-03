use tokio::sync::{Mutex};
use crate::resp::{Resp};
use crate::connection::Connection;

#[derive(Debug)]
pub struct Replica {
    pub stream: Connection,
}

impl Replica {
    pub fn new(stream: Connection) -> Self {
        Replica { stream }
    }
}

#[derive(Debug)]
pub struct History {
    inner: Mutex<HistoryInner>
}

impl History {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HistoryInner::new())
        }
    }

    // todo - handle poison errors here...
    pub async fn add_replica(&self, stream: Connection) {
       self.inner.lock().await.add_replica(stream);
    }

    pub async fn add_write(&self, resp: Resp) {
        self.inner.lock().await.add_write(resp).await;
    }
}

#[derive(Debug)]
pub struct HistoryInner {
    repls: Vec<Replica>,
    write_history: Vec<Resp>,
}

impl HistoryInner {
    pub fn new() -> Self {
        Self {
            repls: Vec::new(),
            write_history: Vec::new(),
        }
    }

    pub fn add_replica(&mut self, stream: Connection) {
        self.repls.push(Replica::new(stream));
    }

    pub async fn add_write(&mut self, resp: Resp) {
        for replica in self.repls.iter_mut() {
            let _ = replica.stream.write_message(&resp).await;
        }
        self.write_history.push(resp);
    }
}
