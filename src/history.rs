use tokio::sync::Mutex;
use bytes::BytesMut;
use crate::resp::{Resp, RespEncoder};
use crate::connection::Connection;

#[derive(Debug)]
pub struct Replica {
    pub stream: Connection,
    // the point in histroy where the replica started receiving command forwards.
    pub start_offset: usize,
    // this is the last offset len we sent to this replica.
    pub last_offset: usize,
}

impl Replica {
    pub fn new(stream: Connection, start_offset: usize, last_offset: usize) -> Self {
        Replica { stream, start_offset, last_offset }
    }

    pub fn update_offset(&mut self, offset: usize) {
        self.last_offset = offset;
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
    write_history: BytesMut,
}

impl HistoryInner {
    pub fn new() -> Self {
        Self {
            repls: Vec::new(),
            write_history: BytesMut::new(),
        }
    }

    pub fn add_replica(&mut self, stream: Connection) {
        let offset = self.write_history.len();
        self.repls.push(Replica::new(stream, offset, offset));
    }

    pub async fn add_write(&mut self, resp: Resp) {
        // encode the resp we just received into the write history.
        RespEncoder::encode_resp(&resp, &mut self.write_history);
        // send the write history to all replicas.
        for replica in self.repls.iter_mut() {
            let _ = replica.stream.write(&self.write_history[replica.last_offset..]);
            let _ = replica.stream.flush().await;
            replica.update_offset(self.write_history.len());
        }
    }
}
