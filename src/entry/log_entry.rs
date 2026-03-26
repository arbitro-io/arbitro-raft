use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{LogIndex, Term};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryPayload(pub Bytes);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub payload: EntryPayload,
}
