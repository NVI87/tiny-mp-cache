use crate::core::{CacheCore, ChunkId};
use crate::error::CacheError;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum WalRecord {
    Set {
        key: String,
        value: Vec<u8>,
        ttl_ms: i64,
    },
    Del {
        key: String,
    },
    Pop {
        key: String,
    },
}

pub struct Wal {
    path: PathBuf,
    file: Mutex<File>,
}

impl Wal {
    pub fn open(path: PathBuf) -> Result<Self, CacheError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|e| CacheError::Internal(format!("open WAL: {}", e)))?;
        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }

    pub fn append(&self, rec: &WalRecord) -> Result<(), CacheError> {
        let mut f = self
            .file
            .lock()
            .map_err(|_| CacheError::Internal("WAL mutex poisoned".into()))?;
        let data =
            bincode::serialize(rec).map_err(|e| CacheError::Serialization(e.to_string()))?;
        let len = (data.len() as u32).to_le_bytes();
        f.write_all(&len)
            .and_then(|_| f.write_all(&data))
            .and_then(|_| f.flush())
            .map_err(|e| CacheError::Internal(format!("write WAL: {}", e)))
    }

    pub fn replay(
        &self,
        core: &CacheCore,
        current_chunk_id: ChunkId,
    ) -> Result<(), CacheError> {
        use std::io::ErrorKind;

        let mut f = File::open(&self.path)
            .map_err(|e| CacheError::Internal(format!("open WAL for replay: {}", e)))?;

        loop {
            let mut len_buf = [0u8; 4];
            match f.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    return Err(CacheError::Internal(format!("read WAL len: {}", e)));
                }
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            f.read_exact(&mut buf)
                .map_err(|e| CacheError::Internal(format!("read WAL rec: {}", e)))?;

            let rec: WalRecord =
                bincode::deserialize(&buf).map_err(|e| CacheError::Serialization(e.to_string()))?;

            match rec {
                WalRecord::Set { key, value, ttl_ms } => {
                    let ttl = if ttl_ms < 0 {
                        None
                    } else {
                        Some(Duration::from_millis(ttl_ms as u64))
                    };
                    core.set(key, value, ttl, current_chunk_id);
                }
                WalRecord::Del { key } => {
                    core.delete(&key);
                }
                WalRecord::Pop { key } => {
                    core.pop(&key);
                }
            }
        }

        Ok(())
    }

    pub fn reset(&self) -> Result<(), CacheError> {
        let mut f = self
            .file
            .lock()
            .map_err(|_| CacheError::Internal("WAL mutex poisoned".into()))?;
        f.set_len(0)
            .and_then(|_| f.seek(SeekFrom::Start(0)))
            .map_err(|e| CacheError::Internal(format!("truncate WAL: {}", e)))?;
        Ok(())
    }
}
