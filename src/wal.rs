use crate::error::CacheError;
use crate::CacheCore;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Mutex;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum WalRecord {
    Set(String, Vec<u8>),
    Del(String),
    Pop(String),
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

    pub fn replay(&self, core: &CacheCore) -> Result<(), CacheError> {
        let mut f = File::open(&self.path)
            .map_err(|e| CacheError::Internal(format!("open WAL for replay: {}", e)))?;
        loop {
            let mut len_buf = [0u8; 4];
            if let Err(e) = f.read_exact(&mut len_buf) {
                use std::io::ErrorKind;
                if e.kind() == ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(CacheError::Internal(format!("read WAL len: {}", e)));
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            f.read_exact(&mut buf)
                .map_err(|e| CacheError::Internal(format!("read WAL rec: {}", e)))?;
            let rec: WalRecord =
                bincode::deserialize(&buf).map_err(|e| CacheError::Serialization(e.to_string()))?;
            match rec {
                WalRecord::Set(k, v) => core.set(k, v),
                WalRecord::Del(k) => {
                    core.delete(&k);
                }
                WalRecord::Pop(k) => {
                    core.pop(&k);
                }
            }
        }
        Ok(())
    }
}
