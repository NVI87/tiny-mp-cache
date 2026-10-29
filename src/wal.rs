use crate::core::{CacheCore, ChunkId};
use crate::error::CacheError;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
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

/// Открытый сегмент WAL: все записи в нём относятся к одному чанку.
struct Segment {
    chunk_id: ChunkId,
    file: File,
}

/// WAL из сегментов wal/log_<chunk_id>.bin.
///
/// Snapshot не обнуляет журнал, а переключает запись на новый сегмент (rotate);
/// старый сегмент удаляется только после того, как его чанк сохранён на диск.
pub struct Wal {
    dir: PathBuf,
    current: Mutex<Segment>,
}

pub fn segment_path(dir: &Path, chunk_id: ChunkId) -> PathBuf {
    dir.join(format!("log_{}.bin", chunk_id))
}

fn open_segment(dir: &Path, chunk_id: ChunkId) -> Result<Segment, CacheError> {
    let path = segment_path(dir, chunk_id);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| CacheError::Internal(format!("open WAL {}: {}", path.display(), e)))?;
    Ok(Segment { chunk_id, file })
}

impl Wal {
    pub fn open(dir: PathBuf, chunk_id: ChunkId) -> Result<Self, CacheError> {
        let seg = open_segment(&dir, chunk_id)?;
        Ok(Self {
            dir,
            current: Mutex::new(seg),
        })
    }

    /// Chunk_id существующих сегментов в порядке возрастания.
    pub fn list_segments(dir: &Path) -> Result<Vec<ChunkId>, CacheError> {
        let mut ids = Vec::new();
        let entries = fs::read_dir(dir)
            .map_err(|e| CacheError::Internal(format!("read wal dir: {}", e)))?;
        for entry in entries {
            let entry = entry.map_err(|e| CacheError::Internal(format!("read wal dir: {}", e)))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(id) = name
                .strip_prefix("log_")
                .and_then(|s| s.strip_suffix(".bin"))
                .and_then(|s| s.parse::<ChunkId>().ok())
            {
                ids.push(id);
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    /// Записать операцию в WAL и применить её к памяти под одной блокировкой,
    /// чтобы rotate не мог вклиниться между записью и применением.
    /// `apply` получает chunk_id текущего сегмента.
    pub fn write<R>(
        &self,
        rec: &WalRecord,
        apply: impl FnOnce(ChunkId) -> R,
    ) -> Result<R, CacheError> {
        let mut seg = self
            .current
            .lock()
            .map_err(|_| CacheError::Internal("WAL mutex poisoned".into()))?;
        let data =
            bincode::serialize(rec).map_err(|e| CacheError::Serialization(e.to_string()))?;
        let len = (data.len() as u32).to_le_bytes();
        seg.file
            .write_all(&len)
            .and_then(|_| seg.file.write_all(&data))
            .and_then(|_| seg.file.flush())
            .map_err(|e| CacheError::Internal(format!("write WAL: {}", e)))?;
        Ok(apply(seg.chunk_id))
    }

    /// Переключить запись на сегмент нового чанка. Возвращает chunk_id прежнего сегмента.
    pub fn rotate(&self, new_chunk_id: ChunkId) -> Result<ChunkId, CacheError> {
        let mut seg = self
            .current
            .lock()
            .map_err(|_| CacheError::Internal("WAL mutex poisoned".into()))?;
        let new_seg = open_segment(&self.dir, new_chunk_id)?;
        // старый File закрывается здесь (важно для удаления файла на Windows)
        let old = std::mem::replace(&mut *seg, new_seg);
        Ok(old.chunk_id)
    }

    pub fn remove_segment(&self, chunk_id: ChunkId) -> Result<(), CacheError> {
        let path = segment_path(&self.dir, chunk_id);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(CacheError::Internal(format!(
                "remove WAL {}: {}",
                path.display(),
                e
            ))),
        }
    }

    /// Доиграть сегмент: все Set попадают в чанк `chunk_id`.
    /// Недописанная последняя запись (процесс убит посреди append) отбрасывается.
    pub fn replay_segment(
        dir: &Path,
        chunk_id: ChunkId,
        core: &CacheCore,
    ) -> Result<(), CacheError> {
        Self::replay_file(&segment_path(dir, chunk_id), chunk_id, core)
    }

    pub fn replay_file(path: &Path, chunk_id: ChunkId, core: &CacheCore) -> Result<(), CacheError> {
        let mut buf = Vec::new();
        File::open(path)
            .and_then(|mut f| f.read_to_end(&mut buf))
            .map_err(|e| CacheError::Internal(format!("read WAL {}: {}", path.display(), e)))?;

        let mut r = &buf[..];
        while !r.is_empty() {
            if r.len() < 4 {
                eprintln!("TinyCache: truncated WAL tail ignored: {}", path.display());
                break;
            }
            let len = u32::from_le_bytes(r[..4].try_into().unwrap()) as usize;
            if r.len() < 4 + len {
                eprintln!("TinyCache: truncated WAL tail ignored: {}", path.display());
                break;
            }
            let rec: WalRecord = bincode::deserialize(&r[4..4 + len])
                .map_err(|e| CacheError::Serialization(e.to_string()))?;
            r = &r[4 + len..];

            match rec {
                WalRecord::Set { key, value, ttl_ms } => {
                    let ttl = if ttl_ms < 0 {
                        None
                    } else {
                        Some(Duration::from_millis(ttl_ms as u64))
                    };
                    core.set(key, value, ttl, chunk_id);
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
}
