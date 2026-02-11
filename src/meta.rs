use crate::core::{CacheCore, KeyMeta, ChunkId};
use crate::error::CacheError;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StateMeta {
    pub format_version: u32,
    pub current_chunk_id: ChunkId,
    pub live_chunks: Vec<ChunkId>,
    pub snapshot_interval_secs: u64,
    pub retention_chunks: u64,
}

impl StateMeta {
    pub const CURRENT_VERSION: u32 = 1;
}

#[derive(Debug, Serialize, Deserialize)]
struct KeyMetaDisk {
    key: String,
    key_id: u64,
    chunk_id: u64,
    ttl_ms: i64,
    updated_at_ms: i64,
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis() as i64
}

#[derive(Clone, Debug)]
pub struct Paths {
    pub meta_dir: PathBuf,
    pub wal_dir: PathBuf,
    pub chunks_dir: PathBuf,
    pub ipc_dir: PathBuf,
}

impl Paths {
    pub fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self, CacheError> {
        let dd = data_dir.as_ref().to_path_buf();
        let meta_dir = dd.join("meta");
        let wal_dir = dd.join("wal");
        let chunks_dir = dd.join("chunks");
        let ipc_dir = dd.join("ipc");

        fs::create_dir_all(&meta_dir)
            .map_err(|e| CacheError::Internal(format!("create meta dir: {}", e)))?;
        fs::create_dir_all(&wal_dir)
            .map_err(|e| CacheError::Internal(format!("create wal dir: {}", e)))?;
        fs::create_dir_all(&chunks_dir)
            .map_err(|e| CacheError::Internal(format!("create chunks dir: {}", e)))?;
        fs::create_dir_all(&ipc_dir)
            .map_err(|e| CacheError::Internal(format!("create ipc dir: {}", e)))?;

        Ok(Self {
            meta_dir,
            wal_dir,
            chunks_dir,
            ipc_dir,
        })
    }

    pub fn state_json(&self) -> PathBuf {
        self.meta_dir.join("state.json")
    }

    pub fn keys_bin(&self) -> PathBuf {
        self.meta_dir.join("keys.bin")
    }

    pub fn wal_log(&self) -> PathBuf {
        self.wal_dir.join("log.bin")
    }

    pub fn chunk_file(&self, id: ChunkId) -> PathBuf {
        self.chunks_dir.join(format!("chunk_{}.bin", id))
    }

    #[cfg(unix)]
    pub fn uds_path(&self) -> PathBuf {
        self.ipc_dir.join("tiny-cache.sock")
    }
}

pub fn load_or_init_state(
    paths: &Paths,
    snapshot_interval_secs: u64,
    retention_chunks: u64,
) -> Result<StateMeta, CacheError> {
    let state_path = paths.state_json();
    if state_path.exists() {
        let mut f = File::open(&state_path)
            .map_err(|e| CacheError::Internal(format!("open state.json: {}", e)))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)
            .map_err(|e| CacheError::Internal(format!("read state.json: {}", e)))?;
        let mut state: StateMeta =
            serde_json::from_slice(&buf).map_err(|e| CacheError::Serialization(e.to_string()))?;
        state.snapshot_interval_secs = snapshot_interval_secs;
        state.retention_chunks = retention_chunks;
        Ok(state)
    } else {
        let now = now_unix_ms() as u64;
        let state = StateMeta {
            format_version: StateMeta::CURRENT_VERSION,
            current_chunk_id: now,
            live_chunks: Vec::new(),
            snapshot_interval_secs,
            retention_chunks,
        };
        save_state(paths, &state)?;
        Ok(state)
    }
}

pub fn save_state(paths: &Paths, state: &StateMeta) -> Result<(), CacheError> {
    let state_path = paths.state_json();

    // Гарантируем существование директории
    if let Some(parent) = state_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| CacheError::Internal(format!("ensure state dir: {}", e)))?;
    }

    let tmp = state_path.with_extension("tmp");
    let data =
        serde_json::to_vec_pretty(state).map_err(|e| CacheError::Serialization(e.to_string()))?;
    {
        let mut f = File::create(&tmp)
            .map_err(|e| CacheError::Internal(format!("create state.tmp: {}", e)))?;
        f.write_all(&data)
            .and_then(|_| f.flush())
            .map_err(|e| CacheError::Internal(format!("write state.tmp: {}", e)))?;
    }
    fs::rename(&tmp, &state_path)
        .map_err(|e| CacheError::Internal(format!("rename state.tmp: {}", e)))?;
    Ok(())
}

/// Загрузить полную мету из keys.bin.
pub fn load_keys_meta(paths: &Paths, core: &CacheCore) -> Result<(), CacheError> {
    let keys_path = paths.keys_bin();
    if !keys_path.exists() {
        return Ok(());
    }

    let mut f = File::open(&keys_path)
        .map_err(|e| CacheError::Internal(format!("open keys.bin: {}", e)))?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)
        .map_err(|e| CacheError::Internal(format!("read keys.bin: {}", e)))?;

    #[derive(Deserialize)]
    struct KeysFile {
        format_version: u32,
        entries: Vec<KeyMetaDisk>,
    }

    let file: KeysFile =
        bincode::deserialize(&buf).map_err(|e| CacheError::Serialization(e.to_string()))?;

    if file.format_version != StateMeta::CURRENT_VERSION {
        return Err(CacheError::Internal(format!(
            "unsupported keys.bin version: {}",
            file.format_version
        )));
    }

    let now = std::time::Instant::now();
    for e in file.entries {
        let ttl = if e.ttl_ms < 0 {
            None
        } else {
            Some(Duration::from_millis(e.ttl_ms as u64))
        };
        let ttl_instant = ttl.map(|d| now + d);
        let meta = KeyMeta {
            key_id: e.key_id,
            chunk_id: e.chunk_id,
            ttl: ttl_instant,
            updated_at: now,
        };
        core.insert_meta_only(e.key, meta);
    }

    Ok(())
}

pub fn save_keys_meta(paths: &Paths, core: &CacheCore) -> Result<(), CacheError> {
    let keys_path = paths.keys_bin();

    // Гарантируем существование директории
    if let Some(parent) = keys_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| CacheError::Internal(format!("ensure keys.bin dir: {}", e)))?;
    }

    let tmp = keys_path.with_extension("tmp");

    #[derive(Serialize)]
    struct KeysFile {
        format_version: u32,
        entries: Vec<KeyMetaDisk>,
    }

    let now_ms = now_unix_ms();
    let entries: Vec<KeyMetaDisk> = core
        .export_meta_for_disk()
        .into_iter()
        .map(|(key, key_id, chunk_id, ttl, _updated_at)| KeyMetaDisk {
            key,
            key_id,
            chunk_id,
            ttl_ms: ttl.map(|d| d.as_millis() as i64).unwrap_or(-1),
            updated_at_ms: now_ms,
        })
        .collect();

    let file = KeysFile {
        format_version: StateMeta::CURRENT_VERSION,
        entries,
    };

    let data =
        bincode::serialize(&file).map_err(|e| CacheError::Serialization(e.to_string()))?;

    {
        let mut f = File::create(&tmp)
            .map_err(|e| CacheError::Internal(format!("create keys.tmp: {}", e)))?;
        f.write_all(&data)
            .and_then(|_| f.flush())
            .map_err(|e| CacheError::Internal(format!("write keys.tmp: {}", e)))?;
    }
    fs::rename(&tmp, &keys_path)
        .map_err(|e| CacheError::Internal(format!("rename keys.tmp: {}", e)))?;
    Ok(())
}
