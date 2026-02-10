#![allow(rust_2024_compatibility)]
#![allow(unsafe_op_in_unsafe_fn)]

mod core;
mod error;
mod wal;
mod meta;

use crate::core::{CacheCore, ChunkId};
use crate::error::CacheError;
use crate::wal::{Wal, WalRecord};
use crate::meta::{
    Paths, StateMeta, load_or_init_state, save_state, load_keys_meta, save_keys_meta,
};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use bincode;
#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};

/// =======================
/// Команды и ответы
/// =======================

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum CacheCommand {
    /// Set(key, value, ttl_ms) — ttl_ms < 0 => без TTL
    Set(String, Vec<u8>, i64),
    Get(String),
    Pop(String),
    Del(String),
    Keys(String),
    Len,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum CacheResponse {
    Ok,
    Value(Vec<u8>),
    Nil,
    Int(i64),
    Keys(Vec<String>),
}

/// =======================
/// Адрес транспорта
/// =======================

#[derive(Clone, Debug)]
enum TransportAddr {
    Tcp(String),
    #[cfg(unix)]
    Unix(PathBuf),
}

impl TransportAddr {
    fn parse(s: &str) -> Self {
        if let Some(rest) = s.strip_prefix("tcp://") {
            TransportAddr::Tcp(rest.to_string())
        } else if let Some(rest) = s.strip_prefix("unix://") {
            #[cfg(unix)]
            {
                TransportAddr::Unix(PathBuf::from(rest))
            }
            #[cfg(not(unix))]
            {
                TransportAddr::Tcp(rest.to_string())
            }
        } else {
            TransportAddr::Tcp(s.to_string())
        }
    }
}

/// =======================
/// PersistentCore: CacheCore + WAL + meta/state
/// =======================

pub struct PersistentCore {
    core: CacheCore,
    wal: Wal,
    state: Arc<Mutex<StateMeta>>,
    paths: Paths,
}

impl PersistentCore {
    pub fn new(
        data_dir: PathBuf,
        snapshot_interval_secs: u64,
        retention_chunks: u64,
    ) -> Result<Self, CacheError> {
        let paths = Paths::new(&data_dir)?;
        let mut state = load_or_init_state(&paths, snapshot_interval_secs, retention_chunks)?;
        let core = CacheCore::new();

        // 1) Загрузка меты из keys.bin
        load_keys_meta(&paths, &core)?;

        // 2) GC по retention (чистим по chunk_id и TTL)
        core.gc(&state.live_chunks);

        // 3) WAL поверх меты (фиксируем незаписанный "текущий" чанк)
        let wal = Wal::open(paths.wal_log())?;
        wal.replay(&core, state.current_chunk_id)?;

        // гарантируем, что current_chunk_id в списке живых
        if !state.live_chunks.contains(&state.current_chunk_id) {
            state.live_chunks.push(state.current_chunk_id);
        }

        let pc = Self {
            core,
            wal,
            state: Arc::new(Mutex::new(state)),
            paths,
        };

        pc.spawn_snapshot_thread();

        Ok(pc)
    }

    fn spawn_snapshot_thread(&self) {
        let core = self.core.clone();
        let wal_for_reset = self.wal_path_reset_handle();
        let state_arc = self.state.clone();
        let paths = self.paths.clone();

        thread::spawn(move || loop {
            let (interval, retention) = {
                let st = state_arc.lock().unwrap();
                (st.snapshot_interval_secs, st.retention_chunks)
            };

            if interval == 0 {
                thread::sleep(Duration::from_secs(1));
                continue;
            }

            thread::sleep(Duration::from_secs(interval));

            if let Err(e) = Self::do_snapshot(&core, &wal_for_reset, &state_arc, &paths, retention)
            {
                eprintln!("snapshot error: {:?}", e);
            }
        });
    }

    fn wal_path_reset_handle(&self) -> Wal {
        Wal::open(self.paths.wal_log()).expect("reopen WAL for snapshot")
    }

    /// STW snapshot+retention:
    /// - удаление старых чанков (по m)
    /// - GC по TTL и не‑живым chunk_id
    /// - запись chunk_{current_chunk_id}.bin
    /// - дамп keys.bin и state.json
    /// - reset WAL
    /// - генерация нового current_chunk_id
    fn do_snapshot(
        core: &CacheCore,
        wal: &Wal,
        state_arc: &Arc<Mutex<StateMeta>>,
        paths: &Paths,
        retention: u64,
    ) -> Result<(), CacheError> {
        // STW begin: mutex на state + core.gc через live_chunks
        let mut state = state_arc.lock().unwrap();

        // 1) ограничиваем список живых чанков последними m
        if retention > 0 && state.live_chunks.len() as u64 > retention {
            let to_remove = state.live_chunks.len() as u64 - retention;
            for _ in 0..to_remove {
                if let Some(old) = state.live_chunks.first().cloned() {
                    let old_path = paths.chunk_file(old);
                    let _ = std::fs::remove_file(&old_path);
                    state.live_chunks.remove(0);
                }
            }
        }

        // 2) GC по TTL + по chunk_id ∉ live_chunks
        core.gc(&state.live_chunks);

        // 3) флашим текущий чанк (live мета с chunk_id == current_chunk_id и значениями)
        let current_chunk_id: ChunkId = state.current_chunk_id;
        let live_for_chunk = core.export_live_for_chunk(current_chunk_id);

        #[derive(Serialize)]
        struct ChunkHeader {
            magic: [u8; 4],
            version: u32,
            chunk_id: ChunkId,
            entry_count: u64,
        }

        let chunk_path = paths.chunk_file(current_chunk_id);
        let tmp = chunk_path.with_extension("tmp");

        {
            let mut f = std::fs::File::create(&tmp)
                .map_err(|e| CacheError::Internal(format!("create chunk tmp: {}", e)))?;

            let header = ChunkHeader {
                magic: *b"TMCK",
                version: StateMeta::CURRENT_VERSION,
                chunk_id: current_chunk_id,
                entry_count: live_for_chunk.len() as u64,
            };
            let header_bytes =
                bincode::serialize(&header).map_err(|e| CacheError::Serialization(e.to_string()))?;
            f.write_all(&header_bytes)
                .and_then(|_| f.flush())
                .map_err(|e| CacheError::Internal(format!("write chunk header: {}", e)))?;

            for (key_id, value, _ttl) in live_for_chunk {
                let key_id_bytes = key_id.to_le_bytes();
                let val_len = value.len() as u32;
                let val_len_bytes = val_len.to_le_bytes();

                f.write_all(&key_id_bytes)
                    .and_then(|_| f.write_all(&val_len_bytes))
                    .and_then(|_| f.write_all(&value))
                    .map_err(|e| CacheError::Internal(format!("write chunk entry: {}", e)))?;
            }

            f.flush()
                .map_err(|e| CacheError::Internal(format!("flush chunk: {}", e)))?;
        }

        std::fs::rename(&tmp, &chunk_path)
            .map_err(|e| CacheError::Internal(format!("rename chunk tmp: {}", e)))?;

        if !state.live_chunks.contains(&current_chunk_id) {
            state.live_chunks.push(current_chunk_id);
        }

        // 4) дамп меты keys.bin
        save_keys_meta(paths, &core, &state)?;

        // 5) обновляем state.json (current_chunk_id ещё старый до смены)
        save_state(paths, &state)?;

        // 6) обнуление WAL
        wal.reset()?;

        // 7) new current_chunk_id
        let new_id = current_chunk_id.wrapping_add(1);
        state.current_chunk_id = new_id;
        if !state.live_chunks.contains(&new_id) {
            state.live_chunks.push(new_id);
        }
        save_state(paths, &state)?;

        // STW end (mutex освобождён при выходе)
        Ok(())
    }

    pub fn set(
        &self,
        key: String,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), CacheError> {
        let ttl_ms = ttl.map(|d| d.as_millis() as i64).unwrap_or(-1);

        let current_chunk_id = {
            let st = self.state.lock().unwrap();
            st.current_chunk_id
        };

        self.wal.append(&WalRecord::Set {
            key: key.clone(),
            value: value.clone(),
            ttl_ms,
            chunk_id: current_chunk_id,
        })?;
        self.core.set(key, value, ttl, current_chunk_id);
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.core.get(key)
    }

    pub fn pop(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        self.wal.append(&WalRecord::Pop {
            key: key.to_string(),
        })?;
        Ok(self.core.pop(key))
    }

    pub fn delete(&self, key: &str) -> Result<i64, CacheError> {
        self.wal.append(&WalRecord::Del {
            key: key.to_string(),
        })?;
        Ok(self.core.delete(key))
    }

    pub fn keys_prefix(&self, prefix: &str) -> Vec<String> {
        self.core.keys_prefix(prefix)
    }

    pub fn len(&self) -> i64 {
        self.core.len()
    }
}

/// =======================
/// Маппинг ошибок в Python
/// =======================

fn map_error(e: CacheError, ctx: &str) -> PyErr {
    PyRuntimeError::new_err(format!("{}: {}", ctx, e))
}

/// =======================
/// Клиентский транспорт (TCP/UDS)
/// =======================

fn write_all(w: &mut impl Write, buf: &[u8]) -> Result<(), CacheError> {
    w.write_all(buf)
        .and_then(|_| w.flush())
        .map_err(|e| CacheError::Network(e.to_string()))
}

fn read_exact(r: &mut impl Read, buf: &mut [u8]) -> Result<(), CacheError> {
    r.read_exact(buf)
        .map_err(|e| CacheError::Network(e.to_string()))
}

fn send_cmd_sync(addr: &TransportAddr, cmd: CacheCommand) -> Result<CacheResponse, CacheError> {
    enum Conn {
        Tcp(TcpStream),
        #[cfg(unix)]
        Unix(UnixStream),
    }

    let mut conn = match addr {
        TransportAddr::Tcp(a) => {
            let s = TcpStream::connect(a).map_err(|e| CacheError::Network(e.to_string()))?;
            Conn::Tcp(s)
        }
        #[cfg(unix)]
        TransportAddr::Unix(path) => {
            let s =
                UnixStream::connect(path).map_err(|e| CacheError::Network(e.to_string()))?;
            Conn::Unix(s)
        }
    };

    let encoded_cmd =
        bincode::serialize(&cmd).map_err(|e| CacheError::Serialization(e.to_string()))?;
    let size = (encoded_cmd.len() as u32).to_le_bytes();

    match &mut conn {
        Conn::Tcp(s) => {
            write_all(s, &size)?;
            write_all(s, &encoded_cmd)?;
        }
        #[cfg(unix)]
        Conn::Unix(s) => {
            write_all(s, &size)?;
            write_all(s, &encoded_cmd)?;
        }
    }

    let mut size_buf = [0u8; 4];
    match &mut conn {
        Conn::Tcp(s) => read_exact(s, &mut size_buf)?,
        #[cfg(unix)]
        Conn::Unix(s) => read_exact(s, &mut size_buf)?,
    }
    let resp_size = u32::from_le_bytes(size_buf) as usize;

    let mut buf = vec![0u8; resp_size];
    match &mut conn {
        Conn::Tcp(s) => read_exact(s, &mut buf)?,
        #[cfg(unix)]
        Conn::Unix(s) => read_exact(s, &mut buf)?,
    }

    bincode::deserialize(&buf).map_err(|e| CacheError::Serialization(e.to_string()))
}

/// =======================
/// Общая обработка соединения
/// =======================

fn handle_connection_impl<S: Read + Write>(
    stream: &mut S,
    core: Arc<PersistentCore>,
) -> Result<(), CacheError> {
    let mut size_buf = [0u8; 4];
    read_exact(stream, &mut size_buf)?;
    let cmd_size = u32::from_le_bytes(size_buf) as usize;
    if cmd_size > 1_000_000 {
        return Err(CacheError::Internal("command too large".into()));
    }

    let mut buf = vec![0u8; cmd_size];
    read_exact(stream, &mut buf)?;
    let cmd: CacheCommand =
        bincode::deserialize(&buf).map_err(|e| CacheError::Serialization(e.to_string()))?;

    let resp = match cmd {
        CacheCommand::Set(key, value, ttl_ms) => {
            let ttl = if ttl_ms < 0 {
                None
            } else {
                Some(Duration::from_millis(ttl_ms as u64))
            };
            core.set(key, value, ttl)?;
            CacheResponse::Ok
        }
        CacheCommand::Get(key) => core
            .get(&key)
            .map(CacheResponse::Value)
            .unwrap_or(CacheResponse::Nil),
        CacheCommand::Pop(key) => core
            .pop(&key)?
            .map(CacheResponse::Value)
            .unwrap_or(CacheResponse::Nil),
        CacheCommand::Del(key) => CacheResponse::Int(core.delete(&key)?),
        CacheCommand::Keys(pattern) => {
            if pattern.ends_with('*') {
                let prefix = &pattern[..pattern.len() - 1];
                CacheResponse::Keys(core.keys_prefix(prefix))
            } else {
                CacheResponse::Keys(Vec::new())
            }
        }
        CacheCommand::Len => CacheResponse::Int(core.len()),
    };

    let encoded =
        bincode::serialize(&resp).map_err(|e| CacheError::Serialization(e.to_string()))?;
    let size = (encoded.len() as u32).to_le_bytes();

    write_all(stream, &size)?;
    write_all(stream, &encoded)?;
    Ok(())
}

fn handle_connection(stream: &mut TcpStream, core: Arc<PersistentCore>) -> Result<(), CacheError> {
    handle_connection_impl(stream, core)
}

#[cfg(unix)]
fn handle_connection_unix(
    stream: &mut UnixStream,
    core: Arc<PersistentCore>,
) -> Result<(), CacheError> {
    handle_connection_impl(stream, core)
}

/// =======================
/// TCP-сервер
/// =======================

#[pyfunction(signature = (data_dir, port=5002, snapshot_interval_secs=60, retention_chunks=3))]
fn serve(
    data_dir: String,
    port: u16,
    snapshot_interval_secs: u64,
    retention_chunks: u64,
) -> PyResult<()> {
    println!("*** NEW SERVE VERSION ***");
    let addr = format!("127.0.0.1:{}", port);
    println!("TinyCache TCP server: {}", addr);

    let core = Arc::new(
        PersistentCore::new(
            PathBuf::from(&data_dir),
            snapshot_interval_secs,
            retention_chunks,
        )
        .map_err(|e| PyRuntimeError::new_err(format!("init persistent core: {}", e)))?,
    );

    let listener = TcpListener::bind(&addr)
        .map_err(|e| PyRuntimeError::new_err(format!("Bind error: {}", e)))?;

    println!("TinyCache TCP ready: {}", addr);

    for stream_res in listener.incoming() {
        match stream_res {
            Ok(mut stream) => {
                let core_clone = core.clone();
                thread::spawn(move || {
                    if let Err(e) = handle_connection(&mut stream, core_clone) {
                        eprintln!("TCP connection error: {:?}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("TCP listener error: {}", e);
                break;
            }
        }
    }

    Ok(())
}

/// =======================
/// UDS-сервер (только Unix)
/// =======================

#[cfg(unix)]
#[pyfunction(signature = (data_dir, snapshot_interval_secs=60, retention_chunks=3))]
fn serve_unix(
    data_dir: String,
    snapshot_interval_secs: u64,
    retention_chunks: u64,
) -> PyResult<()> {
    let paths =
        Paths::new(&data_dir).map_err(|e| PyRuntimeError::new_err(format!("init paths: {}", e)))?;
    let sock_path = paths.uds_path();
    if sock_path.exists() {
        fs::remove_file(&sock_path)
            .map_err(|e| PyRuntimeError::new_err(format!("Remove old socket: {}", e)))?;
    }

    println!("TinyCache UDS server: {:?}", sock_path);

    let core = Arc::new(
        PersistentCore::new(
            PathBuf::from(&data_dir),
            snapshot_interval_secs,
            retention_chunks,
        )
        .map_err(|e| PyRuntimeError::new_err(format!("init persistent core: {}", e)))?,
    );

    let listener = UnixListener::bind(&sock_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Bind UDS error: {}", e)))?;

    println!("TinyCache UDS ready: {:?}", sock_path);

    for stream_res in listener.incoming() {
        match stream_res {
            Ok(mut stream) => {
                let core_clone = core.clone();
                thread::spawn(move || {
                    if let Err(e) = handle_connection_unix(&mut stream, core_clone) {
                        eprintln!("UDS connection error: {:?}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("UDS listener error: {}", e);
                break;
            }
        }
    }

    Ok(())
}

/// =======================
/// Python-клиент TinyCache
/// =======================

#[pyclass]
#[derive(Clone)]
pub struct TinyCache {
    addr: TransportAddr,
}

#[pymethods]
impl TinyCache {
    #[new]
    fn new(addr: String) -> Self {
        // маленькая задержка, чтобы сервер успел подняться
        thread::sleep(Duration::from_millis(10));
        let addr = TransportAddr::parse(&addr);
        Self { addr }
    }

    /// set(key: str, value: bytes, ttl: Optional[float] = None) -> None
    /// ttl в секундах (дробное), None = без TTL.
    #[pyo3(signature = (key, value, ttl=None))]
    fn set(&self, key: String, value: &[u8], ttl: Option<f64>) -> PyResult<()> {
        let v = value.to_vec();
        let ttl_ms_opt: Option<i64> = ttl.map(|sec| {
            if sec <= 0.0 {
                -1
            } else {
                (sec * 1000.0) as i64
            }
        });

        let ttl_field = ttl_ms_opt.unwrap_or(-1);
        match send_cmd_sync(&self.addr, CacheCommand::Set(key, v, ttl_field)) {
            Ok(CacheResponse::Ok) => Ok(()),
            Ok(resp) => Err(PyRuntimeError::new_err(format!(
                "Unexpected response from set: {:?}",
                resp
            ))),
            Err(e) => Err(map_error(e, "set")),
        }
    }

    fn get<'py>(
        &self,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        match send_cmd_sync(&self.addr, CacheCommand::Get(key)) {
            Ok(CacheResponse::Value(v)) => {
                let b = PyBytes::new_bound(py, &v);
                Ok(Some(b))
            }
            Ok(CacheResponse::Nil) => Ok(None),
            Ok(resp) => Err(PyRuntimeError::new_err(format!(
                "Unexpected response from get: {:?}",
                resp
            ))),
            Err(e) => Err(map_error(e, "get")),
        }
    }

    fn pop<'py>(
        &self,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        match send_cmd_sync(&self.addr, CacheCommand::Pop(key)) {
            Ok(CacheResponse::Value(v)) => {
                let b = PyBytes::new_bound(py, &v);
                Ok(Some(b))
            }
            Ok(CacheResponse::Nil) => Ok(None),
            Ok(resp) => Err(PyRuntimeError::new_err(format!(
                "Unexpected response from pop: {:?}",
                resp
            ))),
            Err(e) => Err(map_error(e, "pop")),
        }
    }

    fn delete(&self, key: String) -> PyResult<i64> {
        match send_cmd_sync(&self.addr, CacheCommand::Del(key)) {
            Ok(CacheResponse::Int(n)) => Ok(n),
            Ok(resp) => Err(PyRuntimeError::new_err(format!(
                "Unexpected response from delete: {:?}",
                resp
            ))),
            Err(e) => Err(map_error(e, "delete")),
        }
    }

    fn keys(&self, pattern: String) -> PyResult<Vec<String>> {
        match send_cmd_sync(&self.addr, CacheCommand::Keys(pattern)) {
            Ok(CacheResponse::Keys(keys)) => Ok(keys),
            Ok(resp) => Err(PyRuntimeError::new_err(format!(
                "Unexpected response from keys: {:?}",
                resp
            ))),
            Err(e) => Err(map_error(e, "keys")),
        }
    }

    fn len(&self) -> PyResult<i64> {
        match send_cmd_sync(&self.addr, CacheCommand::Len) {
            Ok(CacheResponse::Int(n)) => Ok(n),
            Ok(resp) => Err(PyRuntimeError::new_err(format!(
                "Unexpected response from len: {:?}",
                resp
            ))),
            Err(e) => Err(map_error(e, "len")),
        }
    }
}

/// =======================
/// Python-модуль
/// =======================

#[pymodule]
fn tiny_mp_cache(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TinyCache>()?;
    m.add_function(wrap_pyfunction!(serve, m)?)?;
    #[cfg(unix)]
    m.add_function(wrap_pyfunction!(serve_unix, m)?)?;
    Ok(())
}
