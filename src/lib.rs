#![allow(rust_2024_compatibility)]
#![allow(unsafe_op_in_unsafe_fn)]

mod core;
mod error;
mod wal;
mod meta;

use crate::core::CacheCore;
use crate::error::CacheError;
use crate::wal::{Wal, WalRecord};
use crate::meta::{Paths, StateMeta, load_or_init_state, load_keys_meta, save_keys_meta};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

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
/// PersistentCore: CacheCore + WAL
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
        retention_snapshots: u64,
    ) -> Result<Self, CacheError> {
        let paths = Paths::new(&data_dir)?;
        let state = load_or_init_state(&paths, snapshot_interval_secs, retention_snapshots)?;
        let core = CacheCore::new();

        load_keys_meta(&paths, &core)?;

        let wal = Wal::open(paths.wal_log())?;
        wal.replay(&core)?;

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
        let wal = self.wal_path_reset_handle();
        let state_arc = self.state.clone();
        let paths = self.paths.clone();

        thread::spawn(move || {
            loop {
                let interval;
                let retention;
                {
                    let st = state_arc.lock().unwrap();
                    interval = st.snapshot_interval_secs;
                    retention = st.retention_snapshots;
                }

                if interval == 0 {
                    thread::sleep(Duration::from_secs(1));
                    continue;
                }

                thread::sleep(Duration::from_secs(interval));

                if let Err(e) =
                    Self::do_snapshot(&core, &wal, &state_arc, &paths, retention)
                {
                    eprintln!("snapshot error: {:?}", e);
                }
            }
        });
    }

    fn wal_path_reset_handle(&self) -> Wal {
        Wal::open(self.paths.wal_log()).expect("reopen WAL for snapshot")
    }

    fn do_snapshot(
        core: &CacheCore,
        wal: &Wal,
        state_arc: &Arc<Mutex<StateMeta>>,
        paths: &Paths,
        retention: u64,
    ) -> Result<(), CacheError> {
        // 1) выгружаем live‑состояние
        let live = core.export_live_with_values();

        #[derive(Serialize)]
        struct SnapshotFile {
            format_version: u32,
            entries: Vec<(String, Vec<u8>, i64)>, // (key, value, ttl_ms)
        }

        let now = Instant::now();
        let entries: Vec<(String, Vec<u8>, i64)> = live
            .into_iter()
            .map(|(k, v, ttl)| {
                let ttl_ms = ttl
                    .and_then(|d| {
                        // ttl уже duration «от сейчас»
                        if d.as_millis() <= 0 {
                            None
                        } else {
                            Some(d.as_millis() as i64)
                        }
                    })
                    .unwrap_or(-1);
                (k, v, ttl_ms)
            })
            .collect();

        let mut state = state_arc.lock().unwrap();
        let snapshot_id = now.elapsed().as_millis() as u64 + state.current_snapshot_id + 1;
        state.current_snapshot_id = snapshot_id;
        state.snapshots.push(snapshot_id);

        let snap_path = paths.snapshot_file(snapshot_id);
        let tmp = snap_path.with_extension("tmp");

        let file = SnapshotFile {
            format_version: StateMeta::CURRENT_VERSION,
            entries,
        };
        let data =
            bincode::serialize(&file).map_err(|e| CacheError::Serialization(e.to_string()))?;

        {
            let mut f = std::fs::File::create(&tmp)
                .map_err(|e| CacheError::Internal(format!("create snapshot.tmp: {}", e)))?;
            f.write_all(&data)
                .and_then(|_| f.flush())
                .map_err(|e| CacheError::Internal(format!("write snapshot.tmp: {}", e)))?;
        }
        std::fs::rename(&tmp, &snap_path)
            .map_err(|e| CacheError::Internal(format!("rename snapshot.tmp: {}", e)))?;

        // 2) сохраняем keys.bin
        save_keys_meta(paths, core)?;

        // 3) retention
        if retention > 0 && state.snapshots.len() as u64 > retention {
            let to_remove = state.snapshots.len() as u64 - retention;
            for _ in 0..to_remove {
                if let Some(old_id) = state.snapshots.first().cloned() {
                    let old_path = paths.snapshot_file(old_id);
                    let _ = std::fs::remove_file(&old_path);
                    state.snapshots.remove(0);
                }
            }
        }

        // 4) сохраняем state.json
        crate::meta::save_state(paths, &state)?;

        // 5) обнуляем WAL
        wal.reset()?;

        Ok(())
    }

    pub fn set(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) -> Result<(), CacheError> {
        let ttl_ms = ttl.map(|d| d.as_millis() as i64).unwrap_or(-1);
        self.wal.append(&WalRecord::Set {
            key: key.clone(),
            value: value.clone(),
            ttl_ms,
        })?;
        self.core.set(key, value, ttl);
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
            let s = UnixStream::connect(path)
                .map_err(|e| CacheError::Network(e.to_string()))?;
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

fn handle_connection(
    stream: &mut TcpStream,
    core: Arc<PersistentCore>,
) -> Result<(), CacheError> {
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
    let paths = Paths::new(&data_dir)
        .map_err(|e| PyRuntimeError::new_err(format!("init paths: {}", e)))?;
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
        thread::sleep(Duration::from_millis(10));
        let addr = TransportAddr::parse(&addr);
        Self { addr }
    }

    /// set(key, value, ttl_ms=None)
    ///
    /// ttl_ms: int | None
    fn set(&self, key: String, value: &[u8], ttl_ms: Option<i64>) -> PyResult<()> {
        let v = value.to_vec();
        let ttl_field = ttl_ms.unwrap_or(-1);
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
