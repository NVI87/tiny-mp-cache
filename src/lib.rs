use crate::core::CacheCore;
use crate::error::CacheError;
use crate::meta::{
    load_keys_meta, load_or_init_state, save_keys_meta, save_state, Paths, StateMeta,
};
use crate::wal::{Wal, WalRecord};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};

mod core;
mod error;
mod meta;
mod wal;

/// =======================
/// Протокол команд
/// =======================

#[derive(Serialize, Deserialize, Debug, Clone)]
enum CacheCommand {
    Set(String, Vec<u8>, i64), // key, value, ttl_ms (-1 = no ttl)
    Get(String),
    Pop(String),
    Del(String),
    Keys(String),
    Len,
}

#[derive(Serialize, Deserialize, Debug)]
enum CacheResponse {
    Ok,
    Value(Vec<u8>),
    Int(i64),
    Keys(Vec<String>),
    Nil,
    Err(String),
}

fn read_exact(stream: &mut impl Read, buf: &mut [u8]) -> Result<(), CacheError> {
    stream
        .read_exact(buf)
        .map_err(|e| CacheError::Network(format!("read_exact: {}", e)))
}

fn write_all(stream: &mut impl Write, buf: &[u8]) -> Result<(), CacheError> {
    stream
        .write_all(buf)
        .and_then(|_| stream.flush())
        .map_err(|e| CacheError::Network(format!("write_all: {}", e)))
}

/// =======================
/// PersistentCore
/// =======================

struct PersistentCore {
    paths: Paths,
    core: CacheCore,
    state: Mutex<StateMeta>,
    wal: Mutex<Wal>,
}

impl PersistentCore {
    fn new(
        data_dir: PathBuf,
        snapshot_interval_secs: u64,
        retention_chunks: u64,
    ) -> Result<Arc<Self>, CacheError> {
        let paths = Paths::new(&data_dir)?;
        let state = load_or_init_state(&paths, snapshot_interval_secs, retention_chunks)?;
        let core = CacheCore::new();

        load_keys_meta(&paths, &core)?;
        let wal = Wal::open(paths.wal_log())?;
        wal.replay(&core, state.current_chunk_id)?;

        if !state.live_chunks.is_empty() {
            core.gc(&state.live_chunks);
        }

        let this = Arc::new(PersistentCore {
            paths,
            core,
            state: Mutex::new(state),
            wal: Mutex::new(wal),
        });

        Self::spawn_snapshot_thread(&this);

        Ok(this)
    }

    fn spawn_snapshot_thread(this: &Arc<Self>) {
        let this_clone = Arc::clone(this);
        thread::spawn(move || loop {
            let interval = {
                let st = this_clone
                    .state
                    .lock()
                    .expect("state mutex poisoned")
                    .snapshot_interval_secs;
                Duration::from_secs(st)
            };
            thread::sleep(interval);

            if let Err(e) = this_clone.do_snapshot() {
                eprintln!("snapshot error: {:?}", e);
            }
        });
    }

    fn do_snapshot(&self) -> Result<(), CacheError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| CacheError::Internal("state mutex poisoned".into()))?
            .clone();

        if !state.live_chunks.is_empty() && state.live_chunks.len() as u64 > state.retention_chunks
        {
            let keep = state.retention_chunks as usize;
            let to_drop = state.live_chunks.len() - keep;
            let old = state.live_chunks.drain(0..to_drop).collect::<Vec<_>>();
            for id in old {
                let p = self.paths.chunk_file(id);
                if p.exists() {
                    let _ = fs::remove_file(&p);
                }
            }
        }

        self.core.gc(&state.live_chunks);

        let current_chunk_id = state.current_chunk_id;
        let live = self.core.export_live_for_chunk(current_chunk_id);
        if !live.is_empty() {
            let chunk_path = self.paths.chunk_file(current_chunk_id);
            let tmp = chunk_path.with_extension("tmp");
            {
                let mut f = fs::File::create(&tmp)
                    .map_err(|e| CacheError::Internal(format!("create chunk tmp: {}", e)))?;

                let magic = b"TMCK";
                let version = 1u32;
                let count = live.len() as u64;
                f.write_all(magic)
                    .map_err(|e| CacheError::Internal(format!("write magic: {}", e)))?;
                f.write_all(&version.to_le_bytes())
                    .map_err(|e| CacheError::Internal(format!("write ver: {}", e)))?;
                f.write_all(&current_chunk_id.to_le_bytes())
                    .map_err(|e| CacheError::Internal(format!("write id: {}", e)))?;
                f.write_all(&count.to_le_bytes())
                    .map_err(|e| CacheError::Internal(format!("write count: {}", e)))?;

                for (key_id, value, _ttl) in live {
                    let key_id_bytes = key_id.to_le_bytes();
                    let len = (value.len() as u32).to_le_bytes();
                    f.write_all(&key_id_bytes)
                        .and_then(|_| f.write_all(&len))
                        .and_then(|_| f.write_all(&value))
                        .map_err(|e| CacheError::Internal(format!("write chunk kv: {}", e)))?;
                }
                f.flush()
                    .map_err(|e| CacheError::Internal(format!("flush chunk: {}", e)))?;
            }
            fs::rename(&tmp, &chunk_path)
                .map_err(|e| CacheError::Internal(format!("rename chunk: {}", e)))?;
        }

        if !state.live_chunks.contains(&current_chunk_id) {
            state.live_chunks.push(current_chunk_id);
        }

        save_keys_meta(&self.paths, &self.core, &state)?;

        let new_chunk_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let new_chunk_id = if new_chunk_id <= current_chunk_id {
            current_chunk_id + 1
        } else {
            new_chunk_id
        };

        state.current_chunk_id = new_chunk_id;
        save_state(&self.paths, &state)?;

        {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| CacheError::Internal("state mutex poisoned".into()))?;
            *guard = state;
        }

        {
            let wal = self
                .wal
                .lock()
                .map_err(|_| CacheError::Internal("wal mutex poisoned".into()))?;
            wal.reset()?;
        }

        Ok(())
    }

    fn set(
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

        {
            let wal = self
                .wal
                .lock()
                .map_err(|_| CacheError::Internal("wal mutex poisoned".into()))?;
            wal.append(&WalRecord::Set {
                key: key.clone(),
                value: value.clone(),
                ttl_ms,
            })?;
        }

        self.core.set(key, value, ttl, current_chunk_id);
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        Ok(self.core.get(key))
    }

    fn pop(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        Ok(self.core.pop(key))
    }

    fn delete(&self, key: &str) -> Result<i64, CacheError> {
        Ok(self.core.delete(key))
    }

    fn keys_prefix(&self, prefix: &str) -> Result<Vec<String>, CacheError> {
        Ok(self.core.keys_prefix(prefix))
    }

    fn len(&self) -> Result<i64, CacheError> {
        Ok(self.core.len())
    }
}

/// =======================
/// Очередь + worker‑тред
/// =======================

enum WorkerRequest {
    Command(CacheCommand, mpsc::Sender<CacheResponse>),
}

struct ServerCore {
    tx: mpsc::Sender<WorkerRequest>,
}

impl ServerCore {
    fn new(persistent: Arc<PersistentCore>) -> Self {
        let (tx, rx) = mpsc::channel::<WorkerRequest>();

        thread::spawn(move || {
            while let Ok(WorkerRequest::Command(cmd, resp_tx)) = rx.recv() {
                let resp = match cmd {
                    CacheCommand::Set(key, value, ttl_ms) => {
                        let ttl = if ttl_ms < 0 {
                            None
                        } else {
                            Some(Duration::from_millis(ttl_ms as u64))
                        };
                        match persistent.set(key, value, ttl) {
                            Ok(()) => CacheResponse::Ok,
                            Err(e) => CacheResponse::Err(e.to_string()),
                        }
                    }
                    CacheCommand::Get(key) => match persistent.get(&key) {
                        Ok(Some(v)) => CacheResponse::Value(v),
                        Ok(None) => CacheResponse::Nil,
                        Err(e) => CacheResponse::Err(e.to_string()),
                    },
                    CacheCommand::Pop(key) => match persistent.pop(&key) {
                        Ok(Some(v)) => CacheResponse::Value(v),
                        Ok(None) => CacheResponse::Nil,
                        Err(e) => CacheResponse::Err(e.to_string()),
                    },
                    CacheCommand::Del(key) => match persistent.delete(&key) {
                        Ok(n) => CacheResponse::Int(n),
                        Err(e) => CacheResponse::Err(e.to_string()),
                    },
                    CacheCommand::Keys(pattern) => {
                        if pattern.ends_with('*') {
                            let prefix = &pattern[..pattern.len() - 1];
                            match persistent.keys_prefix(prefix) {
                                Ok(keys) => CacheResponse::Keys(keys),
                                Err(e) => CacheResponse::Err(e.to_string()),
                            }
                        } else {
                            CacheResponse::Keys(Vec::new())
                        }
                    }
                    CacheCommand::Len => match persistent.len() {
                        Ok(n) => CacheResponse::Int(n),
                        Err(e) => CacheResponse::Err(e.to_string()),
                    },
                };

                let _ = resp_tx.send(resp);
            }
        });

        ServerCore { tx }
    }

    fn execute(&self, cmd: CacheCommand) -> Result<CacheResponse, CacheError> {
        let (resp_tx, resp_rx) = mpsc::channel();
        self.tx
            .send(WorkerRequest::Command(cmd, resp_tx))
            .map_err(|e| CacheError::Internal(format!("worker send: {}", e)))?;
        resp_rx
            .recv()
            .map_err(|e| CacheError::Internal(format!("worker recv: {}", e)))
    }
}

/// =======================
/// Обработка соединения
/// =======================

fn handle_connection_impl<S: Read + Write>(
    stream: &mut S,
    server: Arc<ServerCore>,
) -> Result<(), CacheError> {
    loop {
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(());
            }
            Err(e) => {
                return Err(CacheError::Network(format!("read cmd len: {}", e)));
            }
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        read_exact(stream, &mut buf)?;

        let cmd: CacheCommand =
            bincode::deserialize(&buf).map_err(|e| CacheError::Serialization(e.to_string()))?;

        let resp = server.execute(cmd)?;

        let encoded =
            bincode::serialize(&resp).map_err(|e| CacheError::Serialization(e.to_string()))?;
        let size = (encoded.len() as u32).to_le_bytes();

        write_all(stream, &size)?;
        write_all(stream, &encoded)?;
    }
}

fn handle_connection(stream: &mut TcpStream, server: Arc<ServerCore>) -> Result<(), CacheError> {
    handle_connection_impl(stream, server)
}

#[cfg(unix)]
fn handle_connection_unix(
    stream: &mut UnixStream,
    server: Arc<ServerCore>,
) -> Result<(), CacheError> {
    handle_connection_impl(stream, server)
}

/// =======================
/// TCP‑сервер
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

    let persistent = PersistentCore::new(
        PathBuf::from(&data_dir),
        snapshot_interval_secs,
        retention_chunks,
    )
    .map_err(|e| PyRuntimeError::new_err(format!("init persistent core: {}", e)))?;

    let server_core = Arc::new(ServerCore::new(persistent));

    let listener = TcpListener::bind(&addr)
        .map_err(|e| PyRuntimeError::new_err(format!("Bind error: {}", e)))?;

    println!("TinyCache TCP ready: {}", addr);

    for stream_res in listener.incoming() {
        match stream_res {
            Ok(mut stream) => {
                let server_clone = server_core.clone();
                thread::spawn(move || {
                    if let Err(e) = handle_connection(&mut stream, server_clone) {
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
/// UDS‑сервер (Unix)
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

    let persistent = PersistentCore::new(
        PathBuf::from(&data_dir),
        snapshot_interval_secs,
        retention_chunks,
    )
    .map_err(|e| PyRuntimeError::new_err(format!("init persistent core: {}", e)))?;

    let server_core = Arc::new(ServerCore::new(persistent));

    let listener = UnixListener::bind(&sock_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Bind UDS error: {}", e)))?;

    println!("TinyCache UDS ready: {:?}", sock_path);

    for stream_res in listener.incoming() {
        match stream_res {
            Ok(mut stream) => {
                let server_clone = server_core.clone();
                thread::spawn(move || {
                    if let Err(e) = handle_connection_unix(&mut stream, server_clone) {
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
/// Client helpers
/// =======================

trait ReadWrite: Read + Write + Send {}
impl<T: Read + Write + Send> ReadWrite for T {}

fn connect(addr: &str) -> PyResult<Box<dyn ReadWrite>> {
    if let Some(path) = addr.strip_prefix("unix://") {
        #[cfg(unix)]
        {
            let stream = UnixStream::connect(path)
                .map_err(|e| PyRuntimeError::new_err(format!("connect unix: {}", e)))?;
            Ok(Box::new(stream))
        }
        #[cfg(not(unix))]
        {
            Err(PyRuntimeError::new_err("Unix sockets not supported"))
        }
    } else {
        let addr = if let Some(a) = addr.strip_prefix("tcp://") {
            a
        } else {
            addr
        };
        let stream = TcpStream::connect(addr)
            .map_err(|e| PyRuntimeError::new_err(format!("connect tcp: {}", e)))?;
        stream.set_nodelay(true).ok();
        Ok(Box::new(stream))
    }
}

fn read_response(stream: &mut dyn ReadWrite) -> PyResult<CacheResponse> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .map_err(|e| PyRuntimeError::new_err(format!("read response len: {}", e)))?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .map_err(|e| PyRuntimeError::new_err(format!("read response body: {}", e)))?;

    bincode::deserialize(&buf)
        .map_err(|e| PyRuntimeError::new_err(format!("deserialize response: {}", e)))
}

fn send_cmd(stream: &mut dyn ReadWrite, cmd: CacheCommand) -> PyResult<()> {
    let data =
        bincode::serialize(&cmd).map_err(|e| PyRuntimeError::new_err(format!("serialize cmd: {}", e)))?;
    let len = (data.len() as u32).to_le_bytes();
    stream
        .write_all(&len)
        .and_then(|_| stream.write_all(&data))
        .and_then(|_| stream.flush())
        .map_err(|e| PyRuntimeError::new_err(format!("write cmd: {}", e)))?;

    match read_response(stream)? {
        CacheResponse::Ok => Ok(()),
        CacheResponse::Err(e) => Err(PyRuntimeError::new_err(e)),
        r => Err(PyRuntimeError::new_err(format!("unexpected response: {:?}", r))),
    }
}

fn send_cmd_expect_value(stream: &mut dyn ReadWrite, cmd: CacheCommand) -> PyResult<Option<Vec<u8>>> {
    let data =
        bincode::serialize(&cmd).map_err(|e| PyRuntimeError::new_err(format!("serialize cmd: {}", e)))?;
    let len = (data.len() as u32).to_le_bytes();
    stream
        .write_all(&len)
        .and_then(|_| stream.write_all(&data))
        .and_then(|_| stream.flush())
        .map_err(|e| PyRuntimeError::new_err(format!("write cmd: {}", e)))?;

    match read_response(stream)? {
        CacheResponse::Value(v) => Ok(Some(v)),
        CacheResponse::Nil => Ok(None),
        CacheResponse::Err(e) => Err(PyRuntimeError::new_err(e)),
        r => Err(PyRuntimeError::new_err(format!("unexpected response: {:?}", r))),
    }
}

fn send_cmd_expect_int(stream: &mut dyn ReadWrite, cmd: CacheCommand) -> PyResult<i64> {
    let data =
        bincode::serialize(&cmd).map_err(|e| PyRuntimeError::new_err(format!("serialize cmd: {}", e)))?;
    let len = (data.len() as u32).to_le_bytes();
    stream
        .write_all(&len)
        .and_then(|_| stream.write_all(&data))
        .and_then(|_| stream.flush())
        .map_err(|e| PyRuntimeError::new_err(format!("write cmd: {}", e)))?;

    match read_response(stream)? {
        CacheResponse::Int(n) => Ok(n),
        CacheResponse::Err(e) => Err(PyRuntimeError::new_err(e)),
        r => Err(PyRuntimeError::new_err(format!("unexpected response: {:?}", r))),
    }
}

fn send_cmd_expect_keys(stream: &mut dyn ReadWrite, cmd: CacheCommand) -> PyResult<Vec<String>> {
    let data =
        bincode::serialize(&cmd).map_err(|e| PyRuntimeError::new_err(format!("serialize cmd: {}", e)))?;
    let len = (data.len() as u32).to_le_bytes();
    stream
        .write_all(&len)
        .and_then(|_| stream.write_all(&data))
        .and_then(|_| stream.flush())
        .map_err(|e| PyRuntimeError::new_err(format!("write cmd: {}", e)))?;

    match read_response(stream)? {
        CacheResponse::Keys(k) => Ok(k),
        CacheResponse::Err(e) => Err(PyRuntimeError::new_err(e)),
        r => Err(PyRuntimeError::new_err(format!("unexpected response: {:?}", r))),
    }
}

/// =======================
/// TinyCache (Python API)
/// =======================

#[pyclass]
struct TinyCache {
    addr: String,
    retries: u32,
    retry_delay: Duration,
    stream: Arc<Mutex<Box<dyn ReadWrite>>>,
}

#[pymethods]
impl TinyCache {
    #[new]
    #[pyo3(signature = (addr, retries=None, retry_delay_ms=None))]
    fn new(
        addr: String,
        retries: Option<u32>,
        retry_delay_ms: Option<u64>,
    ) -> PyResult<Self> {
        let retries = retries.unwrap_or(1);
        let retry_delay = Duration::from_millis(retry_delay_ms.unwrap_or(100));
        let stream = connect(&addr)?;
        Ok(TinyCache {
            addr,
            retries,
            retry_delay,
            stream: Arc::new(Mutex::new(stream)),
        })
    }

    fn set_retries(&mut self, retries: u32) {
        self.retries = retries;
    }

    fn set_retry_delay_ms(&mut self, ms: u64) {
        self.retry_delay = Duration::from_millis(ms);
    }

    #[pyo3(signature = (key, value, ttl=None))]
    fn set(&self, _py: Python<'_>, key: &str, value: &[u8], ttl: Option<f64>) -> PyResult<()> {
        let ttl_ms = ttl.map(|s| (s * 1000.0) as i64).unwrap_or(-1);
        let cmd = CacheCommand::Set(key.to_string(), value.to_vec(), ttl_ms);
        self._with_retry_ok(cmd)
    }

    fn get(&self, _py: Python<'_>, key: &str) -> PyResult<Option<Vec<u8>>> {
        let cmd = CacheCommand::Get(key.to_string());
        self._with_retry_value(cmd)
    }

    fn pop(&self, _py: Python<'_>, key: &str) -> PyResult<Option<Vec<u8>>> {
        let cmd = CacheCommand::Pop(key.to_string());
        self._with_retry_value(cmd)
    }

    fn delete(&self, _py: Python<'_>, key: &str) -> PyResult<i64> {
        let cmd = CacheCommand::Del(key.to_string());
        self._with_retry_int(cmd)
    }

    fn keys(&self, _py: Python<'_>, pattern: &str) -> PyResult<Vec<String>> {
        let cmd = CacheCommand::Keys(pattern.to_string());
        self._with_retry_keys(cmd)
    }

    fn len(&self, _py: Python<'_>) -> PyResult<i64> {
        let cmd = CacheCommand::Len;
        self._with_retry_int(cmd)
    }
}

impl TinyCache {
    fn _reconnect(&self) -> PyResult<()> {
        let new_stream = connect(&self.addr)?;
        let mut guard = self.stream.lock().unwrap();
        *guard = new_stream;
        Ok(())
    }

    fn _with_retry_value(&self, cmd: CacheCommand) -> PyResult<Option<Vec<u8>>> {
        let attempts = self.retries.max(1);
        let mut last_err: Option<PyErr> = None;

        for attempt in 0..attempts {
            {
                let mut guard = self.stream.lock().unwrap();
                match send_cmd_expect_value(&mut **guard, cmd.clone()) {
                    Ok(v) => return Ok(v),
                    Err(e) => last_err = Some(e),
                }
            }
            if attempt + 1 < attempts {
                if let Err(e) = self._reconnect() {
                    last_err = Some(e);
                    break;
                }
                thread::sleep(self.retry_delay);
            }
        }

        Err(last_err.unwrap_or_else(|| {
            PyRuntimeError::new_err("operation failed without specific error")
        }))
    }

    fn _with_retry_int(&self, cmd: CacheCommand) -> PyResult<i64> {
        let attempts = self.retries.max(1);
        let mut last_err: Option<PyErr> = None;

        for attempt in 0..attempts {
            {
                let mut guard = self.stream.lock().unwrap();
                match send_cmd_expect_int(&mut **guard, cmd.clone()) {
                    Ok(v) => return Ok(v),
                    Err(e) => last_err = Some(e),
                }
            }
            if attempt + 1 < attempts {
                if let Err(e) = self._reconnect() {
                    last_err = Some(e);
                    break;
                }
                thread::sleep(self.retry_delay);
            }
        }

        Err(last_err.unwrap_or_else(|| {
            PyRuntimeError::new_err("operation failed without specific error")
        }))
    }

    fn _with_retry_keys(&self, cmd: CacheCommand) -> PyResult<Vec<String>> {
        let attempts = self.retries.max(1);
        let mut last_err: Option<PyErr> = None;

        for attempt in 0..attempts {
            {
                let mut guard = self.stream.lock().unwrap();
                match send_cmd_expect_keys(&mut **guard, cmd.clone()) {
                    Ok(v) => return Ok(v),
                    Err(e) => last_err = Some(e),
                }
            }
            if attempt + 1 < attempts {
                if let Err(e) = self._reconnect() {
                    last_err = Some(e);
                    break;
                }
                thread::sleep(self.retry_delay);
            }
        }

        Err(last_err.unwrap_or_else(|| {
            PyRuntimeError::new_err("operation failed without specific error")
        }))
    }

    fn _with_retry_ok(&self, cmd: CacheCommand) -> PyResult<()> {
        let attempts = self.retries.max(1);
        let mut last_err: Option<PyErr> = None;

        for attempt in 0..attempts {
            {
                let mut guard = self.stream.lock().unwrap();
                match send_cmd(&mut **guard, cmd.clone()) {
                    Ok(()) => return Ok(()),
                    Err(e) => last_err = Some(e),
                }
            }
            if attempt + 1 < attempts {
                if let Err(e) = self._reconnect() {
                    last_err = Some(e);
                    break;
                }
                thread::sleep(self.retry_delay);
            }
        }

        Err(last_err.unwrap_or_else(|| {
            PyRuntimeError::new_err("operation failed without specific error")
        }))
    }
}

#[pymodule]
fn tiny_mp_cache(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TinyCache>()?;
    m.add_function(wrap_pyfunction!(serve, m)?)?;
    #[cfg(unix)]
    m.add_function(wrap_pyfunction!(serve_unix, m)?)?;
    Ok(())
}
