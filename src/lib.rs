// src/lib.rs
use crate::core::CacheCore;
use crate::error::CacheError;
use crate::meta::{load_keys_meta, load_or_init_state, save_keys_meta, save_state, Paths, StateMeta};
use crate::wal::Wal;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};

mod core;
mod error;
mod meta;
mod wal;

#[derive(Serialize, Deserialize, Debug)]
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
    wal: Wal,
}

impl PersistentCore {
    fn new(
        data_dir: PathBuf,
        snapshot_interval_secs: u64,
        retention_chunks: u64,
    ) -> Result<Self, CacheError> {
        let paths = Paths::new(&data_dir)?;
        let mut state = load_or_init_state(&paths, snapshot_interval_secs, retention_chunks)?;
        let core = CacheCore::new();

        // Загрузка меты
        load_keys_meta(&paths, &core)?;

        // Загрузка WAL
        let wal = Wal::open(paths.wal_log())?;
        wal.replay(&core, state.current_chunk_id)?;

        // GC по live_chunks (если есть)
        if !state.live_chunks.is_empty() {
            core.gc(&state.live_chunks);
        }

        // Старт snapshot-треда
        let core_clone = core.clone();
        let paths_clone = paths.clone();
        let state_arc = Arc::new(Mutex::new(state.clone()));
        let state_for_thread = state_arc.clone();
        thread::spawn(move || {
            let interval = Duration::from_secs(state.snapshot_interval_secs);
            loop {
                thread::sleep(interval);
                if let Err(e) =
                    do_snapshot(&paths_clone, &core_clone, &state_for_thread, &wal, interval)
                {
                    eprintln!("snapshot error: {:?}", e);
                }
            }
        });

        Ok(Self {
            paths,
            core,
            state: Mutex::new(state),
            wal,
        })
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

        self.wal.append(&crate::wal::WalRecord::Set {
            key: key.clone(),
            value: value.clone(),
            ttl_ms,
        })?;
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

/// Snapshot + retention.
/// Внутри берёт state-мьютекс, но сам core использует свои структуры без глобального стоп-мира.
fn do_snapshot(
    paths: &Paths,
    core: &CacheCore,
    state_arc: &Arc<Mutex<StateMeta>>,
    wal: &Wal,
    interval: Duration,
) -> Result<(), CacheError> {
    let now = Instant::now();

    // 1. Читаем state
    let mut state = state_arc
        .lock()
        .map_err(|_| CacheError::Internal("state mutex poisoned".into()))?
        .clone();

    // 2. Определяем live_chunks по retention
    if !state.live_chunks.is_empty() {
        if state.live_chunks.len() as u64 > state.retention_chunks {
            let keep = state.retention_chunks as usize;
            let to_drop = state.live_chunks.len() - keep;
            let old = state.live_chunks.drain(0..to_drop).collect::<Vec<_>>();
            for id in old {
                let p = paths.chunk_file(id);
                let _ = fs::remove_file(&p);
            }
        }
    }

    // 3. GC по TTL и live_chunks
    core.gc(&state.live_chunks);

    // 4. Записываем текущий чанк (по current_chunk_id)
    let current_chunk_id = state.current_chunk_id;
    let live = core.export_live_for_chunk(current_chunk_id);
    {
        let chunk_path = paths.chunk_file(current_chunk_id);
        let tmp = chunk_path.with_extension("tmp");
        let mut f = fs::File::create(&tmp)
            .map_err(|e| CacheError::Internal(format!("create chunk tmp: {}", e)))?;
        for (key_id, value, _ttl) in live {
            let key_id_bytes = key_id.to_le_bytes();
            let len = (value.len() as u32).to_le_bytes();
            f.write_all(&key_id_bytes)
                .and_then(|_| f.write_all(&len))
                .and_then(|_| f.write_all(&value))
                .map_err(|e| CacheError::Internal(format!("write chunk: {}", e)))?;
        }
        f.flush()
            .map_err(|e| CacheError::Internal(format!("flush chunk: {}", e)))?;
        fs::rename(&tmp, &chunk_path)
            .map_err(|e| CacheError::Internal(format!("rename chunk: {}", e)))?;
    }

    // 5. Обновляем список live_chunks
    if !state.live_chunks.contains(&current_chunk_id) {
        state.live_chunks.push(current_chunk_id);
    }

    // 6. Дамп меты
    save_keys_meta(paths, core, &state)?;

    // 7. Обновляем state.json и меняем current_chunk_id
    let new_chunk_id = (now.elapsed() + interval).as_millis() as u64 + state.current_chunk_id;
    state.current_chunk_id = new_chunk_id;
    save_state(paths, &state)?;

    {
        let mut guard = state_arc
            .lock()
            .map_err(|_| CacheError::Internal("state mutex poisoned".into()))?;
        *guard = state;
    }

    // 8. Обнуляем WAL
    wal.reset()?;

    Ok(())
}

/// =======================
/// ServerCore: очередь команд
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
            while let Ok(WorkerRequest(cmd, resp_tx)) = rx.recv() {
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
            .send(WorkerRequest(cmd, resp_tx))
            .map_err(|e| CacheError::Internal(format!("worker send: {}", e)))?;
        resp_rx
            .recv()
            .map_err(|e| CacheError::Internal(format!("worker recv: {}", e)))
    }
}

/// =======================
/// Протокол обработки соединения
/// =======================

fn handle_connection_impl<S: Read + Write>(
    stream: &mut S,
    server: Arc<ServerCore>,
) -> Result<(), CacheError> {
    loop {
        let mut len_buf = [0u8; 4];
        if let Err(e) = stream.read_exact(&mut len_buf) {
            // клиент закрыл соединение
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(());
            }
            return Err(CacheError::Network(format!("read cmd len: {}", e)));
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

    let persistent = Arc::new(
        PersistentCore::new(
            PathBuf::from(&data_dir),
            snapshot_interval_secs,
            retention_chunks,
        )
        .map_err(|e| PyRuntimeError::new_err(format!("init persistent core: {}", e)))?,
    );

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

    let persistent = Arc::new(
        PersistentCore::new(
            PathBuf::from(&data_dir),
            snapshot_interval_secs,
            retention_chunks,
        )
        .map_err(|e| PyRuntimeError::new_err(format!("init persistent core: {}", e)))?,
    );

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
/// Python-обёртки
/// =======================

#[pyclass]
struct TinyCache {
    addr: String,
}

#[pymethods]
impl TinyCache {
    #[new]
    fn new(addr: String) -> Self {
        TinyCache { addr }
    }

    fn set(&self, py: Python<'_>, key: &str, value: &[u8], ttl: Option<f64>) -> PyResult<()> {
        let ttl_ms = ttl
            .map(|s| (s * 1000.0) as i64)
            .unwrap_or(-1);
        let mut stream = crate::tiny_mp_cache::connect(&self.addr)
            .map_err(|e| PyRuntimeError::new_err(format!("connect: {}", e)))?;
        let cmd = CacheCommand::Set(key.to_string(), value.to_vec(), ttl_ms);
        crate::tiny_mp_cache::send_cmd(py, &mut stream, cmd)?;
        Ok(())
    }

    fn get(&self, py: Python<'_>, key: &str) -> PyResult<Option<Vec<u8>>> {
        let mut stream = crate::tiny_mp_cache::connect(&self.addr)
            .map_err(|e| PyRuntimeError::new_err(format!("connect: {}", e)))?;
        let cmd = CacheCommand::Get(key.to_string());
        crate::tiny_mp_cache::send_cmd_expect_value(py, &mut stream, cmd)
    }

    fn pop(&self, py: Python<'_>, key: &str) -> PyResult<Option<Vec<u8>>> {
        let mut stream = crate::tiny_mp_cache::connect(&self.addr)
            .map_err(|e| PyRuntimeError::new_err(format!("connect: {}", e)))?;
        let cmd = CacheCommand::Pop(key.to_string());
        crate::tiny_mp_cache::send_cmd_expect_value(py, &mut stream, cmd)
    }

    fn delete(&self, py: Python<'_>, key: &str) -> PyResult<i64> {
        let mut stream = crate::tiny_mp_cache::connect(&self.addr)
            .map_err(|e| PyRuntimeError::new_err(format!("connect: {}", e)))?;
        let cmd = CacheCommand::Del(key.to_string());
        crate::tiny_mp_cache::send_cmd_expect_int(py, &mut stream, cmd)
    }

    fn keys(&self, py: Python<'_>, pattern: &str) -> PyResult<Vec<String>> {
        let mut stream = crate::tiny_mp_cache::connect(&self.addr)
            .map_err(|e| PyRuntimeError::new_err(format!("connect: {}", e)))?;
        let cmd = CacheCommand::Keys(pattern.to_string());
        crate::tiny_mp_cache::send_cmd_expect_keys(py, &mut stream, cmd)
    }

    fn len(&self, py: Python<'_>) -> PyResult<i64> {
        let mut stream = crate::tiny_mp_cache::connect(&self.addr)
            .map_err(|e| PyRuntimeError::new_err(format!("connect: {}", e)))?;
        let cmd = CacheCommand::Len;
        crate::tiny_mp_cache::send_cmd_expect_int(py, &mut stream, cmd)
    }
}

#[pymodule]
fn tiny_mp_cache(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<TinyCache>()?;
    m.add_function(wrap_pyfunction!(serve, m)?)?;
    #[cfg(unix)]
    m.add_function(wrap_pyfunction!(serve_unix, m)?)?;
    Ok(())
}
