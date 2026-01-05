#![allow(rust_2024_compatibility)]
#![allow(unsafe_op_in_unsafe_fn)]

mod core;
mod error;

use crate::core::CacheCore;
use crate::error::CacheError;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
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
    Set(String, Vec<u8>),
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
    Tcp(String),     // "127.0.0.1:5002"
    #[cfg(unix)]
    Unix(PathBuf),   // "/tmp/tiny-mp-cache.sock"
}

impl TransportAddr {
    /// Простейший парсер:
    /// - "tcp://host:port" -> Tcp
    /// - "unix:///path.sock" -> Unix (на Unix)
    /// - "host:port" -> Tcp
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

    // 1. устанавливаем соединение
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

    // 2. кодируем команду
    let encoded_cmd =
        bincode::serialize(&cmd).map_err(|e| CacheError::Serialization(e.to_string()))?;
    let size = (encoded_cmd.len() as u32).to_le_bytes();

    // 3. отправляем
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

    // 4. читаем ответ
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
    core: Arc<CacheCore>,
) -> Result<(), CacheError> {
    // 1) длина команды
    let mut size_buf = [0u8; 4];
    read_exact(stream, &mut size_buf)?;
    let cmd_size = u32::from_le_bytes(size_buf) as usize;
    if cmd_size > 1_000_000 {
        return Err(CacheError::Internal("command too large".into()));
    }

    // 2) команда
    let mut buf = vec![0u8; cmd_size];
    read_exact(stream, &mut buf)?;
    let cmd: CacheCommand =
        bincode::deserialize(&buf).map_err(|e| CacheError::Serialization(e.to_string()))?;

    // 3) обработка
    let resp = match cmd {
        CacheCommand::Set(key, value) => {
            core.set(key, value);
            CacheResponse::Ok
        }
        CacheCommand::Get(key) => core
            .get(&key)
            .map(CacheResponse::Value)
            .unwrap_or(CacheResponse::Nil),
        CacheCommand::Pop(key) => core
            .pop(&key)
            .map(CacheResponse::Value)
            .unwrap_or(CacheResponse::Nil),
        CacheCommand::Del(key) => CacheResponse::Int(core.delete(&key)),
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

    // 4) ответ
    let encoded =
        bincode::serialize(&resp).map_err(|e| CacheError::Serialization(e.to_string()))?;
    let size = (encoded.len() as u32).to_le_bytes();

    write_all(stream, &size)?;
    write_all(stream, &encoded)?;
    Ok(())
}

fn handle_connection(
    stream: &mut TcpStream,
    core: Arc<CacheCore>,
) -> Result<(), CacheError> {
    handle_connection_impl(stream, core)
}

#[cfg(unix)]
fn handle_connection_unix(
    stream: &mut UnixStream,
    core: Arc<CacheCore>,
) -> Result<(), CacheError> {
    handle_connection_impl(stream, core)
}

/// =======================
/// TCP-сервер
/// =======================

#[pyfunction]
fn serve(port: u16) -> PyResult<()> {
    let addr = format!("127.0.0.1:{}", port);
    println!("🚀 TinyCache TCP server: {}", addr);

    let listener = TcpListener::bind(&addr)
        .map_err(|e| PyRuntimeError::new_err(format!("Bind error: {}", e)))?;

    println!("🚀 TinyCache TCP ready: {}", addr);

    let core = Arc::new(CacheCore::new());

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
#[pyfunction]
fn serve_unix(path: String) -> PyResult<()> {
    let sock_path = PathBuf::from(path);
    if sock_path.exists() {
        fs::remove_file(&sock_path)
            .map_err(|e| PyRuntimeError::new_err(format!("Remove old socket: {}", e)))?;
    }

    println!("🚀 TinyCache UDS server: {:?}", sock_path);

    let listener = UnixListener::bind(&sock_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Bind UDS error: {}", e)))?;

    println!("🚀 TinyCache UDS ready: {:?}", sock_path);

    let core = Arc::new(CacheCore::new());

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
    /// addr:
    ///   - "127.0.0.1:5002" или "tcp://127.0.0.1:5002" для TCP
    ///   - "unix:///tmp/tiny-mp-cache.sock" для Unix-socket (на Unix)
    #[new]
    fn new(addr: String) -> Self {
        thread::sleep(Duration::from_millis(10));
        let addr = TransportAddr::parse(&addr);
        Self { addr }
    }

    fn set(&self, key: String, value: &[u8]) -> PyResult<()> {
        let v = value.to_vec();
        match send_cmd_sync(&self.addr, CacheCommand::Set(key, v)) {
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
