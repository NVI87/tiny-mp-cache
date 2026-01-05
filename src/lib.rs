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
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bincode;

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
/// Маппинг ошибок в Python
/// =======================

fn map_error(e: CacheError, ctx: &str) -> PyErr {
    PyRuntimeError::new_err(format!("{}: {}", ctx, e))
}

/// =======================
/// Клиентский TCP-транспорт
/// =======================

fn send_cmd_sync(addr: &str, cmd: CacheCommand) -> Result<CacheResponse, CacheError> {
    let mut stream =
        TcpStream::connect(addr).map_err(|e| CacheError::Network(e.to_string()))?;

    // Для локального кэша можно обойтись без жёстких таймаутов.
    // stream.set_read_timeout(None).ok();
    // stream.set_write_timeout(None).ok();

    let encoded_cmd =
        bincode::serialize(&cmd).map_err(|e| CacheError::Serialization(e.to_string()))?;
    let size = (encoded_cmd.len() as u32).to_le_bytes();

    stream
        .write_all(&size)
        .and_then(|_| stream.write_all(&encoded_cmd))
        .and_then(|_| stream.flush())
        .map_err(|e| CacheError::Network(e.to_string()))?;

    let mut size_buf = [0u8; 4];
    stream
        .read_exact(&mut size_buf)
        .map_err(|e| CacheError::Network(e.to_string()))?;
    let resp_size = u32::from_le_bytes(size_buf) as usize;

    let mut buf = vec![0u8; resp_size];
    stream
        .read_exact(&mut buf)
        .map_err(|e| CacheError::Network(e.to_string()))?;

    bincode::deserialize(&buf).map_err(|e| CacheError::Serialization(e.to_string()))
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
                        eprintln!("Connection error: {:?}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Listener error: {}", e);
                break;
            }
        }
    }

    Ok(())
}

fn handle_connection(
    stream: &mut TcpStream,
    core: Arc<CacheCore>,
) -> Result<(), CacheError> {
    // stream.set_read_timeout(None).ok();
    // stream.set_write_timeout(None).ok();

    // 1) читаем длину команды
    let mut size_buf = [0u8; 4];
    stream
        .read_exact(&mut size_buf)
        .map_err(|e| CacheError::Network(e.to_string()))?;
    let cmd_size = u32::from_le_bytes(size_buf) as usize;
    if cmd_size > 1_000_000 {
        return Err(CacheError::Internal("command too large".into()));
    }

    // 2) читаем команду
    let mut buf = vec![0u8; cmd_size];
    stream
        .read_exact(&mut buf)
        .map_err(|e| CacheError::Network(e.to_string()))?;
    let cmd: CacheCommand =
        bincode::deserialize(&buf).map_err(|e| CacheError::Serialization(e.to_string()))?;

    // 3) обрабатываем
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

    // 4) отправляем ответ
    let encoded =
        bincode::serialize(&resp).map_err(|e| CacheError::Serialization(e.to_string()))?;
    let size = (encoded.len() as u32).to_le_bytes();

    stream
        .write_all(&size)
        .and_then(|_| stream.write_all(&encoded))
        .and_then(|_| stream.flush())
        .map_err(|e| CacheError::Network(e.to_string()))?;

    Ok(())
}

/// =======================
/// Python-клиент TinyCache
/// =======================

#[pyclass]
#[derive(Clone)]
pub struct TinyCache {
    addr: String, // "host:port"
}

#[pymethods]
impl TinyCache {
    #[new]
    fn new(addr: String) -> Self {
        // маленький sleep для fork-сценариев
        thread::sleep(Duration::from_millis(10));
        Self { addr }
    }

    /// set(key: str, value: bytes) -> None
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

    /// get(key: str) -> Optional[bytes]
    fn get<'py>(
        &self,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        match send_cmd_sync(&self.addr, CacheCommand::Get(key)) {
            Ok(CacheResponse::Value(v)) => {
                let b = PyBytes::new_bound(py, &v); // актуальный API [web:40][web:66]
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

    /// pop(key: str) -> Optional[bytes]
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

    /// delete(key: str) -> int
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

    /// keys(pattern: str) -> list[str]
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

    /// len(self) -> int
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
    Ok(())
}
