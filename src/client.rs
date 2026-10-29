//! Синхронный Python-клиент TinyCache.
//!
//! Сетевой обмен идёт без GIL: пока один поток ждёт ответа сервера, остальные
//! Python-потоки процесса работают. Кодирование команд и ответов — в protocol.rs,
//! общем с сервером и будущим асинхронным клиентом.

use crate::error::CacheError;
use crate::protocol::{self, CacheCommand, CacheResponse, HEADER_BYTES};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::net::UnixStream;

trait ReadWrite: Read + Write + Send {}
impl<T: Read + Write + Send> ReadWrite for T {}

fn connect(addr: &str) -> Result<Box<dyn ReadWrite>, CacheError> {
    if let Some(path) = addr.strip_prefix("unix://") {
        #[cfg(unix)]
        {
            let stream = UnixStream::connect(path)
                .map_err(|e| CacheError::Network(format!("connect unix: {}", e)))?;
            Ok(Box::new(stream))
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            Err(CacheError::Network("Unix sockets not supported".into()))
        }
    } else {
        let addr = addr.strip_prefix("tcp://").unwrap_or(addr);
        let stream = TcpStream::connect(addr)
            .map_err(|e| CacheError::Network(format!("connect tcp: {}", e)))?;
        stream.set_nodelay(true).ok();
        Ok(Box::new(stream))
    }
}

/// Отправить готовый кадр запроса и прочитать ответ.
fn roundtrip(stream: &mut dyn ReadWrite, request: &[u8]) -> Result<CacheResponse, CacheError> {
    stream
        .write_all(request)
        .and_then(|_| stream.flush())
        .map_err(|e| CacheError::Network(format!("write request: {}", e)))?;

    let mut header = [0u8; HEADER_BYTES];
    stream
        .read_exact(&mut header)
        .map_err(|e| CacheError::Network(format!("read response len: {}", e)))?;
    let mut body = vec![0u8; protocol::body_len(header)];
    stream
        .read_exact(&mut body)
        .map_err(|e| CacheError::Network(format!("read response body: {}", e)))?;

    protocol::decode_response(&body)
}

fn to_py_err(e: CacheError) -> PyErr {
    match e {
        CacheError::TooLarge(msg) => PyValueError::new_err(msg),
        other => PyRuntimeError::new_err(other.to_string()),
    }
}

fn unexpected(resp: CacheResponse) -> PyErr {
    PyRuntimeError::new_err(format!("unexpected response: {:?}", resp))
}

/// Соединение и pid процесса, который его открыл (для переподключения после fork).
struct Conn {
    stream: Box<dyn ReadWrite>,
    pid: u32,
}

/// Клиент кэша с одним постоянным соединением.
///
/// Потокобезопасен: запросы из разных потоков к одному экземпляру выполняются
/// по очереди; для параллельных запросов — свой экземпляр на поток.
/// Экземпляр, унаследованный через fork, при первом запросе переподключается.
#[pyclass]
pub struct TinyCache {
    addr: String,
    retries: AtomicU32,
    retry_delay_ms: AtomicU64,
    conn: Mutex<Conn>,
}

impl TinyCache {
    /// Кодирование под GIL, сетевой обмен — без GIL.
    fn call(&self, py: Python<'_>, cmd: &CacheCommand) -> PyResult<CacheResponse> {
        let request = protocol::encode_command(cmd).map_err(to_py_err)?;
        match py
            .allow_threads(|| self.call_blocking(&request))
            .map_err(to_py_err)?
        {
            CacheResponse::Err(e) => Err(PyRuntimeError::new_err(e)),
            resp => Ok(resp),
        }
    }

    /// До `retries` попыток; перед повтором — пауза и переподключение.
    /// Повторяются только сетевые ошибки, ошибка от сервера возвращается сразу.
    fn call_blocking(&self, request: &[u8]) -> Result<CacheResponse, CacheError> {
        let attempts = self.retries.load(Ordering::Relaxed).max(1);
        let delay = Duration::from_millis(self.retry_delay_ms.load(Ordering::Relaxed));
        let mut conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let mut last_err = None;

        for attempt in 0..attempts {
            if attempt > 0 {
                thread::sleep(delay);
            }
            if attempt > 0 || conn.pid != std::process::id() {
                match connect(&self.addr) {
                    Ok(stream) => {
                        conn.stream = stream;
                        conn.pid = std::process::id();
                    }
                    Err(e) => {
                        last_err = Some(e);
                        continue;
                    }
                }
            }
            match roundtrip(&mut *conn.stream, request) {
                Ok(resp) => return Ok(resp),
                Err(e) => last_err = Some(e),
            }
        }

        Err(last_err.unwrap_or_else(|| CacheError::Network("no attempts made".into())))
    }
}

#[pymethods]
impl TinyCache {
    #[new]
    #[pyo3(signature = (addr, retries=None, retry_delay_ms=None))]
    fn new(
        py: Python<'_>,
        addr: String,
        retries: Option<u32>,
        retry_delay_ms: Option<u64>,
    ) -> PyResult<Self> {
        let stream = py.allow_threads(|| connect(&addr)).map_err(to_py_err)?;
        Ok(TinyCache {
            addr,
            retries: AtomicU32::new(retries.unwrap_or(1)),
            retry_delay_ms: AtomicU64::new(retry_delay_ms.unwrap_or(100)),
            conn: Mutex::new(Conn {
                stream,
                pid: std::process::id(),
            }),
        })
    }

    fn set_retries(&self, retries: u32) {
        self.retries.store(retries, Ordering::Relaxed);
    }

    fn set_retry_delay_ms(&self, ms: u64) {
        self.retry_delay_ms.store(ms, Ordering::Relaxed);
    }

    #[pyo3(signature = (key, value, ttl=None))]
    fn set(&self, py: Python<'_>, key: &str, value: &[u8], ttl: Option<f64>) -> PyResult<()> {
        let ttl_ms = match ttl {
            None => -1,
            Some(s) if s.is_finite() && s >= 0.0 => (s * 1000.0) as i64,
            Some(_) => {
                return Err(PyValueError::new_err(
                    "ttl must be a non-negative number of seconds",
                ))
            }
        };
        match self.call(py, &CacheCommand::Set(key.to_string(), value.to_vec(), ttl_ms))? {
            CacheResponse::Ok => Ok(()),
            r => Err(unexpected(r)),
        }
    }

    fn get<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Option<Bound<'py, PyBytes>>> {
        match self.call(py, &CacheCommand::Get(key.to_string()))? {
            CacheResponse::Value(v) => Ok(Some(PyBytes::new_bound(py, &v))),
            CacheResponse::Nil => Ok(None),
            r => Err(unexpected(r)),
        }
    }

    fn pop<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Option<Bound<'py, PyBytes>>> {
        match self.call(py, &CacheCommand::Pop(key.to_string()))? {
            CacheResponse::Value(v) => Ok(Some(PyBytes::new_bound(py, &v))),
            CacheResponse::Nil => Ok(None),
            r => Err(unexpected(r)),
        }
    }

    fn delete(&self, py: Python<'_>, key: &str) -> PyResult<i64> {
        match self.call(py, &CacheCommand::Del(key.to_string()))? {
            CacheResponse::Int(n) => Ok(n),
            r => Err(unexpected(r)),
        }
    }

    fn keys(&self, py: Python<'_>, pattern: &str) -> PyResult<Vec<String>> {
        match self.call(py, &CacheCommand::Keys(pattern.to_string()))? {
            CacheResponse::Keys(k) => Ok(k),
            r => Err(unexpected(r)),
        }
    }

    fn len(&self, py: Python<'_>) -> PyResult<i64> {
        match self.call(py, &CacheCommand::Len)? {
            CacheResponse::Int(n) => Ok(n),
            r => Err(unexpected(r)),
        }
    }
}
