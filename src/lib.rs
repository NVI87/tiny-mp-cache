use crate::core::{CacheCore, ChunkId};
use crate::error::CacheError;
use crate::meta::{
    load_chunks, load_keys_meta, load_or_init_state, save_keys_meta, save_state, Paths,
    StateMeta,
};
use crate::protocol::{CacheCommand, CacheResponse, HEADER_BYTES, MAX_COMMAND_BYTES};
use crate::wal::{segment_path, Wal, WalRecord};
use pyo3::exceptions::{PyRuntimeError, PySystemExit, PyValueError};
use pyo3::prelude::*;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};

mod client;
mod core;
mod error;
mod meta;
mod protocol;
mod wal;

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

struct PersistentCore {
    paths: Paths,
    core: CacheCore,
    state: Mutex<StateMeta>,
    wal: Wal,
    /// Чанки, у которых есть сегмент WAL, но которые ещё не сохранены snapshot'ом.
    pending: Mutex<Vec<ChunkId>>,
    /// do_snapshot не должен выполняться параллельно сам с собой.
    snapshot_lock: Mutex<()>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Новый chunk_id: unix-время в мс, но строго больше предыдущего.
fn next_chunk_id(prev: ChunkId) -> ChunkId {
    now_ms().max(prev + 1)
}

impl PersistentCore {
    fn new(
        data_dir: PathBuf,
        snapshot_interval_secs: u64,
        retention_secs: u64,
    ) -> Result<Arc<Self>, CacheError> {
        let paths = Paths::new(&data_dir)?;

        // Дополнительная гарантия существования директорий перед загрузкой state
        fs::create_dir_all(&paths.meta_dir)
            .map_err(|e| CacheError::Internal(format!("ensure meta dir: {}", e)))?;
        fs::create_dir_all(&paths.wal_dir)
            .map_err(|e| CacheError::Internal(format!("ensure wal dir: {}", e)))?;
        fs::create_dir_all(&paths.chunks_dir)
            .map_err(|e| CacheError::Internal(format!("ensure chunks dir: {}", e)))?;

        let mut state = load_or_init_state(&paths, snapshot_interval_secs, retention_secs)?;
        let core = CacheCore::new();

        // 1. мета + значения из сохранённых чанков. Берём все chunk-файлы на диске,
        // а не только из state.json: snapshot мог упасть между сохранением
        // keys.bin и state.json, а файлы удаляются последними
        let mut live = state.live_chunks.clone();
        live.extend(paths.list_chunk_files()?);
        live.sort_unstable();
        live.dedup();
        state.live_chunks = live;

        load_keys_meta(&paths, &core)?;
        core.bump_next_key_id();
        load_chunks(&paths, &core, &state.live_chunks)?;
        let orphans = core.drop_meta_without_values();
        if orphans > 0 {
            eprintln!("TinyCache: dropped {} keys without values in chunks", orphans);
        }

        // старый формат (единый wal/log.bin) -> сегмент текущего чанка
        let legacy_wal = paths.wal_log();
        if legacy_wal.exists() {
            fs::rename(&legacy_wal, segment_path(&paths.wal_dir, state.current_chunk_id))
                .map_err(|e| CacheError::Internal(format!("migrate legacy WAL: {}", e)))?;
        }

        // 2. доигрываем все сегменты WAL, ещё не свёрнутые в чанки
        let segments = Wal::list_segments(&paths.wal_dir)?;
        for &id in &segments {
            Wal::replay_segment(&paths.wal_dir, id, &core)?;
        }

        // 3. новые записи идут в свежий чанк; доигранные сегменты сохранит ближайший snapshot
        let max_known = segments
            .iter()
            .chain(state.live_chunks.iter())
            .copied()
            .chain(std::iter::once(state.current_chunk_id))
            .max()
            .unwrap_or(0);
        let current = next_chunk_id(max_known);
        state.current_chunk_id = current;
        let wal = Wal::open(paths.wal_dir.clone(), current)?;

        let mut keep = state.live_chunks.clone();
        keep.extend(&segments);
        core.gc(&keep, current);
        save_state(&paths, &state)?;

        let this = Arc::new(PersistentCore {
            paths,
            core,
            state: Mutex::new(state),
            wal,
            pending: Mutex::new(segments),
            snapshot_lock: Mutex::new(()),
        });

        Self::spawn_snapshot_thread(&this);

        Ok(this)
    }

    fn spawn_snapshot_thread(this: &Arc<Self>) {
        let this_clone = Arc::clone(this);
        thread::spawn(move || loop {
            // snapshot'ы по строгой временной сетке: на границах, кратных интервалу
            let interval_ms = {
                let st = this_clone.state.lock().expect("state mutex poisoned");
                st.snapshot_interval_secs * 1000
            };
            let now = now_ms();
            let boundary = (now / interval_ms + 1) * interval_ms;
            thread::sleep(Duration::from_millis(boundary - now));

            if let Err(e) = this_clone.do_snapshot(boundary) {
                eprintln!("snapshot error: {:?}", e);
            }
        });
    }

    /// Snapshot без потери записей, идущих параллельно:
    /// 1) под блокировкой WAL переключаем запись на новый чанк (rotate);
    /// 2) retention: живые ключи чанков старше retention_secs переносим в сохраняемый чанк;
    /// 3) сохраняем чанки, keys.bin и state.json;
    /// 4) только после этого удаляем устаревшие chunk-файлы и свёрнутые сегменты WAL.
    /// Падение на любом шаге безопасно: при старте читаются все chunk-файлы и сегменты.
    fn do_snapshot(&self, at_ms: u64) -> Result<(), CacheError> {
        let _snap = self
            .snapshot_lock
            .lock()
            .map_err(|_| CacheError::Internal("snapshot mutex poisoned".into()))?;

        let mut state = self
            .state
            .lock()
            .map_err(|_| CacheError::Internal("state mutex poisoned".into()))?
            .clone();

        let new_chunk_id = at_ms.max(state.current_chunk_id + 1);
        let old_chunk_id = self.wal.rotate(new_chunk_id)?;
        state.current_chunk_id = new_chunk_id;
        {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| CacheError::Internal("state mutex poisoned".into()))?;
            guard.current_chunk_id = new_chunk_id;
        }

        let to_flush = {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| CacheError::Internal("pending mutex poisoned".into()))?;
            pending.push(old_chunk_id);
            pending.clone()
        };

        state.live_chunks.extend(&to_flush);
        state.live_chunks.sort_unstable();
        state.live_chunks.dedup();

        // retention: chunk_id — время начала чанка (мс); прежний чанк сам не устаревает,
        // т.к. retention_secs >= snapshot_interval_secs
        let cutoff = new_chunk_id.saturating_sub(state.retention_secs * 1000);
        let expired: HashSet<ChunkId> = state
            .live_chunks
            .iter()
            .copied()
            .filter(|&id| id < cutoff && id != old_chunk_id)
            .collect();
        if !expired.is_empty() {
            self.core.reassign_chunks(&expired, old_chunk_id);
            state.live_chunks.retain(|id| !expired.contains(id));
        }

        self.core.gc(&state.live_chunks, new_chunk_id);

        // прежний чанк пишем всегда: в него могли перенести ключи устаревших
        for &id in &to_flush {
            if !expired.contains(&id) {
                self.write_chunk(id)?;
            }
        }

        save_keys_meta(&self.paths, &self.core)?;
        save_state(&self.paths, &state)?;

        {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| CacheError::Internal("state mutex poisoned".into()))?;
            guard.live_chunks = state.live_chunks.clone();
        }

        // всё нужное уже на диске: можно удалять устаревшие чанки и свёрнутые сегменты
        for &id in &expired {
            let p = self.paths.chunk_file(id);
            if p.exists() {
                let _ = fs::remove_file(&p);
            }
        }
        for &id in &to_flush {
            self.wal.remove_segment(id)?;
        }
        self.pending
            .lock()
            .map_err(|_| CacheError::Internal("pending mutex poisoned".into()))?
            .retain(|id| !to_flush.contains(id));

        Ok(())
    }

    /// Записать значения ключей чанка в chunks/chunk_<id>.bin (через tmp + rename).
    fn write_chunk(&self, chunk_id: ChunkId) -> Result<(), CacheError> {
        let live = self.core.export_live_for_chunk(chunk_id);
        let chunk_path = self.paths.chunk_file(chunk_id);

        // Гарантируем существование директории chunks
        if let Some(parent) = chunk_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| CacheError::Internal(format!("ensure chunks dir: {}", e)))?;
        }

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
            f.write_all(&chunk_id.to_le_bytes())
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
        Ok(())
    }

    fn set(
        &self,
        key: String,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> Result<(), CacheError> {
        let ttl_ms = ttl.map(|d| d.as_millis() as i64).unwrap_or(-1);
        let rec = WalRecord::Set {
            key: key.clone(),
            value: value.clone(),
            ttl_ms,
        };
        self.wal
            .write(&rec, |chunk_id| self.core.set(key, value, ttl, chunk_id))
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        Ok(self.core.get(key))
    }

    fn pop(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let rec = WalRecord::Pop {
            key: key.to_string(),
        };
        self.wal.write(&rec, |_| self.core.pop(key))
    }

    fn delete(&self, key: &str) -> Result<i64, CacheError> {
        let rec = WalRecord::Del {
            key: key.to_string(),
        };
        self.wal.write(&rec, |_| self.core.delete(key))
    }

    fn keys_prefix(&self, prefix: &str) -> Result<Vec<String>, CacheError> {
        Ok(self.core.keys_prefix(prefix))
    }

    fn len(&self) -> Result<i64, CacheError> {
        Ok(self.core.len())
    }
}


/// Выполнить команду. Вызывается параллельно из потоков соединений:
/// изменения упорядочены блокировкой WAL, чтение идёт без блокировок.
fn execute(core: &PersistentCore, cmd: CacheCommand) -> CacheResponse {
    fn or_err<T>(r: Result<T, CacheError>, ok: impl FnOnce(T) -> CacheResponse) -> CacheResponse {
        match r {
            Ok(v) => ok(v),
            Err(e) => CacheResponse::Err(e.to_string()),
        }
    }
    fn value(v: Option<Vec<u8>>) -> CacheResponse {
        v.map(CacheResponse::Value).unwrap_or(CacheResponse::Nil)
    }

    match cmd {
        CacheCommand::Set(key, val, ttl_ms) => {
            let ttl = if ttl_ms < 0 {
                None
            } else {
                Some(Duration::from_millis(ttl_ms as u64))
            };
            or_err(core.set(key, val, ttl), |()| CacheResponse::Ok)
        }
        CacheCommand::Get(key) => or_err(core.get(&key), value),
        CacheCommand::Pop(key) => or_err(core.pop(&key), value),
        CacheCommand::Del(key) => or_err(core.delete(&key), CacheResponse::Int),
        CacheCommand::Keys(pattern) => match pattern.strip_suffix('*') {
            Some(prefix) => or_err(core.keys_prefix(prefix), CacheResponse::Keys),
            None => CacheResponse::Keys(Vec::new()),
        },
        CacheCommand::Len => or_err(core.len(), CacheResponse::Int),
    }
}

fn handle_connection_impl<S: Read + Write>(
    stream: &mut S,
    core: Arc<PersistentCore>,
) -> Result<(), CacheError> {
    loop {
        let mut header = [0u8; HEADER_BYTES];
        match stream.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(());
            }
            Err(e) => {
                return Err(CacheError::Network(format!("read cmd len: {}", e)));
            }
        }
        let len = protocol::body_len(header);
        if len > MAX_COMMAND_BYTES {
            // тело не читаем: сообщаем об ошибке и закрываем соединение
            let msg = format!("command too large: {} bytes > {}", len, MAX_COMMAND_BYTES);
            if let Ok(resp) = protocol::encode_response(&CacheResponse::Err(msg.clone())) {
                let _ = write_all(stream, &resp);
            }
            return Err(CacheError::Network(msg));
        }
        let mut buf = vec![0u8; len];
        read_exact(stream, &mut buf)?;

        let cmd = protocol::decode_command(&buf)?;
        let resp = execute(&core, cmd);
        write_all(stream, &protocol::encode_response(&resp)?)?;
    }
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

/// snapshot_interval_secs > 0, retention_secs >= snapshot_interval_secs
/// (иначе чанк устаревал бы раньше, чем успевает сохраниться).
fn check_snapshot_config(snapshot_interval_secs: u64, retention_secs: u64) -> PyResult<()> {
    if snapshot_interval_secs == 0 {
        return Err(PyValueError::new_err("snapshot_interval_secs must be > 0"));
    }
    if retention_secs < snapshot_interval_secs {
        return Err(PyValueError::new_err(
            "retention_secs must be >= snapshot_interval_secs",
        ));
    }
    Ok(())
}

/// Обработчик SIGTERM: SystemExit(0) — serve() делает финальный snapshot и процесс завершается чисто.
#[pyfunction]
fn graceful_exit(_signum: i32, _frame: Py<PyAny>) -> PyResult<()> {
    Err(PySystemExit::new_err(0))
}

/// Ставит graceful_exit на SIGTERM, если у процесса нет своего обработчика
/// (и мы в главном потоке). Возвращает прежний обработчик для восстановления.
fn install_sigterm_handler(py: Python<'_>) -> Option<Py<PyAny>> {
    let signal = py.import("signal").ok()?;
    let sigterm = signal.getattr("SIGTERM").ok()?;
    let prev = signal.call_method1("getsignal", (sigterm.clone(),)).ok()?;
    let dfl = signal.getattr("SIG_DFL").ok()?;
    if !prev.eq(&dfl).unwrap_or(false) {
        return None;
    }
    let handler = wrap_pyfunction!(graceful_exit, py).ok()?;
    signal.call_method1("signal", (sigterm, handler)).ok()?;
    Some(prev.unbind())
}

fn restore_sigterm_handler(py: Python<'_>, prev: Option<Py<PyAny>>) {
    let Some(prev) = prev else { return };
    if let Ok(signal) = py.import("signal") {
        if let Ok(sigterm) = signal.getattr("SIGTERM") {
            let _ = signal.call_method1("signal", (sigterm, prev));
        }
    }
}

/// Соединения принимает фоновый поток `accept_loop`, а главный поток отпускает GIL
/// и раз в 100 мс проверяет сигналы Python. По сигналу (Ctrl+C, SIGTERM) делается
/// финальный snapshot, и исключение сигнала возвращается в Python.
fn run_until_signal(
    py: Python<'_>,
    persistent: Arc<PersistentCore>,
    accept_loop: impl FnOnce() + Send + 'static,
) -> PyResult<()> {
    let stopped = Arc::new(AtomicBool::new(false));
    {
        let stopped = Arc::clone(&stopped);
        thread::spawn(move || {
            accept_loop();
            stopped.store(true, Ordering::SeqCst);
        });
    }

    let prev_handler = install_sigterm_handler(py);
    let result = loop {
        py.detach(|| thread::sleep(Duration::from_millis(100)));
        if let Err(e) = py.check_signals() {
            break Err(e);
        }
        if stopped.load(Ordering::SeqCst) {
            break Ok(());
        }
    };

    if let Err(e) = py.detach(|| persistent.do_snapshot(now_ms())) {
        eprintln!("final snapshot error: {:?}", e);
    }
    restore_sigterm_handler(py, prev_handler);
    result
}

#[pyfunction(signature = (data_dir, port=5002, snapshot_interval_secs=60, retention_secs=600))]
fn serve(
    py: Python<'_>,
    data_dir: String,
    port: u16,
    snapshot_interval_secs: u64,
    retention_secs: u64,
) -> PyResult<()> {
    check_snapshot_config(snapshot_interval_secs, retention_secs)?;
    let addr = format!("127.0.0.1:{}", port);
    println!("TinyCache TCP server: {}", addr);

    let persistent = PersistentCore::new(
        PathBuf::from(&data_dir),
        snapshot_interval_secs,
        retention_secs,
    )
    .map_err(|e| PyRuntimeError::new_err(format!("init persistent core: {}", e)))?;

    let server_core = Arc::clone(&persistent);

    let listener = TcpListener::bind(&addr)
        .map_err(|e| PyRuntimeError::new_err(format!("Bind error: {}", e)))?;

    println!("TinyCache TCP ready: {}", addr);

    run_until_signal(py, persistent, move || {
        for stream_res in listener.incoming() {
            match stream_res {
                Ok(mut stream) => {
                    stream.set_nodelay(true).ok();
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
    })
}

#[cfg(unix)]
#[pyfunction(signature = (data_dir, snapshot_interval_secs=60, retention_secs=600))]
fn serve_unix(
    py: Python<'_>,
    data_dir: String,
    snapshot_interval_secs: u64,
    retention_secs: u64,
) -> PyResult<()> {
    check_snapshot_config(snapshot_interval_secs, retention_secs)?;
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
        retention_secs,
    )
    .map_err(|e| PyRuntimeError::new_err(format!("init persistent core: {}", e)))?;

    let server_core = Arc::clone(&persistent);

    let listener = UnixListener::bind(&sock_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Bind UDS error: {}", e)))?;

    println!("TinyCache UDS ready: {:?}", sock_path);

    run_until_signal(py, persistent, move || {
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
    })
}

#[pymodule]
fn tiny_mp_cache(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<client::TinyCache>()?;
    m.add_function(wrap_pyfunction!(serve, m)?)?;
    #[cfg(unix)]
    m.add_function(wrap_pyfunction!(serve_unix, m)?)?;
    Ok(())
}
