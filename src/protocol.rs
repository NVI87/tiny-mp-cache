//! Протокол клиент ↔ сервер: команды, ответы и кадрирование.
//!
//! Модуль не делает ввода-вывода: кодирование и декодирование кадров общие
//! для сервера, синхронного клиента и (в будущем) асинхронного клиента.
//! Кадр: длина тела (u32 LE) + тело (bincode).

use crate::error::CacheError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum CacheCommand {
    /// ключ, значение, TTL в мс (-1 — без TTL)
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
    Int(i64),
    Keys(Vec<String>),
    Nil,
    Err(String),
}

/// Максимальный размер тела команды (ключ + значение + служебные байты), как в 0.1.
/// Защищает сервер от выделения памяти под произвольную длину из заголовка кадра.
pub const MAX_COMMAND_BYTES: usize = 1 << 20;

/// Размер заголовка кадра.
pub const HEADER_BYTES: usize = 4;

/// Длина тела из заголовка кадра.
pub fn body_len(header: [u8; HEADER_BYTES]) -> usize {
    u32::from_le_bytes(header) as usize
}

/// Кадр: заголовок + тело одним буфером. Отправляется одним write,
/// иначе Nagle + delayed ACK дают ~40 мс задержки на каждый запрос (Linux).
pub fn frame(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_BYTES + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(body);
    out
}

/// Команда -> готовый к отправке кадр; команда больше лимита отвергается до отправки.
pub fn encode_command(cmd: &CacheCommand) -> Result<Vec<u8>, CacheError> {
    let body = bincode::serialize(cmd).map_err(|e| CacheError::Serialization(e.to_string()))?;
    if body.len() > MAX_COMMAND_BYTES {
        return Err(CacheError::TooLarge(format!(
            "command too large: {} bytes > {} (key + value)",
            body.len(),
            MAX_COMMAND_BYTES
        )));
    }
    Ok(frame(&body))
}

pub fn decode_command(body: &[u8]) -> Result<CacheCommand, CacheError> {
    bincode::deserialize(body).map_err(|e| CacheError::Serialization(e.to_string()))
}

/// Ответ -> готовый к отправке кадр.
pub fn encode_response(resp: &CacheResponse) -> Result<Vec<u8>, CacheError> {
    let body = bincode::serialize(resp).map_err(|e| CacheError::Serialization(e.to_string()))?;
    Ok(frame(&body))
}

pub fn decode_response(body: &[u8]) -> Result<CacheResponse, CacheError> {
    bincode::deserialize(body).map_err(|e| CacheError::Serialization(e.to_string()))
}
