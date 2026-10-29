use thiserror::Error;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("network error: {0}")]
    Network(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("internal error: {0}")]
    Internal(String),

    /// Команда больше protocol::MAX_COMMAND_BYTES.
    #[error("{0}")]
    TooLarge(String),
}
