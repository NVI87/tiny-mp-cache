use dashmap::DashMap;
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct CacheCore {
    inner: Arc<DashMap<String, Vec<u8>>>,
}

impl CacheCore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, key: String, value: Vec<u8>) {
        self.inner.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.inner.get(key).map(|v| v.value().clone())
    }

    pub fn pop(&self, key: &str) -> Option<Vec<u8>> {
        self.inner.remove(key).map(|(_, v)| v)
    }

    pub fn delete(&self, key: &str) -> i64 {
        self.inner.remove(key).is_some() as i64
    }

    pub fn keys_prefix(&self, prefix: &str) -> Vec<String> {
        self.inner
            .iter()
            .filter(|e| e.key().starts_with(prefix))
            .map(|e| e.key().clone())
            .collect()
    }

    pub fn len(&self) -> i64 {
        self.inner.len() as i64
    }
}
