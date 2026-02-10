use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub type KeyId = u64;

#[derive(Clone, Debug)]
pub struct KeyMeta {
    pub key_id: KeyId,
    pub ttl: Option<Instant>,
    pub updated_at: Instant,
}

#[derive(Clone)]
pub struct CacheCore {
    pub(crate) meta_by_key: Arc<DashMap<String, KeyMeta>>,
    pub(crate) values: Arc<DashMap<KeyId, Vec<u8>>>,
    next_key_id: Arc<parking_lot::Mutex<KeyId>>,
}

impl Default for CacheCore {
    fn default() -> Self {
        Self {
            meta_by_key: Arc::new(DashMap::new()),
            values: Arc::new(DashMap::new()),
            next_key_id: Arc::new(parking_lot::Mutex::new(1)),
        }
    }
}

impl CacheCore {
    pub fn new() -> Self {
        Self::default()
    }

    fn gen_key_id(&self) -> KeyId {
        let mut guard = self.next_key_id.lock();
        let id = *guard;
        *guard = id.wrapping_add(1);
        id
    }

    /// Используется при загрузке меты с диска: добавить только KeyMeta, без значения.
    pub fn insert_meta_only(&self, key: String, meta: KeyMeta) {
        self.meta_by_key.insert(key, meta);
    }

    /// Экспорт для записи в keys.bin: (key, key_id, ttl_duration, updated_at)
    pub fn export_meta_for_disk(
        &self,
    ) -> Vec<(String, KeyId, Option<Duration>, Instant)> {
        let mut res = Vec::new();
        for entry in self.meta_by_key.iter() {
            let key = entry.key().clone();
            let key_id = entry.key_id;
            // ttl в виде Duration от now
            let ttl_dur = entry
                .ttl
                .and_then(|t| {
                    if t > Instant::now() {
                        Some(t - Instant::now())
                    } else {
                        None
                    }
                });
            res.push((key, key_id, ttl_dur, entry.updated_at));
        }
        res
    }

    pub fn get_meta(&self, key: &str) -> Option<KeyMeta> {
        let mut entry = self.meta_by_key.get_mut(key)?;
        if let Some(ttl) = entry.ttl {
            if Instant::now() >= ttl {
                let key_id = entry.key_id;
                drop(entry);
                self.meta_by_key.remove(key);
                self.values.remove(&key_id);
                return None;
            }
        }
        Some(entry.clone())
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let meta = self.get_meta(key)?;
        self.values.get(&meta.key_id).map(|v| v.clone())
    }

    pub fn set(&self, key: String, value: Vec<u8>, ttl: Option<Duration>) {
        let now = Instant::now();
        let expires_at = ttl.map(|d| now + d);

        let key_id = if let Some(existing) = self.meta_by_key.get(&key) {
            existing.key_id
        } else {
            self.gen_key_id()
        };

        let meta = KeyMeta {
            key_id,
            ttl: expires_at,
            updated_at: now,
        };

        self.meta_by_key.insert(key, meta);
        self.values.insert(key_id, value);
    }

    pub fn pop(&self, key: &str) -> Option<Vec<u8>> {
        if let Some(meta) = self.get_meta(key) {
            self.meta_by_key.remove(key);
            self.values.remove(&meta.key_id).map(|(_, v)| v)
        } else {
            None
        }
    }

    pub fn delete(&self, key: &str) -> i64 {
        if let Some((_, meta)) = self.meta_by_key.remove(key) {
            self.values.remove(&meta.key_id);
            1
        } else {
            0
        }
    }

    pub fn keys_prefix(&self, prefix: &str) -> Vec<String> {
        let now = Instant::now();
        let mut res = Vec::new();

        for mut entry in self.meta_by_key.iter_mut() {
            if !entry.key().starts_with(prefix) {
                continue;
            }
            if let Some(ttl) = entry.ttl {
                if now >= ttl {
                    let key = entry.key().clone();
                    let key_id = entry.key_id;
                    drop(entry);
                    self.meta_by_key.remove(&key);
                    self.values.remove(&key_id);
                    continue;
                }
            }
            res.push(entry.key().clone());
        }

        res
    }

    pub fn len(&self) -> i64 {
        let now = Instant::now();
        let mut to_delete = Vec::new();
        for entry in self.meta_by_key.iter() {
            if let Some(ttl) = entry.ttl {
                if now >= ttl {
                    to_delete.push((entry.key().clone(), entry.key_id));
                }
            }
        }
        for (k, id) in to_delete {
            self.meta_by_key.remove(&k);
            self.values.remove(&id);
        }

        self.meta_by_key.len() as i64
    }
}
