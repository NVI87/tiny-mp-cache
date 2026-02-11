use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub type KeyId = u64;
pub type ChunkId = u64;

#[derive(Clone, Debug)]
pub struct KeyMeta {
    pub keyid: KeyId,
    pub chunkid: ChunkId,
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
        *guard = guard.wrapping_add(1);
        id
    }

    pub fn insert_meta_only(&self, key: String, meta: KeyMeta) {
        self.meta_by_key.insert(key, meta);
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let now = Instant::now();
        if let Some(meta) = self.meta_by_key.get(key) {
            if let Some(ttl) = meta.ttl {
                if now > ttl {
                    drop(meta);
                    self.meta_by_key.remove(key);
                    return None;
                }
            }
            return self.values.get(&meta.keyid).map(|v| v.clone());
        }
        None
    }

    pub fn pop(&self, key: &str) -> Option<Vec<u8>> {
        if let Some((_, meta)) = self.meta_by_key.remove(key) {
            return self.values.remove(&meta.keyid).map(|(_, v)| v);
        }
        None
    }

    pub fn delete(&self, key: &str) -> i64 {
        if let Some((_, meta)) = self.meta_by_key.remove(key) {
            self.values.remove(&meta.keyid);
            1
        } else {
            0
        }
    }

    pub fn set(&self, key: String, value: Vec<u8>, ttl: Option<Duration>, chunk_id: ChunkId) {
        let now = Instant::now();
        let ttl_instant = ttl.map(|d| now + d);

        let keyid = if let Some(mut meta) = self.meta_by_key.get_mut(&key) {
            let id = meta.keyid;
            meta.chunkid = chunk_id;
            meta.ttl = ttl_instant;
            meta.updated_at = now;
            id
        } else {
            let id = self.gen_key_id();
            let meta = KeyMeta {
                keyid: id,
                chunkid: chunk_id,
                ttl: ttl_instant,
                updated_at: now,
            };
            self.meta_by_key.insert(key, meta);
            id
        };

        self.values.insert(keyid, value);
    }

    pub fn keys_prefix(&self, prefix: &str) -> Vec<String> {
        self.meta_by_key
            .iter()
            .filter_map(|kv| {
                let k = kv.key();
                if k.starts_with(prefix) {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn len(&self) -> i64 {
        self.meta_by_key.len() as i64
    }

    pub fn gc(&self, live_chunks: &[ChunkId]) {
        let mut live_set = HashSet::new();
        for &cid in live_chunks {
            live_set.insert(cid);
        }

        let mut to_remove_keys = Vec::new();
        let now = Instant::now();

        for entry in self.meta_by_key.iter() {
            let meta = entry.value();
            if let Some(ttl) = meta.ttl {
                if now > ttl {
                    to_remove_keys.push(entry.key().clone());
                    continue;
                }
            }
            if !live_set.contains(&meta.chunkid) {
                to_remove_keys.push(entry.key().clone());
            }
        }

        for k in to_remove_keys {
            if let Some((_, meta)) = self.meta_by_key.remove(&k) {
                self.values.remove(&meta.keyid);
            }
        }
    }

    pub fn export_live_for_chunk(&self, chunk_id: ChunkId) -> Vec<(KeyId, Vec<u8>, Option<Duration>)> {
        let now = Instant::now();
        let mut out = Vec::new();

        for entry in self.meta_by_key.iter() {
            let meta = entry.value();
            if meta.chunkid != chunk_id {
                continue;
            }
            if let Some(ttl) = meta.ttl {
                if now > ttl {
                    continue;
                }
                if let Some(v) = self.values.get(&meta.keyid) {
                    let ttl_left = ttl
                        .checked_duration_since(now)
                        .unwrap_or(Duration::from_millis(0));
                    out.push((meta.keyid, v.clone(), Some(ttl_left)));
                }
            } else if let Some(v) = self.values.get(&meta.keyid) {
                out.push((meta.keyid, v.clone(), None));
            }
        }

        out
    }

    pub fn export_meta_for_disk(
        &self,
    ) -> Vec<(String, KeyId, ChunkId, Option<Instant>, Instant)> {
        let mut out = Vec::new();
        for entry in self.meta_by_key.iter() {
            let key = entry.key().clone();
            let meta = entry.value();
            out.push((key, meta.keyid, meta.chunkid, meta.ttl, meta.updated_at));
        }
        out
    }

    pub fn import_meta(&self, data: &[u8]) -> Result<(), crate::error::CacheError> {
        crate::meta::load_keys_meta_from_bytes(self, data)
    }
}
