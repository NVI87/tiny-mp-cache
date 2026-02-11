use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub type KeyId = u64;
pub type ChunkId = u64;

#[derive(Clone, Debug)]
pub struct KeyMeta {
    pub key_id: KeyId,
    pub chunk_id: ChunkId,
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

    pub fn insert_meta_only(&self, key: String, meta: KeyMeta) {
        self.meta_by_key.insert(key, meta);
    }

    /// Экспорт меты для keys.bin: (cache_key, key_id, chunk_id, ttl, updated_at).
    pub fn export_meta_for_disk(
        &self,
    ) -> Vec<(String, KeyId, ChunkId, Option<Duration>, Instant)> {
        let mut res = Vec::new();
        let now = Instant::now();
        for entry in self.meta_by_key.iter() {
            let key = entry.key().clone();
            let key_id = entry.key_id;
            let chunk_id = entry.chunk_id;
            let ttl_dur = entry
                .ttl
                .and_then(|t| if t > now { Some(t - now) } else { None });
            res.push((key, key_id, chunk_id, ttl_dur, entry.updated_at));
        }
        res
    }

    /// Дамп «живых» ключей для snapshot’а текущего чанка.
    pub fn export_live_for_chunk(
        &self,
        current_chunk_id: ChunkId,
    ) -> Vec<(KeyId, Vec<u8>, Option<Duration>)> {
        let mut out = Vec::new();
        let now = Instant::now();

        for entry in self.meta_by_key.iter() {
            if entry.chunk_id != current_chunk_id {
                continue;
            }
            if let Some(ttl) = entry.ttl {
                if now >= ttl {
                    continue;
                }
            }
            if let Some(v) = self.values.get(&entry.key_id) {
                let ttl_dur = entry
                    .ttl
                    .and_then(|t| if t > now { Some(t - now) } else { None });
                out.push((entry.key_id, v.clone(), ttl_dur));
            }
        }

        out
    }

    /// Жёсткая очистка по TTL и по "неживым" chunk_id.
    pub fn gc(&self, live_chunks: &[ChunkId]) {
        let now = Instant::now();
        let live: HashSet<ChunkId> = live_chunks.iter().cloned().collect();
        let mut to_delete = Vec::new();

        for entry in self.meta_by_key.iter() {
            let mut dead = false;
            if let Some(ttl) = entry.ttl {
                if now >= ttl {
                    dead = true;
                }
            }
            if !dead && !live.is_empty() && !live.contains(&entry.chunk_id) {
                dead = true;
            }
            if dead {
                to_delete.push((entry.key().clone(), entry.key_id));
            }
        }

        for (k, id) in to_delete {
            self.meta_by_key.remove(&k);
            self.values.remove(&id);
        }
    }

    /// Берём мету с ленивой очисткой TTL. Без iter_mut на горячем пути.
    pub fn get_meta(&self, key: &str) -> Option<KeyMeta> {
        // Сначала просто смотрим.
        if let Some(entry) = self.meta_by_key.get(key) {
            if let Some(ttl) = entry.ttl {
                if Instant::now() >= ttl {
                    let key_id = entry.key_id;
                    let key_str = entry.key().clone();
                    drop(entry);
                    self.meta_by_key.remove(&key_str);
                    self.values.remove(&key_id);
                    return None;
                }
            }
            return Some(entry.clone());
        }
        None
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let meta = self.get_meta(key)?;
        self.values.get(&meta.key_id).map(|v| v.clone())
    }

    /// set/update с учётом текущего chunk_id.
    pub fn set(&self, key: String, value: Vec<u8>, ttl: Option<Duration>, chunk_id: ChunkId) {
        let now = Instant::now();
        let expires_at = ttl.map(|d| now + d);

        let key_id = if let Some(existing) = self.meta_by_key.get(&key) {
            existing.key_id
        } else {
            self.gen_key_id()
        };

        let meta = KeyMeta {
            key_id,
            chunk_id,
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
        let mut to_delete = Vec::new();

        for entry in self.meta_by_key.iter() {
            if !entry.key().starts_with(prefix) {
                continue;
            }
            if let Some(ttl) = entry.ttl {
                if now >= ttl {
                    to_delete.push((entry.key().clone(), entry.key_id));
                    continue;
                }
            }
            res.push(entry.key().clone());
        }

        for (k, id) in to_delete {
            self.meta_by_key.remove(&k);
            self.values.remove(&id);
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
