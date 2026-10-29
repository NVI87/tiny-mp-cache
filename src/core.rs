use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
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
    next_key_id: Arc<AtomicU64>,
}

impl Default for CacheCore {
    fn default() -> Self {
        Self {
            meta_by_key: Arc::new(DashMap::new()),
            values: Arc::new(DashMap::new()),
            next_key_id: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl CacheCore {
    pub fn new() -> Self {
        Self::default()
    }

    fn gen_key_id(&self) -> KeyId {
        self.next_key_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn insert_meta_only(&self, key: String, meta: KeyMeta) {
        self.meta_by_key.insert(key, meta);
    }

    /// После загрузки меты с диска: продолжаем нумерацию key_id после максимального,
    /// иначе новые ключи получат key_id уже существующих и затрут их значения.
    pub fn bump_next_key_id(&self) {
        let max = self
            .meta_by_key
            .iter()
            .map(|e| e.key_id)
            .max()
            .unwrap_or(0);
        self.next_key_id.fetch_max(max + 1, Ordering::Relaxed);
    }

    /// key_id -> chunk_id по текущей мете (для загрузки значений из chunk-файлов).
    pub fn chunk_by_key_id(&self) -> HashMap<KeyId, ChunkId> {
        self.meta_by_key
            .iter()
            .map(|e| (e.key_id, e.chunk_id))
            .collect()
    }

    /// Retention: перенести ключи устаревших чанков в чанк `target`, который сейчас сохраняется.
    /// Ключ, успевший перезаписаться в другой чанк, не трогаем. Возвращает число перенесённых.
    pub fn reassign_chunks(&self, expired: &HashSet<ChunkId>, target: ChunkId) -> usize {
        let mut moved = 0;
        for mut entry in self.meta_by_key.iter_mut() {
            if expired.contains(&entry.chunk_id) {
                entry.chunk_id = target;
                moved += 1;
            }
        }
        moved
    }

    pub fn insert_value(&self, key_id: KeyId, value: Vec<u8>) {
        self.values.insert(key_id, value);
    }

    /// Удалить мету ключей, для которых не нашлось значения. Возвращает число удалённых.
    pub fn drop_meta_without_values(&self) -> usize {
        let orphans: Vec<String> = self
            .meta_by_key
            .iter()
            .filter(|e| !self.values.contains_key(&e.key_id))
            .map(|e| e.key().clone())
            .collect();
        for k in &orphans {
            self.meta_by_key.remove(k);
        }
        orphans.len()
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

    /// Полный dump «живых» ключей для snapshot'а текущего чанка:
    /// возвращаем (key_id, value, оставшийся TTL).
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

    /// Очистка по TTL и по "неживым" chunk_id (вызывается при snapshot'е).
    /// Текущий (ещё не сохранённый в snapshot) чанк всегда считается живым.
    /// Удаляем через remove_if по key_id: ключ, перезаписанный параллельно, не трогаем.
    pub fn gc(&self, live_chunks: &[ChunkId], current_chunk_id: ChunkId) {
        let now = Instant::now();
        let mut live: HashSet<ChunkId> = live_chunks.iter().cloned().collect();
        live.insert(current_chunk_id);

        let to_delete: Vec<(String, KeyId)> = self
            .meta_by_key
            .iter()
            .filter(|e| is_expired(e.value(), now) || !live.contains(&e.chunk_id))
            .map(|e| (e.key().clone(), e.key_id))
            .collect();

        for (k, id) in to_delete {
            if self.meta_by_key.remove_if(&k, |_, m| m.key_id == id).is_some() {
                self.values.remove(&id);
            }
        }
    }

    /// Мета живого ключа (без истёкшего TTL). Ничего не удаляет: это делает gc.
    fn live_meta(&self, key: &str) -> Option<KeyMeta> {
        let meta = self.meta_by_key.get(key)?.clone();
        if is_expired(&meta, Instant::now()) {
            return None;
        }
        Some(meta)
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        loop {
            let meta = self.live_meta(key)?;
            if let Some(v) = self.values.get(&meta.key_id) {
                return Some(v.clone());
            }
            // значение по старому key_id уже убрал параллельный set/delete:
            // перечитываем мету, пока она не перестанет меняться
            match self.meta_by_key.get(key) {
                Some(m) if m.key_id != meta.key_id => continue,
                _ => return None,
            }
        }
    }

    /// set/update с учётом текущего chunk_id.
    /// Каждый set получает новый key_id: значение пишется до меты, старое удаляется после,
    /// поэтому параллельные чтение и gc никогда не видят и не удаляют чужое значение.
    pub fn set(&self, key: String, value: Vec<u8>, ttl: Option<Duration>, chunk_id: ChunkId) {
        let now = Instant::now();
        let key_id = self.gen_key_id();
        let meta = KeyMeta {
            key_id,
            chunk_id,
            ttl: ttl.map(|d| now + d),
            updated_at: now,
        };

        self.values.insert(key_id, value);
        if let Some(old) = self.meta_by_key.insert(key, meta) {
            self.values.remove(&old.key_id);
        }
    }

    /// Атомарно: при конкурентных pop одного ключа значение получает ровно один вызов.
    pub fn pop(&self, key: &str) -> Option<Vec<u8>> {
        let (_, meta) = self.meta_by_key.remove(key)?;
        let value = self.values.remove(&meta.key_id).map(|(_, v)| v);
        if is_expired(&meta, Instant::now()) {
            return None;
        }
        value
    }

    pub fn delete(&self, key: &str) -> i64 {
        match self.meta_by_key.remove(key) {
            Some((_, meta)) => {
                self.values.remove(&meta.key_id);
                if is_expired(&meta, Instant::now()) {
                    0
                } else {
                    1
                }
            }
            None => 0,
        }
    }

    pub fn keys_prefix(&self, prefix: &str) -> Vec<String> {
        let now = Instant::now();
        self.meta_by_key
            .iter()
            .filter(|e| e.key().starts_with(prefix) && !is_expired(e.value(), now))
            .map(|e| e.key().clone())
            .collect()
    }

    pub fn len(&self) -> i64 {
        let now = Instant::now();
        self.meta_by_key
            .iter()
            .filter(|e| !is_expired(e.value(), now))
            .count() as i64
    }
}

fn is_expired(meta: &KeyMeta, now: Instant) -> bool {
    matches!(meta.ttl, Some(t) if now >= t)
}
