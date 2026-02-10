use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Внутренний ID ключа (можно заменить на что-то своё позже)
pub type KeyId = u64;

/// Метаданные по ключу
#[derive(Clone, Debug)]
pub struct KeyMeta {
    pub key_id: KeyId,
    pub ttl: Option<Instant>,    // момент истечения, None = бессрочно
    pub updated_at: Instant,
}

#[derive(Clone)]
pub struct CacheCore {
    /// Ключ → мета (id, ttl, updated_at)
    meta_by_key: Arc<DashMap<String, KeyMeta>>,
    /// key_id → value
    values: Arc<DashMap<KeyId, Vec<u8>>>,
    /// простой счётчик для генерации новых key_id
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

    /// Получить (key_id, мету) по cache_key, если ключ существует и не протух по TTL.
    pub fn get_meta(&self, key: &str) -> Option<KeyMeta> {
        let mut entry = self.meta_by_key.get_mut(key)?;
        if let Some(ttl) = entry.ttl {
            if Instant::now() >= ttl {
                // TTL истёк — удаляем
                let key_id = entry.key_id;
                drop(entry);
                self.meta_by_key.remove(key);
                self.values.remove(&key_id);
                return None;
            }
        }
        Some(entry.clone())
    }

    /// Низкоуровневый GET по cache_key.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let meta = self.get_meta(key)?;
        self.values.get(&meta.key_id).map(|v| v.clone())
    }

    /// Установить значение с возможным TTL (None = нет TTL).
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

    /// Попытаться вынуть и удалить ключ.
    pub fn pop(&self, key: &str) -> Option<Vec<u8>> {
        // Проверяем TTL
        if let Some(meta) = self.get_meta(key) {
            // ещё жив
            self.meta_by_key.remove(key);
            self.values.remove(&meta.key_id).map(|(_, v)| v)
        } else {
            None
        }
    }

    /// Удалить ключ, вернуть 1 если был, 0 если нет.
    pub fn delete(&self, key: &str) -> i64 {
        if let Some((_, meta)) = self.meta_by_key.remove(key) {
            self.values.remove(&meta.key_id);
            1
        } else {
            0
        }
    }

    /// Список ключей, удовлетворяющих префиксу, с фильтрацией по TTL.
    pub fn keys_prefix(&self, prefix: &str) -> Vec<String> {
        let now = Instant::now();
        let mut res = Vec::new();

        for mut entry in self.meta_by_key.iter_mut() {
            if !entry.key().starts_with(prefix) {
                continue;
            }
            // TTL-фильтрация
            if let Some(ttl) = entry.ttl {
                if now >= ttl {
                    // протух — чистим, не возвращаем
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

    /// Кол-во живых ключей (с учётом TTL)
    pub fn len(&self) -> i64 {
        // Ленивая чистка TTL перед подсчётом
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
