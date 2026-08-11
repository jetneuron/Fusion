use super::Capability;
use crate::runtime::UnitResult;

// ============================================================
// Scan types — store-agnostic pagination
// ============================================================

/// Parameters for a paginated scan operation.
///
/// Decoupled from any specific store. Implementations map these
/// to their native scan / cursor APIs.
#[derive(Debug, Clone, Default)]
pub struct ScanOptions {
    /// Glob-style pattern (e.g. `"user:*"`, `"cache:??"`).
    /// `None` or `"*"` matches all keys.
    pub pattern: Option<String>,
    /// Maximum entries to return per page.
    /// `None` means the store chooses a reasonable default.
    pub page_size: Option<u64>,
}

impl ScanOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the match pattern.
    pub fn pattern(mut self, p: impl Into<String>) -> Self {
        self.pattern = Some(p.into());
        self
    }

    /// Set the page size.
    pub fn page_size(mut self, n: u64) -> Self {
        self.page_size = Some(n);
        self
    }
}

/// Result of one page of a paginated scan.
#[derive(Debug, Clone)]
pub struct ScanResult {
    /// Key-value entries returned in this page.
    pub entries: Vec<(String, Vec<u8>)>,
    /// Opaque cursor for the next page.
    /// `None` means iteration is complete.
    pub next_cursor: Option<String>,
}

// ============================================================
// CapabilityKeyValueStore
// ============================================================

/// Key-value store capability.
///
/// Covers the full spectrum of modern KV operations: basic CRUD,
/// batch operations, atomic counters, expiry/TTL, and cursor-based
/// paginated scanning.
///
/// # Required methods
///
/// Implementations **must** provide: [`get`](Self::get), [`set`](Self::set),
/// [`delete`](Self::delete), [`mget`](Self::mget), [`scan_page`](Self::scan_page).
///
/// # Scanning
///
/// Use [`scan_page`](Self::scan_page) for paginated cursor-based iteration:
///
/// ```ignore
/// let opts = ScanOptions::new().pattern("user:*").page_size(100);
/// let mut cursor = None;
/// loop {
///     let page = store.scan_page(cursor.as_deref(), &opts).await?;
///     for (key, value) in &page.entries { ... }
///     match page.next_cursor {
///         Some(c) => cursor = Some(c),
///         None => break,
///     }
/// }
/// ```
///
/// Convenience methods [`scan_all`](Self::scan_all) and
/// [`scan_keys`](Self::scan_keys) collect all pages for you —
/// use with caution on large datasets.
#[async_trait::async_trait]
pub trait CapabilityKeyValueStore: Capability {
    // ================================================================
    // Required methods
    // ================================================================

    /// Get a value by key. Returns `None` if the key does not exist.
    async fn get(&self, key: &str) -> UnitResult<Option<Vec<u8>>>;

    /// Set a value for a key.
    async fn set(&self, key: &str, value: &[u8]) -> UnitResult<()>;

    /// Delete a key. Returns `Ok(())` even if the key did not exist.
    async fn delete(&self, key: &str) -> UnitResult<()>;

    /// Get values for multiple keys. Returns one `Option<Vec<u8>>` per
    /// key, in the same order as the input.
    async fn mget(&self, keys: &[&str]) -> UnitResult<Vec<Option<Vec<u8>>>>;

    /// Paginated scan with cursor-based iteration.
    ///
    /// Pass `cursor = None` for the first page, then use the returned
    /// [`ScanResult::next_cursor`] for subsequent pages. Iteration is
    /// complete when `next_cursor` is `None`.
    ///
    /// Unlike the convenience [`scan_all`](Self::scan_all), this method
    /// does **not** load all keys into memory at once. It is safe for
    /// production use on large datasets.
    async fn scan_page(
        &self,
        cursor: Option<&str>,
        opts: &ScanOptions,
    ) -> UnitResult<ScanResult>;

    // ================================================================
    // Provided methods — with sensible (but possibly non-atomic) defaults
    // ================================================================

    /// Check whether a key exists.
    async fn exists(&self, key: &str) -> UnitResult<bool> {
        self.get(key).await.map(|v| v.is_some())
    }

    /// Set multiple key-value pairs.
    ///
    /// **Default is not atomic.** Override for stores with native `MSET`.
    async fn mset(&self, pairs: &[(&str, &[u8])]) -> UnitResult<()> {
        for (key, value) in pairs {
            self.set(key, value).await?;
        }
        Ok(())
    }

    /// Delete multiple keys. Returns the number of keys actually deleted.
    ///
    /// **Default is not atomic.** Override for stores with native `DEL`
    /// that accepts multiple keys.
    async fn mdelete(&self, keys: &[&str]) -> UnitResult<u64> {
        let mut count = 0u64;
        for key in keys {
            if self.exists(key).await? {
                self.delete(key).await?;
                count += 1;
            }
        }
        Ok(count)
    }

    /// Set a value only if the key does not already exist.
    ///
    /// Returns `true` if set, `false` if the key already existed.
    /// **Default is not atomic.** Override for native `SETNX`.
    async fn set_nx(&self, key: &str, value: &[u8]) -> UnitResult<bool> {
        if self.exists(key).await? {
            return Ok(false);
        }
        self.set(key, value).await?;
        Ok(true)
    }

    /// Atomically get-and-set.
    ///
    /// **Default is not atomic.** Override for native `GETSET`.
    async fn get_set(&self, key: &str, value: &[u8]) -> UnitResult<Option<Vec<u8>>> {
        let old = self.get(key).await?;
        self.set(key, value).await?;
        Ok(old)
    }

    /// Increment a numeric value by `delta`.
    ///
    /// Returns the new value. **Default parses the value as a string
    /// and is not atomic.** Override for native `INCRBY`.
    async fn incr_by(&self, key: &str, delta: i64) -> UnitResult<i64> {
        let current: i64 = match self.get(key).await? {
            Some(bytes) => String::from_utf8_lossy(&bytes).parse().unwrap_or(0),
            None => 0,
        };
        let new_val = current + delta;
        self.set(key, new_val.to_string().as_bytes()).await?;
        Ok(new_val)
    }

    // ---- Expiry / TTL ----

    /// Set a value with a time-to-live in milliseconds.
    ///
    /// Default falls back to [`set`](Self::set) **without** TTL.
    /// Override for native `PSETEX` / `SETEX`.
    async fn set_ex(&self, key: &str, value: &[u8], _ttl_ms: u64) -> UnitResult<()> {
        self.set(key, value).await
    }

    /// Set or update the TTL of an existing key in milliseconds.
    ///
    /// Returns `true` if the key exists. Default returns an error.
    async fn expire(&self, _key: &str, _ttl_ms: u64) -> UnitResult<bool> {
        Err(crate::runtime::UnitError::unknown(
            "expire is not supported by this store",
        ))
    }

    /// Get the remaining TTL in milliseconds.
    ///
    /// Returns `None` if the key has no expiry, or an error if
    /// the store does not support TTL queries.
    async fn ttl(&self, _key: &str) -> UnitResult<Option<u64>> {
        Err(crate::runtime::UnitError::unknown(
            "ttl is not supported by this store",
        ))
    }

    /// Remove expiry from a key, making it persistent.
    async fn persist(&self, _key: &str) -> UnitResult<bool> {
        Err(crate::runtime::UnitError::unknown(
            "persist is not supported by this store",
        ))
    }

    // ---- Scan convenience methods ----

    /// Collect all pages from [`scan_page`](Self::scan_page) into a single vec.
    ///
    /// Convenient, but loads everything into memory. For large datasets,
    /// use [`scan_page`](Self::scan_page) directly with pagination.
    async fn scan_all(&self, opts: &ScanOptions) -> UnitResult<Vec<(String, Vec<u8>)>> {
        let mut all = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = self.scan_page(cursor.as_deref(), opts).await?;
            all.extend(page.entries);
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        Ok(all)
    }

    /// Collect all keys matching the scan options.
    async fn scan_keys(&self, opts: &ScanOptions) -> UnitResult<Vec<String>> {
        let mut keys = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = self.scan_page(cursor.as_deref(), opts).await?;
            keys.extend(page.entries.into_iter().map(|(k, _)| k));
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        Ok(keys)
    }

    /// Count keys matching the scan options.
    ///
    /// Default iterates all pages. Override for native `DBSIZE`-like
    /// operations if the store supports prefix counting efficiently.
    async fn count(&self, opts: &ScanOptions) -> UnitResult<u64> {
        let mut count = 0u64;
        let mut cursor: Option<String> = None;
        loop {
            let page = self.scan_page(cursor.as_deref(), opts).await?;
            count += page.entries.len() as u64;
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        Ok(count)
    }
}

/// Well-known `CapabilityKeyValueStore` capability names.
pub mod well_known {
    /// Redis — `"redis"`
    pub const REDIS: &str = "redis";
    /// Apache HBase — `"hbase"`
    pub const HBASE: &str = "hbase";
    /// In-memory store (development / testing) — `"inmemory"`
    pub const IN_MEMORY: &str = "inmemory";
    /// RocksDB embedded key-value store — `"rocksdb"`
    pub const ROCKSDB: &str = "rocksdb";
    /// Default / unspecified implementation — `"default"`
    pub const DEFAULT: &str = "default";
}
