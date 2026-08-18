//! [`CapabilitySqlEngine`] implementation backed by the capability dylib's
//! C-ABI engine factory.
//!
//! The engine lives in another binary image (the capability dylib) and
//! runs its own tokio runtime. Its methods are synchronous FFI calls that
//! `block_on` internally, so this wrapper invokes them on `std::thread`
//! threads (not tokio workers) — a tokio worker here would be a thread of
//! the *host* runtime, which this crate's tokio cannot drive and must not
//! block.

use std::ffi::{c_char, c_void};

use fusion_unit_sdk::capability::{Capability, CapabilitySqlEngine};
use fusion_unit_sdk::ffi::sql_engine_ffi::SqlEngineFactory;
use fusion_unit_sdk::proto::transfer::Frame;
use fusion_unit_sdk::runtime::UnitError;
use fusion_unit_sdk::runtime::UnitResult;

/// Run a synchronous engine call on a std thread and await its result.
/// (tokio's `spawn_blocking` needs this crate's tokio runtime context on
/// the calling thread, which does not exist in the dylib deployment.)
async fn ffi_blocking<T: Send + 'static>(
    f: impl FnOnce() -> UnitResult<T> + Send + 'static,
) -> UnitResult<T> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.await
        .map_err(|e| UnitError::unknown(format!("sql engine ffi task failed: {e}")))?
}

/// Thin wrapper over a raw engine handle from the capability dylib.
pub struct FfiSqlEngine {
    factory: SqlEngineFactory,
    handle: *mut c_void,
}

// The raw handle is owned by the capability dylib (safe there); this
// wrapper only forwards calls and drops the handle once.
unsafe impl Send for FfiSqlEngine {}
unsafe impl Sync for FfiSqlEngine {}

impl FfiSqlEngine {
    pub fn new(factory: SqlEngineFactory, config_json: Option<&str>) -> UnitResult<Self> {
        let config = config_json.unwrap_or("").as_ptr() as *const c_char;
        let handle = unsafe { (factory.create_engine)(config) };
        if handle.is_null() {
            return Err(UnitError::unknown("failed to create sql engine"));
        }
        Ok(Self { factory, handle })
    }
}

impl Drop for FfiSqlEngine {
    fn drop(&mut self) {
        unsafe { (self.factory.drop_engine)(self.handle) };
    }
}

fn cstr(s: &str) -> std::ffi::CString {
    // SQL/table names never contain NUL bytes in practice.
    std::ffi::CString::new(s).unwrap_or_default()
}

#[async_trait::async_trait]
impl Capability for FfiSqlEngine {
    fn name(&self) -> &str {
        "datafusion"
    }

    async fn init(&self) -> UnitResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> UnitResult<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl CapabilitySqlEngine for FfiSqlEngine {
    async fn query(&self, sql: &str) -> UnitResult<Vec<Frame>> {
        let factory = self.factory;
        let handle = self.handle as usize;
        let sql = cstr(sql);
        ffi_blocking(move || unsafe {
            let handle = handle as *mut c_void;
            let mut out: *mut Vec<Frame> = std::ptr::null_mut();
            let rc = (factory.query)(handle, sql.as_ptr(), &mut out);
            if rc != 0 || out.is_null() {
                return Err(UnitError::unknown(
                    "sql engine query failed (see engine log)",
                ));
            }
            let frames = Box::from_raw(out);
            Ok(*frames)
        })
        .await
    }

    async fn register_frame_table(
        &self,
        name: &str,
        frames: Vec<Frame>,
    ) -> UnitResult<()> {
        let factory = self.factory;
        let handle = self.handle as usize;
        let name = cstr(name);
        let frames = Box::into_raw(Box::new(frames)) as usize;
        ffi_blocking(move || unsafe {
            let handle = handle as *mut c_void;
            let frames = frames as *mut Vec<Frame>;
            let rc = (factory.register_frame_table)(handle, name.as_ptr(), frames);
            if rc != 0 {
                return Err(UnitError::unknown(
                    "register_frame_table failed (see engine log)",
                ));
            }
            Ok(())
        })
        .await
    }

    async fn finalize_frame_table(&self, name: &str) -> UnitResult<()> {
        let factory = self.factory;
        let handle = self.handle as usize;
        let name = cstr(name);
        ffi_blocking(move || unsafe {
            let handle = handle as *mut c_void;
            let rc = (factory.finalize_frame_table)(handle, name.as_ptr());
            if rc != 0 {
                return Err(UnitError::unknown(
                    "finalize_frame_table failed (see engine log)",
                ));
            }
            Ok(())
        })
        .await
    }

    async fn register_csv_table(&self, name: &str, path: &str) -> UnitResult<()> {
        let factory = self.factory;
        let handle = self.handle as usize;
        let name = cstr(name);
        let path = cstr(path);
        ffi_blocking(move || unsafe {
            let handle = handle as *mut c_void;
            let rc = (factory.register_csv_table)(handle, name.as_ptr(), path.as_ptr());
            if rc != 0 {
                return Err(UnitError::unknown(
                    "register_csv_table failed (see engine log)",
                ));
            }
            Ok(())
        })
        .await
    }

    async fn deregister_table(&self, name: &str) -> UnitResult<()> {
        let factory = self.factory;
        let handle = self.handle as usize;
        let name = cstr(name);
        ffi_blocking(move || unsafe {
            let handle = handle as *mut c_void;
            let rc = (factory.deregister_table)(handle, name.as_ptr());
            if rc != 0 {
                return Err(UnitError::unknown(
                    "deregister_table failed (see engine log)",
                ));
            }
            Ok(())
        })
        .await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
