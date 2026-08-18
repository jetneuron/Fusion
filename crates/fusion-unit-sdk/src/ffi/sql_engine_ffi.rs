//! C-ABI bridge between the host process, capability dylibs and unit dylibs.
//!
//! # Why this exists
//!
//! Rust statics and trait objects are per-binary-image: a dylib's registry
//! is invisible to the host or to other dylibs, and engine types (e.g.
//! DataFusion `SessionContext`) must not cross the boundary either. The
//! bridge protocol is therefore **data over FFI, types stay in their
//! image**:
//!
//! - The capability dylib owns the engine and exports an
//!   [`SqlEngineFactory`] — a plain function-pointer table.
//! - The host loads capability/provider/unit dylibs and injects the factory
//!   table, the provider objects and the serialized config into unit dylibs
//!   via `set_*` symbols.
//! - [`Frame`](crate::proto::transfer::Frame) vectors cross the boundary by
//!   pointer ownership transfer (`Box<Vec<Frame>>`), which is safe because
//!   every image compiles the same SDK source (same layouts, same vtables).
//!
//! All images (host, capability, unit, provider) compile this SDK crate, so
//! the `repr(C)` types below have identical layouts everywhere.

use std::ffi::{c_char, c_void};
use std::sync::Arc;

use crate::proto::transfer::Frame;

/// Engine method table exported by a capability dylib.
///
/// Every method is synchronous — the capability side runs its own async
/// engine (own tokio runtime) via `block_on`; unit-side wrappers call the
/// methods from `spawn_blocking` threads so tokio workers never block.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SqlEngineFactory {
    /// Create an engine handle. `config_json` is reserved for future
    /// engine-level config; may be null.
    pub create_engine: unsafe extern "C" fn(config_json: *const c_char) -> *mut c_void,
    /// Execute SQL, transferring ownership of the result `Vec<Frame>`
    /// into `frames_out`. Returns 0 on success, non-zero on error.
    pub query: unsafe extern "C" fn(
        handle: *mut c_void,
        sql: *const c_char,
        frames_out: *mut *mut Vec<Frame>,
    ) -> i32,
    /// Append frames to a stream table (ownership of `frames` is taken).
    pub register_frame_table: unsafe extern "C" fn(
        handle: *mut c_void,
        name: *const c_char,
        frames: *mut Vec<Frame>,
    ) -> i32,
    /// Freeze a stream table so it becomes queryable.
    pub finalize_frame_table: unsafe extern "C" fn(
        handle: *mut c_void,
        name: *const c_char,
    ) -> i32,
    /// Register a CSV file as a table.
    pub register_csv_table: unsafe extern "C" fn(
        handle: *mut c_void,
        name: *const c_char,
        path: *const c_char,
    ) -> i32,
    /// Deregister a table.
    pub deregister_table: unsafe extern "C" fn(
        handle: *mut c_void,
        name: *const c_char,
    ) -> i32,
    /// Destroy an engine handle.
    pub drop_engine: unsafe extern "C" fn(handle: *mut c_void),
}

/// One provider entry handed from host to unit dylib.
///
/// `provider_data` / `provider_vtable` form the fat pointer of an
/// `Arc<dyn TableDataProvider>` whose ownership is **transferred** to the
/// unit dylib (the host called `Arc::into_raw` on a `(data, vtable)` pair).
#[repr(C)]
pub struct HostProviderEntry {
    /// Borrowed name (lives in the host; valid for the duration of the call).
    pub name: *const c_char,
    pub provider_data: *const (),
    pub provider_vtable: *const (),
}

/// Array of provider entries for host → unit injection.
#[repr(C)]
pub struct HostProviders {
    pub entries: *const HostProviderEntry,
    pub len: usize,
}

impl HostProviders {
    /// Assemble the transferred provider objects into a map.
    ///
    /// # Safety
    ///
    /// `self` must come from the host via FFI and each entry must be a
    /// valid `Arc<dyn TableDataProvider>` fat pointer transferred with
    /// ownership (host must not use them afterwards).
    pub unsafe fn take(self) -> Vec<(String, Arc<dyn crate::providers::TableDataProvider>)> {
        let mut out = Vec::with_capacity(self.len);
        for i in 0..self.len {
            let entry = unsafe { &*self.entries.add(i) };
            let name = unsafe { std::ffi::CStr::from_ptr(entry.name) }
                .to_string_lossy()
                .into_owned();
            // Reassemble the fat pointer (data, vtable) into an
            // Arc<dyn TableDataProvider>; layouts match because every
            // image compiles the same SDK.
            let thin: (*const (), *const ()) = (entry.provider_data, entry.provider_vtable);
            let fat: *const dyn crate::providers::TableDataProvider =
                unsafe { std::mem::transmute(thin) };
            let arc: Arc<dyn crate::providers::TableDataProvider> = unsafe { Arc::from_raw(fat) };
            out.push((name, arc));
        }
        out
    }
}

/// Transfers ownership of a `Vec<Frame>` to the caller.
///
/// # Safety
///
/// `ptr` must be a `Box<Vec<Frame>>` leaked by [`leak_frames`] (or the
/// capability side's equivalent).
pub unsafe fn take_frames(ptr: *mut Vec<Frame>) -> Vec<Frame> {
    unsafe { *Box::from_raw(ptr) }
}

/// Leaks ownership of `frames` to a raw pointer for FFI transfer.
pub fn leak_frames(frames: Vec<Frame>) -> *mut Vec<Frame> {
    Box::into_raw(Box::new(frames))
}
