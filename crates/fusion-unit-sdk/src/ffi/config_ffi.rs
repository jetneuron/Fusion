//! Live config query across binary-image boundaries (host → dylib).
//!
//! Rust statics are per-binary-image: every dylib holds its own copy of
//! the config registry, so a snapshot injected once at load time
//! (`set_config`) goes stale the moment the host registers new entries.
//! The host instead installs a [`HostConfigApi`] function table into
//! dylibs (via their `set_host_config` export); the dylib then queries
//! the host registry live on every read.

use crate::config::InjectedConfig;
use std::ffi::{c_char, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::OnceLock;

/// C-ABI function table the host injects into dylibs via `set_host_config`.
///
/// The dylib calls `list_all` whenever it needs the registry, avoiding the
/// stale-snapshot problem of the legacy `set_config` symbol. Strings
/// returned by `list_all` are allocated by the host (`CString::into_raw`)
/// and must be released with `release` — never freed with the dylib's
/// allocator.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HostConfigApi {
    /// Full registry as `Vec<InjectedConfig>` JSON. Returns NULL on failure
    /// (e.g. the payload contains a NUL byte).
    pub list_all: unsafe extern "C" fn() -> *mut c_char,
    /// Release a string previously handed out by `list_all`.
    pub release: unsafe extern "C" fn(*mut c_char),
}

static HOST_API: OnceLock<HostConfigApi> = OnceLock::new();

/// Install the host's config API into this binary image.
///
/// Called from a dylib's `set_host_config` export. Never call it in the
/// host image — `config::read()` would then query the host's own API from
/// inside the host, recursing forever.
pub fn set_host_api(api: HostConfigApi) {
    let _ = HOST_API.set(api);
}

pub(crate) fn host_api() -> Option<HostConfigApi> {
    HOST_API.get().copied()
}

/// Fetch the full registry from the host.
///
/// Returns `None` when the host returns NULL or the payload fails to
/// parse — callers keep their current registry contents (never wiped by
/// a failed fetch). The host pointer is always released.
pub(crate) fn fetch_all(api: HostConfigApi) -> Option<Vec<InjectedConfig>> {
    // A panic inside the host callback must never unwind into dylib
    // frames (UB across the C ABI) — catch it here.
    let ptr = catch_unwind(AssertUnwindSafe(|| unsafe { (api.list_all)() })).ok()?;
    if ptr.is_null() {
        return None;
    }
    let json = unsafe {
        let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
        (api.release)(ptr);
        s
    };
    match serde_json::from_str::<Vec<InjectedConfig>>(&json) {
        Ok(entries) => Some(entries),
        Err(e) => {
            log::warn!("config: failed to parse host registry payload: {e}");
            None
        }
    }
}
