//! Live config bridge — host callbacks injected into dylibs.
//!
//! The host installs a [`HostConfigApi`] function table into every dylib
//! that exports `set_host_config`. Dylibs call `list_all` on each config
//! read, so entries registered after dylib load are visible immediately —
//! no stale `set_config` snapshot.

use fusion_unit_sdk::config;
use fusion_unit_sdk::ffi::config_ffi::HostConfigApi;
use std::ffi::{CString, c_char};

/// Serialize the process-global config registry as `Vec<InjectedConfig>`
/// JSON. Shared by the live `list_all` callback and the legacy `set_config`
/// snapshot fallback for dylibs without `set_host_config`.
pub(crate) fn serialize_config() -> String {
    let reg = config::read();
    let entries: Vec<config::InjectedConfig> = reg
        .ids()
        .filter_map(|id| {
            reg.entry(id).map(|e| config::InjectedConfig {
                category: e.category.clone(),
                config_type: e.config_type.clone(),
                id: id.clone(),
                data: e.data.clone(),
            })
        })
        .collect();
    serde_json::to_string(&entries).unwrap_or_default()
}

/// Full registry as a host-allocated C string; NULL on failure (a NUL
/// byte in the payload would be UB inside `CString`).
unsafe extern "C" fn list_all() -> *mut c_char {
    CString::new(serialize_config())
        .map(CString::into_raw)
        .unwrap_or(std::ptr::null_mut())
}

/// Release a string previously handed out by `list_all`.
unsafe extern "C" fn release(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(unsafe { CString::from_raw(ptr) });
    }
}

/// The function table injected into dylibs via `set_host_config`.
pub(crate) fn host_config_api() -> HostConfigApi {
    HostConfigApi {
        list_all,
        release,
    }
}
