use crate::datafusion_unit::DataFusionUnit;
use fusion_unit_sdk::runtime::GLOBAL_REGISTRY;
use fusion_unit_sdk::{GraphUnitPlugin, UnitManifest};

pub mod datafusion_unit;
mod types;
mod utils;

// ignored warning: `not FFI-safe`
#[unsafe(no_mangle)]
pub extern "C" fn init_plugin() -> Box<dyn GraphUnitPlugin> {
    Box::new(ApacheDataFusionPlugin {})
}

///
/// Apache data fusion official plugin.
/// https://datafusion.apache.org/
///
pub struct ApacheDataFusionPlugin {}

impl GraphUnitPlugin for ApacheDataFusionPlugin {
    //noinspection RsUnresolvedPath
    fn register_units(&self) -> UnitManifest {
        let mut manifest = UnitManifest::default();
        let version = self.plugin_version();
        DataFusionUnit::register_unit(&mut manifest, version);
        // ... Register other units ...
        manifest
    }
}

impl Drop for ApacheDataFusionPlugin {
    fn drop(&mut self) {
        GLOBAL_REGISTRY.clean_and_shrink();
    }
}
