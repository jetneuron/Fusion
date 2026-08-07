use std::fs;
use std::path::Path;

fn main() {
    // Ensure the plugin lib output directories exist so that
    // `discover_libs()` in fusion-runtime/src/config.rs can
    // always scan them without errors.
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");

    let dirs = [
        root.join("app/assets/libs/capability"),
        root.join("app/assets/libs/unit"),
    ];

    for dir in &dirs {
        fs::create_dir_all(dir).ok();
        let gitkeep = dir.join(".gitkeep");
        if !gitkeep.exists() {
            fs::write(&gitkeep, "").ok();
        }
    }

    // Re-run if these directories are modified (e.g. plugins added/removed).
    for dir in &dirs {
        println!("cargo:rerun-if-changed={}", dir.display());
    }
}
