# Fusion

![Rust](https://img.shields.io/badge/rust-2024-orange)
![Edition](https://img.shields.io/badge/edition-2024-blue)
![Version](https://img.shields.io/badge/version-0.1.0-blue)
![Platform](https://img.shields.io/badge/platform-macOS%20%7C%20Linux-lightgrey)

**Fusion** is a graph-based stream computing engine. Declare a computation graph in YAML/JSON (nodes = computing units, edges = data flow), and the engine compiles it into a physical graph executed over asynchronous mpsc channels — with built-in backpressure, per-node parallelism, multiple script engines, and a dylib plugin system. It embeds into desktop apps (Tauri) and also deploys as a server/cluster mode.

```yaml
# graph.yaml — a complete computation graph
name: example_graph
units:
  - id: input
    type: DebugInputUnitTask        # built-in data source
    version: builtin
    config:
      times: 5
      column_count: 3
  - id: map
    type: MapUnitTask               # built-in Lua transformation node
    version: builtin
    config:
      $script: |
        local frame = ctx:newFrame()
        frame['x'] = data['c0'] * 2
        ctx:send(frame)
      $script_type: Lua
edges:
  - id: e1
    source: input
    target: map
  - id: e2
    source: map
    target: output
```

## ✨ Features

- **Declarative computation graphs** — YAML/JSON defines nodes and edges; `LogicalGraph` → petgraph → physical graph compiles automatically
- **Async execution + built-in backpressure** — one mpsc channel (capacity 1024) per edge; the channel itself is the flow control, no watermark polling; fan-out sends in parallel, so a slow consumer never blocks fast ones
- **Per-node parallelism** — `config.parallelism: N` runs compute across N workers; with `parallelism > 1` each worker owns an independent Lua VM for truly parallel scripts
- **Multiple script engines** — Lua (row transforms, edge-condition filtering), Tera (template rendering), TypeScript (deno_core)
- **Two-layer plugin system** — unit plugins (graph node types) + capability plugins (process-global services: KV store, SQL engine), loaded as dylibs at runtime
- **DataFusion SQL integration** — static tables / upstream stream frames become SQL tables (stream JOIN, aggregation); CSV, SQLite, and custom providers supported
- **Rich built-in units** — Debug source/output, Lua Map, HTTP, Excel, Redis, SSH, generic filesystem
- **Concurrent graph execution** — `GraphExecutor` submits multiple graphs concurrently, each with its own Lua/Tera context
- **Embedding-friendly** — one-line startup via `FusionRuntime::init_app()`; the official Tauri desktop app ships with prebuilt plugin dylibs

## 🏗️ Architecture Overview

```
┌────────────────────────────────────────────────────────────┐
│                     Fusion Runtime                         │
│                                                            │
│  YAML/JSON ──▶ LogicalGraph ──▶ PhysicalGraph              │
│  (graph def)    (PetGraph)        │                        │
│                                  ▼                        │
│  Source ── mpsc(1024) ──▶ Map/Sink ── mpsc ──▶ Sink       │
│  (launch)   backpressure   (compute/on_eof)               │
│                                                            │
│  PluginManager: unit dylib + capability dylib (lazy load)  │
│  Script engines: Lua / Tera / TypeScript (per-exec context)│
└────────────────────────────────────────────────────────────┘
```

Detailed design: **[Architecture](doc/architecture.md)**; the cross-image FFI design of the SQL engine: **[DataFusion Integration](doc/datafusion.md)**.

## 🚀 Quick Start

### Embedded Mode (App / Tauri)

```rust
use fusion_runtime::FusionRuntime;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Reads config/fusion-conf.yaml (falls back to the built-in app
    // config), registers built-in units, and auto-scans the libs
    // directory to load plugin dylibs.
    let runtime = FusionRuntime::init_app().await?;

    // Execute a graph: file:// URL or inline YAML/JSON
    runtime.execute("file:///path/to/graph.yaml", None).await?;
    Ok(())
}
```

### Server / Cluster Mode

```rust
use fusion_runtime::{FusionRuntimeBuilder, config::FusionConfig};

let cfg = FusionConfig::load()?;   // config/fusion-conf.yaml (cluster mode)

let runtime = FusionRuntimeBuilder::new()
    .with_builtin_units()          // built-in Debug/Map/HTTP units
    .with_all_units()              // or on demand: with_datafusion() / with_redis() / ...
    .with_config(cfg)              // populate the config registry and inject into plugin dylibs
    .build()
    .await?;

// Concurrent graph execution
use fusion_runtime::GraphExecutor;
use std::sync::Arc;
let executor = GraphExecutor::new(Arc::new(runtime));
let gid = executor.submit("graph.yaml", None).await;
match executor.status(&gid).await {
    Some(fusion_runtime::GraphStatus::Done) => println!("graph done"),
    _ => {}
}
```

### Your First Graph

Run a built-in test graph (DataFusion stream JOIN):

```sh
cargo test -p fusion-unit-tests --test plugin_base_test -- test_datafusion_stream_join
```

More graph examples: [`tests/fusion-unit-tests/tests/graphs/`](tests/fusion-unit-tests/tests/graphs/).

## 🔧 Build & Test

```sh
# Build the workspace (Rust 2024 edition)
cargo build

# Check compilation only (faster)
cargo check

# Build plugin dylibs and install them into app/assets/libs/
bash scripts/build-plugins.sh               # debug
bash scripts/build-plugins.sh --release     # release
bash scripts/build-plugins.sh --only-capabilities
bash scripts/build-plugins.sh --only fusion-unit-datafusion

# Run all tests
cargo test

# Run a single integration test
cargo test -p fusion-unit-tests --test plugin_base_test -- test_simple_filesystem

# Tauri desktop app
cd app && yarn install && yarn tauri dev
```

## 📂 Directory Layout

```
Fusion/
├── fusion-runtime/              # Entry point: FusionRuntime / Builder / GraphExecutor
├── config/                      # Config files (app embedded / server cluster)
├── crates/
│   ├── fusion-streaming/        # Core engine: graph parsing, physical execution, plugin management, channels, storage
│   ├── fusion-unit-sdk/         # SDK: traits, Frame data model, capability / config systems
│   ├── fusion-derive/           # Proc macros (derive LogicTask / ScriptEngine)
│   ├── fusion-plugins/          # Plugins
│   │   ├── capability/          # Capability plugins (KeyValueStore, DataFusion engine, Redis)
│   │   └── units/               # Unit plugins (DataFusion, Excel, Redis, SSH, Net, generic filesystem)
│   └── scripts/fusion-script-ts # TypeScript script engine (deno_core)
├── app/                         # Tauri desktop app (standalone workspace)
│   └── assets/libs/             # Plugin dylib artifacts (capability/ + unit/)
├── tests/fusion-unit-tests/     # Integration tests (YAML graphs + plugin registration → execution)
├── scripts/build-plugins.sh     # Plugin dylib build script
└── doc/                         # Documentation
```

## 📖 Documentation

| Doc | Description |
|-----|-------------|
| [Architecture](doc/architecture.md) | Layered architecture, core concepts, execution model, backpressure, parallelism, scripts, capability/config systems, extension guide |
| [DataFusion Integration](doc/datafusion.md) | Three-image split of the SQL engine, C-ABI FFI, injection protocol, table registration flow, provider extension |

## 🧩 Plugin System

Fusion has two kinds of plugins, both loaded as dylibs at runtime:

- **Unit plugins** — provide graph node types (source / map / sink); implement the SDK `GraphUnitPlugin` and export `init_plugin`
- **Capability plugins** — provide process-global services (e.g. KV store, SQL engine); implement a `Capability*` trait and export `init_capability_plugin`

The host injects config and dependencies into plugin dylibs through three C symbols: `set_config` / `set_sql_engine_factory` / `set_host_providers` (see [DataFusion Integration](doc/datafusion.md#33-injection-protocol-host-dylib)).

## 🤝 Contributing

Issues and PRs are welcome. Before developing, please read the [Architecture](doc/architecture.md) and the development conventions in `CLAUDE.md`.

## License

License not yet specified — please confirm with the author before release.
