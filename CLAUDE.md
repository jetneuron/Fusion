# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```sh
# Build everything (default features: common = http + excel)
cargo build

# Build with specific features
cargo build -p fusion-streaming --features "common,trace-physical,trace-logical"

# Run all tests
cargo test

# Run a single integration test
cargo test -p fusion-unit-tests --test plugin_base_test -- test_simple_filesystem

# Run with trace logging (exercise caution: trace-logical may produce huge output)
RUST_LOG=trace cargo test -p fusion-unit-tests -- test_simple_map --nocapture

# Check compilation only (faster than full build)
cargo check
```

There is no Makefile, justfile, or custom toolchain config. Edition is Rust 2024 with resolver 3.

## Architecture

Fusion is a graph-based stream computing engine. Users define **logical graphs** in YAML/JSON that describe nodes (computing units) and edges (data flow). The engine compiles these into a **physical graph** and executes them asynchronously with backpressure-controlled channels.

### Layers

1. **SDK** (`fusion-unit-sdk`) — trait definitions shared by the engine and all unit implementations: `LogicalTask`, `SourceUnit`, `MapUnit`, `InitUnit`, `TaskContext`, `Watermark`, the `Row`/`Column` data model, error types, and script engine interfaces.
2. **Core engine** (`fusion-streaming`) — graph parsing, physical execution orchestration, plugin management, network channels, storage backends (Redis, ZooKeeper), and script runtimes (Lua via mlua, Tera templates, TypeScript via deno_core).
3. **Proc macros** (`fusion-derive`) — `#[derive(SrcLogicTask)]`, `#[derive(MapLogicTask)]`, `#[derive(SinkLogicTask)]` generate the `LogicalTask` trait impl (including `internal_launch`, `internal_compute`, and `event` dispatch). `#[derive(ScriptEngine)]` generates the `ScriptEngineFactory` impl.
4. **Unit plugins** (`fusion-unit-*` crates) — concrete implementations: DataFusion for SQL, Excel read/write, SSH, Redis, universal filesystem sources, network I/O.
5. **Script engine** (`fusion-script-ts`) — TypeScript/Deno runtime integration for script-based unit logic.
6. **Integration tests** (`fusion-unit-tests`) — each test loads a YAML graph definition, registers all needed plugins into a `SandboxRuntime`, and executes the graph.

### Key types and flow

- `LogicalGraph` — a `units` vec + `edges` vec parsed from YAML/JSON. Converts into a `PetGraph` (petgraph `Graph<ComputingUnit, ComputingEdge>`).
- `PhysicalGraph::execute()` — converts the petgraph to `PhysicalTask` instances, wires channels between neighbors, handles edge-condition filtering (Lua predicates on edges), launches source nodes via DFS traversal, and awaits all join handles.
- `TaskCore` — holds the `LocalTaskChannel` (tokio broadcast), the `ComputingUnit` config, and a backpressure `Watermark`.
- `PluginManager` — maps `"{type}#{version}"` keys to `GraphUnitPlugin` implementations. Built-in units are always registered; external `.dylib`/`.so` plugins can be loaded at runtime.

### Node roles

A node's role is determined by its edge count:
- **Source** (incoming=0, outgoing>0) → calls `SourceUnit::launch()` to emit rows
- **Map** (incoming>0, outgoing>0) → calls `MapUnit::compute(row, ctx)` per incoming row
- **Sink** (incoming>0, outgoing=0) → calls `MapUnit::compute(row, ctx)`, typically with side effects

### Backpressure

Each `PhysicalTask` has a `Watermark` (max/high/low thresholds). When `send_offset - recv_offset >= max`, the sender pauses; when >= high, it slows down. Each node sends watermark acknowledgements back upstream via a separate internal/feedback channel.

### Script execution

Two script contexts are available per graph execution:
- **Lua** (`GraphLua` state): used for `MapUnitTask` row transformations and edge-condition filtering. Each unit gets its own Lua function (`lua_script_{task_id}`) with `ctx` and `data` (row) params.
- **Tera** (`GraphTera` state): used for template rendering in `LaunchEnv` params/env at graph startup (e.g., `{{ time() }}`).

Edge conditions are Lua one-liners compiled into a `function(row) ... end` that return a bool.

### Adding a new unit type

1. Define a struct with `#[derive(Default, SrcLogicTask)]` (or `MapLogicTask`/`SinkLogicTask`).
2. Implement `InitUnit` (parse config) and `SourceUnit`/`MapUnit` (business logic).
3. Register it in a `GraphUnitPlugin::register_units()` via `YourTask::register_unit(&mut manifest, version)`.
4. Add the plugin to the test harness (`TestPlugin::register_units()` in `plugin_base_test.rs`) and write a YAML test graph.
