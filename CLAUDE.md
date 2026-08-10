# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Structure

```
Fusion/
├── fusion-runtime/              ← 主入口（独立 crate，lib 初始化 + 配置加载 + 插件扫描）
├── config/                      ← 配置文件
│   ├── fusion-conf-app.yaml     ← 嵌入 app 模式（singleton）
│   └── fusion-conf.yaml         ← 服务器模式（cluster）
├── scripts/
│   └── build-plugins.sh         ← 构建 capability/unit dylib 并复制到 app/assets/libs/
├── tests/
│   └── fusion-unit-tests/       ← 集成测试（YAML 图 + 插件注册 → 执行）
├── app/                         ← Tauri 桌面应用（独立 workspace）
│   └── assets/libs/             ← 编译产物：capability/ + unit/ dylib
└── crates/
    ├── fusion-streaming/        ← 核心引擎（图解析、物理执行、插件管理、通道、存储）
    ├── fusion-unit-sdk/         ← SDK（LogicalTask / SourceUnit / MapUnit trait、
    │                                Row/Column 数据模型、capability 系统、config 系统）
    ├── fusion-derive/           ← proc macros（SrcLogicTask / MapLogicTask / SinkLogicTask / ScriptEngine）
    ├── fusion-plugins/          ← 所有插件
    │   ├── capability/          ← 能力插件（实现 Capability* trait，如 KeyValueStore）
    │   │   └── fusion-capability-example/
    │   └── units/               ← 单元插件（提供图节点类型，如 DataFusion、Excel、SSH）
    │       ├── fusion-unit-datafusion/
    │       ├── fusion-unit-excel/
    │       ├── fusion-unit-net/
    │       ├── fusion-unit-redis/
    │       ├── fusion-unit-ssh/
    │       └── fusion-unit-universal-fs/
    └── scripts/
        └── fusion-script-ts/    ← TypeScript 脚本引擎（deno_core）
```

## Build & Test

```sh
# Build everything
cargo build

# Build with specific features
cargo build -p fusion-streaming --features "common,trace-physical,trace-logical"

# Build and install plugin dylibs
bash scripts/build-plugins.sh                 # debug
bash scripts/build-plugins.sh --release        # release
bash scripts/build-plugins.sh --only-capabilities  # only capability crates
bash scripts/build-plugins.sh --only fusion-unit-datafusion  # single crate

# Run all tests
cargo test

# Run a single integration test
cargo test -p fusion-unit-tests --test plugin_base_test -- test_simple_filesystem

# Check compilation only (faster than full build)
cargo check

# Tauri app
cd app && yarn install && yarn tauri dev
```

Edition is Rust 2024 with resolver 3.

## Architecture

Fusion is a graph-based stream computing engine. Users define **logical graphs** in YAML/JSON that describe nodes (computing units) and edges (data flow). The engine compiles these into a **physical graph** and executes them asynchronously with backpressure-controlled channels.

### Layers

1. **SDK** (`crates/fusion-unit-sdk`) — trait definitions shared by the engine and all implementations: `LogicalTask`, `SourceUnit`, `MapUnit`, `InitUnit`, `TaskContext`, `Watermark`, the `Row`/`Column` data model, error types, capability system, config system, and script engine interfaces.
2. **Core engine** (`crates/fusion-streaming`) — graph parsing, physical execution orchestration, plugin management, network channels, storage backends (Redis, ZooKeeper), and script runtimes (Lua via mlua, Tera templates).
3. **Proc macros** (`crates/fusion-derive`) — `#[derive(SrcLogicTask)]`, `#[derive(MapLogicTask)]`, `#[derive(SinkLogicTask)]` generate the `LogicalTask` trait impl. `#[derive(ScriptEngine)]` generates the `ScriptEngineFactory` impl.
4. **Runtime** (`fusion-runtime/`) — the main entry point. `FusionRuntimeBuilder` wires together plugin loading, capability loading, config parsing, and graph execution. `FusionRuntime::init_app()` is the one-line startup for embedded mode.
5. **Plugins** (`crates/fusion-plugins/`) — two categories:
   - **Capability** (`capability/`): provide trait implementations consumed by unit plugins (e.g. `CapabilityKeyValueStore`). Loaded via `PluginManager::load_capability_plugin()`.
   - **Unit** (`units/`): provide task types (source/map/sink) referenced in graph YAML. Loaded via `PluginManager::register_plugin()`.
6. **Tests** (`tests/`) — integration tests load YAML graphs, register plugins, and execute.

### Key types and flow

- `LogicalGraph` — a `units` vec + `edges` vec parsed from YAML/JSON. Converts into a `PetGraph` (petgraph `Graph<ComputingUnit, ComputingEdge>`).
- `PhysicalGraph::execute()` — converts the petgraph to `PhysicalTask` instances, wires channels between neighbors, handles edge-condition filtering (Lua predicates on edges), launches source nodes via DFS traversal, and awaits all join handles.
- `TaskCore` — holds the `LocalTaskChannel` (tokio broadcast), the `ComputingUnit` config, and a backpressure `Watermark`.
- `PluginManager` — maps `"{type}#{version}"` keys to `GraphUnitPlugin` implementations. Built-in units are always registered; external `.dylib`/`.so` plugins can be loaded at runtime.

### Capability system

Capabilities are process-global services that unit plugins consume. They have a defined lifecycle:

```
register → init → (use) → shutdown
```

- Trait definitions in `crates/fusion-unit-sdk/src/capability/` (all prefixed `Capability*`)
- Each trait file has a `well_known` sub-module listing canonical implementation names
- Global registry via `capability::read()` / `capability::register()`
- Capability plugins live in `crates/fusion-plugins/capability/`

### Config system

Datasource configurations are centralized in `ConfigRegistry`:

```yaml
# config/fusion-conf.yaml
datasources:
  redis-cache:
    type: redis
    host: localhost
    port: 6379
```

- `DataSourceConfig` trait + `GenericDataSourceConfig` + `ConfigRegistry` in SDK
- `FileConfigProvider` (YAML) + `ProgrammaticConfigProvider` (code)
- Typed access via `config::read_config().get_typed::<RedisDataSourceConfig>("redis-cache")`

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

1. Create `crates/fusion-plugins/units/fusion-unit-<name>/` with `crate-type = ["cdylib", "lib"]`.
2. Define a struct with `#[derive(Default, SrcLogicTask)]` (or `MapLogicTask`/`SinkLogicTask`).
3. Implement `InitUnit` (parse config) and `SourceUnit`/`MapUnit` (business logic).
4. Implement `GraphUnitPlugin` and export `init_plugin` FFI symbol.
5. Register in the test harness and write a YAML test graph.

### Adding a new capability type

1. Create `crates/fusion-plugins/capability/fusion-capability-<name>/` with `crate-type = ["cdylib", "lib"]`.
2. Implement a `Capability*` trait from `fusion-unit-sdk::capability`.
3. Implement `CapabilityPlugin` and export `init_capability_plugin` FFI symbol.
4. Register via `capability::register(|reg| reg.set_xxx(Arc::new(...)))`.
