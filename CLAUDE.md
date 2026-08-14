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
    │   │   ├── fusion-capability-example/
    │   │   └── fusion-capability-datafusion/   ← DataFusion SQL 引擎能力
    │   └── units/               ← 单元插件（提供图节点类型，如 DataFusion、Excel、SSH）
    │       ├── fusion-unit-datafusion/         ← SqlUnitTask + provider 插件体系
    │       │   └── providers/
    │       │       └── fusion-unit-datafusion-sqlite/  ← SQLite 表 provider
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

Fusion is a graph-based stream computing engine. Users define **logical graphs** in YAML/JSON that describe nodes (computing units) and edges (data flow). The engine compiles these into a **physical graph** and executes them asynchronously over mpsc channels with built-in backpressure and configurable per-node parallelism.

### Layers

1. **SDK** (`crates/fusion-unit-sdk`) — trait definitions shared by the engine and all implementations: `LogicalTask` (`Send + Sync`), `SourceUnit`, `MapUnit`, `InitUnit`, `TaskContext`, the `Row`/`Column` data model, error types, capability system, config system, and script engine interfaces.
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
- `TaskCore` — holds the `LocalTaskChannel` (mpsc senders/receivers pre-allocated per outgoing edge) and `upstream_remain: Arc<AtomicI8>` for EOF tracking.
- `LocalTaskChannel` — `prepare_outputs(outgoing)` creates N mpsc channels at `set_unit` time. Each `link()` call pops one receiver; the source's `BackpressureSender` holds all senders and fans out to them.
- `BackpressureSender` — per-node offset counter + barrier_ref stamping. `send()` fans out to all mpsc senders in parallel (`join_all`); mpsc provides built-in backpressure.
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
- The registry is injected into dylibs as JSON (`InjectedConfig` entries) via their `set_config` C symbol; the YAML `config:` section (category → type → id → data) populates it at runtime build

### Node roles

A node's role is determined by its edge count:
- **Source** (incoming=0, outgoing>0) → calls `SourceUnit::launch()` to emit rows
- **Map** (incoming>0, outgoing>0) → calls `MapUnit::compute(row, ctx)` per incoming row
- **Sink** (incoming>0, outgoing=0) → calls `MapUnit::compute(row, ctx)`, typically with side effects

### Backpressure

Backpressure is built into the transport: each edge is a `tokio::sync::mpsc` channel (capacity 1024). `BackpressureSender::send()` awaits each `tx.send(row)`, which blocks automatically when the downstream buffer is full. No watermark polling or feedback channel exists — flow control is the channel itself.

Fan-out (1→N) uses parallel sends (`futures::join_all`): a slow consumer does not block fast ones. Each target has an independent buffer and independent backpressure.

### Unit parallelism

Any node can configure `parallelism: N` (default 1) in its YAML config to process incoming rows concurrently:

```yaml
units:
  - id: map
    type: MapUnitTask
    config:
      parallelism: 4
```

- **Dispatcher + workers**: for each incoming edge, one dispatcher task receives rows, applies the edge-condition filter (single point, serial), and round-robins data rows to N worker tasks.
- **Workers**: each worker runs `target_logical.internal_compute()` on the shared `Arc<Box<dyn LogicalTask + Send + Sync>>` — lock-free, since units keep mutable state behind `Arc<Mutex>` internally.
- **EOF drain**: the dispatcher sends EOF to all workers, joins their handles (guaranteeing all in-flight computes finish), then fires `on_eof` and `shutdown`.
- **Per-worker Lua VM** (`parallelism > 1`): each worker owns its own `Arc<Mutex<Lua>>` with a private scope table (`init_script_env` creates N VMs), so Lua scripts run truly in parallel instead of contending on the global `GraphLua`. `TaskContext.worker_lua` carries the worker's VM; `row_eval`/Redis `this`-injection prefer it, falling back to global `GraphLua`.

**Semantics** (declared contract):
- `parallelism > 1` does **not** guarantee row order downstream.
- Scripts must be **stateless** — the same script is replicated to N Lua VMs; cross-row state in scripts splits across workers.
- Lock-free parallel compute requires units to be `Sync` (all built-in units satisfy this via internal `Arc<Mutex>` state).

### Script execution

Two script contexts are available per graph execution:
- **Lua**: used for `MapUnitTask` row transformations and edge-condition filtering. By default a single global `GraphLua` VM (`Arc<Mutex<Lua>>` registered in `GraphStates`) is shared; each unit gets a private scope table (`globals[unit_id]`) holding the compiled script function (`func`) and optional `this` userdata. With `parallelism > 1`, each worker gets its own VM instead (see Unit parallelism). The script signature is `(ctx, data, this)` where `ctx` exposes `send(row)`/`newRow()` and `this` is a capability-injected userdata (e.g. Redis KV store).
- **Tera** (`GraphTera` state): used for template rendering in `LaunchEnv` params/env at graph startup (e.g., `{{ time() }}`) and per-row dynamic scripts in Redis (`ScriptMode::Dynamic`).

Edge conditions are Lua one-liners compiled into a `function(row) ... end` that return a bool.

### DataFusion unit (`SqlUnitTask`)

SQL execution is split across three binary images — only the capability dylib contains DataFusion code:

- **Capability** (`fusion-capability-datafusion`): owns the process-global `SessionContext` and all DataFusion types. Exposes the C-ABI `SqlEngineFactory` table (`create_engine`/`query`/`register_frame_table`/`finalize_frame_table`/`register_csv_table`/`deregister_table`/`drop_engine`, from SDK `sql_engine_ffi`) via `init_sql_engine_factory`.
- **Unit** (`fusion-unit-datafusion`): engine-free. Wraps the factory as `FfiSqlEngine` implementing SDK `CapabilitySqlEngine`; each call hops to a worker thread (`std::thread` + tokio oneshot — async never crosses the C ABI) and blocks on the engine's runtime.
- **Providers** (`fusion-unit-datafusion-<name>`, e.g. `-sqlite`): data-source adapters. Implement SDK `TableDataProvider::load_frames(sql) -> Vec<Frame>` (rusqlite, …) and `ProviderPlugin::register_providers()`.

**Isolation model**: Rust statics/vtables are per-binary-image, so the host injects everything dylibs need through `set_*` symbols — `set_config` (config registry JSON), `set_sql_engine_factory` (unit ← capability), `set_host_providers` (unit ← provider fat pointers). Data crosses FFI as protobuf `Frame`s; engine/provider types never leave their owning image.

**Layout consistency**: SDK types passed by value across images (`ComputingUnit`, `serde_json::Value`) must have identical memory layout in every image. `serde_json` `preserve_order` changes `Value::Map` (IndexMap vs BTreeMap → different size), which shifts `ComputingUnit.config`'s offset — if the host tree enables it (e.g. via `deno_core`) but a dylib doesn't, the dylib reads garbage discriminants and crashes (SIGILL in `Value::clone`'s jump table). `fusion-unit-sdk` pins `preserve_order` so all images agree; never gate it behind features.

- **Static tables** (`tables` in YAML): reference a `provider` (e.g. `csv`, `sqlite`) and a `config_id` into the central `ConfigRegistry`. `csv` resolves the file path from the registry and calls `register_csv_table`; other providers look up `HOST_PROVIDERS["{provider}#{config_id}"]`, pull frames via `load_frames(sql)`, and register them as frame tables.
- **Stream tables** (`stream_tables`): upstream frames are buffered per source (`tokio::sync::Mutex<Vec<Frame>>`, parallel-worker safe); at EOF each buffer drains → `register_frame_table` + `finalize_frame_table` → SQL runs → results emit downstream → tables deregister (no leak into other graphs sharing the session). Registration is last-writer-wins so parallel graphs reusing table names don't error.
- **Node roles**: as a source (`incoming=0`) it executes immediately in `launch()`; as a map it buffers stream rows in `compute()` and executes in `on_eof()`.

### Adding a new unit type

1. Create `crates/fusion-plugins/units/fusion-unit-<name>/` with `crate-type = ["cdylib", "lib"]`.
2. Define a struct with `#[derive(Default, SrcLogicTask)]` (or `MapLogicTask`/`SinkLogicTask`).
3. Implement `InitUnit` (parse config) and `SourceUnit`/`MapUnit` (business logic).
4. Implement `GraphUnitPlugin` and export `init_plugin` FFI symbol.
5. Register in the test harness and write a YAML test graph.

### Adding a new table provider (DataFusion)

1. Create `crates/fusion-plugins/units/fusion-unit-datafusion/providers/fusion-unit-datafusion-<name>/` with `crate-type = ["cdylib", "lib"]`.
2. Implement SDK `TableDataProvider` — async `load_frames(sql)` returning one protobuf `Frame` per row (probe column names by running the query, then iterate the result set).
3. Implement `ProviderPlugin` — `register_providers()` returns `Vec<(String, Arc<dyn TableDataProvider>)>` keyed `"{provider}#{config_id}"` (one entry per datasource config it owns, read from the injected config registry).
4. Export `init_provider_plugin` FFI symbol (host scans for it when loading provider dylibs).
5. Reference it from YAML as `provider: <name>` with a matching `config_id`; static tests inject via `register_providers()` + `inject_providers()`.

### Adding a new capability type

1. Create `crates/fusion-plugins/capability/fusion-capability-<name>/` with `crate-type = ["cdylib", "lib"]`.
2. Implement a `Capability*` trait from `fusion-unit-sdk::capability`.
3. Implement `CapabilityPlugin` and export `init_capability_plugin` FFI symbol.
4. Register via `capability::register(|reg| reg.set_xxx(Arc::new(...)))`.
