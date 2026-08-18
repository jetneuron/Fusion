# Fusion Architecture

> Version: 0.1 · applies to the current main branch · English terms and code identifiers kept consistent

## 1. Design Goals

Fusion is a **graph-based stream computing engine**. Users declare a "logical computation graph" in YAML/JSON (nodes = computing units, edges = data flow), and the engine compiles it into a "physical computation graph" executed over asynchronous mpsc channels. The design prioritizes:

| Goal | How |
|------|-----|
| **Declarative** | Computation logic defined in YAML — zero code assembly |
| **Embeddable** | `fusion-runtime` single-crate entry point; one-line startup for App/Tauri |
| **Plugin-based** | Two-layer dylib plugin system: unit (graph nodes) / capability (process-global services) |
| **Scalable** | Nodes scale horizontally via `parallelism`; built-in backpressure |
| **Multi-language scripting** | Lua (row transforms), Tera (templates), TypeScript (deno_core) |
| **Heterogeneous data sources** | Unified SQL access to CSV / SQLite / custom providers via DataFusion |

## 2. Layered Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                Fusion Runtime (upper layer)                  │
│   app/ (Tauri desktop app)  ·  user code                    │
├──────────────────────────────────────────────────────────────┤
│  fusion-runtime             entry: FusionRuntime / Builder   │
│                             config loading · plugin scan ·   │
│                             GraphExecutor                   │
├──────────────────────────────────────────────────────────────┤
│  fusion-streaming           core engine                      │
│   LogicalGraph parse · PhysicalGraph execute · plugin mgmt   │
│   mpsc channels · Lua/Tera script engines · Redis/ZK storage │
├──────────────────────────────────────────────────────────────┤
│  fusion-unit-sdk            shared SDK (one source, compiled │
│                             into every binary image)         │
│   LogicalTask / SourceUnit / MapUnit / TaskContext           │
│   Frame data model · capability · config · providers · FFI   │
├──────────────────────────────────────────────────────────────┤
│  fusion-derive              proc macros (Src/Map/Sink        │
│                             LogicTask)                      │
├──────────────────────────────────────────────────────────────┤
│  fusion-plugins             plugin dylibs (loaded on demand) │
│   capability/   unit/                                       │
└──────────────────────────────────────────────────────────────┘
```

Layer responsibilities:

- **SDK (`fusion-unit-sdk`)** — the shared contract between the engine and all implementations: trait definitions (`LogicalTask`, `SourceUnit`, `MapUnit`, `InitUnit`, `TaskContext`), the `Frame`/`Column` data model, error types, the capability system, the config system, the provider interface, and cross-dylib FFI types (`SqlEngineFactory`, `HostProviders`). The same SDK source compiles into every binary image, guaranteeing consistent layout across the FFI boundary.
- **Core engine (`fusion-streaming`)** — graph parsing, physical execution orchestration, plugin management (`PluginManager`), network channels, storage backends (Redis, ZooKeeper), script runtimes (Lua/mlua, Tera).
- **Proc macros (`fusion-derive`)** — `#[derive(SrcLogicTask)]` / `#[derive(MapLogicTask)]` / `#[derive(SinkLogicTask)]` generate the `LogicalTask` trait implementation; `#[derive(ScriptEngine)]` generates the script engine factory.
- **Runtime (`fusion-runtime`)** — `FusionRuntimeBuilder` wires together plugin loading, capability loading, config parsing, and graph execution; `FusionRuntime::init_app()` is the one-line startup for embedded mode; `GraphExecutor` provides concurrent graph submission, status query, and cancellation.
- **Plugins (`fusion-plugins`)** — two kinds: capability (process-global service implementations, e.g. `CapabilityKeyValueStore`, the DataFusion SQL engine) and unit (graph node types, e.g. DataFusion, Excel, SSH, Redis, generic filesystem).

## 3. Core Concepts

### 3.1 Logical Graph (LogicalGraph)

Parsed from YAML/JSON — everything the user sees:

```yaml
name: example_graph
units:                       # nodes (computing units)
  - id: input
    type: DebugInputUnitTask
    version: builtin
    config:
      times: 5
      column_count: 3
  - id: map
    type: MapUnitTask        # built-in Lua transformation node
    version: builtin
    config:
      $script: |
        local frame = ctx:newFrame()
        frame['x'] = data['c0'] * 2
        ctx:send(frame)
      $script_type: Lua
edges:                       # edges (data flow)
  - id: e1
    source: input
    target: map
  - id: e2
    source: map
    target: output
```

`LogicalGraph` (`units` + `edges`) converts into a petgraph `Graph<ComputingUnit, ComputingEdge>`.

### 3.2 Data Model (Frame / Column)

Data flows through the graph as `Frame` (protobuf-defined, SDK `proto::transfer`):

- **One frame = one row**: `columns: Vec<Column>`; each `Column` carries a field name `field`, a `DataType` (str/bool/f64/f32/i32/i64/bytes/json/unknown), a null marker, and the corresponding value slot.
- `Frame` also carries `source` (the upstream node ID), so downstream nodes can route by origin.
- The key reason for protobuf encoding: **data can cross the FFI boundary** — the host process, capability dylib, and unit dylib all exchange `Frame` byte streams (see [DataFusion Integration](datafusion.md)).

### 3.3 Node Roles

A node's role is determined by its edge counts (the runtime picks the call path by role):

| Role | Edge shape | Invocation |
|------|------------|------------|
| Source | incoming=0, outgoing>0 | `SourceUnit::launch()` emits frames |
| Map | incoming>0, outgoing>0 | `MapUnit::compute(frame, ctx)` per frame |
| Sink | incoming>0, outgoing=0 | `MapUnit::compute(frame, ctx)`, typically with side effects |

### 3.4 Physical Graph (PhysicalGraph / PhysicalTask)

`PhysicalGraph::execute()` converts the petgraph into `PhysicalTask` instances:

- **`TaskCore`** — holds a `LocalTaskChannel` (mpsc senders/receivers pre-allocated per outgoing edge) and `upstream_remain: Arc<AtomicI8>` (upstream EOF counter, so `on_eof` fires exactly once).
- **`LocalTaskChannel`** — `prepare_outputs(outgoing)` creates N mpsc channels at `set_unit` time; each `link()` pops one receiver; the source end keeps all senders in a `BackpressureSender` and broadcasts to them.
- **`BackpressureSender`** — per-node offset counter + barrier stamp; `send()` fans out to all senders in parallel via `futures::join_all` (a slow consumer never blocks fast ones).

## 4. Execution Model

```
                ┌──────────────────────────────────────────┐
                │ PhysicalGraph::execute                   │
                │                                          │
                │  LogicalGraph ──▶ PetGraph ──▶ Physical  │
                │                          │  Task         │
                │                          ▼               │
                │  DFS launch: all Source nodes launch()   │
                │  │                                       │
                │  ▼                                       │
                │  Source ──mpsc(1024)──▶ Map/Sink         │
                │  (launch)              (compute per frame│
                │                                          │
                │  EOF propagation → on_eof() → shutdown() │
                └──────────────────────────────────────────┘
```

1. **Compile**: `LogicalGraph` → petgraph (`ComputingUnit` nodes + `ComputingEdge` edges) → `PhysicalTask` instances.
2. **Launch**: DFS traversal calls `launch()` on source nodes (incoming=0); every node gets one async task that loops `recv` on its upstream channel.
3. **Transport**: every edge is a `tokio::sync::mpsc` channel (capacity 1024); frames flow edge by edge.
4. **Termination**: an upstream task sends EOF when it finishes; each node counts EOF per incoming edge via `upstream_remain`; once all upstreams have EOF'd, `on_eof()` fires once (aggregation/SQL finalization), then `shutdown()` cleans up the script environment. All join handles are awaited — the graph execution is done.

## 5. Backpressure

Backpressure is built into the transport layer — no watermark polling, no feedback channel: **the channel itself is the flow control**:

- Each edge is an mpsc channel with capacity 1024; `BackpressureSender::send()` awaits every `tx.send(frame)`, blocking automatically when the downstream buffer is full.
- Fan-out (1→N) sends in parallel (`join_all`): a slow consumer only blocks its own channel, never the other targets; each target has an independent buffer and independent backpressure.

## 6. Unit Parallelism

Any node can process inbound frames concurrently with `config.parallelism: N` (default 1):

```yaml
units:
  - id: map
    type: MapUnitTask
    config:
      parallelism: 4
```

### 6.1 Dispatch Model

```
             ┌──────────────┐
 upstream ───▶│  Dispatcher  │   edge-condition filter (single point, serial)
             └──────┬───────┘   round-robin dispatch of data frames
        ┌───────────┼───────────┐
        ▼           ▼           ▼
   ┌────────┐  ┌────────┐  ┌────────┐
   │worker 0│  │worker 1│  │worker N│   ← N independent tokio tasks,
   └────────┘  └────────┘  └────────┘     each with its own Lua VM
        └───────┴────┬────┴────────┘
                     ▼
            EOF drain → on_eof() → shutdown()
```

- **Dispatcher task (one per incoming edge)**: receives frames → edge-condition filter (serial, single point) → round-robin delivery to N workers; on EOF, broadcasts EOF to all workers, `join_all` waits for every worker to exit (all in-flight computes finish), then fires `on_eof()` and `shutdown()`.
- **Worker task**: each worker runs `target.internal_compute()` on a shared `Arc<Box<dyn LogicalTask + Send + Sync>>` — units keep mutable state behind an internal `Arc<Mutex>`, so sharing is lock-free.
- **Per-worker Lua VM** (`parallelism > 1`): each worker owns an independent `Arc<Mutex<Lua>>` with a private scope table (`init_script_env` creates N VMs), so Lua scripts run truly in parallel instead of contending on the global `GraphLua`. `TaskContext.worker_lua` carries the worker's VM; `row_eval` and Redis `this` injection prefer it, falling back to the global `GraphLua`.
- With `parallelism = 1` this degenerates to a single-worker passthrough, behaviorally identical to the serial version.

### 6.2 Semantics Contract (declared)

- `parallelism > 1` does **not** guarantee row order downstream.
- Scripts must be **stateless** — the same script is replicated to N Lua VMs; cross-row state splits across workers.
- Lock-free parallel compute requires units to be `Sync` (all built-in units satisfy this via internal `Arc<Mutex>` state).

## 7. Script Engines

Each graph execution creates its own script contexts (concurrent graphs are fully isolated):

| Engine | Purpose | Implementation |
|--------|---------|----------------|
| **Lua** | Row transforms (`$script` on `MapUnitTask`), edge-condition filtering | `mlua` (lua54). Default: one global `GraphLua` shared VM + per-unit private scope tables; with `parallelism > 1`, each worker gets its own VM |
| **Tera** | `LaunchEnv` param/env templates at graph startup (e.g. `{{ time() }}`); Redis dynamic scripts (`ScriptMode::Dynamic`) | `GraphTera` state |
| **TypeScript** | Script engine extension | `fusion-script-ts` (deno_core) |

Script signature `(ctx, data, this)`: `ctx` exposes `send(frame)` / `newFrame()` (or `newRow()`) and friends; `this` is a capability-injected userdata (e.g. the Redis KV store). Edge conditions are Lua one-liners compiled into `function(row) ... end` that return a bool.

## 8. Capability System

Capabilities are **process-global services** consumed by unit plugins (e.g. Redis key-value store, DataFusion SQL engine), with a defined lifecycle:

```
register → init → (use) → shutdown
```

- Trait definitions live in the SDK `capability/` directory, all prefixed `Capability*`; each trait file has a `well_known` sub-module listing canonical implementation names.
- Process-global registry: `capability::read()` / `capability::register()`.
- Capability plugins live in `crates/fusion-plugins/capability/`, loaded as dylibs (`PluginManager::load_capability_plugin`); they implement `CapabilityPlugin` and export the `init_capability_plugin` symbol.

## 9. Config System

Datasource and other configuration is centralized in the `ConfigRegistry`:

```yaml
# config/fusion-conf.yaml
config:
  datasource:
    redis:
      redis-cache:
        host: localhost
        port: 6379
  setting:
    pool:
      default:
        max_size: 16
```

- Three-level hierarchy: **category → type → id → data** (e.g. `datasource → redis → redis-cache → {host, port}`).
- `DataSourceConfig` trait + `GenericDataSourceConfig` + `ConfigRegistry` in the SDK; `FileConfigProvider` (YAML) and `ProgrammaticConfigProvider` (code) are the two provider styles.
- Typed access: `config::read_config().get_typed::<RedisDataSourceConfig>("redis-cache")`.
- **dylib injection**: the host injects a live config query API into each dylib (`set_host_config` → `HostConfigApi` fn table). Dylibs query the host registry on every `config::read()`, so entries registered after dylib load — including the YAML `config:` section populated at `FusionRuntimeBuilder::build()` time — are always current. `set_config` (a load-time JSON snapshot) remains as a fallback for older dylibs that don't export `set_host_config`.

## 10. Plugin System

### 10.1 Plugin Loading (PluginManager)

`PluginManager` maps `"{type}#{version}"` keys to `GraphUnitPlugin` implementations; built-in units are always registered, external `.dylib`/`.so` plugins can be loaded at runtime.

The injection protocol for plugin dylibs (host → dylib):

| Symbol | Content | Direction |
|--------|---------|-----------|
| `set_host_config` | Live config query API (`HostConfigApi` fn table) — dylibs refresh from the host registry on every read | host → all dylibs |
| `set_config` | Config registry as JSON (**legacy snapshot** — fallback for dylibs without `set_host_config`) | host → all dylibs |
| `set_sql_engine_factory` | `SqlEngineFactory` function table | host → unit dylib |
| `set_host_providers` | Provider objects (fat pointer transfer) | host → unit dylib |

`PluginManager` keeps every loaded `Library` handle alive (to prevent dangling vtables).

### 10.2 Built-in Units

The engine ships `DebugInputUnitTask` (data generation), `DebugMapUnitTask`, `MapUnitTask` (Lua transforms), `DebugOutputUnitTask` (output/report) and other debug/general-purpose nodes, plus an example unit (`graph-unit-example`).

### 10.3 Third-party Units

Standalone crates implementing the SDK traits + `GraphUnitPlugin`, exporting `init_plugin`. See the [Extension Guide](#12-extension-guide).

## 11. DataFusion Integration

SQL execution is split across **three binary images** — only the capability dylib contains DataFusion code (details in [DataFusion Integration](datafusion.md)):

- **capability** (`fusion-capability-datafusion`) — the process-global `SessionContext`, exposed through the C-ABI `SqlEngineFactory` function table.
- **unit** (`fusion-unit-datafusion`) — engine-free; wraps the function table as `FfiSqlEngine`.
- **provider** (`fusion-unit-datafusion-*`) — data-source adapters (e.g. SQLite) producing `Frame` streams.

Data crosses the FFI boundary; types stay in their owning image — the host injects all cross-image dependencies through `set_*` symbols.

## 12. Extension Guide

### Adding a unit type

1. Create `crates/fusion-plugins/units/fusion-unit-<name>/` with `crate-type = ["cdylib", "lib"]`.
2. Define a struct with `#[derive(Default, SrcLogicTask)]` (or `MapLogicTask` / `SinkLogicTask`).
3. Implement `InitUnit` (parse config) and `SourceUnit` / `MapUnit` (business logic).
4. Implement `GraphUnitPlugin` and export the `init_plugin` FFI symbol.
5. Register it in the test harness and write a YAML test graph.

### Adding a capability type

1. Create `crates/fusion-plugins/capability/fusion-capability-<name>/` with `crate-type = ["cdylib", "lib"]`.
2. Implement a `Capability*` trait from the SDK `capability::` module.
3. Implement `CapabilityPlugin` and export the `init_capability_plugin` FFI symbol.
4. Register via `capability::register(|reg| reg.set_xxx(Arc::new(...)))`.

### Adding a DataFusion table provider

See [DataFusion Integration · Extension Guide](datafusion.md#6-extension-guide).
