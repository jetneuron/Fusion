# DataFusion Integration (SqlUnitTask)

> Fusion provides SQL capability through the `SqlUnitTask` node. This design splits SQL execution across **three binary images** — only the capability dylib contains DataFusion code.

## 1. Background: Rust dylib Image Isolation

Once Rust compiles to a cdylib, **every binary image** (the host executable / each dylib) holds its own copy of:

- Global statics and registries (e.g. the `config` registry, the capability registry)
- Trait vtables and `Arc` / `Box` layouts
- tokio TLS (runtime handles, etc.)

So the host cannot pass its process-local objects to a dylib — a `OnceLock` inside the dylib is a different instance. The same SDK source compiles into every image with consistent layout, so **passing fat pointers / transferring `Box` ownership across FFI is safe**, but every dependency must be injected through the explicit `set_*` protocol. The config registry is injected as a **live query API** (`set_host_config`): dylibs call back into the host on every read, so entries registered after dylib load are visible immediately — no stale load-time snapshot.

## 2. Three-Image Architecture

```
┌─────────────────────────────┐
│ Host (fusion-runtime/        │
│       fusion-streaming)      │
│                              │
│  PluginManager               │
│   └─ _libs: keep dylibs alive│
└──────┬──────────┬────────────┘
       │          │
 inject at load:  │  set_host_providers(provider fat pointers)
 set_host_config │        ┌──────────────────────────────────┐
       │          ▼        │ fusion-unit-datafusion (unit)    │
       │  ┌─────────────────────────────────┐   engine-free, ~1.3M │
       │  │ fusion-capability-datafusion    │  ENGINE_FACTORY    │
       │  │ the only DataFusion image, ~99M │  HOST_PROVIDERS    │
       │  │ SessionContext (process-global) │◀─────────────────┘
       │  │ implements SDK CapabilitySqlEngine│  FfiSqlEngine (wrapper)
       │  └───────────────┬─────────────────┘  C-ABI function table
       │                  │
       │  set_sql_engine_factory(SqlEngineFactory)
       │  (repr(C) function table, 7 extern "C" fn pointers)
       ▼
┌─────────────────────────────────┐
│ fusion-unit-datafusion-<name>  │  provider dylib (e.g. -sqlite, ~2.1M)
│ SqliteTableDataProvider        │  reads DB directly via rusqlite → Frame
│ implements SDK TableDataProvider│
│ exports init_provider_plugin   │
└─────────────────────────────────┘
```

| Image | Content | Depends on DataFusion | Size (debug) |
|-------|---------|:---:|:---:|
| `fusion-capability-datafusion` | Process-global `SessionContext`, all DataFusion types, CSV registration, frame-table management | ✅ | ~99M |
| `fusion-unit-datafusion` | `SqlUnitTask` node, `FfiSqlEngine` wrapper, static/stream table orchestration | ❌ | ~1.3M |
| `fusion-unit-datafusion-sqlite` | SQLite datasource adapter (rusqlite) | ❌ | ~2.1M |

## 3. FFI Interface (SDK `sql_engine_ffi`)

### 3.1 `SqlEngineFactory` (repr(C) function table)

Exported by the capability via `init_sql_engine_factory`, received by the unit via `set_sql_engine_factory`:

```rust
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SqlEngineFactory {
    pub create_engine: unsafe extern "C" fn() -> *mut c_void,
    pub query: unsafe extern "C" fn(*mut c_void, *const c_char, *mut *mut Vec<Frame>) -> i32,
    pub register_frame_table: unsafe extern "C" fn(*mut c_void, *const c_char, *mut Vec<Frame>) -> i32,
    pub finalize_frame_table: unsafe extern "C" fn(*mut c_void, *const c_char) -> i32,
    pub register_csv_table: unsafe extern "C" fn(*mut c_void, *const c_char, *const c_char) -> i32,
    pub deregister_table: unsafe extern "C" fn(*mut c_void, *const c_char) -> i32,
    pub drop_engine: unsafe extern "C" fn(*mut c_void),
}
```

Data crosses the boundary as raw `Box<Vec<Frame>>` pointers (`Box::into_raw` / `Box::from_raw` ownership transfer); frame contents are protobuf bytes — **engine types never leave the capability image**.

### 3.2 Async Never Crosses the C ABI

- **Capability side**: runs its own tokio runtime (`OnceLock<Runtime>`) and executes synchronously via `block_on()` (the sync `register_table` API is called directly; `register_csv` is async and `block_on`-ed inside its own runtime).
- **Unit side**: each call hops to a `std::thread` + `tokio::sync::oneshot` and blocks on the result. **Do not use `tokio::task::spawn_blocking`** — unit code runs on the host runtime's tokio workers, the unit image's tokio TLS is empty, and it would panic with "no reactor running"; `oneshot` needs no runtime context and resolves once the thread returns.
- Raw pointers captured by closures are passed as `usize` and re-cast inside — this sidesteps closure disjoint-field-capture issues with the wrapper struct.

### 3.3 Injection Protocol (host → dylib)

| C symbol | Content | Direction |
|----------|---------|-----------|
| `set_host_config(HostConfigApi)` | Live config query API (`list_all` / `release` fn table); dylibs refresh from the host registry on every read — entries registered after dylib load stay visible | host → all dylibs |
| `set_config(*const c_char)` | Config registry serialized as `Vec<InjectedConfig>` JSON (**legacy snapshot** — fallback for dylibs without `set_host_config`; goes stale on later host registration) | host → unit / provider |
| `set_sql_engine_factory(SqlEngineFactory)` | Capability's function table | host → unit |
| `set_host_providers(HostProviders)` | Provider object collection, fat pointer transfer | host → unit |

Provider keys follow **`"{provider}#{config_id}"`** (e.g. `sqlite#sqlite-test-db`); the unit looks them up with `format!("{}#{}", provider, config_id)`.

## 4. Table Registration Flow

### 4.1 Static Tables (`tables` in YAML)

```yaml
units:
  - id: sql-reader
    type: SqlUnitTask
    config:
      datasource: datafusion
      sql: "SELECT * FROM my_csv"
      tables:
        - name: my_csv
          provider: csv
          config_id: csv-file-1
```

- `provider: csv`: resolve `path` from the `ConfigRegistry` → `engine.register_csv_table(name, path)` (the capability reads the file).
- Other providers: look up `HOST_PROVIDERS["{provider}#{config_id}"]` → `load_frames(sql)` (the provider produces `Vec<Frame>`) → `register_frame_table` + `finalize_frame_table`.

### 4.2 Stream Tables (`stream_tables`)

```yaml
units:
  - id: sql-join
    type: SqlUnitTask
    config:
      datasource: datafusion
      sql: "SELECT a.c0, b.c0 FROM stream_a a JOIN stream_b b ON a.c0 = b.c0"
      stream_tables:
        - name: stream_a
          source: src-a
        - name: stream_b
          source: src-b
```

1. Frames from upstream `src-a` / `src-b` are buffered per source in `tokio::sync::Mutex<Vec<Frame>>` (parallel-worker safe).
2. At the node's EOF (`on_eof`), drain each source's buffer → `register_frame_table` → `finalize_frame_table` (the accumulator is marked finalized).
3. Execute SQL; result frames are sent downstream.
4. `deregister_table` cleanup — no leak into other graphs sharing the same session.

**Concurrency safety**: parallel graphs share the process-global `SessionContext` and may reuse table names (e.g. concurrent tests both use `stream_a`). Registration is last-writer-wins — `register_frame_table` overwrites an already-finalized accumulator directly; `finalize_frame_table` deregisters and re-registers under the `ctx` lock (DataFusion returns Err on same-name registration).

### 4.3 Node Roles

- **source** (incoming=0): executes the query immediately in `launch()`.
- **map / sink** (incoming>0): `compute()` buffers stream frames, `on_eof()` executes the query; declaring `stream_tables` is mandatory for map/sink roles (otherwise `compute()` would re-run the SQL per frame).

## 5. Provider Interface (SDK `providers`)

```rust
/// Datasource adapter: produces Frame streams consumable by the SQL engine.
#[async_trait]
pub trait TableDataProvider: Send + Sync {
    async fn load_frames(&self, sql: Option<&str>) -> UnitResult<Vec<Frame>>;
}

/// Plugin entry: registers the set of datasource providers it owns.
pub trait ProviderPlugin: Send + Sync {
    fn register_providers(&self) -> Vec<(String, Arc<dyn TableDataProvider>)>;
}
```

The keys returned by `register_providers()` are `"{provider}#{config_id}"` — a provider dylib emits one entry per datasource config it owns (read from the host registry via the injected live config API).

Static in-process tests skip dylib loading and inject directly:

```rust
let providers = fusion_unit_datafusion_sqlite::SqliteProviderPlugin.register_providers();
fusion_unit_datafusion::inject_providers(providers);
```

## 6. Extension Guide

### Adding a table provider

1. Create `crates/fusion-plugins/units/fusion-unit-datafusion/providers/fusion-unit-datafusion-<name>/` with `crate-type = ["cdylib", "lib"]`.
2. Implement SDK `TableDataProvider` — `load_frames(sql)` produces one protobuf `Frame` per row (probe column names with `SELECT * FROM ({query}) LIMIT 1`, then iterate the result set).
3. Implement `ProviderPlugin` — `register_providers()` returns `Vec<(String, Arc<dyn TableDataProvider>)>` keyed `"{provider}#{config_id}"` (one entry per datasource config it owns in the injected config).
4. Export the `init_provider_plugin` FFI symbol (scanned by the host when loading provider dylibs); also export `set_host_config` to receive the live config query API (`set_config` remains the legacy fallback for hosts that only inject snapshots).
5. Reference it from YAML as `provider: <name>` with a matching `config_id`; static tests inject via `register_providers()` + `inject_providers()`.

### Adding a capability (including SQL engines)

1. Create `crates/fusion-plugins/capability/fusion-capability-<name>/` with `crate-type = ["cdylib", "lib"]`.
2. Implement a `Capability*` trait from the SDK `capability::` module.
3. Implement `CapabilityPlugin` and export the `init_capability_plugin` FFI symbol.
4. If providing a SQL engine: export `init_sql_engine_factory` returning the `SqlEngineFactory` function table.
5. Register via `capability::register(|reg| reg.set_xxx(Arc::new(...)))` (static mode).
