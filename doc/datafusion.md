# DataFusion 集成（SqlUnitTask）

> Fusion 通过 `SqlUnitTask` 节点提供 SQL 能力。本设计把 SQL 执行拆分为**三个二进制镜像**，只有 capability dylib 包含 DataFusion 代码。

## 1. 背景：Rust dylib 的镜像隔离

Rust 编译成 cdylib 后，**每个二进制镜像**（宿主可执行文件 / 每个 dylib）持有自己的一份：

- 全局 statics 与注册表（如 `config` 注册表、capability 注册表）
- trait vtable 与 `Arc` / `Box` 的布局
- tokio TLS（runtime 句柄等）

因此宿主无法直接把本进程的对象传给 dylib——dylib 里的 `OnceLock` 是另一个实例。同一份 SDK 源码在所有镜像中编译，布局一致，所以**跨 FFI 传递 fat pointer / 转移 `Box` 所有权是安全的**，但一切依赖必须通过显式的 `set_*` 注入协议。

## 2. 三镜像架构

```
┌─────────────────────────────┐
│ Host (fusion-runtime/        │
│       fusion-streaming)      │
│                              │
│  PluginManager               │
│   └─ _libs: 保持 dylib 存活   │
└──────┬──────────┬────────────┘
       │          │
  加载时注入：     │  set_host_providers(provider fat pointers)
  set_config(json)│        ┌──────────────────────────────────┐
       │          ▼        │ fusion-unit-datafusion（unit）    │
       │  ┌─────────────────────────────────┐  引擎无关，~1.3M  │
       │  │ fusion-capability-datafusion    │  ENGINE_FACTORY    │
       │  │ 唯一的 DataFusion 镜像，~99M    │  HOST_PROVIDERS    │
       │  │ SessionContext（进程级全局）     │◀─────────────────┘
       │  │ 实现 SDK CapabilitySqlEngine    │  FfiSqlEngine（包装）
       │  └───────────────┬─────────────────┘  C-ABI 函数表
       │                  │
       │  set_sql_engine_factory(SqlEngineFactory)
       │  （repr(C) 函数表，7 个 extern "C" fn 指针）
       ▼
┌─────────────────────────────────┐
│ fusion-unit-datafusion-<name>  │  provider dylib（如 -sqlite，~2.1M）
│ SqliteTableDataProvider        │  rusqlite 直接读库 → 产出 Frame
│ 实现 SDK TableDataProvider     │
│ 导出 init_provider_plugin      │
└─────────────────────────────────┘
```

| 镜像 | 内容 | 依赖 DataFusion | 体积（debug） |
|------|------|:---:|:---:|
| `fusion-capability-datafusion` | 进程级 `SessionContext`、全部 DataFusion 类型、CSV 注册、Frame 表管理 | ✅ | ~99M |
| `fusion-unit-datafusion` | `SqlUnitTask` 节点、`FfiSqlEngine` 包装、静态/流式表编排 | ❌ | ~1.3M |
| `fusion-unit-datafusion-sqlite` | SQLite 数据源适配（rusqlite） | ❌ | ~2.1M |

## 3. FFI 接口（SDK `sql_engine_ffi`）

### 3.1 `SqlEngineFactory`（repr(C) 函数表）

capability 通过 `init_sql_engine_factory` 导出，unit 通过 `set_sql_engine_factory` 接收：

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

数据以 `Box<Vec<Frame>>` 原始指针跨边界（`Box::into_raw` / `Box::from_raw` 所有权转移），帧内容为 protobuf 字节——**引擎类型永远不离开 capability 镜像**。

### 3.2 async 不跨 C ABI

- **capability 侧**：用自有 tokio runtime（`OnceLock<Runtime>`）`block_on()` 同步执行（`register_table` 同步 API 直接调用；`register_csv` 为 async，在自有 runtime 内 `block_on`）。
- **unit 侧**：每个调用跳转到 `std::thread` + `tokio::sync::oneshot` 并阻塞等待结果。**不得使用 `tokio::task::spawn_blocking`**——unit 代码运行在宿主 runtime 的 tokio worker 上，unit 镜像的 tokio TLS 为空，会触发 "no reactor running" panic；oneshot 不需要 runtime 上下文，线程返回后即可 resolve。
- 闭包捕获的原始指针以 `usize` 传入、内部重新 cast——规避 closure 对包装结构体的 disjoint field capture 问题。

### 3.3 注入协议（host → dylib）

| C 符号 | 内容 | 方向 |
|--------|------|------|
| `set_config(*const c_char)` | 配置注册表序列化为 `Vec<InjectedConfig>` JSON | host → unit / provider |
| `set_sql_engine_factory(SqlEngineFactory)` | capability 的函数表 | host → unit |
| `set_host_providers(HostProviders)` | provider 对象集合，fat pointer 转移 | host → unit |

provider 的 key 约定为 **`"{provider}#{config_id}"`**（如 `sqlite#sqlite-test-db`），unit 通过 `format!("{}#{}", provider, config_id)` 查找。

## 4. 表注册流程

### 4.1 静态表（`tables` in YAML）

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

- `provider: csv`：从 `ConfigRegistry` 解析 `path` → `engine.register_csv_table(name, path)`（capability 读文件）。
- 其他 provider：查 `HOST_PROVIDERS["{provider}#{config_id}"]` → `load_frames(sql)`（provider 产出 `Vec<Frame>`）→ `register_frame_table` + `finalize_frame_table`。

### 4.2 流式表（`stream_tables`）

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

1. 上游 `src-a` / `src-b` 的帧按 source 缓冲在 `tokio::sync::Mutex<Vec<Frame>>`（并行 worker 安全）。
2. 节点 EOF（`on_eof`）时逐 source drain 缓冲 → `register_frame_table` → `finalize_frame_table`（accumulator 置为 finalized）。
3. 执行 SQL，结果帧发送到下游。
4. `deregister_table` 清理——不泄漏到共享同一会话的其他图。

**并发安全**：并行图共享进程级 `SessionContext` 并可能复用表名（如并发测试都用 `stream_a`）。注册采用 last-writer-wins——`register_frame_table` 对已 finalized 的 accumulator 直接覆盖；`finalize_frame_table` 在 `ctx` 锁下先 `deregister_table` 再 `register_table`（DataFusion 同名注册返回 Err）。

### 4.3 节点角色

- **source**（入边=0）：`launch()` 中立即执行查询。
- **map / sink**（入边>0）：`compute()` 缓冲流式帧，`on_eof()` 执行查询；声明了 `stream_tables` 是 map/sink 角色的强制要求（否则 `compute()` 会对每帧重跑 SQL）。

## 5. provider 接口（SDK `providers`）

```rust
/// 数据源适配器：产出可供 SQL 引擎消费的 Frame 流。
#[async_trait]
pub trait TableDataProvider: Send + Sync {
    async fn load_frames(&self, sql: Option<&str>) -> UnitResult<Vec<Frame>>;
}

/// 插件入口：注册自己拥有的一组数据源 provider。
pub trait ProviderPlugin: Send + Sync {
    fn register_providers(&self) -> Vec<(String, Arc<dyn TableDataProvider>)>;
}
```

`register_providers()` 返回的 key 即 `"{provider}#{config_id}"`——一个 provider dylib 为它拥有的每个数据源配置产出一个条目（配置从注入的 `set_config` 注册表读取）。

静态进程内测试则绕过 dylib 加载，直接：

```rust
let providers = fusion_unit_datafusion_sqlite::SqliteProviderPlugin.register_providers();
fusion_unit_datafusion::inject_providers(providers);
```

## 6. 扩展指南

### 新增一个表 provider

1. 创建 `crates/fusion-plugins/units/fusion-unit-datafusion/providers/fusion-unit-datafusion-<name>/`，`crate-type = ["cdylib", "lib"]`。
2. 实现 SDK `TableDataProvider`——`load_frames(sql)` 逐行产出 protobuf `Frame`（先以 `SELECT * FROM ({query}) LIMIT 1` 探测列名，再遍历结果集）。
3. 实现 `ProviderPlugin`——`register_providers()` 返回 `Vec<(String, Arc<dyn TableDataProvider>)>`，key 为 `"{provider}#{config_id}"`（对注入配置中每个属于它的数据源产出一个条目）。
4. 导出 `init_provider_plugin` FFI 符号（宿主加载 provider dylib 时扫描）；同时导出 `set_config` 接收配置 JSON。
5. YAML 中以 `provider: <name>` + 匹配的 `config_id` 引用；静态测试用 `register_providers()` + `inject_providers()` 注入。

### 新增一个 capability（含 SQL 引擎）

1. 创建 `crates/fusion-plugins/capability/fusion-capability-<name>/`，`crate-type = ["cdylib", "lib"]`。
2. 实现 SDK `capability::` 下的 `Capability*` trait。
3. 实现 `CapabilityPlugin` 并导出 `init_capability_plugin` FFI 符号。
4. 若提供 SQL 引擎：导出 `init_sql_engine_factory` 返回 `SqlEngineFactory` 函数表。
5. 通过 `capability::register(|reg| reg.set_xxx(Arc::new(...)))` 注册（静态模式）。
