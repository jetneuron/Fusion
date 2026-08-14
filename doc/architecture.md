# Fusion 架构设计

> 版本：0.1 · 适用于当前主分支 · 英文术语与代码标识保持一致

## 1. 设计目标

Fusion 是一个**基于图模型的流式计算引擎**。用户用 YAML/JSON 声明一张「逻辑计算图」（节点 = 计算单元，边 = 数据流），引擎将其编译为「物理计算图」，并以异步 mpsc 通道为骨架执行。设计上优先满足：

| 目标 | 手段 |
|------|------|
| **声明式** | 计算逻辑用 YAML 定义，代码零组装 |
| **可嵌入** | `fusion-runtime` 单 crate 入口，App/Tauri 一行启动 |
| **插件化** | unit（图节点）/ capability（进程级服务）双层 dylib 插件体系 |
| **可扩展** | 节点可通过 `parallelism` 横向并行，内置背压 |
| **多语言脚本** | Lua（行级转换）、Tera（模板）、TypeScript（deno_core） |
| **异构数据源** | 通过 DataFusion 统一 SQL 接入 CSV / SQLite / 自定义 provider |

## 2. 分层架构

```
┌──────────────────────────────────────────────────────────────────┐
│                      融合运行时（上层应用）                        │
│   app/（Tauri 桌面应用）  ·  用户代码                            │
├──────────────────────────────────────────────────────────────────┤
│  fusion-runtime           主入口：FusionRuntime / Builder        │
│                            配置加载 · 插件扫描 · GraphExecutor   │
├──────────────────────────────────────────────────────────────────┤
│  fusion-streaming         核心引擎                                │
│   LogicalGraph 解析 · PhysicalGraph 执行 · 插件管理              │
│   mpsc 通道 · Lua/Tera 脚本引擎 · Redis/ZK 存储                  │
├──────────────────────────────────────────────────────────────────┤
│  fusion-unit-sdk          公共 SDK（所有 crate 依赖同一源码）     │
│   LogicalTask / SourceUnit / MapUnit / TaskContext               │
│   Frame 数据模型 · capability · config · providers · FFI 定义    │
├──────────────────────────────────────────────────────────────────┤
│  fusion-derive            proc macros（Src/Map/SinkLogicTask）   │
├──────────────────────────────────────────────────────────────────┤
│  fusion-plugins           插件 dylib（按需加载）                  │
│   capability/   unit/                                          │
└──────────────────────────────────────────────────────────────────┘
```

各层职责：

- **SDK（`fusion-unit-sdk`）**——引擎与所有实现的共享契约：trait 定义（`LogicalTask`、`SourceUnit`、`MapUnit`、`InitUnit`、`TaskContext`）、`Frame`/`Column` 数据模型、错误类型、capability 系统、config 系统、provider 接口、跨 dylib 的 FFI 类型（`SqlEngineFactory`、`HostProviders`）。同一份 SDK 源码编译进每个二进制镜像，保证跨 FFI 布局一致。
- **核心引擎（`fusion-streaming`）**——图解析、物理执行编排、插件管理（`PluginManager`）、网络通道、存储后端（Redis、ZooKeeper）、脚本运行时（Lua/mlua、Tera）。
- **Proc macros（`fusion-derive`）**——`#[derive(SrcLogicTask)]` / `#[derive(MapLogicTask)]` / `#[derive(SinkLogicTask)]` 生成 `LogicalTask` trait 实现；`#[derive(ScriptEngine)]` 生成脚本引擎工厂实现。
- **运行时（`fusion-runtime`）**——`FusionRuntimeBuilder` 装配插件加载、capability 加载、配置解析、图执行；`FusionRuntime::init_app()` 是嵌入模式的一行启动；`GraphExecutor` 提供多图并发提交/状态查询/取消。
- **插件（`fusion-plugins`）**——两类：capability（进程级能力实现，如 `CapabilityKeyValueStore`、DataFusion SQL 引擎）与 unit（图节点类型，如 DataFusion、Excel、SSH、Redis、通用文件系统）。

## 3. 核心概念

### 3.1 逻辑图（LogicalGraph）

由 YAML/JSON 解析而来，是用户看到的全部：

```yaml
name: example_graph
units:                       # 节点（计算单元）
  - id: input
    type: DebugInputUnitTask
    version: builtin
    config:
      times: 5
      column_count: 3
  - id: map
    type: MapUnitTask        # 内置 Lua 转换节点
    version: builtin
    config:
      $script: |
        local frame = ctx:newFrame()
        frame['x'] = data['c0'] * 2
        ctx:send(frame)
      $script_type: Lua
edges:                       # 边（数据流）
  - id: e1
    source: input
    target: map
  - id: e2
    source: map
    target: output
```

`LogicalGraph`（`units` + `edges`）可转换为 petgraph 的 `Graph<ComputingUnit, ComputingEdge>`。

### 3.2 数据模型（Frame / Column）

数据在图中流动的单位是 `Frame`（protobuf 定义，SDK `proto::transfer`）：

- **一帧 = 一行数据**：`columns: Vec<Column>`；`Column` 含字段名 `field`、类型 `DataType`（str/bool/f64/f32/i32/i64/bytes/json/unknown）、空值标记及对应取值槽。
- `Frame` 同时携带 `source`（上游节点 ID），供下游按来源路由。
- 选择 protobuf 编码的关键原因：**数据可以跨 FFI 传输**——宿主进程、capability dylib、unit dylib 之间的数据交换统一走 `Frame` 字节流（见 [DataFusion 集成](datafusion.md)）。

### 3.3 节点角色

节点角色由其边的数量决定（运行时按角色选择调用路径）：

| 角色 | 边形态 | 调用 |
|------|--------|------|
| Source | 入边=0，出边>0 | `SourceUnit::launch()` 发射帧 |
| Map | 入边>0，出边>0 | 每帧调用 `MapUnit::compute(frame, ctx)` |
| Sink | 入边>0，出边=0 | `MapUnit::compute(frame, ctx)`，通常带副作用 |

### 3.4 物理图（PhysicalGraph / PhysicalTask）

`PhysicalGraph::execute()` 把 petgraph 转换成 `PhysicalTask` 实例：

- **`TaskCore`**——持有 `LocalTaskChannel`（按出边预分配的 mpsc 发送端/接收端）和 `upstream_remain: Arc<AtomicI8>`（上游 EOF 计数，用于 on_eof 只触发一次）。
- **`LocalTaskChannel`**——`prepare_outputs(outgoing)` 在 `set_unit` 时创建 N 条 mpsc 通道；每条 `link()` 弹出一个接收端；源端由 `BackpressureSender` 持有全部发送端并广播。
- **`BackpressureSender`**——每节点偏移计数器 + barrier 戳；`send()` 通过 `futures::join_all` 并行向所有发送端投递（慢消费者不阻塞快消费者）。

## 4. 执行模型

```
                ┌──────────────────────────────────────────┐
                │ PhysicalGraph::execute                    │
                │                                           │
                │  LogicalGraph ──▶ PetGraph ──▶ Physical   │
                │                          │  Task 实例      │
                │                          ▼                │
                │  DFS 遍历启动所有 Source 节点 launch()     │
                │  │                                        │
                │  ▼                                        │
                │  Source ──mpsc(1024)──▶ Map/Sink          │
                │  (launch)              (compute per frame)│
                │                                           │
                │  EOF 传播 → on_eof() → shutdown()         │
                └──────────────────────────────────────────┘
```

1. **编译**：`LogicalGraph` → petgraph（`ComputingUnit` 节点 + `ComputingEdge` 边）→ `PhysicalTask` 实例。
2. **启动**：DFS 遍历，从入边为 0 的 source 节点调用 `launch()`；每个节点一个异步 task 从上游接收端循环 recv。
3. **传输**：所有边都是 `tokio::sync::mpsc` 通道（容量 1024），帧按边流动。
4. **终结**：上游 task 结束时发送 EOF；节点对每条入边维护 `upstream_remain` 计数，全部上游 EOF 后触发一次 `on_eof()`（执行聚合/SQL 等收尾逻辑），随后 `shutdown()` 清理脚本环境。所有 join handle 等待完成，图执行结束。

## 5. 背压

背压内建于传输层，无水位轮询、无反馈通道——**通道本身就是流控**：

- 每条边是一个容量 1024 的 mpsc 通道，`BackpressureSender::send()` 会 await 每次 `tx.send(frame)`，下游缓冲满时自动阻塞。
- 扇出（1→N）使用并行发送（`join_all`）：慢消费者只阻塞自己的通道，不影响其他目标；每个目标拥有独立缓冲与独立背压。

## 6. 单元并行（parallelism）

任何节点可通过 `config.parallelism: N`（默认 1）并发处理入站帧：

```yaml
units:
  - id: map
    type: MapUnitTask
    config:
      parallelism: 4
```

### 6.1 分发模型

```
             ┌──────────────┐
 上游 mpsc ──▶│  Dispatcher  │   边缘条件过滤（单点、串行）
             └──────┬───────┘   round-robin 分发数据帧
        ┌───────────┼───────────┐
        ▼           ▼           ▼
   ┌────────┐  ┌────────┐  ┌────────┐
   │worker 0│  │worker 1│  │worker N│   ← N 个独立 tokio task
   └────────┘  └────────┘  └────────┘    每个持有独立 Lua VM
        └───────┴────┬────┴────────┘
                     ▼
            EOF drain → on_eof() → shutdown()
```

- **分发 task（每入边一个）**：接收帧 → 边缘条件过滤（串行单点）→ round-robin 投递给 N 个 worker；收到 EOF 后向所有 worker 广播 EOF，`join_all` 等待全部 worker 退出（保证在途 compute 全部完成），再触发 `on_eof()` 与 `shutdown()`。
- **worker task**：每个 worker 运行 `target.internal_compute()`，目标单元为共享 `Arc<Box<dyn LogicalTask + Send + Sync>>`——单元的可变状态在内部 `Arc<Mutex>` 之后，因此无锁共享。
- **per-worker Lua VM**（`parallelism > 1`）：每个 worker 拥有独立 `Arc<Mutex<Lua>>` 与私有 scope table（`init_script_env` 创建 N 个 VM），Lua 脚本真正并行，不再争用全局 `GraphLua`。`TaskContext.worker_lua` 携带 worker 的 VM；`row_eval` 与 Redis `this` 注入优先使用它，回退到全局 `GraphLua`。
- `parallelism = 1` 时退化为单 worker 直通，行为与串行版本完全一致。

### 6.2 语义契约（声明）

- `parallelism > 1` **不保证**下游行顺序。
- 脚本必须**无状态**——同一脚本被复制到 N 个 Lua VM，跨行状态会随 worker 分裂。
- 无锁并行 compute 要求单元满足 `Sync`（内置单元均通过内部 `Arc<Mutex>` 满足）。

## 7. 脚本引擎

每次图执行创建独立的脚本上下文（并发图完全隔离）：

| 引擎 | 用途 | 实现 |
|------|------|------|
| **Lua** | 行级转换（`MapUnitTask` 的 `$script`）、边缘条件过滤 | `mlua`（lua54）。默认全局 `GraphLua` 共享 VM + 每单元私有 scope table；`parallelism > 1` 时每 worker 独立 VM |
| **Tera** | 图启动时 `LaunchEnv` 参数/环境变量模板（如 `{{ time() }}`）；Redis 动态脚本（`ScriptMode::Dynamic`） | `GraphTera` 状态 |
| **TypeScript** | 脚本引擎扩展 | `fusion-script-ts`（deno_core） |

脚本签名 `(ctx, data, this)`：`ctx` 暴露 `send(frame)` / `newFrame()`（或 `newRow()`）等操作；`this` 是 capability 注入的 userdata（如 Redis KV store）。边缘条件是编译成 `function(row) ... end` 返回 bool 的 Lua 单行表达式。

## 8. 能力系统（Capability）

Capability 是**进程级服务**，供 unit 插件消费（例如 Redis 键值存储、DataFusion SQL 引擎），具有明确生命周期：

```
register → init → (use) → shutdown
```

- trait 定义在 SDK `capability/` 下，全部以 `Capability*` 前缀命名；每个 trait 文件带 `well_known` 子模块列出规范实现名。
- 进程级全局注册表：`capability::read()` / `capability::register()`。
- 能力插件位于 `crates/fusion-plugins/capability/`，以 dylib 加载（`PluginManager::load_capability_plugin`），实现 `CapabilityPlugin` 并导出 `init_capability_plugin` 符号。

## 9. 配置系统

数据源等配置集中在 `ConfigRegistry`：

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

- 三级层级：**category → type → id → data**（如 `datasource → redis → redis-cache → {host, port}`）。
- `DataSourceConfig` trait + `GenericDataSourceConfig` + `ConfigRegistry` 在 SDK；`FileConfigProvider`（YAML）+ `ProgrammaticConfigProvider`（代码）两种提供方式。
- 类型化访问：`config::read_config().get_typed::<RedisDataSourceConfig>("redis-cache")`。
- **dylib 注入**：宿主把整个注册表序列化为 JSON（`InjectedConfig` 条目列表），通过 dylib 的 `set_config` C 符号注入；YAML 的 `config:` 段在 `FusionRuntimeBuilder::build()` 时填充注册表并随 `set_config` 进入插件。

## 10. 插件体系

### 10.1 插件加载（PluginManager）

`PluginManager` 以 `"{type}#{version}"` 为键映射 `GraphUnitPlugin` 实现；内置单元始终注册，外部 `.dylib`/`.so` 可运行时加载。

插件 dylib 的注入协议（宿主 → dylib）：

| 符号 | 内容 | 流向 |
|------|------|------|
| `set_config` | 配置注册表 JSON | host → 所有 dylib |
| `set_sql_engine_factory` | `SqlEngineFactory` 函数表 | host → unit dylib |
| `set_host_providers` | provider 对象（fat pointer 转移） | host → unit dylib |

`PluginManager` 保存所有已加载 `Library` 句柄（防止 vtable 悬空）。

### 10.2 内置单元

引擎内置 `DebugInputUnitTask`（数据生成）、`DebugMapUnitTask`、`MapUnitTask`（Lua 转换）、`DebugOutputUnitTask`（输出/报告）等调试与通用节点，另有示例 unit（`graph-unit-example`）。

### 10.3 第三方单元

独立 crate，实现 SDK trait + `GraphUnitPlugin`，导出 `init_plugin`。参见 [扩展指南](#12-扩展指南)。

## 11. DataFusion 集成

SQL 执行拆分为**三个二进制镜像**，仅 capability dylib 携带 DataFusion 代码（详见 [DataFusion 集成](datafusion.md)）：

- **capability**（`fusion-capability-datafusion`）——进程级 `SessionContext`，C-ABI 函数表 `SqlEngineFactory`。
- **unit**（`fusion-unit-datafusion`）——无引擎，通过 `FfiSqlEngine` 包装函数表。
- **provider**（`fusion-unit-datafusion-*`）——数据源适配器（如 SQLite），产出 `Frame` 流。

数据跨 FFI 传输、类型留在各自镜像——宿主通过 `set_*` 符号注入所有跨镜像依赖。

## 12. 扩展指南

### 新增一个 unit 类型

1. 创建 `crates/fusion-plugins/units/fusion-unit-<name>/`，`crate-type = ["cdylib", "lib"]`。
2. 定义结构体并 `#[derive(Default, SrcLogicTask)]`（或 `MapLogicTask` / `SinkLogicTask`）。
3. 实现 `InitUnit`（解析配置）与 `SourceUnit` / `MapUnit`（业务逻辑）。
4. 实现 `GraphUnitPlugin` 并导出 `init_plugin` FFI 符号。
5. 在测试 harness 中注册并编写 YAML 测试图。

### 新增一个 capability 类型

1. 创建 `crates/fusion-plugins/capability/fusion-capability-<name>/`，`crate-type = ["cdylib", "lib"]`。
2. 实现 SDK `capability::` 下的 `Capability*` trait。
3. 实现 `CapabilityPlugin` 并导出 `init_capability_plugin` FFI 符号。
4. 通过 `capability::register(|reg| reg.set_xxx(Arc::new(...)))` 注册。

### 新增一个 DataFusion 表 provider

见 [DataFusion 集成 · 扩展 provider](datafusion.md#6-扩展指南)。
