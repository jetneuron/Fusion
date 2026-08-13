use crate::graph::types::{TaskContext, UnitIdx};
use crate::proto::transfer::Frame;
use crate::runtime::state::GraphStates;
use crate::runtime::UnitResult;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Default, Clone)]
pub struct Script {
    inner: Arc<Mutex<Option<Box<dyn Scripter + Send>>>>,
}

#[derive(Default, Clone)]
pub struct ScriptContext {}

impl Script {
    pub async fn runtime<T>(&self, context: ScriptContext, eval: T) -> anyhow::Result<()>
    where
        T: AsyncFnOnce(String) -> anyhow::Result<()>,
    {
        let mutex = self.inner.lock().await;
        let t = mutex.as_ref().unwrap();
        let result = t.eval(context).await.ok().unwrap();
        eval(result).await?;
        Ok(())
    }
}

pub trait Scripter: 'static {
    fn create(
        origin_script: String,
        states: GraphStates,
    ) -> anyhow::Result<Box<dyn Scripter + Send>>
    where
        Self: Sized;

    fn initialize(&mut self, init_fn: Box<dyn FnMut()>) {}

    fn eval<'life0, 'async_trait>(
        &self,
        context: ScriptContext,
    ) -> Pin<Box<dyn Future<Output = UnitResult<String>> + Send>>
    where
        'life0: 'async_trait,
        Self: 'async_trait;

    fn frame_eval<'life0, 'async_trait>(
        &self,
        task_id: &UnitIdx,
        states: GraphStates,
        ctx: &TaskContext,
        frame: Frame,
    ) -> Pin<Box<dyn Future<Output = UnitResult<()>> + Send>>
    where
        'life0: 'async_trait,
        Self: 'async_trait;
}

pub mod script_registry {
    use crate::runtime::script::Scripter;
    use crate::runtime::script_engine_factory::ScriptEngineFactory;
    use crate::runtime::state::GraphStates;
    use anyhow::bail;
    use linkme::distributed_slice;
    use std::any::TypeId;
    use std::collections::HashMap;
    use std::sync::{Arc, OnceLock, RwLock};
    use tokio::sync::Mutex;

    // 分布式切片收集所有工厂注册器
    #[distributed_slice]
    pub static FACTORY_REGISTRATIONS: [fn() -> FactoryRegistrar] = [..];

    // 全局注册表
    static GLOBAL_REGISTRY: OnceLock<Arc<RwLock<FactoryRegistry>>> = OnceLock::new();

    pub fn get_global_registry() -> Arc<RwLock<FactoryRegistry>> {
        GLOBAL_REGISTRY
            .get_or_init(|| {
                let mut registry = FactoryRegistry::new();

                // 执行所有注册
                for registrar_func in FACTORY_REGISTRATIONS {
                    let registrar = registrar_func();
                    registrar.register(&mut registry);
                }

                Arc::new(RwLock::new(registry))
            })
            .clone()
    }

    pub fn create_scripter(
        script_type_name: &String,
        script_content: String,
        states: GraphStates,
    ) -> Arc<Mutex<Box<dyn Scripter + Send>>> {
        let global_script_registry = get_global_registry();
        let gbl_script_registry = global_script_registry.read().unwrap();
        let scripter = gbl_script_registry
            .create(script_type_name, script_content, states)
            .unwrap();
        Arc::new(Mutex::new(scripter))
    }

    // 工厂注册器
    pub struct FactoryRegistrar {
        register_fn: fn(&mut FactoryRegistry),
    }

    impl FactoryRegistrar {
        pub fn new<T: ScriptEngineFactory + 'static>() -> Self {
            Self {
                register_fn: |registry| {
                    registry.register::<T>();
                },
            }
        }

        pub fn register(self, registry: &mut FactoryRegistry) {
            (self.register_fn)(registry);
        }
    }

    impl FactoryRegistry {
        pub fn new() -> Self {
            Self {
                factories: HashMap::new(),
            }
        }

        pub fn register<T: ScriptEngineFactory + 'static>(&mut self) {
            let type_id = TypeId::of::<T>();
            let data = FactoryData {
                name: T::name(),
                constructor: Box::new(|| {
                    Box::new(|origin_script, states| T::create(origin_script, states))
                }),
            };
            self.factories.insert(type_id, data);
        }

        pub fn create(
            &self,
            name: &str,
            origin_script: String,
            states: GraphStates,
        ) -> anyhow::Result<Box<dyn Scripter + Send>> {
            for data in self.factories.values() {
                if data.name == name {
                    let create_func = (data.constructor)();
                    return create_func(origin_script, states);
                }
            }
            bail!("Could not create scripter: {name}")
        }
    }

    pub struct FactoryRegistry {
        factories: HashMap<TypeId, FactoryData>,
    }

    struct FactoryData {
        name: &'static str,
        constructor: Box<
            dyn Fn() -> Box<
                    dyn Fn(String, GraphStates) -> anyhow::Result<Box<dyn Scripter + Send>>
                        + Send
                        + Sync,
                > + Send
                + Sync,
        >,
    }
}
