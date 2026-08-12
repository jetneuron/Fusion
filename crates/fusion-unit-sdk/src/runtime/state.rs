use anyhow::{anyhow, bail};
use parking_lot::RwLock;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default, Clone)]
pub struct GraphStates {
    graph_id: String,
    states: Arc<RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>>,
}

pub trait State: Send + Sync {}

pub struct StateRef<T: State + Send + Sync + 'static>(Arc<T>);

impl<T: State + Send + Sync + 'static> StateRef<T> {
    pub fn inner(&self) -> &T {
        &self.0
    }

    pub fn into_inner(self) -> Arc<T> {
        self.0
    }
}

impl<T: State + Send + Sync + 'static> std::ops::Deref for StateRef<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl GraphStates {
    pub fn new(task_id: String) -> Self {
        Self {
            states: Arc::new(RwLock::new(HashMap::new())),
            graph_id: task_id,
        }
    }

    /// Returns the graph's unique identifier.
    pub fn graph_id(&self) -> &str {
        &self.graph_id
    }

    pub fn register<T: State + Send + Sync + 'static>(&self, state: T) -> anyhow::Result<()> {
        let type_id = TypeId::of::<T>();
        let type_name = std::any::type_name::<T>();

        let mut states = self.states.write();
        if states.contains_key(&type_id) {
            bail!("Already registered: {}", type_name);
        }
        states.insert(type_id, Arc::new(state));
        Ok(())
    }

    pub async fn register_async<T, F, Fut>(&self, factory: F) -> anyhow::Result<()>
    where
        T: State + Send + Sync + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = anyhow::Result<T>> + Send + 'static,
    {
        let state = factory().await?;
        self.register(state)?;
        Ok(())
    }

    pub fn state<T: State + Send + Sync + 'static>(&self) -> anyhow::Result<StateRef<T>> {
        let type_id = TypeId::of::<T>();
        let type_name = std::any::type_name::<T>();
        let states = self.states.read();
        states
            .get(&type_id)
            .and_then(|any| any.clone().downcast::<T>().ok())
            .map(|arc| StateRef(arc))
            .ok_or_else(|| anyhow!("Could not found state: {}", type_name.to_string()))
    }
}
