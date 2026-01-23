use std::sync::Arc;

use async_trait::async_trait;
use cqrs_es::{
    Aggregate, View,
    persist::{PersistenceError, ViewContext, ViewRepository},
};
use dashmap::DashMap;

pub struct InMemoryViewRepository<V, A> {
    _phantom: std::marker::PhantomData<(V, A)>,
    views: Arc<DashMap<String, (V, ViewContext)>>,
}

impl<V, A> Default for InMemoryViewRepository<V, A> {
    fn default() -> Self {
        Self {
            _phantom: Default::default(),
            views: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl<V, A> ViewRepository<V, A> for InMemoryViewRepository<V, A>
where
    V: View<A> + Clone + Send + Sync,
    A: Aggregate,
{
    async fn load(&self, view_id: &str) -> Result<Option<V>, PersistenceError> {
        Ok(self.views.get(view_id).map(|v| v.0.clone()))
    }

    async fn load_with_context(
        &self,
        view_id: &str,
    ) -> Result<Option<(V, ViewContext)>, PersistenceError> {
        // Ok(self.views.get(view_id).map(|v| v.value().clone()))
        unimplemented!()
    }

    async fn update_view(&self, view: V, context: ViewContext) -> Result<(), PersistenceError> {
        let view_id = context.view_instance_id.clone();
        self.views.insert(view_id, (view, context));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::doc::{Customer, CustomerEvent};
    use cqrs_es::persist::{ViewContext, ViewRepository};

    use crate::InMemoryViewRepository;
    use crate::utils::tests::CustomerView;

    #[tokio::test]
    async fn test_view_repository() {
        let repository = InMemoryViewRepository::<CustomerView, Customer>::default();

        let test_view_id = "1";

        let view = CustomerView {
            events: vec![CustomerEvent::NameAdded {
                name: "Ferris".to_string(),
            }],
        };

        repository
            .update_view(view.clone(), ViewContext::new(test_view_id.to_string(), 0))
            .await
            .unwrap();

        let loaded = repository.load(test_view_id).await.unwrap().unwrap();

        assert_eq!(loaded, view);
    }
}
