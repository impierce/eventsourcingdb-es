use std::future::Future;
use std::sync::Arc;

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

impl<V, A> InMemoryViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    pub fn new(_view_name: &str) -> Self {
        Self {
            _phantom: Default::default(),
            views: Arc::new(DashMap::new()),
        }
    }
}

impl<V, A> ViewRepository<V, A> for InMemoryViewRepository<V, A>
where
    V: View<A> + Clone,
    A: Aggregate,
{
    fn load(
        &self,
        view_id: &str,
    ) -> impl Future<Output = Result<Option<V>, PersistenceError>> + Send {
        async move { Ok(self.views.get(view_id).map(|v| v.0.clone())) }
    }

    fn load_with_context(
        &self,
        view_id: &str,
    ) -> impl Future<Output = Result<Option<(V, ViewContext)>, PersistenceError>> + Send {
        async move {
            Ok(self.views.get(view_id).map(|v| {
                (
                    v.0.clone(),
                    ViewContext::new(v.1.view_instance_id.clone(), v.1.version),
                )
            }))
        }
    }

    fn update_view(
        &self,
        view: V,
        context: ViewContext,
    ) -> impl Future<Output = Result<(), PersistenceError>> + Send {
        async move {
            let view_id = context.view_instance_id.clone();
            self.views.insert(view_id, (view, context));
            Ok(())
        }
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

    #[tokio::test]
    async fn test_view_repository_load_missing_returns_none() {
        let repository = InMemoryViewRepository::<CustomerView, Customer>::default();

        let loaded = repository.load("missing").await.unwrap();
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_view_repository_load_with_context_roundtrip() {
        let repository = InMemoryViewRepository::<CustomerView, Customer>::new("customer_view");
        let view_id = "ctx-1";

        let view = CustomerView {
            events: vec![CustomerEvent::EmailUpdated {
                new_email: "ferris@example.test".to_string(),
            }],
        };

        repository
            .update_view(view.clone(), ViewContext::new(view_id.to_string(), 7))
            .await
            .unwrap();

        let loaded = repository
            .load_with_context(view_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(loaded.0, view);
        assert_eq!(loaded.1.view_instance_id, view_id.to_string());
        assert_eq!(loaded.1.version, 7);
    }

    #[tokio::test]
    async fn test_view_repository_load_with_context_missing_returns_none() {
        let repository = InMemoryViewRepository::<CustomerView, Customer>::default();

        let loaded = repository.load_with_context("missing").await.unwrap();
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_view_repository_update_view_overwrites_existing_entry() {
        let repository = InMemoryViewRepository::<CustomerView, Customer>::default();
        let view_id = "replace-1";

        let first = CustomerView {
            events: vec![CustomerEvent::NameAdded {
                name: "Ferris".to_string(),
            }],
        };

        let second = CustomerView {
            events: vec![CustomerEvent::EmailUpdated {
                new_email: "updated@example.test".to_string(),
            }],
        };

        repository
            .update_view(first, ViewContext::new(view_id.to_string(), 0))
            .await
            .unwrap();

        repository
            .update_view(second.clone(), ViewContext::new(view_id.to_string(), 1))
            .await
            .unwrap();

        let loaded = repository
            .load_with_context(view_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.0, second);
        assert_eq!(loaded.1.version, 1);
    }
}
