use std::sync::Arc;

use cqrs_es::{Aggregate, CqrsFramework, Query, persist::PersistedEventStore};

use crate::{
    conversion::ReversedDomain, event_repository::EventSourcingDbEventRepository,
    types::EventSourcingDbCqrs,
};

pub fn esdb_cqrs<A>(
    client: eventsourcingdb::Client,
    domain: ReversedDomain,
    query_processor: Vec<Box<dyn Query<A>>>,
    services: A::Services,
) -> EventSourcingDbCqrs<A>
where
    A: Aggregate,
{
    let repo = EventSourcingDbEventRepository::new(Arc::new(client), domain);
    let store = PersistedEventStore::new_event_store(repo);
    CqrsFramework::new(store, query_processor, services)
}
