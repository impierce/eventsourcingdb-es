use cqrs_es::{Aggregate, CqrsFramework, Query, persist::PersistedEventStore};

use crate::{EventSourcingDbCqrs, EventSourcingDbEventRepository};

pub async fn default_client(base_url: url::Url, api_token: String) -> eventsourcingdb::Client {
    eventsourcingdb::Client::new(base_url, api_token)
}

// pub async fn esdb_cqrs<A>(
//     client: eventsourcingdb::Client,
//     query_processor: Vec<Box<dyn Query<A>>>,
//     services: A::Services,
// ) -> EventSourcingDbCqrs<A>
// where
//     A: Aggregate,
// {
//     let repository = EventSourcingDbEventRepository::new(client).await.unwrap();
//     let store = PersistedEventStore::new_event_store(repository);
//     CqrsFramework::new(store, query_processor, services)
// }
