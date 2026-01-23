use cqrs_es::{CqrsFramework, persist::PersistedEventStore};

use crate::EventSourcingDbEventRepository;

pub use eventsourcingdb::Client;

pub type EventSourcingDbCqrs<A> =
    CqrsFramework<A, PersistedEventStore<EventSourcingDbEventRepository, A>>;
