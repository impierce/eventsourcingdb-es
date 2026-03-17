use cqrs_es::{CqrsFramework, persist::PersistedEventStore};

use crate::event_repository::EventSourcingDbEventRepository;

pub type EventSourcingDbCqrs<A> =
    CqrsFramework<A, PersistedEventStore<EventSourcingDbEventRepository, A>>;
