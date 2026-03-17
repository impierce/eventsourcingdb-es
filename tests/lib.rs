use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use cqrs_es::{
    Aggregate, DomainEvent,
    EventStore,
    doc::{Customer, CustomerEvent},
    persist::{PersistedEventRepository, PersistedEventStore, PersistenceError, SerializedEvent},
};
use eventsourcingdb::request_options::{Ordering as EventOrdering, ReadEventsOptions};
use eventsourcingdb_es::{
    conversion::ReversedDomain, event_repository::EventSourcingDbEventRepository,
};
use futures::TryStreamExt;
use serde_json::{Value, json};

static TEST_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn esdb_client() -> eventsourcingdb::Client {
    eventsourcingdb::Client::new(
        url::Url::parse("http://localhost:3000").unwrap(),
        "secret".to_string(),
    )
}

fn new_repository() -> EventSourcingDbEventRepository {
    EventSourcingDbEventRepository::new(
        Arc::new(esdb_client()),
        ReversedDomain::new(["io", "eventsourcingdb"]),
    )
}

fn new_snapshot_store() -> PersistedEventStore<EventSourcingDbEventRepository, Customer> {
    PersistedEventStore::new_snapshot_store(new_repository(), 2)
}

fn unique_aggregate_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let counter = TEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{nanos}-{counter}")
}

fn serialized_customer_event(
    aggregate_id: &str,
    sequence: usize,
    event: CustomerEvent,
    metadata: Value,
) -> SerializedEvent {
    SerializedEvent::new(
        aggregate_id.to_string(),
        sequence,
        Customer::TYPE.to_string(),
        event.event_type(),
        event.event_version(),
        serde_json::to_value(event).expect("customer event should serialize"),
        metadata,
    )
}

#[tokio::test]
async fn persist_and_load_events_round_trip() {
    let repository = new_repository();
    let aggregate_id = unique_aggregate_id("persist-and-load");

    assert_eq!(
        0,
        repository
            .get_events::<Customer>(&aggregate_id)
            .await
            .expect("loading empty stream should succeed")
            .len()
    );

    let first_batch = vec![
        serialized_customer_event(
            &aggregate_id,
            1,
            CustomerEvent::NameAdded {
                name: "Alice".to_string(),
            },
            json!({ "request_id": "initial-name" }),
        ),
        serialized_customer_event(
            &aggregate_id,
            2,
            CustomerEvent::EmailUpdated {
                new_email: "alice@example.com".to_string(),
            },
            json!({ "request_id": "initial-email" }),
        ),
    ];

    repository
        .persist::<Customer>(&first_batch, None)
        .await
        .expect("initial events should persist");

    let loaded_after_first_commit = repository
        .get_events::<Customer>(&aggregate_id)
        .await
        .expect("events should load after first commit");

    assert_eq!(2, loaded_after_first_commit.len());
    assert_eq!(1, loaded_after_first_commit[0].sequence);
    assert_eq!(2, loaded_after_first_commit[1].sequence);
    assert_eq!("NameAdded", loaded_after_first_commit[0].event_type);
    assert_eq!("1.0", loaded_after_first_commit[0].event_version);
    assert_eq!("EmailUpdated", loaded_after_first_commit[1].event_type);
    assert_eq!("1.0", loaded_after_first_commit[1].event_version);
    assert_eq!(
        json!({ "NameAdded": { "name": "Alice" } }),
        loaded_after_first_commit[0].payload
    );
    assert_eq!(
        json!({ "EmailUpdated": { "new_email": "alice@example.com" } }),
        loaded_after_first_commit[1].payload
    );

    let second_batch = vec![serialized_customer_event(
        &aggregate_id,
        3,
        CustomerEvent::EmailUpdated {
            new_email: "alice+1@example.com".to_string(),
        },
        json!({ "request_id": "follow-up-email" }),
    )];

    repository
        .persist::<Customer>(&second_batch, None)
        .await
        .expect("follow-up event should persist");

    let all_events = repository
        .get_events::<Customer>(&aggregate_id)
        .await
        .expect("full history should load");

    assert_eq!(3, all_events.len());
    assert_eq!(3, all_events[2].sequence);
    assert_eq!("EmailUpdated", all_events[2].event_type);
    assert_eq!("1.0", all_events[2].event_version);
    assert_eq!(
        json!({ "EmailUpdated": { "new_email": "alice+1@example.com" } }),
        all_events[2].payload
    );

    let last_events = repository
        .get_last_events::<Customer>(&aggregate_id, 2)
        .await
        .expect("tail events should load");

    assert_eq!(1, last_events.len());
    assert_eq!(3, last_events[0].sequence);
    assert_eq!("EmailUpdated", last_events[0].event_type);
    assert_eq!("1.0", last_events[0].event_version);
    assert_eq!(
        json!({ "EmailUpdated": { "new_email": "alice+1@example.com" } }),
        last_events[0].payload
    );
}

#[tokio::test]
async fn persist_rejects_stale_sequence_numbers() {
    let repository = new_repository();
    let aggregate_id = unique_aggregate_id("optimistic-lock");

    let initial_events = vec![serialized_customer_event(
        &aggregate_id,
        1,
        CustomerEvent::NameAdded {
            name: "Bob".to_string(),
        },
        json!({ "request_id": "create-name" }),
    )];

    repository
        .persist::<Customer>(&initial_events, None)
        .await
        .expect("initial event should persist");

    let stale_events = vec![serialized_customer_event(
        &aggregate_id,
        1,
        CustomerEvent::EmailUpdated {
            new_email: "bob@example.com".to_string(),
        },
        json!({ "request_id": "stale-update" }),
    )];

    let err = repository
        .persist::<Customer>(&stale_events, None)
        .await
        .expect_err("stale event sequence should fail");

    assert!(matches!(err, PersistenceError::OptimisticLockError));
}

#[tokio::test]
async fn snapshot_store_persists_snapshots_and_loads_tail_events() {
    let repository = new_repository();
    let event_store = new_snapshot_store();
    let aggregate_id = unique_aggregate_id("snapshot-store");

    let initial_context = event_store
        .load_aggregate(&aggregate_id)
        .await
        .expect("loading empty aggregate should succeed");

    event_store
        .commit(
            vec![
                CustomerEvent::NameAdded {
                    name: "Carol".to_string(),
                },
                CustomerEvent::EmailUpdated {
                    new_email: "carol@example.com".to_string(),
                }
            ],
            initial_context,
            Default::default(),
        )
        .await
        .expect("first commit should write events and snapshot");

    let snapshot = repository
        .get_snapshot::<Customer>(&aggregate_id)
        .await
        .expect("snapshot lookup should succeed")
        .expect("snapshot should exist after threshold is reached");

    assert_eq!(aggregate_id, snapshot.aggregate_id);
    assert_eq!(2, snapshot.current_sequence);
    assert_eq!(1, snapshot.current_snapshot);
    assert_eq!(
        json!({
            "customer_id": "",
            "name": "Carol",
            "email": "carol@example.com",
            "data_populated": false
        }),
        snapshot.aggregate
    );

    let context_after_snapshot = event_store
        .load_aggregate(&aggregate_id)
        .await
        .expect("aggregate should load from snapshot");

    assert_eq!(2, context_after_snapshot.current_sequence);
    assert_eq!(Some(1), context_after_snapshot.current_snapshot);
    assert_eq!("Carol", context_after_snapshot.aggregate.name);
    assert_eq!("carol@example.com", context_after_snapshot.aggregate.email);

    event_store
        .commit(
            vec![CustomerEvent::EmailUpdated {
                new_email: "carol+1@example.com".to_string(),
            }],
            context_after_snapshot,
            Default::default(),
        )
        .await
        .expect("tail event should persist after snapshot");

    let tail_events = repository
        .get_last_events::<Customer>(&aggregate_id, snapshot.current_sequence)
        .await
        .expect("tail events should load from snapshot boundary");

    assert_eq!(1, tail_events.len());
    assert_eq!(3, tail_events[0].sequence);
    assert_eq!("EmailUpdated", tail_events[0].event_type);
    assert_eq!("1.0", tail_events[0].event_version);
    assert_eq!(
        json!({ "EmailUpdated": { "new_email": "carol+1@example.com" } }),
        tail_events[0].payload
    );

    let reloaded_context = event_store
        .load_aggregate(&aggregate_id)
        .await
        .expect("aggregate should reload from snapshot plus tail events");

    assert_eq!(3, reloaded_context.current_sequence);
    assert_eq!("Carol", reloaded_context.aggregate.name);
    assert_eq!("carol+1@example.com", reloaded_context.aggregate.email);

    let no_new_events = repository
        .get_last_events::<Customer>(&aggregate_id, reloaded_context.current_sequence)
        .await
        .expect("reading past the end of the stream should succeed");

    assert!(no_new_events.is_empty());
}

#[tokio::test]
async fn persisted_customer_events_use_explicit_v1_event_type_in_eventsourcingdb() {
    let repository = new_repository();
    let client = esdb_client();
    let aggregate_id = unique_aggregate_id("wire-shape");

    let events = vec![serialized_customer_event(
        &aggregate_id,
        1,
        CustomerEvent::NameAdded {
            name: "Dana".to_string(),
        },
        json!({ "request_id": "wire-shape" }),
    )];

    repository
        .persist::<Customer>(&events, None)
        .await
        .expect("customer event should persist");

    let subject = format!("/Customer/{aggregate_id}");
    let stored_events = client
        .read_events(
            &subject,
            Some(ReadEventsOptions {
                order: Some(EventOrdering::Chronological),
                ..Default::default()
            }),
        )
        .await
        .expect("stored events should be readable")
        .try_collect::<Vec<_>>()
        .await
        .expect("stored events should collect");

    assert_eq!(1, stored_events.len());
    assert_eq!("io.eventsourcingdb.name-added.v1", stored_events[0].ty());
}
