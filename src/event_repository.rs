use std::future::Future;
use std::sync::Arc;

use cqrs_es::{
    Aggregate,
    persist::{
        PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent,
        SerializedSnapshot,
    },
};
use eventsourcingdb::{Client, EventCandidate, request_options::ReadEventsOptions};
use futures::StreamExt;
use serde_json::{Value, json};

use crate::{
    error::EventSourcingDbError,
    mappers::{
        map_aggregate_type_and_id_to_subject, map_event_type_to_reverse_domain_name,
        map_reverse_domain_name_to_type, map_subject_to_aggregate_type_and_id,
    },
};

/// An event repository using EventSourcingDB for persistence.
pub struct EventSourcingDbEventRepository {
    client: Arc<Client>,
    // event_collection: String,
    // snapshot_collection: String,
    stream_channel_size: usize,
}

impl EventSourcingDbEventRepository {
    pub async fn new(client: Client) -> Result<Self, eventsourcingdb::error::ClientError> {
        let repository = Self {
            client: Arc::new(client),
            // event_collection: "events".to_string(),
            // snapshot_collection: "snapshots".to_string(),
            stream_channel_size: 100,
        };
        Ok(repository)
    }

    pub(crate) async fn insert_events(
        &self,
        events: &[SerializedEvent],
    ) -> Result<(), EventSourcingDbError> {
        if events.is_empty() {
            return Ok(());
        }

        let candidates: Vec<EventCandidate> = events
            .iter()
            .map(esdb_event_candidate)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| EventSourcingDbError::UnknownError(Box::new(err)))?;

        self.client.write_events(candidates, vec![]).await?;

        Ok(())
    }

    pub(crate) async fn query_events(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
        min_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, EventSourcingDbError> {
        let subject = map_aggregate_type_and_id_to_subject(aggregate_type, aggregate_id);
        let mut event_stream = self.client.read_events(&subject, None).await?;
        let mut events = vec![];
        while let Some(event) = event_stream.next().await {
            let event = event?;
            let serialized = serialized_event(event)
                .map_err(|err| EventSourcingDbError::UnknownError(Box::new(err)))?;
            if serialized.sequence >= min_sequence {
                events.push(serialized);
            }
        }
        Ok(events)
    }
}

// Maps an event from EventSourcingDB to a SerializedEvent
fn serialized_event(
    event: eventsourcingdb::Event,
) -> Result<SerializedEvent, eventsourcingdb::error::EventError> {
    let (event_type, event_version) = map_reverse_domain_name_to_type(&event.ty());

    let (aggregate_type, aggregate_id) = map_subject_to_aggregate_type_and_id(event.subject());

    Ok(SerializedEvent {
        aggregate_id,
        sequence: event.id().parse().map_err(|err| {
            eventsourcingdb::error::EventError::SerdeError(serde_json::Error::io(
                std::io::Error::new(std::io::ErrorKind::InvalidData, err),
            ))
        })?,
        aggregate_type,
        event_type,
        event_version,
        payload: event.data().clone(),
        metadata: json!({ "TODO": "unimplemented" }),
    })
}

// Maps a SerializedEvent to an EventCandidate for EventSourcingDB
fn esdb_event_candidate(
    event: &SerializedEvent,
) -> Result<eventsourcingdb::EventCandidate, eventsourcingdb::error::EventError> {
    let subject = map_aggregate_type_and_id_to_subject(&event.aggregate_type, &event.aggregate_id);

    let ty = map_event_type_to_reverse_domain_name(&event.event_type, &event.event_version);

    Ok(eventsourcingdb::EventCandidate::builder()
        .data(event.payload.clone())
        .source("tag:example.org,2026:esdb".to_string())
        .subject(subject)
        .ty(ty)
        .build())
}

impl PersistedEventRepository for EventSourcingDbEventRepository {
    fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> impl Future<Output = Result<Vec<SerializedEvent>, PersistenceError>> + Send {
        async move {
            let events = self.query_events(A::TYPE, aggregate_id, 0).await?;
            Ok(events)
        }
    }

    fn get_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> impl Future<Output = Result<Vec<SerializedEvent>, PersistenceError>> + Send {
        async move {
            let events = self
                .query_events(A::TYPE, aggregate_id, last_sequence)
                .await?;
            Ok(events)
        }
    }

    fn get_snapshot<A: Aggregate>(
        &self,
        _aggregate_id: &str,
    ) -> impl Future<Output = Result<Option<SerializedSnapshot>, PersistenceError>> + Send {
        // Snapshots are currently not supported by this repository.
        async move { Ok(None) }
    }

    fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        _snapshot_update: Option<(String, Value, usize)>,
    ) -> impl Future<Output = Result<(), PersistenceError>> + Send {
        async move {
            // Always persist events, even when snapshot updates are requested.
            self.insert_events(events).await?;
            Ok(())
        }
    }

    fn stream_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> impl Future<Output = Result<ReplayStream, PersistenceError>> + Send {
        let subject = map_aggregate_type_and_id_to_subject(A::TYPE, aggregate_id);

        let (mut feed, stream) = ReplayStream::new(self.stream_channel_size);

        let client = Arc::clone(&self.client);

        async move {
            tokio::spawn(async move {
                let mut event_stream = match client.read_events(&subject, None).await {
                    Ok(stream) => stream,
                    Err(err) => {
                        let _ = feed.push(Err(EventSourcingDbError::from(err).into())).await;
                        return;
                    }
                };

                while let Some(result) = event_stream.next().await {
                    match result {
                        Ok(event) => match serialized_event(event) {
                            Ok(serialized_event) => {
                                if feed.push(Ok(serialized_event)).await.is_err() {
                                    break;
                                };
                            }
                            Err(err) => {
                                if feed
                                    .push(Err(
                                        EventSourcingDbError::UnknownError(Box::new(err)).into()
                                    ))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        },
                        Err(e) => {
                            if feed
                                .push(Err(EventSourcingDbError::from(e).into()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                }
            });

            Ok(stream)
        }
    }

    fn stream_all_events<A: Aggregate>(
        &self,
    ) -> impl Future<Output = Result<ReplayStream, PersistenceError>> + Send {
        let (mut feed, stream) = ReplayStream::new(self.stream_channel_size);
        let client = Arc::clone(&self.client);
        let aggregate_subject_prefix = format!("/{}/", A::TYPE.to_lowercase());

        async move {
            tokio::spawn(async move {
                let mut event_stream = match client
                    .read_events(
                        "/",
                        Some(ReadEventsOptions {
                            recursive: true,
                            ..Default::default()
                        }),
                    )
                    .await
                {
                    Ok(stream) => stream,
                    Err(err) => {
                        let _ = feed.push(Err(EventSourcingDbError::from(err).into())).await;
                        return;
                    }
                };

                while let Some(result) = event_stream.next().await {
                    match result {
                        Ok(event) => {
                            if !event.subject().starts_with(&aggregate_subject_prefix) {
                                continue;
                            }
                            match serialized_event(event) {
                                Ok(serialized_event) => {
                                    if feed.push(Ok(serialized_event)).await.is_err() {
                                        break;
                                    };
                                }
                                Err(err) => {
                                    if feed
                                        .push(Err(EventSourcingDbError::UnknownError(Box::new(
                                            err,
                                        ))
                                        .into()))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            if feed
                                .push(Err(EventSourcingDbError::from(e).into()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                }
            });

            Ok(stream)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use cqrs_es::doc::{Customer, CustomerEvent};
    use cqrs_es::persist::PersistedEventRepository;
    use rand::distr::{Alphabetic, SampleString};
    use std::collections::HashSet;

    use crate::utils::tests::{esdb_client, test_event};

    #[tokio::test]
    async fn test_event_repository_inserts_successfully() {
        let client = esdb_client().await;
        let repository = EventSourcingDbEventRepository::new(client).await.unwrap();
        let aggregate_id = Alphabetic.sample_string(&mut rand::rng(), 16);
        let events = repository
            .get_events::<Customer>(&aggregate_id)
            .await
            .unwrap();
        assert!(events.is_empty());

        // Insert events
        repository
            .insert_events(&[
                test_event(
                    &aggregate_id,
                    1,
                    CustomerEvent::NameAdded {
                        name: "Ferris".to_string(),
                    },
                ),
                test_event(
                    &aggregate_id,
                    2,
                    CustomerEvent::EmailUpdated {
                        new_email: "ferris@example.test".to_string(),
                    },
                ),
            ])
            .await
            .unwrap();

        let events = repository
            .get_events::<Customer>(&aggregate_id)
            .await
            .unwrap();

        println!("Events: {:#?}", events);

        assert_eq!(2, events.len());
    }

    #[tokio::test]
    async fn test_event_repository_replay_stream_aggregate_instance() {
        let client = esdb_client().await;
        let repository = EventSourcingDbEventRepository::new(client).await.unwrap();
        let aggregate_id = Alphabetic.sample_string(&mut rand::rng(), 16);

        // Create 10 test events
        let events: Vec<SerializedEvent> = (1..=10)
            .map(|i| {
                test_event(
                    &aggregate_id,
                    i,
                    CustomerEvent::EmailUpdated {
                        new_email: format!("{}@example.test", i),
                    },
                )
            })
            .collect();
        repository.insert_events(&events).await.unwrap();

        let mut stream = repository
            .stream_events::<Customer>(&aggregate_id)
            .await
            .unwrap();
        let mut num_events = 0;
        while (stream.next::<Customer>(&[]).await).is_some() {
            num_events += 1;
        }
        assert_eq!(10, num_events);
    }

    #[tokio::test]
    async fn test_event_repository_replay_stream_all_aggregate_type() {
        let client = esdb_client().await;
        let repository = EventSourcingDbEventRepository::new(client).await.unwrap();
        let aggregate_ids: Vec<String> = (0..3)
            .map(|_| Alphabetic.sample_string(&mut rand::rng(), 16))
            .collect();

        for aggregate_id in &aggregate_ids {
            let events: Vec<SerializedEvent> = (1..=5)
                .map(|i| {
                    test_event(
                        aggregate_id,
                        i,
                        CustomerEvent::EmailUpdated {
                            new_email: format!("{}@example.test", i),
                        },
                    )
                })
                .collect();
            repository.insert_events(&events).await.unwrap();
        }

        let target_ids: HashSet<String> = aggregate_ids.into_iter().collect();

        let mut stream = repository.stream_all_events::<Customer>().await.unwrap();
        let mut matched_events = 0;

        while let Some(item) = stream.next::<Customer>(&[]).await {
            let event = item.unwrap();
            if target_ids.contains(&event.aggregate_id) {
                matched_events += 1;
            }
        }

        assert_eq!(15, matched_events);
    }

    #[tokio::test]
    async fn test_event_repository_insert_events_empty_is_noop() {
        let client = esdb_client().await;
        let repository = EventSourcingDbEventRepository::new(client).await.unwrap();
        repository.insert_events(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn test_event_repository_get_last_events_filters_sequence() {
        let client = esdb_client().await;
        let repository = EventSourcingDbEventRepository::new(client).await.unwrap();
        let aggregate_id = Alphabetic.sample_string(&mut rand::rng(), 16);

        let before = repository
            .get_events::<Customer>(&aggregate_id)
            .await
            .unwrap();
        let cutoff = before.iter().map(|e| e.sequence).max().unwrap_or(0) + 1;

        repository
            .insert_events(&[
                test_event(
                    &aggregate_id,
                    1,
                    CustomerEvent::NameAdded {
                        name: "Ferris".to_string(),
                    },
                ),
                test_event(
                    &aggregate_id,
                    2,
                    CustomerEvent::EmailUpdated {
                        new_email: "one@example.test".to_string(),
                    },
                ),
                test_event(
                    &aggregate_id,
                    3,
                    CustomerEvent::EmailUpdated {
                        new_email: "two@example.test".to_string(),
                    },
                ),
                test_event(
                    &aggregate_id,
                    4,
                    CustomerEvent::EmailUpdated {
                        new_email: "three@example.test".to_string(),
                    },
                ),
            ])
            .await
            .unwrap();

        let all_events = repository
            .get_events::<Customer>(&aggregate_id)
            .await
            .unwrap();
        let expected_count = all_events.iter().filter(|e| e.sequence >= cutoff).count();

        let events = repository
            .get_last_events::<Customer>(&aggregate_id, cutoff)
            .await
            .unwrap();

        assert_eq!(expected_count, events.len());
        assert!(events.iter().all(|e| e.sequence >= cutoff));
    }

    #[tokio::test]
    async fn test_event_repository_get_snapshot_returns_none() {
        let client = esdb_client().await;
        let repository = EventSourcingDbEventRepository::new(client).await.unwrap();
        let aggregate_id = Alphabetic.sample_string(&mut rand::rng(), 16);

        let snapshot = repository
            .get_snapshot::<Customer>(&aggregate_id)
            .await
            .unwrap();

        assert!(snapshot.is_none());
    }

    #[tokio::test]
    async fn test_event_repository_persist_ignores_snapshot_update_and_persists_events() {
        let client = esdb_client().await;
        let repository = EventSourcingDbEventRepository::new(client).await.unwrap();
        let aggregate_id = Alphabetic.sample_string(&mut rand::rng(), 16);
        let before_count = repository
            .get_events::<Customer>(&aggregate_id)
            .await
            .unwrap()
            .len();
        let event = test_event(
            &aggregate_id,
            1,
            CustomerEvent::NameAdded {
                name: "Ferris".to_string(),
            },
        );

        repository
            .persist::<Customer>(
                std::slice::from_ref(&event),
                Some((
                    aggregate_id.clone(),
                    serde_json::json!({ "ignored": true }),
                    1,
                )),
            )
            .await
            .unwrap();

        let events = repository
            .get_events::<Customer>(&aggregate_id)
            .await
            .unwrap();
        assert_eq!(before_count + 1, events.len());
    }

    #[tokio::test]
    async fn test_event_repository_stream_all_events_excludes_other_aggregate_type_subjects() {
        let client = esdb_client().await;
        let repository = EventSourcingDbEventRepository::new(client).await.unwrap();

        let customer_id = Alphabetic.sample_string(&mut rand::rng(), 16);
        let order_id = Alphabetic.sample_string(&mut rand::rng(), 16);

        repository
            .insert_events(&[test_event(
                &customer_id,
                1,
                CustomerEvent::NameAdded {
                    name: "Ferris".to_string(),
                },
            )])
            .await
            .unwrap();

        // Same payload shape but on a different aggregate type subject.
        let non_customer_event = SerializedEvent {
            aggregate_id: order_id,
            sequence: 1,
            aggregate_type: "Order".to_string(),
            event_type: "NameAdded".to_string(),
            event_version: "1".to_string(),
            payload: serde_json::json!({ "name": "not-customer" }),
            metadata: serde_json::json!({}),
        };
        repository
            .insert_events(std::slice::from_ref(&non_customer_event))
            .await
            .unwrap();

        let mut stream = repository.stream_all_events::<Customer>().await.unwrap();
        let mut matched_customer_events = 0;

        while let Some(item) = stream.next::<Customer>(&[]).await {
            let event = item.unwrap();
            if event.aggregate_id == customer_id {
                matched_customer_events += 1;
            }
        }

        assert_eq!(1, matched_customer_events);
    }
}
