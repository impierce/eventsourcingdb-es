use std::{collections::HashMap, sync::Arc};

use cqrs_es::{
    Aggregate,
    persist::{
        PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent,
        SerializedSnapshot,
    },
};
use eventsourcingdb::{
    Client, Event, EventCandidate, Precondition,
    request_options::{Bound, BoundType, Ordering, ReadEventsOptions},
};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    conversion::{ReversedDomain, map_event, qualify_event_type, wrap_event_data},
    errors::{EventSourcingDbError, EventSourcingDbResult},
};

//TODO: maybe not the best idea to hardcode that
const SNAPSHOT_EVENT_TYPE: &str = "io.eventsourcingdb.cqrs-es.snapshot-record.v1";
const SNAPSHOT_EVENT_SOURCE: &str = "urn:eventsourcingdb-es:snapshot";
const EVENT_SOURCE: &str = "urn:eventsourcingdb-es:event";
//TODO: make part of struct
const STREAM_CHANNEL_SIZE: usize = 2048;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EventSourcingDbSnapshotRecord {
    aggregate_type: String,
    aggregate_id: String,
    aggregate: Value,
    current_sequence: usize,
    current_snapshot: usize,
    last_event_id: Option<String>,
}

#[derive(Debug, Clone)]
struct SubjectState {
    logical_sequence_count: usize,
    last_event_id: Option<String>,
}

pub struct EventSourcingDbEventRepository {
    client: Arc<Client>,
    domain: ReversedDomain,
}

impl EventSourcingDbEventRepository {
    pub fn new(client: Arc<Client>, domain: ReversedDomain) -> Self {
        Self { client, domain }
    }

    fn get_subject<A: Aggregate>(id: &str) -> String {
        format!("/{}/{}", &A::TYPE, id)
    }

    fn get_snapshot_subject<A: Aggregate>(id: &str) -> String {
        format!("/__snapshots__/{}/{}", &A::TYPE, id)
    }

    // We enforce the convention /aggregate_type/aggregate_id hard. Every deviation is
    // treated as error.
    fn aggregate_id_from_subject(subject: &str) -> EventSourcingDbResult<String> {
        let mut segments = subject.split('/').filter(|segment| !segment.is_empty());
        let aggregate_type = segments
            .next()
            .ok_or_else(|| EventSourcingDbError::InvalidSubject(subject.to_string()))?;
        let aggregate_id = segments
            .next()
            .ok_or_else(|| EventSourcingDbError::InvalidSubject(subject.to_string()))?;

        if segments.next().is_some() {
            return Err(EventSourcingDbError::InvalidSubject(subject.to_string()));
        }

        if aggregate_type.is_empty() || aggregate_id.is_empty() {
            return Err(EventSourcingDbError::InvalidSubject(subject.to_string()));
        }

        Ok(aggregate_id.to_string())
    }

    fn serialize_stream_event(
        event: Event,
        sequence: usize,
    ) -> Result<SerializedEvent, PersistenceError> {
        map_event(event, sequence)
            .map(|(serialized, _)| serialized)
            .map_err(Into::into)
    }

    fn stream_client_error(err: eventsourcingdb::error::ClientError) -> PersistenceError {
        EventSourcingDbError::from(err).into()
    }

    async fn read_subject_events<A: Aggregate>(
        client: Arc<Client>,
        aggregate_id: String,
    ) -> EventSourcingDbResult<Vec<Event>> {
        let subject = Self::get_subject::<A>(&aggregate_id);
        Self::read_events_from_subject(client, subject, None).await
    }

    async fn read_events_from_subject(
        client: Arc<Client>,
        subject: String,
        lower_bound_id: Option<String>,
    ) -> EventSourcingDbResult<Vec<Event>> {
        let options = Some(ReadEventsOptions {
            lower_bound: lower_bound_id.as_deref().map(|id| Bound {
                bound_type: BoundType::Exclusive,
                id,
            }),
            order: Some(Ordering::Chronological),
            ..Default::default()
        });

        client
            .read_events(&subject, options)
            .await?
            .try_collect::<Vec<Event>>()
            .await
            .map_err(Into::into)
    }

    fn map_subject_events(
        events: Vec<Event>,
        _domain: &ReversedDomain,
        skip: usize,
    ) -> EventSourcingDbResult<Vec<SerializedEvent>> {
        events
            .into_iter()
            .enumerate()
            .skip(skip)
            .map(|(index, event)| map_event(event, index + 1).map(|(event, _)| event))
            .collect()
    }

    async fn query_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        skip: usize,
    ) -> EventSourcingDbResult<Vec<SerializedEvent>> {
        let events =
            Self::read_subject_events::<A>(Arc::clone(&self.client), aggregate_id.to_string())
                .await?;
        Self::map_subject_events(events, &self.domain, skip)
    }

    async fn load_snapshot_record<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> EventSourcingDbResult<Option<EventSourcingDbSnapshotRecord>> {
        let subject = Self::get_snapshot_subject::<A>(aggregate_id);
        let mut stream = self
            .client
            .read_events(
                &subject,
                Some(ReadEventsOptions {
                    order: Some(Ordering::Antichronological),
                    ..Default::default()
                }),
            )
            .await?;

        let record: Option<EventSourcingDbSnapshotRecord> = stream
            .try_next()
            .await?
            .map(|event| serde_json::from_value(event.data().clone()))
            .transpose()?;
        Ok(record)
    }

    async fn read_events_since_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
        snapshot: &EventSourcingDbSnapshotRecord,
    ) -> EventSourcingDbResult<Vec<SerializedEvent>> {
        let Some(last_event_id) = snapshot.last_event_id.clone() else {
            return self
                .query_events::<A>(aggregate_id, snapshot.current_sequence)
                .await;
        };

        let subject = Self::get_subject::<A>(aggregate_id);
        let events =
            Self::read_events_from_subject(Arc::clone(&self.client), subject, Some(last_event_id))
                .await?;

        events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                map_event(event, snapshot.current_sequence + index + 1).map(|(event, _)| event)
            })
            .collect()
    }

    async fn subject_state<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> EventSourcingDbResult<SubjectState> {
        //TODO: use a clever eventql query here instead of reading the complete stream
        let events =
            Self::read_subject_events::<A>(Arc::clone(&self.client), aggregate_id.to_string())
                .await?;
        let logical_sequence_count = events.len();
        let last_event_id = events.last().map(|event| event.id().to_string());

        Ok(SubjectState {
            logical_sequence_count,
            last_event_id,
        })
    }

    fn build_event_candidate<A: Aggregate>(
        &self,
        event: &SerializedEvent,
    ) -> EventSourcingDbResult<EventCandidate> {
        let event_type = qualify_event_type(&self.domain, &event.event_type, &event.event_version)?;

        Ok(EventCandidate::builder()
            .source(EVENT_SOURCE.to_string())
            .subject(Self::get_subject::<A>(&event.aggregate_id))
            .ty(event_type)
            .data(wrap_event_data(
                event.payload.clone(),
                event.metadata.clone(),
            ))
            .build())
    }

    async fn store_snapshot_record<A: Aggregate>(
        &self,
        record: EventSourcingDbSnapshotRecord,
    ) -> EventSourcingDbResult<()> {
        let candidate = EventCandidate::builder()
            .source(SNAPSHOT_EVENT_SOURCE.to_string())
            .subject(Self::get_snapshot_subject::<A>(&record.aggregate_id))
            .ty(SNAPSHOT_EVENT_TYPE.to_string())
            .data(serde_json::to_value(record)?)
            .build();

        self.client.write_events(vec![candidate], vec![]).await?;
        Ok(())
    }
}

impl PersistedEventRepository for EventSourcingDbEventRepository {
    async fn get_events<A: cqrs_es::Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.query_events::<A>(aggregate_id, 0)
            .await
            .map_err(Into::into)
    }

    async fn get_last_events<A: cqrs_es::Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        if let Some(snapshot) = self.load_snapshot_record::<A>(aggregate_id).await? {
            if snapshot.current_sequence == last_sequence {
                return self
                    .read_events_since_snapshot::<A>(aggregate_id, &snapshot)
                    .await
                    .map_err(Into::into);
            }
        }

        self.query_events::<A>(aggregate_id, last_sequence)
            .await
            .map_err(Into::into)
    }

    async fn get_snapshot<A: cqrs_es::Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        self.load_snapshot_record::<A>(aggregate_id)
            .await
            .map(|snapshot| {
                snapshot.map(|snapshot| SerializedSnapshot {
                    aggregate_id: snapshot.aggregate_id,
                    aggregate: snapshot.aggregate,
                    current_sequence: snapshot.current_sequence,
                    current_snapshot: snapshot.current_snapshot,
                })
            })
            .map_err(Into::into)
    }

    async fn persist<A: cqrs_es::Aggregate>(
        &self,
        events: &[cqrs_es::persist::SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        if events.is_empty() {
            return Ok(());
        }

        let first_event = &events[0];
        let aggregate_id = first_event.aggregate_id.clone();
        let expected_last_sequence = first_event.sequence.saturating_sub(1);
        let subject_state = self.subject_state::<A>(&aggregate_id).await?;

        if subject_state.logical_sequence_count != expected_last_sequence {
            return Err(PersistenceError::OptimisticLockError);
        }

        let precondition = match subject_state.last_event_id.clone() {
            Some(event_id) => Precondition::IsSubjectOnEventId {
                subject: Self::get_subject::<A>(&aggregate_id),
                event_id,
            },
            None => Precondition::IsSubjectPristine {
                subject: Self::get_subject::<A>(&aggregate_id),
            },
        };

        let candidates = events
            .iter()
            .map(|event| self.build_event_candidate::<A>(event))
            .collect::<EventSourcingDbResult<Vec<_>>>()?;

        let written_events = match self
            .client
            .write_events(candidates, vec![precondition])
            .await
        {
            Ok(events) => events,
            Err(err) => return Err(PersistenceError::from(EventSourcingDbError::from(err))),
        };

        if let Some((snapshot_aggregate_id, aggregate, current_snapshot)) = snapshot_update {
            let last_event_id = written_events.last().map(|event| event.id().to_string());
            let current_sequence = events.last().map(|event| event.sequence).unwrap_or(0);

            self.store_snapshot_record::<A>(EventSourcingDbSnapshotRecord {
                aggregate_type: A::TYPE.to_string(),
                aggregate_id: snapshot_aggregate_id,
                aggregate,
                current_sequence,
                current_snapshot,
                last_event_id,
            })
            .await?;
        }

        Ok(())
    }

    async fn stream_events<A: cqrs_es::Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<ReplayStream, PersistenceError> {
        let client = Arc::clone(&self.client);
        let subject = Self::get_subject::<A>(&aggregate_id);
        let (mut feed, stream) = ReplayStream::new(STREAM_CHANNEL_SIZE);

        tokio::spawn(async move {
            let mut event_stream = match client
                .read_events(
                    &subject,
                    Some(ReadEventsOptions {
                        order: Some(Ordering::Chronological),
                        ..Default::default()
                    }),
                )
                .await
            {
                Ok(stream) => stream,
                Err(err) => {
                    let _ = feed.push(Err(Self::stream_client_error(err))).await;
                    return;
                }
            };
            let mut sequence = 0usize;

            while let Some(result) = event_stream.next().await {
                let mapped = match result {
                    Ok(event) => {
                        sequence += 1;
                        Self::serialize_stream_event(event, sequence)
                    }
                    Err(err) => Err(Self::stream_client_error(err)),
                };

                if feed.push(mapped).await.is_err() {
                    break;
                }
            }
        });
        Ok(stream)
    }

    async fn stream_all_events<A: cqrs_es::Aggregate>(
        &self,
    ) -> Result<ReplayStream, PersistenceError> {
        let client = Arc::clone(&self.client);

        // query all aggregates of this type using the root aggregate subject.
        let subject = format!("/{}", A::TYPE);

        let (mut feed, stream) = ReplayStream::new(STREAM_CHANNEL_SIZE);

        tokio::spawn(async move {
            let mut event_stream = match client
                .read_events(
                    &subject,
                    Some(ReadEventsOptions {
                        recursive: true,
                        order: Some(Ordering::Chronological),
                        ..Default::default()
                    }),
                )
                .await
            {
                Ok(stream) => stream,
                Err(err) => {
                    let _ = feed.push(Err(Self::stream_client_error(err))).await;
                    return;
                }
            };

            let mut sequences: HashMap<String, usize> = HashMap::new();

            while let Some(result) = event_stream.next().await {
                let mapped = match result {
                    Ok(event) => {
                        let sequence = match Self::aggregate_id_from_subject(event.subject()) {
                            Ok(aggregate_id) => {
                                let next_sequence = sequences.entry(aggregate_id).or_insert(0);
                                *next_sequence += 1;
                                *next_sequence
                            }
                            Err(err) => {
                                if feed.push(Err(err.into())).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                        };

                        Self::serialize_stream_event(event, sequence)
                    }
                    Err(err) => Err(Self::stream_client_error(err)),
                };

                if feed.push(mapped).await.is_err() {
                    break;
                }
            }
        });
        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrs_es::persist::PersistenceError;
    use serde_json::json;

    fn sample_event(id: &str, event_type: &str, payload: Value) -> Event {
        serde_json::from_value(json!({
            "data": payload,
            "datacontenttype": "application/json",
            "hash": "hash",
            "id": id,
            "predecessorhash": "predecessor",
            "source": "urn:test",
            "specversion": "1.0",
            "subject": "/BookAggregate/42",
            "time": "2026-03-17T10:00:00Z",
            "type": event_type,
            "signature": null
        }))
        .expect("event JSON should deserialize")
    }

    #[test]
    fn map_subject_events_assigns_logical_sequences_after_skip() {
        let domain = ReversedDomain::new(["io", "eventsourcingdb"]);
        let events = vec![
            sample_event(
                "1",
                "io.eventsourcingdb.book-created",
                wrap_event_data(json!({ "number": 1 }), json!({})),
            ),
            sample_event(
                "2",
                "io.eventsourcingdb.book-updated",
                wrap_event_data(json!({ "number": 2 }), json!({})),
            ),
            sample_event(
                "3",
                "io.eventsourcingdb.book-updated",
                wrap_event_data(json!({ "number": 3 }), json!({})),
            ),
        ];

        let serialized =
            EventSourcingDbEventRepository::map_subject_events(events, &domain, 1).unwrap();

        assert_eq!(serialized.len(), 2);
        assert_eq!(serialized[0].sequence, 2);
        assert_eq!(serialized[0].payload, json!({ "number": 2 }));
        assert_eq!(serialized[1].sequence, 3);
        assert_eq!(serialized[1].payload, json!({ "number": 3 }));
    }

    #[test]
    fn aggregate_id_from_subject_requires_exact_aggregate_subject() {
        let aggregate_id =
            EventSourcingDbEventRepository::aggregate_id_from_subject("/BookAggregate/42")
                .expect("subject should parse");

        assert_eq!(aggregate_id, "42");
        assert!(matches!(
            EventSourcingDbEventRepository::aggregate_id_from_subject("/BookAggregate/42/chapter"),
            Err(EventSourcingDbError::InvalidSubject(_))
        ));
    }

    #[test]
    fn serialize_stream_event_preserves_supplied_sequence() {
        let event = sample_event(
            "1",
            "io.eventsourcingdb.book-created.v2",
            wrap_event_data(json!({ "number": 1 }), json!({ "request_id": "abc" })),
        );

        let serialized = EventSourcingDbEventRepository::serialize_stream_event(event, 5)
            .expect("event should serialize");

        assert_eq!(serialized.sequence, 5);
        assert_eq!(serialized.aggregate_id, "42");
        assert_eq!(serialized.event_type, "BookCreated");
        assert_eq!(serialized.event_version, "2.0");
    }

    #[test]
    fn serialize_stream_event_converts_mapping_failures_to_persistence_errors() {
        let invalid_event: Event = serde_json::from_value(json!({
            "data": json!({}),
            "datacontenttype": "application/json",
            "hash": "hash",
            "id": "1",
            "predecessorhash": "predecessor",
            "source": "urn:test",
            "specversion": "1.0",
            "subject": "/BookAggregate/42",
            "time": "2026-03-17T10:00:00Z",
            "type": "invalid",
            "signature": null
        }))
        .expect("event JSON should deserialize");

        let err = EventSourcingDbEventRepository::serialize_stream_event(invalid_event, 1)
            .expect_err("invalid event type should fail");

        assert!(matches!(err, PersistenceError::UnknownError(_)));
    }
}
