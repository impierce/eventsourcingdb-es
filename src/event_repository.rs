use std::sync::Arc;

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
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    conversion::{ReversedDomain, map_event, qualify_event_type, wrap_event_data},
    errors::{EventSourcingDbError, EventSourcingDbResult},
};

const SNAPSHOT_EVENT_TYPE: &str = "io.eventsourcingdb.cqrs-es.snapshot-record.v1";
const SNAPSHOT_EVENT_SOURCE: &str = "urn:eventsourcingdb-es:snapshot";
const EVENT_SOURCE: &str = "urn:eventsourcingdb-es:event";

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

        let Some(event) = stream.try_next().await? else {
            return Ok(None);
        };

        let record = serde_json::from_value(event.data().clone())?;
        Ok(Some(record))
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
        Ok(EventCandidate::builder()
            .source(EVENT_SOURCE.to_string())
            .subject(Self::get_subject::<A>(&event.aggregate_id))
            .ty(qualify_event_type(
                &self.domain,
                &event.event_type,
                &event.event_version,
            ))
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
            Err(err) => {
                return Err(match err {
                    eventsourcingdb::error::ClientError::DBApiError(status, _)
                        if matches!(status.as_u16(), 409 | 412) =>
                    {
                        PersistenceError::OptimisticLockError
                    }
                    other => PersistenceError::from(EventSourcingDbError::ClientError(other)),
                });
            }
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
        _aggregate_id: &str,
    ) -> Result<ReplayStream, PersistenceError> {
        todo!()
    }

    async fn stream_all_events<A: cqrs_es::Aggregate>(
        &self,
    ) -> Result<ReplayStream, PersistenceError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
                "01JAAA",
                "io.eventsourcingdb.book-created",
                wrap_event_data(json!({ "number": 1 }), json!({})),
            ),
            sample_event(
                "01JAAB",
                "io.eventsourcingdb.book-updated",
                wrap_event_data(json!({ "number": 2 }), json!({})),
            ),
            sample_event(
                "01JAAC",
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
}
