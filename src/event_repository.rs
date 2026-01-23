use async_trait::async_trait;
use cqrs_es::{
    Aggregate,
    persist::{
        PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent,
        SerializedSnapshot,
    },
};
use eventsourcingdb::{Client, EventCandidate};
use futures::StreamExt;
use serde_json::{Value, json};

use crate::{error::EventSourcingDbError, mappers::map_type_to_reverse_domain_name};

/// An event repository using EventSourcingDB for persistence.
pub struct EventSourcingDbEventRepository {
    client: Client,
    event_collection: String,
    snapshot_collection: String,
    stream_channel_size: usize,
}

impl EventSourcingDbEventRepository {
    pub async fn new(client: Client) -> Result<Self, eventsourcingdb::error::ClientError> {
        let repository = Self {
            client,
            event_collection: "events".to_string(),
            snapshot_collection: "snapshots".to_string(),
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
            .map(|event| deserialized_event(event).unwrap())
            .collect();

        let result = self.client.write_events(candidates, vec![]).await?;

        Ok(())
    }

    pub(crate) async fn query_events(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
        min_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, EventSourcingDbError> {
        let mut event_stream = self
            .client
            .read_events(aggregate_id, None)
            .await
            .expect("Failed to read events");
        let mut events = vec![];
        while let Some(event) = event_stream.next().await {
            let event = event.expect("Error while reading events");
            events.push(serialized_event(event).unwrap());
        }
        Ok(events)
    }

    pub(crate) async fn update_snapshot<A: Aggregate>(
        &self,
        aggregate_payload: Value,
        aggregate_id: String,
        current_snapshot: usize,
        events: &[SerializedEvent],
    ) -> Result<(), EventSourcingDbError> {
        Ok(())
    }
}

// Maps an event from EventSourcingDB to a SerializedEvent
fn serialized_event(
    event: eventsourcingdb::Event,
) -> Result<SerializedEvent, eventsourcingdb::error::EventError> {
    Ok(SerializedEvent {
        aggregate_id: event.subject().to_string(),
        sequence: event.id().parse().unwrap(),
        aggregate_type: event.datacontenttype().to_string(),
        event_type: event.ty().to_string(),
        event_version: "TODO".to_string(),
        payload: event.data().clone(),
        metadata: json!({}),
    })
}

// Maps a SerializedEvent to an EventCandidate for EventSourcingDB
fn deserialized_event(
    event: &SerializedEvent,
) -> Result<eventsourcingdb::EventCandidate, eventsourcingdb::error::EventError> {
    let subject = if event.aggregate_id.starts_with('/') {
        event.aggregate_id.clone()
    } else {
        format!("/{}", event.aggregate_id)
    };

    let ty = map_type_to_reverse_domain_name(&event.event_type);

    Ok(eventsourcingdb::EventCandidate::builder()
        .data(event.payload.clone())
        .source("https://example.org".to_string())
        .subject(subject)
        .ty(ty)
        .build())
}

#[async_trait]
impl PersistedEventRepository for EventSourcingDbEventRepository {
    async fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let events = self
            .query_events(&A::aggregate_type(), aggregate_id, 0)
            .await?;
        Ok(events)
    }

    async fn get_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let events = self
            .query_events(&A::aggregate_type(), aggregate_id, last_sequence)
            .await?;
        Ok(events)
    }

    async fn get_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        unimplemented!()
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        match snapshot_update {
            None => {
                self.insert_events(events).await?;
            }
            Some((aggregate_id, aggregate_payload, current_snapshot)) => {
                self.update_snapshot::<A>(
                    aggregate_payload,
                    aggregate_id,
                    current_snapshot,
                    events,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn stream_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<ReplayStream, PersistenceError> {
        Err(PersistenceError::UnknownError(
            "Not implemented: stream_events".into(),
        ))
    }

    async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
        Err(PersistenceError::UnknownError(
            "Not implemented: stream_all_events".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use cqrs_es::doc::{Customer, CustomerEvent};
    use rand::distr::{Alphabetic, SampleString};

    use crate::utils::tests::{esdb_client, test_event};

    #[tokio::test]
    async fn test_event_repository_inserts_successfully() {
        let client = esdb_client().await;
        let repository = EventSourcingDbEventRepository::new(client).await.unwrap();
        let aggregate_id = format!("/{}", Alphabetic.sample_string(&mut rand::rng(), 16));
        let events = repository
            .get_events::<Customer>(&aggregate_id)
            .await
            .unwrap();
        assert!(events.is_empty());

        // Insert events
        repository
            .insert_events(&[test_event(
                &aggregate_id,
                1,
                CustomerEvent::NameAdded {
                    name: "Ferris".to_string(),
                },
            )])
            .await
            .unwrap();

        let events = repository
            .get_events::<Customer>(&aggregate_id)
            .await
            .unwrap();

        assert_eq!(1, events.len());
    }
}
