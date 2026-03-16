use std::sync::Arc;

use cqrs_es::{
    Aggregate,
    persist::{
        PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent,
        SerializedSnapshot,
    },
};
use eventsourcingdb::{
    Client, Event,
    request_options::{Bound, BoundType, ReadEventsOptions},
};
use futures::TryStreamExt;
use serde_json::Value;

use crate::{
    conversion::{ReversedDomain, map_event},
    errors::EventSourcingDbResult,
};

pub struct EventSourcingDbEventRepository {
    client: Arc<Client>,
    domain: ReversedDomain,
}

impl EventSourcingDbEventRepository {
    fn get_subject<A: Aggregate>(id: &str) -> String {
        format!("/{}/{}", &A::TYPE, id)
    }

    async fn query_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        min_sequence: usize,
    ) -> EventSourcingDbResult<Vec<SerializedEvent>> {
        let subject = Self::get_subject::<A>(aggregate_id);
        let sequence_string = min_sequence.to_string();
        let bound = Bound {
            bound_type: BoundType::Inclusive,
            id: &sequence_string,
        };
        let events: Vec<SerializedEvent> = self
            .client
            .read_events(
                &subject,
                Some(ReadEventsOptions {
                    lower_bound: Some(bound),
                    ..Default::default()
                }),
            )
            .await?
            .try_collect::<Vec<Event>>()
            .await?
            .into_iter()
            .map(map_event)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(events)
    }
}

impl PersistedEventRepository for EventSourcingDbEventRepository {
    async fn get_events<A: cqrs_es::Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let events = self.query_events::<A>(aggregate_id, 0).await?;

        Ok(events)
    }

    async fn get_last_events<A: cqrs_es::Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let events = self.query_events::<A>(aggregate_id, last_sequence).await?;
        Ok(events)
    }

    async fn get_snapshot<A: cqrs_es::Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        todo!()
    }

    async fn persist<A: cqrs_es::Aggregate>(
        &self,
        events: &[cqrs_es::persist::SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        todo!()
    }

    async fn stream_events<A: cqrs_es::Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<ReplayStream, PersistenceError> {
        todo!()
    }

    async fn stream_all_events<A: cqrs_es::Aggregate>(
        &self,
    ) -> Result<ReplayStream, PersistenceError> {
        todo!()
    }
}
