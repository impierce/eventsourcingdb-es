#[cfg(test)]
pub(crate) mod tests {
    use cqrs_es::doc::{Customer, CustomerEvent};
    use cqrs_es::persist::SerializedEvent;
    use cqrs_es::{Aggregate, DomainEvent};
    use eventsourcingdb::EventCandidate;
    use serde_json::json;

    pub(crate) fn test_event(id: &str, sequence: usize, event: CustomerEvent) -> SerializedEvent {
        let payload = serde_json::to_value(&event).unwrap();

        SerializedEvent {
            aggregate_id: id.to_string(),
            sequence,
            aggregate_type: Customer::aggregate_type().to_string(),
            event_type: event.event_type().to_string(),
            event_version: "1".to_string(),
            payload,
            metadata: Default::default(),
        }
    }

    pub async fn esdb_client() -> eventsourcingdb::Client {
        eventsourcingdb::Client::new(
            url::Url::parse("http://localhost:3000").unwrap(),
            "secret".to_string(),
        )
    }
}
