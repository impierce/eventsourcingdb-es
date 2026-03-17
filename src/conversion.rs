use std::str::FromStr;

use chrono::{DateTime, Utc};
use cqrs_es::persist::SerializedEvent;
use eventsourcingdb::{Event, TraceInfo};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::errors::{EventSourcingDbError, EventSourcingDbResult};

pub const STORED_EVENT_ENVELOPE_MARKER: &str = "eventsourcingdb-es@1";

fn to_pascal_case(s: &str) -> String {
    s.split('-')
        .map(|w| {
            let mut w = w.to_string();
            w[..1].make_ascii_uppercase();
            w
        })
        .collect()
}

fn pascal_to_kebab_case(s: &str) -> String {
    let mut out = String::new();

    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i != 0 {
                out.push('-');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }

    out
}

// we assume that the subject always has the shape /aggregate_type/aggregate_id
// e.g. /books/42, everything else will be rejected
pub(crate) fn map_subject_to_aggregate_type_and_id(
    subject: &str,
) -> Result<(String, String), EventSourcingDbError> {
    let mut parts = subject.trim_start_matches('/').split('/');

    let aggregate_type = parts
        .next()
        .ok_or_else(|| EventSourcingDbError::InvalidSubject(subject.into()))?;

    let aggregate_id = parts
        .next()
        .ok_or_else(|| EventSourcingDbError::InvalidSubject(subject.into()))?;

    if parts.next().is_some() {
        return Err(EventSourcingDbError::InvalidSubject(subject.into()));
    }

    Ok((aggregate_type.to_string(), aggregate_id.to_string()))
}

/// Reverse-domain prefix used when building persisted EventSourcingDB event types.
/// For example, `ReversedDomain::new(["com", "flangator", "banking"])` makes the
/// CQRS event `CustomerDepositedMoney` version `1.0` persist as
/// `com.flangator.banking.customer-deposited-money.v1`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReversedDomain(Vec<String>);

impl ReversedDomain {
    pub fn new<I, S>(labels: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self(labels.into_iter().map(Into::into).collect())
    }

    pub fn labels(&self) -> &[String] {
        &self.0
    }
}

/// The fully qualified event name follows EventSourcingDB [convention](https://docs.eventsourcingdb.io/fundamentals/event-types/#required-field)
/// and wrapts the reversed domain,the actual type as presented to cqrs-es and an optional version.
/// The absence of a version will be treated as 1.0.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedEventType {
    pub reversed_domain: ReversedDomain,
    pub event_type: String,
    pub event_version: Option<usize>,
}

fn parse_major_version_str(value: &str) -> Result<usize, EventSourcingDbError> {
    if let Some((major, minor)) = value.split_once('.') {
        if minor != "0" {
            return Err(EventSourcingDbError::InvalidEventVersion(value.into()));
        }

        let major: usize = major.parse()?;
        if major == 0 {
            return Err(EventSourcingDbError::InvalidEventVersion(value.into()));
        }

        return Ok(major);
    }

    let major: usize = value.parse()?;
    if major == 0 {
        return Err(EventSourcingDbError::InvalidEventVersion(value.into()));
    }

    Ok(major)
}

fn format_cqrs_event_version(major: usize) -> String {
    format!("{major}.0")
}

impl FromStr for QualifiedEventType {
    type Err = EventSourcingDbError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut parts: Vec<&str> = value.split('.').collect();

        if parts.len() < 2 {
            return Err(EventSourcingDbError::InvalidEventTypeIdentifier(
                value.into(),
            ));
        }

        // the last part might be a version
        let mut version = None;

        if let Some(last) = parts.last() {
            if let Some(num) = last.strip_prefix('v') {
                version = Some(num.parse()?);
                parts.pop(); // version entfernen
            }
        }

        // the remainder has at least the event type and the reversed domain
        if parts.len() < 2 {
            return Err(EventSourcingDbError::InvalidEventTypeIdentifier(
                value.into(),
            ));
        }

        let event_type = parts.pop().unwrap().to_string();

        let reversed_domain = ReversedDomain(parts.into_iter().map(|s| s.to_string()).collect());

        Ok(QualifiedEventType {
            reversed_domain,
            event_type,
            event_version: version,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventMetadata {
    pub source: String,
    pub subject: String,
    pub time: DateTime<Utc>,
    pub traceinfo: Option<TraceInfo>,
    pub datacontenttype: String,
    pub specversion: String,
    pub hash: String,
    pub predecessorhash: String,
    pub signature: Option<String>,
}

impl From<&Event> for EventMetadata {
    fn from(event: &Event) -> Self {
        Self {
            source: event.source().to_string(),
            subject: event.subject().to_string(),
            time: event.time().clone(),
            traceinfo: event.traceinfo().cloned(),
            datacontenttype: event.datacontenttype().to_string(),
            specversion: event.specversion().to_string(),
            hash: event.hash().to_string(),
            predecessorhash: event.predecessorhash().to_string(),
            signature: event.signature().map(ToString::to_string),
        }
    }
}

pub fn qualify_event_type(
    domain: &ReversedDomain,
    event_type: &str,
    event_version: &str,
) -> EventSourcingDbResult<String> {
    let major = parse_major_version_str(event_version)?;
    let mut parts: Vec<String> = domain.labels().to_vec();
    parts.push(pascal_to_kebab_case(event_type));
    parts.push(format!("v{major}"));

    Ok(parts.join("."))
}

/// Since both metadata and data of the cqrs-es event needs to be persisted in the
/// `data` field of [`Event`], a JSON object with a special tag [`STORED_EVENT_ENVELOPE_MARKER`]
/// and two fields is created.
pub fn wrap_event_data(payload: Value, metadata: Value) -> Value {
    json!({
        "_cqrs_es": STORED_EVENT_ENVELOPE_MARKER,
        "payload": payload,
        "metadata": metadata,
    })
}

pub fn unwrap_event_data(data: &Value) -> (Value, Value) {
    let Some(object) = data.as_object() else {
        return (data.clone(), json!({}));
    };

    let is_wrapped = object
        .get("_cqrs_es")
        .and_then(Value::as_str)
        .map(|marker| marker == STORED_EVENT_ENVELOPE_MARKER)
        .unwrap_or(false);

    if !is_wrapped {
        return (data.clone(), json!({}));
    }

    let payload = object.get("payload").cloned().unwrap_or(Value::Null);
    let metadata = object.get("metadata").cloned().unwrap_or_else(|| json!({}));

    (payload, metadata)
}

pub fn map_event(
    event: Event,
    sequence: usize,
) -> EventSourcingDbResult<(SerializedEvent, EventMetadata)> {
    let event_type: QualifiedEventType = event.ty().parse()?;
    let (aggregate_type, aggregate_id) = map_subject_to_aggregate_type_and_id(event.subject())?;
    let (payload, metadata) = unwrap_event_data(event.data());
    let meta = EventMetadata::from(&event);
    let serialized = SerializedEvent {
        aggregate_id,
        sequence,
        aggregate_type,
        event_type: to_pascal_case(&event_type.event_type),
        // a non-existing version defaults to legacy v1.
        event_version: format_cqrs_event_version(event_type.event_version.unwrap_or(1)),
        payload,
        metadata,
    };

    Ok((serialized, meta))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_event(data: Value) -> Event {
        serde_json::from_value(json!({
            "data": data,
            "datacontenttype": "application/json",
            "hash": "hash",
            "id": "1",
            "predecessorhash": "predecessor",
            "source": "urn:test",
            "specversion": "1.0",
            "subject": "/BookAggregate/42",
            "time": "2026-03-17T10:00:00Z",
            "type": "io.eventsourcingdb.book-created.v2",
            "signature": null
        }))
        .expect("event JSON should deserialize")
    }

    #[test]
    fn qualify_event_type_uses_domain_and_kebab_case() {
        let domain = ReversedDomain::new(["io", "eventsourcingdb"]);
        let qualified = qualify_event_type(&domain, "BookCreated", "2.0").unwrap();

        assert_eq!(qualified, "io.eventsourcingdb.book-created.v2");
    }

    #[test]
    fn qualify_event_type_normalizes_v1_and_major_only_versions() {
        let domain = ReversedDomain::new(["io", "eventsourcingdb"]);

        assert_eq!(
            qualify_event_type(&domain, "BookCreated", "1").unwrap(),
            "io.eventsourcingdb.book-created.v1"
        );
        assert_eq!(
            qualify_event_type(&domain, "BookCreated", "1.0").unwrap(),
            "io.eventsourcingdb.book-created.v1"
        );
        assert_eq!(
            qualify_event_type(&domain, "BookCreated", "2").unwrap(),
            "io.eventsourcingdb.book-created.v2"
        );
    }

    #[test]
    fn qualify_event_type_rejects_non_major_versions() {
        let domain = ReversedDomain::new(["io", "eventsourcingdb"]);

        assert!(matches!(
            qualify_event_type(&domain, "BookCreated", "0"),
            Err(EventSourcingDbError::InvalidEventVersion(_))
        ));
        assert!(matches!(
            qualify_event_type(&domain, "BookCreated", "1.1"),
            Err(EventSourcingDbError::InvalidEventVersion(_))
        ));
        assert!(matches!(
            qualify_event_type(&domain, "BookCreated", "1.2.3"),
            Err(EventSourcingDbError::InvalidEventVersion(_))
        ));
    }

    #[test]
    fn wrap_and_unwrap_event_data_round_trip_payload_and_metadata() {
        let payload = json!({ "title": "DDD" });
        let metadata = json!({ "request_id": "abc-123" });

        let wrapped = wrap_event_data(payload.clone(), metadata.clone());
        let (actual_payload, actual_metadata) = unwrap_event_data(&wrapped);

        assert_eq!(actual_payload, payload);
        assert_eq!(actual_metadata, metadata);
    }

    #[test]
    fn unwrap_raw_event_data_keeps_payload_and_defaults_metadata() {
        let raw_payload = json!({ "title": "DDD" });
        let (payload, metadata) = unwrap_event_data(&raw_payload);

        assert_eq!(payload, raw_payload);
        assert_eq!(metadata, json!({}));
    }

    #[test]
    fn map_event_uses_logical_sequence_not_opaque_event_id() {
        let event = sample_event(wrap_event_data(
            json!({ "title": "DDD" }),
            json!({ "request_id": "abc-123" }),
        ));

        let (serialized, meta) = map_event(event, 7).expect("event should map");

        assert_eq!(serialized.sequence, 7);
        assert_eq!(serialized.aggregate_id, "42");
        assert_eq!(serialized.aggregate_type, "BookAggregate");
        assert_eq!(serialized.event_type, "BookCreated");
        assert_eq!(serialized.event_version, "2.0");
        assert_eq!(serialized.payload, json!({ "title": "DDD" }));
        assert_eq!(serialized.metadata, json!({ "request_id": "abc-123" }));
        assert_eq!(meta.subject, "/BookAggregate/42");
    }

    #[test]
    fn map_event_treats_legacy_unversioned_event_type_as_v1() {
        let event: Event = serde_json::from_value(json!({
            "data": wrap_event_data(json!({ "title": "DDD" }), json!({})),
            "datacontenttype": "application/json",
            "hash": "hash",
            "id": "1",
            "predecessorhash": "predecessor",
            "source": "urn:test",
            "specversion": "1.0",
            "subject": "/BookAggregate/42",
            "time": "2026-03-17T10:00:00Z",
            "type": "io.eventsourcingdb.book-created",
            "signature": null
        }))
        .expect("event JSON should deserialize");

        let (serialized, _) = map_event(event, 1).expect("legacy event should map");

        assert_eq!(serialized.event_type, "BookCreated");
        assert_eq!(serialized.event_version, "1.0");
    }
}
