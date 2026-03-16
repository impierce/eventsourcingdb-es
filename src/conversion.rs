use std::str::FromStr;

use chrono::{DateTime, Utc};
use cqrs_es::persist::SerializedEvent;
use eventsourcingdb::{Event, TraceInfo};
use serde::{Deserialize, Serialize};

use crate::errors::{EventSourcingDbError, EventSourcingDbResult};

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
// e.g. /books/42
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReversedDomain(Vec<String>);

impl ReversedDomain {
    pub fn labels(&self) -> &[String] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedEventType {
    pub reversed_domain: ReversedDomain,
    pub event_type: String,
    pub event_version: Option<usize>,
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

pub fn map_event(event: Event) -> EventSourcingDbResult<SerializedEvent> {
    let event_type: QualifiedEventType = event.ty().parse()?;
    let (aggregate_type, aggregate_id) = map_subject_to_aggregate_type_and_id(event.subject())?;
    let meta = EventMetadata::from(&event);
    Ok(SerializedEvent {
        aggregate_id: aggregate_id,
        sequence: event.id().parse()?,
        aggregate_type,
        event_type: event_type.event_type,
        // a non-existing version defaults to 1
        event_version: event_type.event_version.unwrap_or(1 as usize).to_string(),
        payload: event.data().clone(),
        metadata: serde_json::to_value(&meta)?,
    })
}
