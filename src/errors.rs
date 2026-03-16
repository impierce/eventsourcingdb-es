use cqrs_es::persist::PersistenceError;
use eventsourcingdb::error::ClientError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EventSourcingDbError {
    #[error("{0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("invalid event type identifier: {0}")]
    InvalidEventTypeIdentifier(String),
    #[error("invalid subject: {0}")]
    InvalidSubject(String),
    #[error("invalid sequence number: {0}")]
    InvalidSequence(#[from] std::num::ParseIntError),
    //TODO: this can be unmangled/ matched
    #[error("{0}")]
    ClientError(#[from] ClientError),
}

pub type EventSourcingDbResult<T> = Result<T, EventSourcingDbError>;

impl From<EventSourcingDbError> for PersistenceError {
    //TODO: this conversion can be improved
    fn from(value: EventSourcingDbError) -> Self {
        match value {
            EventSourcingDbError::ClientError(err) => {
                PersistenceError::ConnectionError(Box::new(err))
            }
            _ => PersistenceError::UnknownError(Box::new(value)),
        }
    }
}
