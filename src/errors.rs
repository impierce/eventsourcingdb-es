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
    #[error("invalid numeric suffix: {0}")]
    InvalidSequence(#[from] std::num::ParseIntError),
    #[error("invalid event version: {0}")]
    InvalidEventVersion(String),
    #[error("{0}")]
    Client(ClientError),
    #[error("optimistic lock error")]
    OptimisticLock,
}

pub type EventSourcingDbResult<T> = Result<T, EventSourcingDbError>;

impl From<ClientError> for EventSourcingDbError {
    fn from(value: ClientError) -> Self {
        match value {
            ClientError::DBApiError(status, _) if status.as_u16() == 409 => {
                Self::OptimisticLock
            }
            other => Self::Client(other),
        }
    }
}

impl From<EventSourcingDbError> for PersistenceError {
    fn from(value: EventSourcingDbError) -> Self {
        match value {
            EventSourcingDbError::OptimisticLock => PersistenceError::OptimisticLockError,
            EventSourcingDbError::Client(err) => match err {
                ClientError::IoError(_)
                | ClientError::ReqwestError(_)
                | ClientError::URLParseError(_)
                | ClientError::PingFailed => PersistenceError::ConnectionError(Box::new(err)),
                ClientError::SerdeJsonError(_) | ClientError::InvalidResponseType(_) => {
                    PersistenceError::DeserializationError(Box::new(err))
                }
                _ => PersistenceError::UnknownError(Box::new(err)),
            }
            _ => PersistenceError::UnknownError(Box::new(value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_conflict_maps_to_optimistic_lock() {
        let err = EventSourcingDbError::from(ClientError::DBApiError(
            "409".parse().expect("409 should parse as status code"),
            "conflict".to_string(),
        ));

        assert!(matches!(err, EventSourcingDbError::OptimisticLock));
        assert!(matches!(
            PersistenceError::from(err),
            PersistenceError::OptimisticLockError
        ));
    }

    #[test]
    fn non_conflict_db_api_error_maps_to_unknown_error() {
        let err = EventSourcingDbError::from(ClientError::DBApiError(
            "400".parse().expect("400 should parse as status code"),
            "bad request".to_string(),
        ));

        assert!(matches!(err, EventSourcingDbError::Client(_)));
        assert!(matches!(
            PersistenceError::from(err),
            PersistenceError::UnknownError(_)
        ));
    }

    #[test]
    fn invalid_response_type_maps_to_deserialization_error() {
        let err = EventSourcingDbError::from(ClientError::InvalidResponseType("text/html".into()));

        assert!(matches!(
            PersistenceError::from(err),
            PersistenceError::DeserializationError(_)
        ));
    }
}
