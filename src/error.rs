use cqrs_es::persist::PersistenceError;

#[derive(Debug)]
pub enum EventSourcingDbError {
    DeserializationError(Box<dyn std::error::Error + Send + Sync>),
    UnknownError(Box<dyn std::error::Error + Send + Sync>),
}

impl From<eventsourcingdb::error::ClientError> for EventSourcingDbError {
    fn from(error: eventsourcingdb::error::ClientError) -> Self {
        match error {
            eventsourcingdb::error::ClientError::SerdeJsonError(error) => {
                EventSourcingDbError::DeserializationError(Box::new(error))
            }
            _ => EventSourcingDbError::UnknownError(Box::new(error)),
        }
    }
}

impl From<EventSourcingDbError> for PersistenceError {
    fn from(error: EventSourcingDbError) -> Self {
        match error {
            EventSourcingDbError::DeserializationError(err) => {
                PersistenceError::DeserializationError(err)
            }
            EventSourcingDbError::UnknownError(err) => PersistenceError::UnknownError(err),
        }
    }
}
