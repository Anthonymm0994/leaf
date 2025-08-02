use std::fmt;

#[derive(Debug)]
pub enum LeafError {
    Io(std::io::Error),
    Csv(csv::Error),
    Arrow(datafusion::arrow::error::ArrowError),
    Custom(String),
    Database(String),
}

impl fmt::Display for LeafError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LeafError::Io(err) => write!(f, "IO error: {}", err),
            LeafError::Csv(err) => write!(f, "CSV error: {}", err),
            LeafError::Arrow(err) => write!(f, "Arrow error: {}", err),
            LeafError::Custom(msg) => write!(f, "Custom error: {}", msg),
            LeafError::Database(msg) => write!(f, "Database error: {}", msg),
        }
    }
}

impl std::error::Error for LeafError {}

impl From<std::io::Error> for LeafError {
    fn from(err: std::io::Error) -> Self {
        LeafError::Io(err)
    }
}

impl From<csv::Error> for LeafError {
    fn from(err: csv::Error) -> Self {
        LeafError::Csv(err)
    }
}

impl From<datafusion::arrow::error::ArrowError> for LeafError {
    fn from(err: datafusion::arrow::error::ArrowError) -> Self {
        LeafError::Arrow(err)
    }
}

pub type Result<T> = std::result::Result<T, LeafError>; 