use std::fmt;

#[derive(Debug)]
pub enum FreshError {
    Io(std::io::Error),
    Csv(csv::Error),
    Arrow(datafusion::arrow::error::ArrowError),
    Custom(String),
    Database(String),
}

impl fmt::Display for FreshError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FreshError::Io(err) => write!(f, "IO error: {}", err),
            FreshError::Csv(err) => write!(f, "CSV error: {}", err),
            FreshError::Arrow(err) => write!(f, "Arrow error: {}", err),
            FreshError::Custom(msg) => write!(f, "Custom error: {}", msg),
            FreshError::Database(msg) => write!(f, "Database error: {}", msg),
        }
    }
}

impl std::error::Error for FreshError {}

impl From<std::io::Error> for FreshError {
    fn from(err: std::io::Error) -> Self {
        FreshError::Io(err)
    }
}

impl From<csv::Error> for FreshError {
    fn from(err: csv::Error) -> Self {
        FreshError::Csv(err)
    }
}

impl From<datafusion::arrow::error::ArrowError> for FreshError {
    fn from(err: datafusion::arrow::error::ArrowError) -> Self {
        FreshError::Arrow(err)
    }
}

pub type Result<T> = std::result::Result<T, FreshError>; 