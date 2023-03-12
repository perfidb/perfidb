use std::fmt::{Display, Formatter};

/// Simplifies the return signature when a function can fail and we don't care about the specific error type
pub type ResultError<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    msg: String
}

impl Error {
    pub fn new(msg: String) -> Error {
        Error { msg }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for Error {}

