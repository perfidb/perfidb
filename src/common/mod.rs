/// Simplifies the return signature when a function can fail and we don't care about the specific error type
pub type ResultError<T> = Result<T, Box<dyn std::error::Error>>;