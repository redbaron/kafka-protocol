use std::io;
use thiserror::Error;

/// The result of a serialization or deserialization operation.
pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO")]
    Io(#[from] io::Error),

    #[error("Conversion")]
    IntError(#[from] std::num::TryFromIntError),
}
