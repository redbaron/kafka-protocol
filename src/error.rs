use std::io;
use thiserror::Error;

/// The result of a serialization or deserialization operation.
pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO")]
    Io(#[from] io::Error),

    #[error("Bounds check")]
    BoundsCheck(#[from] std::num::TryFromIntError),

    // Deserialization errors below
    #[error("Message length")]
    OutOfBounds,

    #[error("Incorrect varint")]
    IncorrectVarint,

    #[error("UTF8 decoding error")]
    UTF8Error(#[from] std::str::Utf8Error),
}
