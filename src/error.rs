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
}
