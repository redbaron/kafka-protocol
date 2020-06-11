use serde;
use std::fmt;
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

    #[error("Primitive '{0}' is not supported")]
    Unsupported(&'static str),

    #[error("serde")]
    Custom(String),
}

impl serde::de::Error for Error {
    fn custom<T: fmt::Display>(desc: T) -> Error {
        Error::Custom(desc.to_string()).into()
    }
}

impl serde::ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Custom(msg.to_string()).into()
    }
}
