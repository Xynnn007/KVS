use failure::*;
use std::fmt;

use serde::{Serialize, Deserialize};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Error {
    inner: Context<ErrorKind>,
}

#[derive(Copy, Clone, Debug, Fail, Serialize, Deserialize)]
pub enum ErrorKind {
    #[fail(display = "I/O error")]
    IOError,
    #[fail(display = "Subcommand error")]
    SubCmdError,
    #[fail(display = "Key not found")]
    NoEntryError,
    #[fail(display = "Inproper parameters")]
    ParameterError,
    #[fail(display = "Invalid log error")]
    LogError,
    #[fail(display = "Network error")]
    NetworkError,
    #[fail(display = "Serialize error")]
    SerializeError,
    #[fail(display = "Operation error")]
    OperationError,
    #[fail(display = "Sled error")]
    SledError,
    #[fail(display = "Utf8 encode/decode error")]
    Utf8Error,
    #[fail(display = "Engine error")]
    EngineError,
}

impl Fail for Error {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl Error {
    pub fn kind(&self) -> ErrorKind {
        *self.inner.get_context()
    }
}

impl From<ErrorKind> for Error {
    fn from (kind: ErrorKind) -> Error {
        Error { inner: Context::new(kind) }
    }
}

impl From<Context<ErrorKind>> for Error {
    fn from (inner: Context<ErrorKind>) -> Error {
        Error { inner }
    }
}