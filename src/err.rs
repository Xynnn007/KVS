use std::{io, string::FromUtf8Error, net::AddrParseError};

use failure::*;
use rayon::ThreadPoolBuildError;

pub type Result<T> = std::result::Result<T, KvsError>;

#[derive(Debug, Fail)]
pub enum KvsError {
    #[fail(display = "I/O error {}", _0)]
    IOError(io::Error),
    #[fail(display = "Serialize error, {}", _0)]
    SerializeError(serde_json::Error),
    #[fail(display = "Operation error")]
    OperationError,
    #[fail(display = "Sled error, {}", _0)]
    SledError(sled::Error),

    #[fail(display = "Key not found")]
    NoEntryError,
    #[fail(display = "Log type wrong")]
    LogError,
    #[fail(display = "Subcommand type wrong")]
    SubCmdError,
    #[fail(display = "Utf8 encode/decode error: {}", _0)]
    Utf8Error(FromUtf8Error),
    #[fail(display = "Engine error")]
    EngineError,
    #[fail(display = "Rayon error, {}", _0)]
    RayonError(ThreadPoolBuildError),

    #[fail(display = "Addr Parse Error, {}", _0)]
    AddrParseError(AddrParseError),
}

impl From<serde_json::Error> for KvsError {
    fn from (e: serde_json::Error) -> KvsError {
        KvsError::SerializeError(e)
    }
}

impl From<io::Error> for KvsError {
    fn from (e: io::Error) -> KvsError {
        KvsError::IOError(e)
    }
}

impl From<sled::Error> for KvsError {
    fn from (e: sled::Error) -> KvsError {
        KvsError::SledError(e)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from (e: FromUtf8Error) -> KvsError {
        KvsError::Utf8Error(e)
    }
}

impl From<ThreadPoolBuildError> for KvsError {
    fn from (e: ThreadPoolBuildError) -> KvsError {
        KvsError::RayonError(e)
    }
}

impl From<AddrParseError> for KvsError {
    fn from (e: AddrParseError) -> KvsError {
        KvsError::AddrParseError(e)
    }
}