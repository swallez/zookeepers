use std::fmt::{Display, Formatter};

use serde::{de, ser};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    Message(String),
    TooLarge(usize),
    NegativeValue,
    Eof,
}

impl From<std::io::Error> for Error {
    fn from(io_err: std::io::Error) -> Self {
        use std::io::ErrorKind;
        match io_err.kind() {
            ErrorKind::WouldBlock | ErrorKind::UnexpectedEof => Error::Eof,
            _ => Error::Message(io_err.to_string()),
        }
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Self {
        Error::Message(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Error::Message(err.to_string())
    }
}

impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            Error::Message(ref msg) => f.write_str(msg),
            Error::TooLarge(size) => f.write_fmt(format_args!("too large: {}", size)),
            Error::NegativeValue => f.write_str("negative value"),
            Error::Eof => f.write_str("unexpected end of input"),
        }
    }
}

impl std::error::Error for Error {}
