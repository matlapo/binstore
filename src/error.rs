use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    IoError(std::io::Error),
    BincodeError(bincode::Error),
    JsonError(serde_json::Error),
    ChronoError(chrono::ParseError),
    BadMagic,
    BadVersion,
    DateParseError,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Error::IoError(ref err) => write!(f, "io error: {}", err),
            &Error::BincodeError(ref err) => write!(f, "bincode error: {}", err),
            &Error::JsonError(ref err) => write!(f, "json error: {}", err),
            &Error::ChronoError(ref err) => write!(f, "chrono error: {}", err),
            &Error::BadMagic => write!(f, "bad magic number"),
            &Error::BadVersion => write!(f, "bad version number"),
            &Error::DateParseError => write!(f, "invalid date format"),
        }
    }
}

macro_rules! impl_error {
    ($external_error:ty, $prophet_error:expr) => (
        impl From<$external_error> for Error {
            fn from(e: $external_error) -> Error {
                $prophet_error(e)
            }
        }
    );
}


impl_error!(std::io::Error, Error::IoError);
impl_error!(bincode::Error, Error::BincodeError);
impl_error!(serde_json::Error, Error::JsonError);
impl_error!(chrono::ParseError, Error::ChronoError);
