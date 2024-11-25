use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
};

use bytes::BytesMut;
use enum_dispatch::enum_dispatch;
use thiserror::Error;

mod decode;
mod encode;

/*
Redis RESP data types:

Simple strings: +OK\r\n
Simple Errors: -Error message\r\n
Integers: :[<+|->]<value>\r\n
Bulk strings: $<length>\r\n<data>\r\n  $5\r\nhello\r\n
Arrays: *<number-of-elements>\r\n<element-1>...<element-n>
Nulls: _\r\n
Booleans: #<t|f>\r\n
Doubles: ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
Big numbers: ([+|-]<number>\r\n
Bulk errors: !<length>\r\n<error>\r\n
Verbatim strings: =<length>\r\n<encoding>:<data>\r\n
Maps: %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
Sets: ~<number-of-elements>\r\n<element-1>...<element-n>

*/

#[enum_dispatch(RespEncode)]
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum RespFrame {
    SimpleString(SimpleString),
    SimpleError(SimpleError),
    Integer(i64),
    BulkString(BulkString),
    NullBulkString(NullBulkString),
    Array(RespArray),
    Null(RespNull),
    NullArray(RespNullArray),
    Boolean(bool),
    Double(f64),
    Map(RespMap),
    Set(RespSet),
}

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct SimpleString(String);

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct SimpleError(String);

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct BulkString(Vec<u8>);

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct RespArray(Vec<RespFrame>);

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct NullBulkString;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct RespNull;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct RespNullArray;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct RespMap(BTreeMap<String, RespFrame>);

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct RespSet(Vec<RespFrame>);

impl SimpleString {
    pub fn new(s: String) -> Self {
        SimpleString(s)
    }
}

impl SimpleError {
    pub fn new(s: String) -> Self {
        SimpleError(s)
    }
}

impl BulkString {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        BulkString(s.into())
    }
}

impl RespArray {
    pub fn new(v: impl Into<Vec<RespFrame>>) -> Self {
        RespArray(v.into())
    }
}

impl RespMap {
    pub fn new() -> Self {
        RespMap(BTreeMap::new())
    }
}

impl Default for RespMap {
    fn default() -> Self {
        Self::new()
    }
}

impl RespSet {
    pub fn new(v: impl Into<Vec<RespFrame>>) -> Self {
        RespSet(v.into())
    }
}

#[enum_dispatch]
pub trait RespEncode {
    fn encode(self) -> Vec<u8>;
}

pub trait RespDecode: Sized {
    const PREFIX: &'static str;
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError>;
}

impl Deref for SimpleString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for SimpleError {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for RespMap {
    type Target = BTreeMap<String, RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RespMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, Error)]
pub enum RespError {
    #[error("Invalid frame: {0}")]
    InvalidFrame(String),
    #[error("Invalid length: {0}")]
    InvalidFrameLength(String),
    #[error("Invalid frame type: {0}")]
    InvalidFrameType(String),
    #[error("Not complete")]
    NotComplete,

    #[error("Parse int error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("Parse float error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),
}
