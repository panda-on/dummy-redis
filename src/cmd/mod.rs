use enum_dispatch::enum_dispatch;
use hmap::{HGet, HGetAll, HSet};
use lazy_static::lazy_static;
use map::{Get, Set};
use thiserror::Error;

use crate::{Backend, RespArray, RespFrame, SimpleString};

mod hmap;
mod map;

lazy_static! {
    static ref RESP_OK: RespFrame = RespFrame::SimpleString(SimpleString("OK".into()));
}

#[enum_dispatch]
pub trait CommandExecutor {
    fn execute(self, backend: &Backend) -> RespFrame;
}

#[enum_dispatch(CommandExecutor)]
#[derive(Debug, PartialEq, PartialOrd)]
pub enum Command {
    Get(Get),
    Set(Set),
    HGet(HGet),
    HSet(HSet),
    HGetAll(HGetAll),
}

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("Invalid command: {0}")]
    InvalidCommand(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Utf8 error: {0}")]
    FromUTF8Error(#[from] std::string::FromUtf8Error),
}

impl TryFrom<RespArray> for Command {
    type Error = CommandError;

    fn try_from(v: RespArray) -> Result<Self, Self::Error> {
        match v.first() {
            Some(RespFrame::BulkString(ref cmd)) => match cmd.as_ref() {
                b"get" => Ok(Command::Get(v.try_into()?)),
                b"set" => Ok(Command::Set(v.try_into()?)),
                b"hget" => Ok(Command::HGet(v.try_into()?)),
                b"hset" => Ok(Command::HSet(v.try_into()?)),
                _ => Err(CommandError::InvalidCommand(format!(
                    "Invalid Command: {:?}",
                    String::from_utf8_lossy(cmd.as_ref()),
                ))),
            },
            _ => Err(CommandError::InvalidCommand(
                "Command must have a BulkString as the first element".to_string(),
            )),
        }
    }
}

fn validate_command(
    value: &RespArray,
    names: &[&'static str],
    n_args: usize,
) -> Result<(), CommandError> {
    if value.len() != n_args + names.len() {
        return Err(CommandError::InvalidArgument(format!(
            "{} command must have exactly {} arguments",
            names.join(" "),
            n_args
        )));
    };

    for (i, name) in names.iter().enumerate() {
        match value[i] {
            RespFrame::BulkString(ref cmd) => {
                if cmd.as_ref().to_ascii_lowercase() != name.as_bytes() {
                    return Err(CommandError::InvalidCommand(format!(
                        "Invalid command: expected {}, got {}",
                        name,
                        String::from_utf8_lossy(cmd.as_ref())
                    )));
                }
            }
            _ => {
                return Err(CommandError::InvalidCommand(
                    "Command must start with a BulkString argument".to_string(),
                ))
            }
        }
    }

    Ok(())
}

fn extract_args(value: RespArray, start: usize) -> Result<Vec<RespFrame>, CommandError> {
    Ok(value.0.into_iter().skip(start).collect::<Vec<RespFrame>>())
}

#[cfg(test)]
mod tests {
    use crate::{
        backend::Backend,
        cmd::{Command, CommandExecutor},
        RespArray, RespDecode, RespFrame, RespNull,
    };
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_command() -> Result<()> {
        let backend = Backend::new();

        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$3\r\nget\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let cmd: Command = frame.try_into()?;

        let ret = cmd.execute(&backend);
        assert_eq!(ret, RespFrame::Null(RespNull));

        Ok(())
    }
}
