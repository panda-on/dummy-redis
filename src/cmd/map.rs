use crate::{backend::Backend, RespArray, RespFrame, RespNull};

use super::{extract_args, validate_command, CommandError, CommandExecutor, RESP_OK};

#[derive(Debug, PartialEq, PartialOrd)]
pub struct Get {
    pub(crate) key: String,
}

#[derive(Debug, PartialEq, PartialOrd)]
pub struct Set {
    key: String,
    value: RespFrame,
}

impl CommandExecutor for Get {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.get(&self.key) {
            Some(v) => v,
            None => RespFrame::Null(RespNull),
        }
    }
}

impl CommandExecutor for Set {
    fn execute(self, backend: &Backend) -> RespFrame {
        backend.set(self.key, self.value);
        RESP_OK.clone()
    }
}

impl TryFrom<RespArray> for Get {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["get"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(Get {
                key: String::from_utf8(key.0)?,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid Key".to_string())),
        }
    }
}

impl TryFrom<RespArray> for Set {
    type Error = CommandError;

    fn try_from(v: RespArray) -> Result<Self, Self::Error> {
        validate_command(&v, &["set"], 2)?;

        let mut args = extract_args(v, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(value)) => Ok(Set {
                key: String::from_utf8(key.0)?,
                value,
            }),
            _ => Err(CommandError::InvalidArgument(
                "Invalid Key or Value".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        backend::Backend,
        cmd::{
            map::{Get, Set},
            CommandExecutor, RESP_OK,
        },
        BulkString, RespArray, RespDecode, RespFrame,
    };
    use anyhow::{Ok, Result};
    use bytes::BytesMut;

    #[test]
    fn test_get_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$3\r\nget\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: Get = Get::try_from(frame)?;
        assert_eq!(result.key, "hello");
        Ok(())
    }

    #[test]
    fn test_set_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$3\r\nset\r\n$5\r\nhello\r\n$5\r\nworld\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: Set = Set::try_from(frame)?;
        assert_eq!(result.key, "hello");
        assert_eq!(
            result.value,
            RespFrame::BulkString(BulkString::new(b"world"))
        );
        Ok(())
    }

    #[test]
    fn test_set_get_command() -> Result<()> {
        let backend = Backend::new();

        let set_cmd = Set {
            key: "hello".to_string(),
            value: RespFrame::BulkString(BulkString::new(b"world")),
        };

        let resp = set_cmd.execute(&backend);
        assert_eq!(resp, RESP_OK.clone());

        let get_cmd = Get {
            key: "hello".to_string(),
        };

        let resp = get_cmd.execute(&backend);
        assert_eq!(resp, RespFrame::BulkString(BulkString::new(b"world")));

        Ok(())
    }
}
