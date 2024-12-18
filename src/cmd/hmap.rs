use crate::{backend::Backend, RespArray, RespFrame, RespMap, RespNull};

use super::{extract_args, validate_command, CommandError, CommandExecutor, RESP_OK};

#[derive(Debug, PartialEq, PartialOrd)]
pub struct HGet {
    key: String,
    field: String,
}

impl TryFrom<RespArray> for HGet {
    type Error = CommandError;

    fn try_from(arr: RespArray) -> Result<Self, Self::Error> {
        validate_command(&arr, &["hget"], 2)?;

        let mut args = extract_args(arr, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(map)), Some(RespFrame::BulkString(key))) => Ok(Self {
                key: String::from_utf8(map.0)?,
                field: String::from_utf8(key.0)?,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid Arguments".into())),
        }
    }
}

impl CommandExecutor for HGet {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.hget(&self.key, &self.field) {
            Some(value) => value,
            None => RespFrame::Null(RespNull),
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd)]
pub struct HSet {
    key: String,
    field: String,
    value: RespFrame,
}

impl TryFrom<RespArray> for HSet {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hset"], 3)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next(), args.next()) {
            (Some(RespFrame::BulkString(map)), Some(RespFrame::BulkString(key)), Some(value)) => {
                Ok(Self {
                    key: String::from_utf8(map.0)?,
                    field: String::from_utf8(key.0)?,
                    value,
                })
            }
            _ => Err(CommandError::InvalidArgument("Invalid Arguments".into())),
        }
    }
}

impl CommandExecutor for HSet {
    fn execute(self, backend: &Backend) -> RespFrame {
        backend.hset(self.key, self.field, self.value);
        RESP_OK.clone()
    }
}

#[derive(Debug, PartialEq, PartialOrd)]
pub struct HGetAll {
    key: String,
}

impl TryFrom<RespArray> for HGetAll {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hgetall"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(Self {
                key: String::from_utf8(key.0)?,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid Arguments".into())),
        }
    }
}

impl CommandExecutor for HGetAll {
    fn execute(self, backend: &Backend) -> RespFrame {
        if let Some(map) = backend.hgetall(self.key.as_str()) {
            let mut ret = RespMap::new();
            map.iter().for_each(|e| {
                ret.insert(e.key().clone(), e.value().clone());
            });
            ret.into()
        } else {
            RespFrame::Null(RespNull)
        }
    }
}
#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{
        backend::Backend,
        cmd::{hmap::HGetAll, CommandExecutor, RESP_OK},
        BulkString, RespArray, RespFrame, RespMap,
    };

    use super::{HGet, HSet};

    #[test]
    fn test_hget_from_resp_array() -> Result<()> {
        let resp_arr = RespArray::new(vec![
            RespFrame::BulkString(BulkString(b"hget".into())),
            RespFrame::BulkString(BulkString(b"map1".into())),
            RespFrame::BulkString(BulkString(b"key".into())),
        ]);
        let cmd = HGet::try_from(resp_arr)?;
        assert_eq!(cmd.key, "map1");
        assert_eq!(cmd.field, "key");
        Ok(())
    }

    #[test]
    fn test_hset_from_resp_array() -> Result<()> {
        let resp_arr = RespArray::new(vec![
            RespFrame::BulkString(BulkString(b"hset".into())),
            RespFrame::BulkString(BulkString(b"map1".into())),
            RespFrame::BulkString(BulkString(b"key".into())),
            RespFrame::BulkString(BulkString(b"value".into())),
        ]);
        let cmd = HSet::try_from(resp_arr)?;
        assert_eq!(cmd.key, "map1");
        assert_eq!(cmd.field, "key");
        assert_eq!(cmd.value, RespFrame::BulkString(BulkString::new(b"value")));
        Ok(())
    }

    #[test]
    fn test_hgetall_from_resp_array() -> Result<()> {
        let resp_arr = RespArray::new(vec![
            RespFrame::BulkString(BulkString(b"hgetall".into())),
            RespFrame::BulkString(BulkString(b"map1".into())),
        ]);
        let hgetall = HGetAll::try_from(resp_arr)?;
        assert_eq!(hgetall.key, "map1");
        Ok(())
    }

    #[test]
    fn test_hset_get_command() -> Result<()> {
        let backend = Backend::new();

        let sets = vec![
            HSet {
                key: "map1".to_string(),
                field: "hello".to_string(),
                value: RespFrame::BulkString(BulkString::new(b"world")),
            },
            HSet {
                key: "map1".to_string(),
                field: "foo".to_string(),
                value: RespFrame::BulkString(BulkString::new(b"bar")),
            },
        ];
        for cmd in sets {
            let resp = cmd.execute(&backend);
            assert_eq!(resp, RESP_OK.clone());
        }
        let hget_cmd = HGet {
            key: "map1".to_string(),
            field: "hello".to_string(),
        };
        let resp = hget_cmd.execute(&backend);
        assert_eq!(resp, RespFrame::BulkString(BulkString::new(b"world")));

        let hgetall_cmd = HGetAll {
            key: "map1".to_string(),
        };
        let resp = hgetall_cmd.execute(&backend);
        let mut rval = RespMap::new();
        rval.insert(
            "hello".to_string(),
            RespFrame::BulkString(BulkString::new(b"world")),
        );
        rval.insert(
            "foo".to_string(),
            RespFrame::BulkString(BulkString::new(b"bar")),
        );
        assert_eq!(resp, rval.into());
        Ok(())
    }
}
