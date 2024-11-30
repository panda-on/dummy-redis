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

use bytes::{Buf, BytesMut};

use super::{
    BulkString, NullBulkString, RespArray, RespDecode, RespError, RespFrame, RespNull,
    RespNullArray, RespSet, SimpleError, SimpleString,
};

const CRLF: &[u8] = b"\r\n";
const CRLF_LEN: usize = CRLF.len();

impl RespDecode for RespFrame {
    const PREFIX: &'static str = "";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let mut iter = buf.iter().peekable();
        let ret = match iter.peek() {
            Some(b'+') => {
                let frame = SimpleString::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'_') => {
                let frame = RespNull::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'#') => {
                let frame = bool::decode(buf)?;
                Ok(frame.into())
            }
            Some(b',') => {
                let frame = f64::decode(buf)?;
                Ok(frame.into())
            }
            Some(b'$') => Ok(BulkString::decode(buf)?.into()),
            Some(b'*') => Ok(RespArray::decode(buf)?.into()),
            // b'%' => RespMap::decode(buf),
            Some(b'~') => Ok(RespSet::decode(buf)?.into()),
            // b'!' => SimpleError::decode(buf),
            _ => Err(RespError::Incomplete),
        };
        ret
    }
}

// Simple strings: +OK\r\n
impl RespDecode for SimpleString {
    const PREFIX: &'static str = "+";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let iter = buf.iter().peekable();
        // split by \r\n and trim the first byte, leave the rest converted to string
        let mut buf = Vec::new();
        for b in iter {
            if *b != b'+' && *b != b'\r' && *b != b'\n' {
                buf.push(*b);
            }
        }
        Ok(SimpleString::new(String::from_utf8_lossy(&buf).to_string()))
    }
}

impl RespDecode for SimpleError {
    const PREFIX: &'static str = "-";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        // check the prefix
        if buf.len() < 3 || buf[0] != b'-' {
            return Err(RespError::InvalidFrameType(format!(
                "expected prefix {:?} - but got {:?}",
                Self::PREFIX,
                buf[0]
            )));
        }
        // get the start index and end index of the string
        let start_idx = 1;
        let end_idx = buf.len() - 2;
        Ok(SimpleError(
            String::from_utf8_lossy(&buf[start_idx..end_idx]).to_string(),
        ))
    }
}

impl RespDecode for RespNull {
    const PREFIX: &'static str = "_";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if buf[0] == Self::PREFIX.as_bytes()[0] && buf.len() == 3 {
            Ok(RespNull)
        } else {
            Err(RespError::InvalidFrameType(format!(
                "expected prefix {:?} - but got {:?}",
                Self::PREFIX,
                buf[0]
            )))
        }
    }
}

impl RespDecode for RespNullArray {
    const PREFIX: &'static str = "_";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if buf.len() < 3 || buf[0] != Self::PREFIX.as_bytes()[0] {
            Err(RespError::InvalidFrameType(format!(
                "expected prefix {:?} - but got {:?}",
                Self::PREFIX,
                buf[0]
            )))
        } else {
            Ok(RespNullArray)
        }
    }
}

impl RespDecode for BulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if let Some(second_crlf_idx) = find_crlf(buf, 2) {
            let crlf_1st_idx = extra_simple_frame_data(Self::PREFIX, buf)?;
            let data = buf.split_to(second_crlf_idx + CRLF_LEN);
            Ok(BulkString(
                data[crlf_1st_idx + CRLF_LEN..second_crlf_idx].to_vec(),
            ))
        } else {
            Err(RespError::Incomplete)
        }
    }
}
// $-1\r\n
impl RespDecode for NullBulkString {
    const PREFIX: &'static str = "$-1";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if buf.len() < 3 && buf[0..3] != Self::PREFIX.as_bytes()[0..3] {
            Err(RespError::InvalidFrameType(format!(
                "expected prefix {:?} - but got {:?}",
                Self::PREFIX,
                buf[0]
            )))
        } else {
            Ok(NullBulkString)
        }
    }
}

// Integer: :[<+|->]<value>\r\n
impl RespDecode for i64 {
    const PREFIX: &'static str = ":";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let ret = if buf.len() < 3 && buf[0] != Self::PREFIX.as_bytes()[0] {
            Err(RespError::InvalidFrameType(
                String::from_utf8_lossy(buf).to_string(),
            ))
        } else {
            let start_idx = 1;
            let end_idx = buf.len() - 2;
            let s = String::from_utf8_lossy(&buf[start_idx..end_idx]);
            let res = if let Ok(i) = s.parse::<i64>() {
                Ok(i)
            } else {
                Err(RespError::InvalidFrameType(
                    String::from_utf8_lossy(buf).to_string(),
                ))
            };
            res
        };
        ret
    }
}

// Booleans: #<t|f>\r\n
impl RespDecode for bool {
    const PREFIX: &'static str = "#";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let crlf_idx = extra_simple_frame_data(Self::PREFIX, buf)?;
        let data = buf.split_to(crlf_idx + CRLF_LEN);
        let res_str = String::from_utf8_lossy(&data[Self::PREFIX.len()..crlf_idx]);
        if res_str == "t" {
            Ok(true)
        } else if res_str == "f" {
            Ok(false)
        } else {
            Err(RespError::InvalidFrame(format!(
                "expected #<t|f>\r\n, got {:?}",
                data
            )))
        }
    }
}
// Doubles: ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
impl RespDecode for f64 {
    const PREFIX: &'static str = ",";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let crlf_end = extra_simple_frame_data(Self::PREFIX, buf)?;

        // flash out all bytes
        let data = buf.split_to(crlf_end + CRLF_LEN);
        let num_str = String::from_utf8_lossy(&data[Self::PREFIX.len()..crlf_end]);
        let res = num_str.parse::<f64>()?;
        Ok(res)
    }
}

fn extra_simple_frame_data(prefix: &str, buf: &mut BytesMut) -> Result<usize, RespError> {
    if buf.len() < 3 {
        return Err(RespError::Incomplete);
    };

    if !buf.starts_with(prefix.as_bytes()) {
        return Err(RespError::InvalidFrame(format!(
            "expected {} but got {:?}",
            prefix, buf[0]
        )));
    };
    let crlf_end = find_crlf(buf, 1).ok_or(RespError::Incomplete)?;
    Ok(crlf_end)
}

fn find_crlf(buf: &mut BytesMut, nth: i32) -> Option<usize> {
    let mut cnt = 0;
    for i in 0..buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            cnt += 1;
            if cnt == nth {
                return Some(i);
            }
        }
    }
    None
}

// *<number-of-elements>\r\n<element-1>...<element-n>
impl RespDecode for RespArray {
    const PREFIX: &'static str = "*";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (crlf_1st_idx, elem_len) = calc_total_length(buf, Self::PREFIX)?;
        let mut ret = Vec::with_capacity(elem_len);
        buf.advance(crlf_1st_idx + CRLF_LEN);
        for _ in 0..elem_len {
            let elem = RespFrame::decode(buf)?;
            ret.push(elem);
        }
        Ok(RespArray::new(ret))
    }
}

fn calc_total_length(buf: &mut BytesMut, prefix: &str) -> Result<(usize, usize), RespError> {
    let crlf_idx = extra_simple_frame_data(prefix, buf)?;
    let elem_len = String::from_utf8_lossy(&buf[prefix.len()..crlf_idx]);
    Ok((crlf_idx, elem_len.parse::<usize>()?))
}

// impl RespDecode for RespMap {
//     const PREFIX: &'static str = "%";
//     fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
//         todo!()
//     }
// }

impl RespDecode for RespSet {
    const PREFIX: &'static str = "~";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (crlf_1st_idx, len) = calc_total_length(buf, Self::PREFIX)?;
        let mut frames = Vec::with_capacity(len);
        buf.advance(crlf_1st_idx + CRLF_LEN);
        for _ in 0..len {
            frames.push(RespFrame::decode(buf)?);
        }
        Ok(RespSet::new(frames))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_simple_string_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"+OK\r\n \r\nHello\r\n");
        let frame = SimpleString::decode(&mut buf)?;
        assert_eq!(frame, SimpleString("OK Hello".to_string()));
        println!("{:?}", frame);
        Ok(())
    }

    #[test]
    fn test_simple_error_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"-Error Message\r\n");
        let frame = SimpleError::decode(&mut buf)?;
        assert_eq!(frame, SimpleError("Error Message".to_string()));
        Ok(())
    }

    #[test]
    fn test_null_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"$-1\r\n");
        let frame = NullBulkString::decode(&mut buf)?;
        assert_eq!(frame, NullBulkString);
        Ok(())
    }

    #[test]
    fn test_resp_null_array_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"_\r\n");
        let frame = RespNull::decode(&mut buf)?;
        assert_eq!(frame, RespNull);
        Ok(())
    }

    #[test]
    fn test_null_encode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"_\r\n");
        let frame = RespNull::decode(&mut buf)?;
        assert_eq!(frame, RespNull);
        println!("{:?}", frame);
        Ok(())
    }

    #[test]
    fn test_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"$5\r\nHello\r\n");
        let frame = BulkString::decode(&mut buf)?;
        assert_eq!(frame, BulkString::new("Hello".to_string()));

        let _ = buf.split();
        buf.extend_from_slice(b"$11\r\nHHHHHHHHHHH\r\n");
        let frame = BulkString::decode(&mut buf)?;
        assert_eq!(frame, BulkString::new("HHHHHHHHHHH".to_string()));
        Ok(())
    }

    #[test]
    fn test_boolean_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"#t\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespFrame::Boolean(true));
        Ok(())
    }

    #[test]
    fn test_double_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b",1.23e3\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespFrame::Double(1230.0));
        buf.extend_from_slice(b",1.23e-9\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(frame, RespFrame::Double(1.23e-9));
        Ok(())
    }

    #[test]
    fn test_array_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n#t\r\n#f\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(
            frame,
            RespFrame::Array(RespArray(vec![
                RespFrame::Boolean(true),
                RespFrame::Boolean(false)
            ]))
        );
        buf.extend_from_slice(b"*2\r\n$5\r\nHello\r\n$5\r\nWorld\r\n");
        let lval = RespFrame::decode(&mut buf)?;
        let bulk_string1 = RespFrame::BulkString(BulkString::new(b"Hello"));
        let bulk_string2 = RespFrame::BulkString(BulkString::new(b"World"));
        let rval = RespFrame::Array(RespArray::new(vec![bulk_string1, bulk_string2]));
        assert_eq!(lval, rval);

        let data = b"*10\r\n#t\r\n#t\r\n#t\r\n#t\r\n#t\r\n#t\r\n#t\r\n#t\r\n#t\r\n#t\r\n";
        buf.extend_from_slice(data);
        let frame = RespFrame::decode(&mut buf)?;
        assert_eq!(
            frame,
            RespFrame::Array(RespArray(vec![RespFrame::Boolean(true); 10]))
        );
        Ok(())
    }

    #[test]
    fn test_respset_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"~2\r\n$5\r\nHello\r\n$5\r\nWorld\r\n");
        let frame = RespFrame::decode(&mut buf)?;
        let bulk_string1 = RespFrame::BulkString(BulkString::new(b"Hello"));
        let bulk_string2 = RespFrame::BulkString(BulkString::new(b"World"));
        let rval = RespFrame::Set(RespSet::new(vec![bulk_string1, bulk_string2]));
        assert_eq!(frame, rval);
        Ok(())
    }
}
