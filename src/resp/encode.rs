/*
Redis RESP data types:

Simple strings: +OK\r\n
Simple Errors: -Error message\r\n
Integers: :[<+|->]<value>\r\n
Bulk strings: $<length>\r\n<data>\r\n  $5\r\nhello\r\n
NullBulkStrings: $-1\r\n
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

const BUF_CAP: usize = 4096;

use super::{
    BulkString, NullBulkString, RespArray, RespEncode, RespMap, RespNull, RespNullArray, RespSet,
    SimpleError, SimpleString,
};

impl RespEncode for SimpleString {
    fn encode(self) -> Vec<u8> {
        format!("+{}\r\n", *self).into_bytes()
    }
}

impl RespEncode for SimpleError {
    fn encode(self) -> Vec<u8> {
        format!("-{}\r\n", *self).into_bytes()
    }
}

impl RespEncode for i64 {
    fn encode(self) -> Vec<u8> {
        let sign = if self > 0 { "+" } else { "" };
        format!(":{}{}\r\n", sign, self).into_bytes()
    }
}

impl RespEncode for BulkString {
    fn encode(self) -> Vec<u8> {
        let len = self.0.len();
        let data = String::from_utf8_lossy(&self.0).to_string();
        format!("${}\r\n{}\r\n", len, data).into_bytes()
    }
}

// Arrays: *<number-of-elements>\r\n<element-1>...<element-n>
impl RespEncode for RespArray {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAP);
        buf.extend_from_slice(&format!("*{}\r\n", self.0.len()).into_bytes());
        for frame in self.0 {
            buf.extend_from_slice(&frame.encode());
        }
        buf
    }
}

// Nulls: _\r\n
impl RespEncode for RespNull {
    fn encode(self) -> Vec<u8> {
        b"_\r\n".to_vec()
    }
}

// Null Arrays: _\r\n
impl RespEncode for RespNullArray {
    fn encode(self) -> Vec<u8> {
        b"_\r\n".to_vec()
    }
}

impl RespEncode for NullBulkString {
    fn encode(self) -> Vec<u8> {
        b"$-1\r\n".to_vec()
    }
}

// Maps: %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
impl RespEncode for RespMap {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAP);
        buf.extend_from_slice(&format!("%{}\r\n", self.0.len()).into_bytes());
        for (key, value) in self.0 {
            buf.extend_from_slice(&SimpleString::new(key).encode());
            buf.extend_from_slice(&value.encode());
        }
        buf
    }
}

// Sets: ~<number-of-elements>\r\n<element-1>...<element-n>
impl RespEncode for RespSet {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAP);
        buf.extend_from_slice(&format!("~{}\r\n", self.0.len()).into_bytes());
        for frame in self.0 {
            buf.extend_from_slice(&frame.encode());
        }
        buf
    }
}

// Booleans: #<t|f>\r\n
impl RespEncode for bool {
    fn encode(self) -> Vec<u8> {
        let val = if self { "t" } else { "f" };
        format!("#{}\r\n", val).into_bytes()
    }
}

// Doubles: ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
impl RespEncode for f64 {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAP);
        let ret = if self.abs() > 1e8 || self.abs() < 1e-8 {
            format!(",{:+e}\r\n", self)
        } else {
            let sign = if self < 0.0 { "" } else { "+" };
            format!(",{}{}\r\n", sign, self)
        };
        buf.extend_from_slice(&ret.into_bytes());
        buf
    }
}

#[cfg(test)]
mod tests {

    use crate::resp::RespFrame;

    use super::*;

    #[test]
    fn test_simple_string_encode() {
        let s = SimpleString::new("hello".to_string());
        let encoded = s.encode();
        assert_eq!(encoded, b"+hello\r\n");
    }

    #[test]
    fn test_error_encode() {
        let e = SimpleError::new("ErrorMessage".to_string());
        let encoded = e.encode();
        assert_eq!(encoded, b"-ErrorMessage\r\n");
    }

    #[test]
    fn test_boolean_encode() {
        let b = true;
        let encoded = b.encode();
        assert_eq!(encoded, b"#t\r\n");

        let f = false;
        let encoded = f.encode();
        assert_eq!(encoded, b"#f\r\n");
    }

    #[test]
    fn test_integer_encode() {
        let i = 123;
        let encoded = i.encode();
        assert_eq!(encoded, b":+123\r\n");

        let i = -123;
        let encoded = i.encode();
        assert_eq!(encoded, b":-123\r\n");
    }

    #[test]
    fn test_double_encode() {
        let frame: RespFrame = 123.45.into();
        assert_eq!(frame.encode(), b",+123.45\r\n");

        let frame: RespFrame = (-123.45).into();
        assert_eq!(frame.encode(), b",-123.45\r\n");

        let frame: RespFrame = 1.2345e8.into();
        assert_eq!(frame.encode(), b",+1.2345e8\r\n");

        let frame: RespFrame = (-0.12345e-8).into();
        println!("{:?}", String::from_utf8_lossy(&(frame.clone().encode())));
        assert_eq!(frame.encode(), b",-1.2345e-9\r\n");

        let frame: RespFrame = 1.2345e-9.into();
        assert_eq!(frame.encode(), b",+1.2345e-9\r\n");
    }

    #[test]
    fn test_bulk_string_encode() {
        let frame: RespFrame = BulkString::new(b"hello").into();
        assert_eq!(frame.encode(), b"$5\r\nhello\r\n")
    }

    #[test]
    fn test_null_bulk_string_encode() {
        let frame: RespFrame = NullBulkString.into();
        assert_eq!(frame.encode(), b"$-1\r\n")
    }

    #[test]
    fn test_array_encode() {
        let frame: RespFrame = RespArray::new(vec![1.into(), 2.into(), 3.into()]).into();
        assert_eq!(frame.encode(), b"*3\r\n:+1\r\n:+2\r\n:+3\r\n");
        let frame: RespFrame = RespArray::new(vec![
            BulkString::new(b"hello").into(),
            BulkString::new(b"world").into(),
        ])
        .into();
        assert_eq!(frame.encode(), b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
    }

    #[test]
    fn test_null_encode() {
        let frame: RespFrame = RespNull.into();
        assert_eq!(frame.encode(), b"_\r\n");
    }

    #[test]
    fn test_null_array_encode() {
        let frame: RespFrame = RespNullArray.into();
        assert_eq!(frame.encode(), b"_\r\n");
    }

    #[test]
    fn test_respmap_encode() {
        let mut map = RespMap::new();

        map.insert("foo".to_string(), (-123456.789).into());

        map.insert(
            "hello".to_string(),
            BulkString::new("world".to_string()).into(),
        );

        let frame: RespFrame = map.into();
        println!("{:?}", String::from_utf8_lossy(&frame.clone().encode()));
        assert_eq!(
            &frame.encode(),
            b"%2\r\n+foo\r\n,-123456.789\r\n+hello\r\n$5\r\nworld\r\n"
        );
    }

    #[test]
    fn test_respset_encode() {
        let frame: RespFrame = RespSet::new([
            RespArray::new([1234.into(), true.into()]).into(),
            BulkString::new("world".to_string()).into(),
        ])
        .into();
        assert_eq!(
            frame.encode(),
            b"~2\r\n*2\r\n:+1234\r\n#t\r\n$5\r\nworld\r\n"
        );
    }
}
