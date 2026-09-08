use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum BencodeValue {
    ByteString(Vec<u8>),
    Integer(i64),
    List(Vec<BencodeValue>),
    Dictionary(HashMap<String, BencodeValue>),
}

impl BencodeValue {
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::ByteString(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        str::from_utf8(self.as_bytes()?).ok()
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Integer(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[BencodeValue]> {
        match self {
            Self::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_dict(&self) -> Option<&HashMap<String, BencodeValue>> {
        match self {
            Self::Dictionary(d) => Some(d),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Self> {
        self.as_dict()?.get(key)
    }

    pub fn as_str_list(&self) -> Option<Vec<&str>> {
        self.as_list()?.iter().map(Self::as_str).collect()
    }

    pub fn as_i64_list(&self) -> Option<Vec<i64>> {
        self.as_list()?.iter().map(Self::as_i64).collect()
    }


    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            BencodeValue::ByteString(byte_string) => {
                let mut encoded = byte_string.len().to_string().into_bytes();
                encoded.push(b':');
                encoded.extend(byte_string);
                encoded
            },
            BencodeValue::Integer(integer) => {
                let mut encoded = vec![b'i'];
                encoded.extend(integer.to_string().bytes());
                encoded.push(b'e');
                encoded
            },
            BencodeValue::List(list) => {
            let mut encoded = vec![b'l'];
                encoded.extend(
                    list.iter().flat_map(Self::to_bytes)
                );
                encoded.push(b'e');
                encoded 
            }
            BencodeValue::Dictionary(dic) => {
                let mut encoded = vec![b'd'];
                let mut entries: Vec<_> = dic.iter().collect();
                entries.sort_by_key(|(key, _)| key.as_bytes());
                for (key, val) in entries {
                    encoded.extend(
                        BencodeValue::ByteString(key.as_bytes().to_vec()).to_bytes()
                    );
                    encoded.extend(val.to_bytes());
                }
                encoded.push(b'e');
                encoded
            },
        }
    }

    pub fn from_bytes(encoded_value: &[u8]) -> Result<(Option<Self>, &[u8]), String> {
        if encoded_value.is_empty() {
            return Ok((None, &[]));
        }

        let first_char = encoded_value[0];

        match first_char {
            b'e' => Ok((None, &encoded_value[1..])),

            b'd' => {
                let mut key_vals = HashMap::new();
                let mut remaining = &encoded_value[1..];

                while let (Some(key), rest) = Self::from_bytes(remaining)? {
                    if let Self::ByteString(string_key) = key {
                        let string_key = String::from_utf8(string_key)
                            .map_err(|e| format!("Dictionary key is not valid UTF-8: {e}"))?;
                        if let (Some(val), rest) = Self::from_bytes(rest)? {
                            key_vals.insert(string_key, val);
                            remaining = rest;
                        } else {
                            return Err(format!("Dictionary key has no value: {string_key}"));
                        }
                    } else {
                        return Err(format!("Dictionary key must be a string, got: {key:?}"));
                    }
                }

                Ok((
                    Some(Self::Dictionary(key_vals)),
                    &remaining[1..],
                ))
            }

            b'l' => {
                let mut elements = vec![];
                let mut remaining = &encoded_value[1..];

                while let (Some(element), rest) = Self::from_bytes(remaining)? {
                    elements.push(element);
                    remaining = rest;
                }

                Ok((
                    Some(Self::List(elements)),
                    &remaining[1..],
                ))
            }

            b'i' => {
                let e_index = encoded_value
                    .iter()
                    .position(|&byte| byte == b'e')
                    .ok_or_else(|| {
                        format!(
                            "No terminating 'e' in bencoded integer: {:02x?}",
                            encoded_value
                        )
                    })?;

                let integer_bytes = &encoded_value[1..e_index];

                let integer_string = str::from_utf8(integer_bytes)
                    .map_err(|e| {
                        format!(
                            "Bencoded integer contains invalid UTF-8: {:02x?} ({e})",
                            integer_bytes
                        )
                    })?;

                let integer = integer_string.parse::<i64>()
                    .map_err(|e| {
                        format!(
                            "Invalid bencoded integer {:?}: {e}",
                            integer_string
                        )
                    })?;

                Ok((
                    Some(Self::Integer(integer)),
                    &encoded_value[e_index + 1..],
                ))
            }

            _ if first_char.is_ascii_digit() => {
                let colon_index = encoded_value
                    .iter()
                    .position(|&byte| byte == b':')
                    .ok_or_else(|| {
                        format!(
                            "No ':' in bencoded string length: {:02x?}",
                            encoded_value
                        )
                    })?;

                let number_bytes = &encoded_value[..colon_index];

                let number_string = str::from_utf8(number_bytes)
                    .map_err(|e| {
                        format!(
                            "Bencoded string length contains invalid UTF-8: {:02x?} ({e})",
                            number_bytes
                        )
                    })?;

                let number = number_string.parse::<usize>()
                    .map_err(|e| {
                        format!(
                            "Invalid bencoded string length {:?}: {e}",
                            number_string
                        )
                    })?;

                let string_start = colon_index + 1;
                let string_end = string_start + number;

                if encoded_value.len() < string_end {
                    return Err(format!(
                        "Bencoded string is too short: expected {number} bytes, \
                        but only {} bytes remain",
                        encoded_value.len() - string_start
                    ));
                }

                let string = &encoded_value[string_start..string_end];

                Ok((
                    Some(Self::ByteString(string.to_vec())),
                    &encoded_value[string_end..],
                ))
            }

            _ => Err(format!("Unhandled bencoded value: byte 0x{first_char:02x}")),
        }
    }


    pub fn required(&self, key: &str) -> Result<&Self, String> {
        self.get(key).ok_or_else(|| format!("'{key}' must be present"))
    }

    pub fn required_bytes(&self, key: &str) -> Result<Vec<u8>, String> {
        self.required(key)?
            .as_bytes().ok_or_else(|| format!("'{key}' must be a byte string"))
            .map(|s| s.to_vec())
    }

    pub fn optional_bytes(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        match self.get(key) {
            Some(bytes) => bytes
                .as_bytes().ok_or_else(|| format!("'{key}' must be a byte string"))
                .map(|s| Some(s.to_vec())),
            None => Ok(None),
        }
    }

    pub fn required_string(&self, key: &str) -> Result<String, String> {
        self.required(key)?
            .as_str().ok_or_else(|| format!("'{key}' must be a UTF-8 string"))
            .map(|s| s.to_owned())
    }

    pub fn optional_string(&self, key: &str) -> Result<Option<String>, String> {
        match self.get(key) {
            Some(string) => string
                .as_str().ok_or_else(|| format!("'{key}' must be a UTF-8 string"))
                .map(|s| Some(s.to_owned())),
            None => Ok(None),
        }
    }

    pub fn unsigned(&self, name: &str) -> Result<u64, String> {
        self .as_i64().ok_or_else(|| format!("'{name}' must be an integer"))
            .map(|s| s as u64)
    }

    pub fn required_unsigned(&self, key: &str) -> Result<u64, String> {
        self.required(key)?.unsigned(key)
    }

    pub fn optional_unsigned(&self, key: &str) -> Result<Option<u64>, String> {
        match self.get(key) {
            Some(integer) => integer.unsigned(key)
                .map(|s| Some(s)),
            None => Ok(None),
        }
    }

    pub fn string_list(&self, name: &str) -> Result<Vec<String>, String> {
        let strings = self
            .as_str_list() .ok_or_else(|| format!("'{name}' must be a list of UTF-8 strings"))?
            .into_iter() .map(str::to_owned)
            .collect();
        Ok(strings)
    }

    pub fn required_string_list(&self, key: &str) -> Result<Vec<String>, String> {
        self.required(key)?.string_list(key)
    }
}
