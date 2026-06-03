use super::{Decode, DecodeError, Encode, EncodeError};
use core::hash::{Hash, Hasher};

/// Tuple Field
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TupleField {
    pub value: Vec<u8>,
}

impl TupleField {
    // Tuples are in MOQ are only used for TrackNamespace. The RFC limits the
    // total size of the Tracknamespace + TrackName to be 4096 bytes.  So an
    // individual field should never be larger than 4096 bytes.
    pub const MAX_VALUE_SIZE: usize = 4096;
}

impl Hash for TupleField {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl Decode for TupleField {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let size = usize::decode(r)?;
        if size > Self::MAX_VALUE_SIZE {
            return Err(DecodeError::FieldBoundsExceeded("TupleField".to_string()));
        }
        Self::decode_remaining(r, size)?;
        let mut buf = vec![0; size];
        r.copy_to_slice(&mut buf);
        Ok(Self { value: buf })
    }
}

impl Encode for TupleField {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        if self.value.len() > Self::MAX_VALUE_SIZE {
            return Err(EncodeError::FieldBoundsExceeded("TupleField".to_string()));
        }
        self.value.len().encode(w)?;
        Self::encode_remaining(w, self.value.len())?;
        w.put_slice(&self.value);
        Ok(())
    }
}

impl TupleField {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_utf8(path: &str) -> Self {
        let mut field = TupleField::new();
        field.value = path.as_bytes().to_vec();
        field
    }

    /// Allow an encodable structure (ie. implements the Encode trait) to be set as the value.
    // TODO SLG - is this really useful?
    pub fn set<P: Encode>(&mut self, p: P) -> Result<(), EncodeError> {
        let mut value = Vec::new();
        p.encode(&mut value)?;
        self.value = value;
        Ok(())
    }

    /// Try to decode the value as the specified Decodable structure (ie. implements the Decode trait).
    // TODO SLG - is this really useful?
    pub fn get<P: Decode>(&self) -> Result<P, DecodeError> {
        P::decode(&mut bytes::Bytes::from(self.value.clone()))
    }
}

/// Tuple
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Tuple {
    pub fields: Vec<TupleField>,
}

impl Hash for Tuple {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fields.hash(state);
    }
}

impl Decode for Tuple {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let count = usize::decode(r)?;
        let mut fields = Vec::new();
        for _ in 0..count {
            fields.push(TupleField::decode(r)?);
        }
        Ok(Self { fields })
    }
}

impl Encode for Tuple {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.fields.len().encode(w)?;
        for field in &self.fields {
            field.encode(w)?;
        }
        Ok(())
    }
}

impl Tuple {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, field: TupleField) {
        self.fields.push(field);
    }

    pub fn set(&mut self, index: usize, f: TupleField) -> Result<(), EncodeError> {
        self.fields[index].set(f)
    }

    pub fn get(&self, index: usize) -> Result<TupleField, DecodeError> {
        self.fields[index].get()
    }

    pub fn clear(&mut self) {
        self.fields.clear();
    }

    pub fn from_utf8_path(path: &str) -> Self {
        let mut tuple = Tuple::new();
        for part in path.split('/') {
            tuple.add(TupleField::from_utf8(part));
        }
        tuple
    }

    pub fn to_utf8_path(&self) -> String {
        let mut path = String::new();
        for field in &self.fields {
            path.push('/');
            path.push_str(&String::from_utf8_lossy(&field.value));
        }
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use bytes::BytesMut;

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();

        let t = Tuple::from_utf8_path("test/path/to/resource");
        t.encode(&mut buf).unwrap();
        #[rustfmt::skip]
        assert_eq!(
            buf.to_vec(),
            vec![
                0x04, // 4 tuple fields
                // Field 1: "test"
                0x04, 0x74, 0x65, 0x73, 0x74,
                // Field 2: "path"
                0x04, 0x70, 0x61, 0x74, 0x68,
                // Field 3: "to"
                0x02, 0x74, 0x6f,
                // Field 4: "resource"
                0x08, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65
            ]
        );
        let decoded = Tuple::decode(&mut buf).unwrap();
        assert_eq!(decoded, t);

        // Alternate construction
        let mut t = Tuple::new();
        t.add(TupleField::from_utf8("test"));
        t.encode(&mut buf).unwrap();
        assert_eq!(
            buf.to_vec(),
            vec![
                0x01, // 1 tuple field
                0x04, 0x74, 0x65, 0x73, 0x74
            ]
        ); // Field 1: "test"
        let decoded = Tuple::decode(&mut buf).unwrap();
        assert_eq!(decoded, t);
    }

    #[test]
    fn encode_tuplefield_too_large() {
        let mut buf = BytesMut::new();

        let t = TupleField {
            value: vec![0; TupleField::MAX_VALUE_SIZE + 1], // Create a field larger than the max size
        };

        let encoded = t.encode(&mut buf);
        assert!(matches!(
            encoded.unwrap_err(),
            EncodeError::FieldBoundsExceeded(_)
        ));
    }

    #[test]
    fn decode_tuplefield_too_large() {
        let mut data: Vec<u8> = vec![0x00; TupleField::MAX_VALUE_SIZE + 1]; // Create a vector with 256 bytes
                                                                            // 4097 encoded as VarInt
        data[0] = 0x50;
        data[1] = 0x01;
        let mut buf: Bytes = data.into();
        let decoded = TupleField::decode(&mut buf);
        assert!(matches!(
            decoded.unwrap_err(),
            DecodeError::FieldBoundsExceeded(_)
        ));
    }
}
