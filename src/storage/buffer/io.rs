#![allow(dead_code)]

/// This file implements an IO API which includes functions to encode/decode arbitrary structures as long as they implement the
/// required traits
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::shared::PAGE_SIZE;

/// Used to encode a generic item to a vector of u8s as long as it implements the Sized and Serialize traits
pub fn encode<T>(item: T) -> Option<Vec<u8>>
where
    T: Sized + Serialize,
{
    if let Ok(encoded) = bincode::serialize(&item) {
        return Some(encoded);
    }
    None
}

/// Used to decode a vector of u8s into a generic item as long as it implements the Sized, Serialize, and DeserializeOwned traits
pub fn decode<T>(bytes: Vec<u8>) -> Option<T>
where
    T: Sized + Serialize + DeserializeOwned,
{
    if let Ok(decoded) = bincode::deserialize(&bytes) {
        return Some(decoded);
    }
    None
}

/// Used to convert a generic item into a buffer of a static size that's writable by file APIs. Calls `encode` internally
pub fn to_buffer<T>(item: T) -> Option<[u8; PAGE_SIZE]>
where
    T: Sized + Serialize,
{
    if let Some(encoded) = encode(item) {
        let mut buf = [0u8; PAGE_SIZE];
        buf[..std::mem::size_of_val(&*encoded)].copy_from_slice(&*encoded);
        return Some(buf);
    }
    None
}

/// Used to convert a buffer of a static size to a generic item. Calls `decode` internally
pub fn from_buffer<T>(buf: &[u8; PAGE_SIZE]) -> Option<T>
where
    T: Sized + Serialize + DeserializeOwned,
{
    decode::<T>(buf.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::Song;

    #[test]
    fn encode_decode() {
        let cry_baby = Song::new(1, "Cry Baby", "The Neighbourhood");
        let bytes = encode(cry_baby).unwrap();
        let decoded = decode::<Song>(bytes).unwrap();

        assert_eq!(cry_baby.id, decoded.id);
        assert_eq!(cry_baby.artist, decoded.artist);
        assert_eq!(cry_baby.title, decoded.title);
    }

    #[test]
    fn encode_decode_buffer() {
        let cry_baby = Song::new(1, "Cry Baby", "The Neighbourhood");
        let buf = to_buffer(cry_baby).unwrap();
        let decoded = from_buffer::<Song>(&buf).unwrap();

        assert_eq!(cry_baby.id, decoded.id);
        assert_eq!(cry_baby.artist, decoded.artist);
        assert_eq!(cry_baby.title, decoded.title);
    }
}
