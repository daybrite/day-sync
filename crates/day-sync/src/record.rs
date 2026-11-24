// SPDX-License-Identifier: MPL-2.0
//! Typed application records inside the existing bounded v5 envelope. The serialized key
//! includes the model and field group, so independently editable values have independent clocks.
use crate::{BookmarkValue, Change, Error, wire};
use serde::{Serialize, de::DeserializeOwned};

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecordKey {
    pub model: String,
    pub key: String,
    pub field: String,
}

impl RecordKey {
    fn encoded(&self) -> Result<String, Error> {
        if [&self.model, &self.key, &self.field]
            .iter()
            .any(|s| s.is_empty())
        {
            return Err(Error::InvalidWire);
        }
        serde_json::to_string(self).map_err(|_| Error::InvalidWire)
    }

    /// Encode an update or tombstone with a stable object identity.
    /// # Errors
    /// Returns an error for an invalid key, oversized record, or unserializable payload.
    pub fn change<T: Serialize>(&self, value: Option<&T>) -> Result<Change, Error> {
        let key = self.encoded()?;
        if key.len() > 8192 {
            return Err(Error::Limit);
        }
        let id = wire::digest(key.as_bytes());
        let value = value
            .map(|v| {
                let mut body = serde_json::to_value(v)?;
                body.sort_all_objects();
                serde_json::to_string(&body).map(|title| BookmarkValue { url: key, title })
            })
            .transpose()
            .map_err(|_| Error::InvalidWire)?;
        if value.as_ref().is_some_and(|v| !wire::validate_value(v)) {
            return Err(Error::Limit);
        }
        Ok(Change { id, value })
    }

    /// Decode a live record, checking canonical encoding and its object identity.
    /// # Errors
    /// Returns an error on a key, type, encoding, or hash mismatch.
    pub fn decode<T: DeserializeOwned + Serialize>(
        id: &str,
        v: &BookmarkValue,
    ) -> Result<(Self, T), Error> {
        let key: Self = serde_json::from_str(&v.url).map_err(|_| Error::InvalidWire)?;
        let payload = serde_json::from_str(&v.title).map_err(|_| Error::InvalidWire)?;
        if key.change(Some(&payload))?
            != (Change {
                id: id.to_owned(),
                value: Some(v.clone()),
            })
        {
            return Err(Error::InvalidWire);
        }
        Ok((key, payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
    struct Fixture {
        z: u64,
        a: String,
    }

    #[test]
    fn canonical_body_and_tombstone_share_a_stable_field_identity() {
        let key = RecordKey {
            model: "fixture".into(),
            key: "edition-1".into(),
            field: "position".into(),
        };
        let payload = Fixture {
            z: 5,
            a: "text".into(),
        };
        let change = key.change(Some(&payload)).unwrap();
        let value = change.value.unwrap();
        assert_eq!(value.title, r#"{"a":"text","z":5}"#);
        assert_eq!(
            RecordKey::decode(&change.id, &value).unwrap(),
            (key.clone(), payload)
        );
        assert_eq!(key.change::<Fixture>(None).unwrap().id, change.id);
        let other = RecordKey {
            field: "rating".into(),
            ..key.clone()
        };
        let other_id = other.change::<Fixture>(None).unwrap().id;
        assert!(RecordKey::decode::<Fixture>(&other_id, &value).is_err());
        let noncanonical = BookmarkValue {
            title: r#"{"z":5,"a":"text"}"#.into(),
            ..value
        };
        assert!(RecordKey::decode::<Fixture>(&change.id, &noncanonical).is_err());
        assert!(key.change(Some(&"x".repeat(8192))).is_err());
    }
}
