use rand::Rng;
use serde::{Deserialize, Serialize};
use tapestry_weave::ulid::Ulid;
use wasm_bindgen::prelude::*;
use web_time::{Duration, SystemTime};

#[wasm_bindgen]
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Identifier(pub u128);

#[wasm_bindgen]
impl Identifier {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let timestamp_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis();
        let rng = &mut rand::rng();

        Identifier(Ulid::from_parts(timestamp_ms as u64, rng.random()).0)
    }
    pub fn from_uint(value: u128) -> Self {
        Identifier(value)
    }
    #[wasm_bindgen(getter = unix_time_ms)]
    pub fn get_unix_time_ms(&self) -> u64 {
        Ulid(self.0).timestamp_ms()
    }
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(self) -> String {
        Ulid(self.0).to_string()
    }
    pub fn try_from_string(value: &str) -> Option<Self> {
        Ulid::from_string(value).ok().map(|id| Self(id.0))
    }
}

#[wasm_bindgen]
pub fn bytes_to_string_lossy(value: &[u8]) -> String {
    String::from_utf8_lossy(value).to_string()
}

#[wasm_bindgen]
pub fn string_to_bytes(value: &str) -> Vec<u8> {
    value.as_bytes().to_vec()
}
