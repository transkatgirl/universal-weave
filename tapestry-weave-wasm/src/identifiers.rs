use rand::Rng;
use tapestry_weave::ulid::Ulid;
use wasm_bindgen::prelude::*;
use web_time::{Duration, SystemTime};

#[wasm_bindgen]
pub fn new_identifier() -> u128 {
    let timestamp_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();
    let rng = &mut rand::rng();

    Ulid::from_parts(timestamp_ms as u64, rng.random()).0
}

#[wasm_bindgen]
pub fn get_unix_time_ms_from_identifier(value: u128) -> u64 {
    Ulid(value).timestamp_ms()
}

#[wasm_bindgen]
pub fn identifier_to_string(value: u128) -> String {
    Ulid(value).to_string()
}

#[wasm_bindgen]
pub fn identifier_from_string(value: &str) -> Option<u128> {
    Ulid::from_string(value).ok().map(|id| id.0)
}
