mod utils;

use rand::Rng;
use serde::{Deserialize, Serialize};
use tapestry_weave::{ulid::Ulid, universal_weave::indexmap::IndexSet};
use wasm_bindgen::prelude::*;
use web_time::{Duration, Instant, SystemTime};

#[derive(Serialize, Deserialize, Debug)]
pub struct Node {
    id: u128,
    from: Vec<u128>,
    to: Vec<u128>,
    active: bool,
    bookmarked: bool,
}

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

//#[wasm_bindgen]
//pub fn greet() {
//    alert("Hello, tapestry-weave-wasm!");
//}
