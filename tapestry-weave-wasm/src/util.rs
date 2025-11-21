use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn bytes_to_string_lossy(value: &[u8]) -> String {
    String::from_utf8_lossy(value).to_string()
}

#[wasm_bindgen]
pub fn string_to_bytes(value: &str) -> Vec<u8> {
    value.as_bytes().to_vec()
}
