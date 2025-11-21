mod identifiers;
mod utils;

use serde::{Deserialize, Serialize};
use tapestry_weave::{ulid::Ulid, universal_weave::indexmap::IndexSet};
use wasm_bindgen::prelude::*;

#[derive(Serialize, Deserialize, Debug)]
pub struct Node {
    id: u128,
    from: Vec<u128>,
    to: Vec<u128>,
    active: bool,
    bookmarked: bool,
}

//#[wasm_bindgen]
//pub fn greet() {
//    alert("Hello, tapestry-weave-wasm!");
//}
