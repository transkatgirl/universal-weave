mod identifiers;
mod utils;

use std::{borrow::Cow, collections::HashMap};

use serde::{Deserialize, Serialize};
use tapestry_weave::{ulid::Ulid, universal_weave::indexmap::IndexSet};
use wasm_bindgen::prelude::*;

#[derive(Serialize, Deserialize, Debug)]
pub struct Node<'a> {
    id: u128,
    from: Vec<u128>,
    to: Vec<u128>,
    active: bool,
    bookmarked: bool,
    content: NodeContent<'a>,
    metadata: Cow<'a, HashMap<String, String>>,
    model: Model<'a>,
}

#[allow(clippy::type_complexity)]
#[derive(Serialize, Deserialize, Debug)]
pub enum NodeContent<'a> {
    Snippet(Cow<'a, Vec<u8>>),
    Tokens(Cow<'a, Vec<(Vec<u8>, HashMap<String, String>)>>),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Model<'a> {
    pub label: Cow<'a, String>,
    pub metadata: Cow<'a, HashMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WeaveMetadata<'a> {
    pub metadata: Cow<'a, HashMap<String, String>>,
}

//#[wasm_bindgen]
//pub fn greet() {
//    alert("Hello, tapestry-weave-wasm!");
//}
