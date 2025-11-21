use std::{borrow::Cow, collections::HashMap};

use serde::{Deserialize, Serialize};
use tapestry_weave::{
    ulid::Ulid,
    universal_weave::indexmap::{IndexMap, IndexSet},
    v0::TapestryWeave,
};
use wasm_bindgen::prelude::*;

#[derive(Serialize, Deserialize, Debug)]
pub struct Node<'a> {
    pub id: u128,
    pub from: Vec<u128>,
    pub to: Vec<u128>,
    pub active: bool,
    pub bookmarked: bool,
    pub content: NodeContent<'a>,
    pub metadata: Cow<'a, HashMap<String, String>>,
    pub model: Model<'a>,
}

impl<'a> Node<'a> {
    fn from_json(value: &str) -> Result<Self, String> {
        serde_json::from_str(value).map_err(|err| err.to_string())
    }
    fn to_json(&self) -> Result<String, String> {
        serde_json::to_string(self).map_err(|err| err.to_string())
    }
}

#[allow(clippy::type_complexity)]
#[derive(Serialize, Deserialize, Debug)]
pub enum NodeContent<'a> {
    Snippet(Cow<'a, [u8]>),
    Tokens(Cow<'a, Vec<(Vec<u8>, HashMap<String, String>)>>),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Model<'a> {
    pub label: Cow<'a, str>,
    pub metadata: Cow<'a, HashMap<String, String>>,
}

#[wasm_bindgen]
pub struct Weave {
    weave: TapestryWeave,
}

#[wasm_bindgen]
impl Weave {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();

        Self {
            weave: TapestryWeave::with_capacity(16384, IndexMap::new()),
        }
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();

        let mut weave = TapestryWeave::from_bytes(bytes).map_err(|err| err.to_string())?;

        if weave.capacity() < 16384 {
            weave.reserve(16384 - weave.capacity());
        }

        Ok(Self { weave })
    }
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        self.weave
            .to_bytes()
            .map(|bytes| bytes.into_vec())
            .map_err(|err| err.to_string())
    }
    pub fn len(&self) -> usize {
        self.weave.len()
    }
    pub fn is_empty(&self) -> bool {
        self.weave.is_empty()
    }
    #[wasm_bindgen(getter = metadata_json)]
    pub fn get_metadata(&self) -> Result<String, String> {
        serde_json::to_string(&self.weave.weave.metadata).map_err(|err| err.to_string())
    }
    #[wasm_bindgen(setter = metadata_json)]
    pub fn set_metadata(&mut self, value: &str) -> Result<(), String> {
        self.weave.weave.metadata = serde_json::from_str(value).map_err(|err| err.to_string())?;

        Ok(())
    }
}
