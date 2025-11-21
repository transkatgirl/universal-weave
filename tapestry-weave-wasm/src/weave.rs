use std::{borrow::Cow, collections::HashMap, str};

use js_sys::Map;
use serde::{Deserialize, Serialize};
use tapestry_weave::{
    ulid::Ulid,
    universal_weave::indexmap::{IndexMap, IndexSet},
    v0::TapestryWeave,
};
use tsify::Tsify;
use wasm_bindgen::prelude::*;

#[derive(Tsify, Serialize, Deserialize, Debug)]
#[tsify(into_wasm_abi, from_wasm_abi)]
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

#[allow(clippy::type_complexity)]
#[derive(Tsify, Serialize, Deserialize, Debug)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub enum NodeContent<'a> {
    Snippet(Cow<'a, [u8]>),
    Tokens(Cow<'a, Vec<(Vec<u8>, HashMap<String, String>)>>),
}

#[derive(Tsify, Serialize, Deserialize, Debug)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct Model<'a> {
    pub label: Cow<'a, str>,
    pub metadata: Cow<'a, HashMap<String, String>>,
}

#[wasm_bindgen]
pub struct Weave {
    weave: TapestryWeave,
}

#[derive(Tsify, Serialize, Deserialize, Debug)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WeaveMetadata(IndexMap<String, String>);

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
    #[wasm_bindgen(getter = metadata)]
    pub fn get_metadata(&self) -> WeaveMetadata {
        WeaveMetadata(self.weave.weave.metadata.to_owned())
    }
    #[wasm_bindgen(setter = metadata)]
    pub fn set_metadata(&mut self, value: WeaveMetadata) -> Result<(), String> {
        self.weave.weave.metadata = value.0;

        Ok(())
    }
}
