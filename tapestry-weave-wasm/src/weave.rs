use std::{collections::HashMap, str};

use serde::{Deserialize, Serialize};
use tapestry_weave::{
    ulid::Ulid,
    universal_weave::{
        DiscreteWeave, Weave as UniversalWeave,
        dependent::DependentNode,
        indexmap::{IndexMap, IndexSet},
    },
    v0::{InnerNodeContent, NodeContent as TapestryNodeContent, TapestryWeave},
};
use tsify::Tsify;
use wasm_bindgen::prelude::*;

#[derive(Tsify, Serialize, Deserialize, Debug)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct Node {
    pub id: u128,
    pub from: Option<u128>,
    pub to: Vec<u128>,
    pub active: bool,
    pub bookmarked: bool,
    pub content: NodeContent,
    pub metadata: HashMap<String, String>,
    pub model: Option<Model>,
}

impl From<&DependentNode<TapestryNodeContent>> for Node {
    fn from(value: &DependentNode<TapestryNodeContent>) -> Self {
        let contents = value.contents.clone();

        Self {
            id: value.id,
            from: value.from,
            to: value.to.iter().copied().collect(),
            active: value.active,
            bookmarked: value.bookmarked,
            content: match contents.content {
                InnerNodeContent::Snippet(snippet) => NodeContent::Snippet(snippet),
                InnerNodeContent::Tokens(tokens) => NodeContent::Tokens(
                    tokens
                        .into_iter()
                        .map(|(token, metadata)| (token, HashMap::from_iter(metadata)))
                        .collect(),
                ),
            },
            metadata: HashMap::from_iter(contents.metadata),
            model: contents.model.map(|model| Model {
                label: model.label,
                metadata: HashMap::from_iter(model.metadata),
            }),
        }
    }
}

impl From<DependentNode<TapestryNodeContent>> for Node {
    fn from(value: DependentNode<TapestryNodeContent>) -> Self {
        Self {
            id: value.id,
            from: value.from,
            to: value.to.into_iter().collect(),
            active: value.active,
            bookmarked: value.bookmarked,
            content: match value.contents.content {
                InnerNodeContent::Snippet(snippet) => NodeContent::Snippet(snippet),
                InnerNodeContent::Tokens(tokens) => NodeContent::Tokens(
                    tokens
                        .into_iter()
                        .map(|(token, metadata)| (token, HashMap::from_iter(metadata)))
                        .collect(),
                ),
            },
            metadata: HashMap::from_iter(value.contents.metadata),
            model: value.contents.model.map(|model| Model {
                label: model.label,
                metadata: HashMap::from_iter(model.metadata),
            }),
        }
    }
}

impl From<Node> for DependentNode<TapestryNodeContent> {
    fn from(value: Node) -> Self {
        todo!()
    }
}

#[allow(clippy::type_complexity)]
#[derive(Tsify, Serialize, Deserialize, Debug)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub enum NodeContent {
    Snippet(Vec<u8>),
    Tokens(Vec<(Vec<u8>, HashMap<String, String>)>),
}

#[derive(Tsify, Serialize, Deserialize, Debug)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct Model {
    pub label: String,
    pub metadata: HashMap<String, String>,
}

#[wasm_bindgen]
pub struct Weave {
    weave: TapestryWeave,
}

#[derive(Tsify, Serialize, Deserialize, Debug)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct WeaveMetadata(HashMap<String, String>);

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
    pub fn v0_from_bytes(bytes: &[u8]) -> Result<Self, String> {
        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();

        let mut weave = TapestryWeave::from_bytes(bytes).map_err(|err| err.to_string())?;

        if weave.capacity() < 16384 {
            weave.reserve(16384 - weave.capacity());
        }

        Ok(Self { weave })
    }
    pub fn v0_to_bytes(&self) -> Result<Vec<u8>, String> {
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
        WeaveMetadata(HashMap::from_iter(
            self.weave
                .weave
                .metadata
                .iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned())),
        ))
    }
    #[wasm_bindgen(setter = metadata)]
    pub fn set_metadata(&mut self, value: WeaveMetadata) {
        self.weave.weave.metadata = IndexMap::from_iter(value.0);
    }
    pub fn get_node(&self, id: u128) -> Option<Node> {
        self.weave.weave.get_node(&id).map(Node::from)
    }
    pub fn get_roots(&self) -> Vec<Node> {
        self.weave
            .weave
            .get_roots()
            .filter_map(|id| self.weave.weave.get_node(&id).map(Node::from))
            .collect()
    }
    pub fn get_bookmarks(&self) -> Vec<Node> {
        self.weave
            .weave
            .get_bookmarks()
            .filter_map(|id| self.weave.weave.get_node(&id).map(Node::from))
            .collect()
    }
    pub fn get_active_thread(&self) -> Vec<Node> {
        self.weave
            .weave
            .get_active_thread()
            .filter_map(|id| self.weave.weave.get_node(&id).map(Node::from))
            .collect()
    }
    pub fn add_node(&mut self, node: Node) -> bool {
        self.weave.add_node(node.into())
    }
    pub fn set_node_active_status(&mut self, id: u128, value: bool) -> bool {
        self.weave.weave.set_node_active_status(&id, value)
    }
    pub fn set_node_bookmarked_status(&mut self, id: u128, value: bool) -> bool {
        self.weave.weave.set_node_bookmarked_status(&id, value)
    }
    pub fn split_node(&mut self, id: u128, at: usize) -> Option<u128> {
        let new_id = super::identifiers::new_identifier();

        if self.weave.weave.split_node(&id, at, new_id) {
            Some(new_id)
        } else {
            None
        }
    }
    pub fn merge_with_parent(&mut self, id: u128) -> bool {
        self.weave.weave.merge_with_parent(&id)
    }
    pub fn is_mergeable_with_parent(&mut self, id: u128) -> bool {
        self.weave.is_mergeable_with_parent(&Ulid(id))
    }
    pub fn remove_node(&mut self, id: u128) -> Option<Node> {
        self.weave.weave.remove_node(&id).map(Node::from)
    }
}
