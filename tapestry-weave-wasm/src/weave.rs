use std::{collections::HashMap, str};

use serde::{Deserialize, Serialize};
use tapestry_weave::{
    ulid::Ulid,
    universal_weave::{
        DiscreteWeave, Weave as UniversalWeave, dependent::DependentNode, indexmap::IndexMap,
    },
    v0::{
        InnerNodeContent, Model as TapestryModel, NodeContent as TapestryNodeContent, TapestryWeave,
    },
};
use tsify::Tsify;
use wasm_bindgen::prelude::*;

use crate::shared::Identifier;

#[derive(Tsify, Serialize, Deserialize, Debug)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct Node {
    pub id: Identifier,
    pub from: Option<Identifier>,
    pub to: Vec<Identifier>,
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
            id: Identifier(value.id),
            from: value.from.map(Identifier),
            to: value.to.iter().copied().map(Identifier).collect(),
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
            id: Identifier(value.id),
            from: value.from.map(Identifier),
            to: value.to.into_iter().map(Identifier).collect(),
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
        Self {
            id: value.id.0,
            from: value.from.map(|id| id.0),
            to: value.to.into_iter().map(|id| id.0).collect(),
            active: value.active,
            bookmarked: value.bookmarked,
            contents: TapestryNodeContent {
                content: match value.content {
                    NodeContent::Snippet(snippet) => InnerNodeContent::Snippet(snippet),
                    NodeContent::Tokens(tokens) => InnerNodeContent::Tokens(
                        tokens
                            .into_iter()
                            .map(|(token, metadata)| (token, IndexMap::from_iter(metadata)))
                            .collect(),
                    ),
                },
                metadata: IndexMap::from_iter(value.metadata),
                model: value.model.map(|model| TapestryModel {
                    label: model.label,
                    metadata: IndexMap::from_iter(model.metadata),
                }),
            },
        }
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
pub struct WeaveMetadata(pub HashMap<String, String>);

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
    pub fn get_node(&self, id: Identifier) -> Option<Node> {
        self.weave.weave.get_node(&id.0).map(Node::from)
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
    pub fn set_node_active_status(&mut self, id: Identifier, value: bool) -> bool {
        self.weave.weave.set_node_active_status(&id.0, value)
    }
    pub fn set_node_bookmarked_status(&mut self, id: Identifier, value: bool) -> bool {
        self.weave.weave.set_node_bookmarked_status(&id.0, value)
    }
    pub fn split_node(&mut self, id: Identifier, at: usize) -> Option<Identifier> {
        let new_id = Identifier::new();

        if self.weave.weave.split_node(&id.0, at, new_id.0) {
            Some(new_id)
        } else {
            None
        }
    }
    pub fn merge_with_parent(&mut self, id: Identifier) -> bool {
        self.weave.weave.merge_with_parent(&id.0)
    }
    pub fn is_mergeable_with_parent(&mut self, id: Identifier) -> bool {
        self.weave.is_mergeable_with_parent(&Ulid(id.0))
    }
    pub fn remove_node(&mut self, id: Identifier) -> Option<Node> {
        self.weave.weave.remove_node(&id.0).map(Node::from)
    }
}
