use std::collections::HashMap;

use universal_weave::{
    dependent::DependentWeave,
    rkyv::{Archive, Deserialize, Serialize},
};

#[derive(Archive, Deserialize, Serialize, Debug)]
pub struct NodeContent {
    pub content: InnerNodeContent,
    pub metadata: HashMap<String, String>,
    pub model: Option<Model>,
}

#[derive(Archive, Deserialize, Serialize, Debug)]
pub enum InnerNodeContent {
    Snippet(String),
    Tokens(Vec<(String, HashMap<String, String>)>),
}

#[derive(Archive, Deserialize, Serialize, Debug)]
pub struct Model {
    pub label: String,
    pub metadata: HashMap<String, String>,
}

pub struct Weave {
    pub weave: DependentWeave<NodeContent, HashMap<String, String>>,
}

impl Weave {
    pub fn with_capacity(capacity: usize, metadata: HashMap<String, String>) -> Self {
        Self {
            weave: DependentWeave::with_capacity(capacity, metadata),
        }
    }
}
