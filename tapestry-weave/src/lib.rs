use std::collections::HashMap;

use universal_weave::{
    dependent::DependentWeave,
    rkyv::{Archive, Deserialize, Serialize},
};

#[derive(Archive, Deserialize, Serialize, Debug)]
pub struct NodeContent {
    pub content: String,
    pub metadata: HashMap<String, String>,
    pub model: Option<Model>,
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
