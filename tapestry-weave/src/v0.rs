use std::{collections::HashMap, rc::Rc};

use ulid::Ulid;
use universal_weave::{
    DeduplicatableContents, DiscreteContentResult, DiscreteContents, Node, Weave,
    dependent::{DependentNode, DependentWeave},
    rkyv::{Archive, Deserialize, Serialize},
};

const MAGIC_STRING: &[u8] = b"TapestryWeave_version=000000;";

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub struct NodeContent {
    pub content: InnerNodeContent,
    pub metadata: Rc<HashMap<String, String>>,
    pub model: Option<Rc<Model>>,
}

impl DiscreteContents for NodeContent {
    fn split(mut self, at: usize) -> DiscreteContentResult<Self> {
        match self.content.split(at) {
            DiscreteContentResult::Two((left, right)) => {
                self.content = left;

                let right_content = NodeContent {
                    content: right,
                    metadata: self.metadata.clone(),
                    model: self.model.clone(),
                };

                DiscreteContentResult::Two((self, right_content))
            }
            DiscreteContentResult::One(center) => {
                self.content = center;
                DiscreteContentResult::One(self)
            }
        }
    }
    fn merge(mut self, mut value: Self) -> DiscreteContentResult<Self> {
        if self.metadata != value.metadata || self.model != value.model {
            return DiscreteContentResult::Two((self, value));
        }

        match self.content.merge(value.content) {
            DiscreteContentResult::Two((left, right)) => {
                self.content = left;
                value.content = right;

                DiscreteContentResult::Two((self, value))
            }
            DiscreteContentResult::One(center) => {
                self.content = center;
                DiscreteContentResult::One(self)
            }
        }
    }
}

impl DeduplicatableContents for NodeContent {
    fn is_duplicate_of(&self, value: &Self) -> bool {
        self == value
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub enum InnerNodeContent {
    Snippet(Vec<u8>),
    Tokens(Vec<(Vec<u8>, HashMap<String, String>)>),
}

impl InnerNodeContent {
    fn split(self, at: usize) -> DiscreteContentResult<Self> {
        if at == 0 {
            return DiscreteContentResult::One(self);
        }

        match self {
            Self::Snippet(mut snippet) => {
                if snippet.len() >= at {
                    return DiscreteContentResult::One(Self::Snippet(snippet));
                }

                let right = snippet.split_off(at);
                snippet.shrink_to_fit();

                DiscreteContentResult::Two((Self::Snippet(snippet), Self::Snippet(right)))
            }
            Self::Tokens(tokens) => {
                if tokens.iter().map(|token| token.0.len()).sum::<usize>() >= at {
                    return DiscreteContentResult::One(Self::Tokens(tokens));
                }

                let mut content_index = 0;

                let location = tokens.iter().enumerate().find_map(|(location, token)| {
                    if content_index + token.0.len() > at {
                        return Some(location);
                    }
                    content_index += token.0.len();

                    None
                });

                if let Some(location) = location {
                    let mut left = tokens;
                    let mut right = left.split_off(location);
                    left.shrink_to_fit();

                    let mut left_token = right[0].0.clone();
                    let right_token = left_token.split_off(at - content_index);

                    if !left_token.is_empty() {
                        left_token.shrink_to_fit();
                        left.push((left_token, right[0].1.clone()));
                    }
                    right[0].0 = right_token;

                    DiscreteContentResult::Two((Self::Tokens(left), Self::Tokens(right)))
                } else {
                    DiscreteContentResult::One(Self::Tokens(tokens))
                }
            }
        }
    }
    fn merge(self, value: Self) -> DiscreteContentResult<Self> {
        match self {
            Self::Snippet(mut left_snippet) => match value {
                Self::Snippet(mut right_snippet) => {
                    left_snippet.append(&mut right_snippet);
                    DiscreteContentResult::One(Self::Snippet(left_snippet))
                }
                Self::Tokens(right_tokens) => DiscreteContentResult::Two((
                    Self::Snippet(left_snippet),
                    Self::Tokens(right_tokens),
                )),
            },
            Self::Tokens(mut left_tokens) => match value {
                Self::Snippet(right_snippet) => DiscreteContentResult::Two((
                    Self::Tokens(left_tokens),
                    Self::Snippet(right_snippet),
                )),
                Self::Tokens(mut right_tokens) => {
                    left_tokens.append(&mut right_tokens);
                    DiscreteContentResult::One(Self::Tokens(right_tokens))
                }
            },
        }
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub struct Model {
    pub label: String,
    pub metadata: HashMap<String, String>,
}

pub struct TapestryWeave {
    pub weave: DependentWeave<NodeContent, HashMap<String, String>>,
}

impl TapestryWeave {
    pub fn with_capacity(capacity: usize, metadata: HashMap<String, String>) -> Self {
        Self {
            weave: DependentWeave::with_capacity(capacity, metadata),
        }
    }
    pub fn capacity(&self) -> usize {
        self.weave.capacity()
    }
    pub fn reserve(&mut self, additional: usize) {
        self.weave.reserve(additional);
    }
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.weave.shrink_to(min_capacity);
    }
    pub fn len(&self) -> usize {
        self.weave.len()
    }
    pub fn is_empty(&self) -> bool {
        self.weave.is_empty()
    }
    pub fn contains(&self, id: &Ulid) -> bool {
        self.weave.contains(&id.0)
    }
    pub fn get_node(&self, id: &Ulid) -> Option<&DependentNode<NodeContent>> {
        self.weave.get_node(&id.0)
    }
    pub fn get_roots(&self) -> impl Iterator<Item = Ulid> {
        self.weave.get_roots().map(Ulid)
    }
    pub fn get_bookmarks(&self) -> impl Iterator<Item = Ulid> {
        self.weave.get_bookmarks().map(Ulid)
    }
    pub fn get_active_thread(&self) -> impl Iterator<Item = &DependentNode<NodeContent>> {
        self.weave
            .get_active_thread()
            .filter_map(|id| self.weave.get_node(&id))
    }
    pub fn add_node(&mut self, node: DependentNode<NodeContent>) -> bool {
        self.weave.add_node(node)
    }
}
