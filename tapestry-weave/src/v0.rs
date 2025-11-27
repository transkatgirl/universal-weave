use std::borrow::Cow;

use ulid::Ulid;
use universal_weave::{
    DeduplicatableContents, DiscreteContentResult, DiscreteContents, DiscreteWeave,
    DuplicatableWeave, Weave,
    dependent::{DependentNode, DependentWeave},
    indexmap::{IndexMap, IndexSet},
    rkyv::{
        Archive, Deserialize, Serialize, from_bytes, rancor::Error, to_bytes, util::AlignedVec,
    },
};

#[cfg(feature = "serde")]
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

use crate::versioning::{MixedData, VersionedBytes};

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub struct NodeContent {
    pub content: InnerNodeContent,
    pub metadata: IndexMap<String, String>,
    pub model: Option<Model>,
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

impl NodeContent {
    fn is_mergeable_with(&self, value: &Self) -> bool {
        if self.metadata != value.metadata || self.model != value.model {
            return false;
        }

        self.content.is_mergeable_with(&value.content)
    }
}

impl DeduplicatableContents for NodeContent {
    fn is_duplicate_of(&self, value: &Self) -> bool {
        self == value
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub enum InnerNodeContent {
    Snippet(Vec<u8>),
    Tokens(Vec<(Vec<u8>, IndexMap<String, String>)>),
}

impl InnerNodeContent {
    fn split(self, at: usize) -> DiscreteContentResult<Self> {
        if at == 0 {
            return DiscreteContentResult::One(self);
        }

        match self {
            Self::Snippet(mut snippet) => {
                if snippet.len() <= at {
                    return DiscreteContentResult::One(Self::Snippet(snippet));
                }

                let right = snippet.split_off(at);
                snippet.shrink_to_fit();

                DiscreteContentResult::Two((Self::Snippet(snippet), Self::Snippet(right)))
            }
            Self::Tokens(tokens) => {
                if tokens.iter().map(|token| token.0.len()).sum::<usize>() <= at {
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
    fn is_mergeable_with(&self, value: &Self) -> bool {
        match self {
            Self::Snippet(_) => match value {
                Self::Snippet(_) => true,
                Self::Tokens(_) => false,
            },
            Self::Tokens(_) => match value {
                Self::Snippet(_) => false,
                Self::Tokens(_) => true,
            },
        }
    }
    pub fn as_bytes(&'_ self) -> Cow<'_, Vec<u8>> {
        match self {
            Self::Snippet(snippet) => Cow::Borrowed(snippet),
            Self::Tokens(tokens) => {
                Cow::Owned(tokens.iter().flat_map(|token| token.0.clone()).collect())
            }
        }
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub struct Model {
    pub label: String,
    pub metadata: IndexMap<String, String>,
}

pub struct TapestryWeave {
    pub weave: DependentWeave<NodeContent, IndexMap<String, String>>,
}

impl TapestryWeave {
    pub fn from_unversioned_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self {
            weave: from_bytes::<_, Error>(bytes)?,
        })
    }
    pub fn to_unversioned_bytes(&self) -> Result<AlignedVec, Error> {
        to_bytes::<Error>(&self.weave)
    }
    pub fn to_versioned_bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(VersionedBytes {
            version: 0,
            data: MixedData::Output(self.to_unversioned_bytes()?),
        }
        .to_bytes())
    }
    pub fn with_capacity(capacity: usize, metadata: IndexMap<String, String>) -> Self {
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
        self.weave.get_roots().iter().copied().map(Ulid)
    }
    pub fn get_bookmarks(&self) -> impl Iterator<Item = Ulid> {
        self.weave.get_bookmarks().iter().copied().map(Ulid)
    }
    pub fn get_active_thread(&mut self) -> impl Iterator<Item = &DependentNode<NodeContent>> {
        let active: Vec<u128> = self.weave.get_active_thread().iter().copied().collect();

        active.into_iter().filter_map(|id| self.weave.get_node(&id))
    }

    pub fn add_node(&mut self, node: DependentNode<NodeContent>) -> bool {
        let identifier = node.id;
        let last_active = if node.active {
            self.weave.get_active_thread().front().copied()
        } else {
            None
        };

        let status = self.weave.add_node(node);

        if self.weave.find_duplicates(&identifier).next().is_some() {
            if let Some(last_active) = last_active {
                self.weave.set_node_active_status(&last_active, true);
            }
            self.weave.remove_node(&identifier);
        }

        status
    }
    pub fn set_node_active_status(&mut self, id: &Ulid, value: bool) -> bool {
        self.weave.set_node_active_status(&id.0, value)
    }
    pub fn set_node_bookmarked_status(&mut self, id: &Ulid, value: bool) -> bool {
        self.weave.set_node_bookmarked_status(&id.0, value)
    }
    pub fn set_active_content<F>(
        &mut self,
        value: &[u8],
        metadata: IndexMap<String, String>,
        mut id_generator: F,
    ) -> bool
    where
        F: FnMut(Option<u64>) -> Ulid,
    {
        let mut modified = false;
        let mut offset: usize = 0;

        let value_len = value.len();

        let active_thread: Vec<u128> = self.weave.get_active_thread().iter().copied().collect();

        let mut last_node = None;

        for active_identifier in active_thread {
            let node = self.weave.get_node(&active_identifier).unwrap();
            let content_bytes = node.contents.content.as_bytes();

            let content_len = content_bytes.len();

            last_node = Some(node.id);

            if value_len >= offset + content_len
                && value[offset..(offset + content_len)] == *content_bytes
            {
                offset += content_len;
            } else {
                let start_offset = offset;

                while offset < value_len
                    && offset < content_len
                    && value[offset] == content_bytes[offset]
                {
                    offset += 1;
                }

                let target = node.id;

                if offset > start_offset {
                    let split_identifier = id_generator(Some(Ulid(target).timestamp_ms()));

                    assert!(self.weave.split_node(
                        &target,
                        offset - start_offset,
                        split_identifier.0
                    ));

                    last_node = Some(split_identifier.0);
                }

                modified = true;

                break;
            }
        }

        if let Some(node) = last_node.and_then(|id| self.weave.get_node(&id))
            && node.to.len() <= 1
            && !node.bookmarked
            && node.contents.model.is_none()
            && node.contents.metadata == metadata
        {
            last_node = node.from;

            let identifier = node.id;
            self.weave.remove_node(&identifier);
        }

        if offset < value.len() {
            assert!(self.add_node(DependentNode {
                id: id_generator(None).0,
                from: last_node,
                to: IndexSet::default(),
                active: true,
                bookmarked: false,
                contents: NodeContent {
                    content: InnerNodeContent::Snippet(value[offset..].to_vec()),
                    metadata: metadata.clone(),
                    model: None,
                },
            }));

            modified = true;
        }

        modified
    }
    pub fn split_node<F>(&mut self, id: &Ulid, at: usize, id_generator: F) -> Option<Ulid>
    where
        F: FnOnce(u64) -> Ulid,
    {
        let new_id = id_generator(id.timestamp_ms());

        if self.weave.split_node(&id.0, at, new_id.0) {
            Some(new_id)
        } else {
            None
        }
    }
    pub fn merge_with_parent(&mut self, id: &Ulid) -> bool {
        self.weave.merge_with_parent(&id.0)
    }
    pub fn is_mergeable_with_parent(&mut self, id: &Ulid) -> bool {
        if let Some(node) = self.weave.get_node(&id.0) {
            if let Some(parent) = node.from.and_then(|id| self.weave.get_node(&id)) {
                parent.to.len() == 1 && parent.contents.is_mergeable_with(&node.contents)
            } else {
                false
            }
        } else {
            false
        }
    }
    pub fn remove_node(&mut self, id: &Ulid) -> Option<DependentNode<NodeContent>> {
        self.weave.remove_node(&id.0)
    }
}
