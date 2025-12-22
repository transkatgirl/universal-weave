use std::{borrow::Cow, collections::HashSet, hash::BuildHasherDefault};

use contracts::ensures;
use rkyv::rend::u128_le;
use ulid::Ulid;
use universal_weave::{
    ArchivedWeave, DeduplicatableContents, DeduplicatableWeave, DiscreteContentResult,
    DiscreteContents, DiscreteWeave, IndependentContents, Weave,
    independent::{ArchivedIndependentNode, IndependentNode, IndependentWeave},
    indexmap::{IndexMap, IndexSet},
    rkyv::{
        Archive, Deserialize, Serialize, from_bytes, rancor::Error, to_bytes, util::AlignedVec,
    },
};

#[cfg(feature = "serde")]
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

use crate::{
    VersionedWeave,
    hashers::UlidHasher,
    versioning::{MixedData, VersionedBytes},
};

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub struct NodeContent {
    pub modified: bool,
    pub content: InnerNodeContent,
    pub metadata: IndexMap<String, String>,
    pub creator: Option<Creator>,
}

impl IndependentContents for NodeContent {}

impl DiscreteContents for NodeContent {
    fn split(mut self, at: usize) -> DiscreteContentResult<Self> {
        match self.content.split(at) {
            DiscreteContentResult::Two((left, right)) => {
                self.content = left;
                self.modified = true;

                let right_content = NodeContent {
                    modified: true,
                    content: right,
                    metadata: self.metadata.clone(),
                    creator: self.creator.clone(),
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
        if self.metadata != value.metadata || self.creator != value.creator {
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
                self.modified = true;
                DiscreteContentResult::One(self)
            }
        }
    }
}

impl NodeContent {
    fn is_mergeable_with(&self, value: &Self) -> bool {
        if self.metadata != value.metadata || self.creator != value.creator {
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
    Tokens(Vec<InnerNodeToken>),
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub struct InnerNodeToken {
    bytes: Vec<u8>,
    metadata: IndexMap<String, String>,
    modified: bool,
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
                if tokens.iter().map(|token| token.bytes.len()).sum::<usize>() <= at {
                    return DiscreteContentResult::One(Self::Tokens(tokens));
                }

                let mut content_index = 0;

                let location = tokens.iter().enumerate().find_map(|(location, token)| {
                    if content_index + token.bytes.len() > at {
                        return Some(location);
                    }
                    content_index += token.bytes.len();

                    None
                });

                if let Some(location) = location {
                    let mut left = tokens;
                    let mut right = left.split_off(location);
                    left.shrink_to_fit();

                    let mut left_token = right[0].bytes.clone();
                    let right_token = left_token.split_off(at - content_index);

                    if !left_token.is_empty() {
                        left_token.shrink_to_fit();
                        left.push(InnerNodeToken {
                            bytes: left_token,
                            metadata: right[0].metadata.clone(),
                            modified: true,
                        });
                        right[0].modified = true;
                    }
                    right[0].bytes = right_token;

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
                    DiscreteContentResult::One(Self::Tokens(left_tokens))
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
            Self::Tokens(tokens) => Cow::Owned(
                tokens
                    .iter()
                    .flat_map(|token| token.bytes.clone())
                    .collect(),
            ),
        }
    }
    pub fn len(&self) -> usize {
        match self {
            Self::Snippet(snippet) => snippet.len(),
            Self::Tokens(tokens) => tokens.iter().map(|token| token.bytes.len()).sum(),
        }
    }
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Snippet(snippet) => snippet.is_empty(),
            Self::Tokens(tokens) => tokens.iter().all(|token| token.bytes.is_empty()),
        }
    }
}

impl ArchivedInnerNodeContent {
    pub fn as_bytes(&'_ self) -> Vec<u8> {
        match self {
            Self::Snippet(snippet) => snippet.to_vec(),
            Self::Tokens(tokens) => tokens
                .iter()
                .flat_map(|token| token.bytes.to_vec())
                .collect(),
        }
    }
    pub fn len(&self) -> usize {
        match self {
            Self::Snippet(snippet) => snippet.len(),
            Self::Tokens(tokens) => tokens.iter().map(|token| token.bytes.len()).sum(),
        }
    }
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Snippet(snippet) => snippet.is_empty(),
            Self::Tokens(tokens) => tokens.iter().all(|token| token.bytes.is_empty()),
        }
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub enum Creator {
    Model(Model),
    Human(Author),
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub struct Model {
    pub label: String,
    pub identifier: Option<u128>,
    pub metadata: IndexMap<String, String>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub struct Author {
    pub label: String,
    pub identifier: Option<u128>,
}

pub type TapestryNode = IndependentNode<u128, NodeContent, BuildHasherDefault<UlidHasher>>;
pub type ArchivedTapestryNode =
    ArchivedIndependentNode<u128, NodeContent, BuildHasherDefault<UlidHasher>>;
pub type TapestryWeaveInner =
    IndependentWeave<u128, NodeContent, IndexMap<String, String>, BuildHasherDefault<UlidHasher>>;

pub struct TapestryWeave {
    pub weave: TapestryWeaveInner,
}

impl TapestryWeave {
    pub fn from_unversioned_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Self {
            weave: from_bytes::<_, Error>(bytes)?,
        })
    }
    pub fn to_unversioned_bytes(&self) -> Result<AlignedVec, Error> {
        assert!(self.weave.validate());
        to_bytes::<Error>(&self.weave)
    }
    /*pub fn to_versioned_bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(VersionedBytes {
            version: 1,
            data: MixedData::Output(self.to_unversioned_bytes()?),
        }
        .to_bytes())
    }
    pub fn to_versioned_weave(self) -> VersionedWeave {
        VersionedWeave::V1(self)
    }*/
    pub fn with_capacity(capacity: usize, metadata: IndexMap<String, String>) -> Self {
        Self {
            weave: IndependentWeave::with_capacity(capacity, metadata),
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
    pub fn get_node(&self, id: &Ulid) -> Option<&TapestryNode> {
        self.weave.get_node(&id.0)
    }
    pub fn get_roots(&self) -> impl ExactSizeIterator<Item = Ulid> {
        self.weave.get_roots().iter().copied().map(Ulid)
    }
    pub fn get_bookmarks(&self) -> impl ExactSizeIterator<Item = Ulid> {
        self.weave.get_bookmarks().iter().copied().map(Ulid)
    }
    pub fn get_active_thread(&mut self) -> impl DoubleEndedIterator<Item = &TapestryNode> {
        let active: Vec<u128> = self.weave.get_active_thread().collect();

        active.into_iter().filter_map(|id| self.weave.get_node(&id))
    }

    pub fn add_node(&mut self, node: TapestryNode) -> bool {
        let identifier = node.id;
        let last_active_set: HashSet<u128> = if node.active {
            HashSet::from_iter(self.weave.get_active_thread())
        } else {
            HashSet::default()
        };
        let is_active = node.active;

        let status = self.weave.add_node(node);

        let duplicates: Vec<u128> = self.weave.find_duplicates(&identifier).collect();

        if !duplicates.is_empty() {
            if is_active {
                let mut has_active = false;

                for duplicate in &duplicates {
                    if last_active_set.contains(duplicate) {
                        self.weave.set_node_active_status_in_place(duplicate, true);
                        has_active = true;
                        break;
                    }
                }

                if !has_active {
                    self.weave
                        .set_node_active_status_in_place(duplicates.first().unwrap(), true);
                }
            }
            self.weave.remove_node(&identifier);
        }

        status
    }
    pub fn set_node_active_status(&mut self, id: &Ulid, value: bool, alternate: bool) -> bool {
        self.weave.set_node_active_status(&id.0, value, alternate)
    }
    pub fn set_node_active_status_in_place(&mut self, id: &Ulid, value: bool) -> bool {
        self.weave.set_node_active_status_in_place(&id.0, value)
    }
    pub fn set_node_bookmarked_status(&mut self, id: &Ulid, value: bool) -> bool {
        self.weave.set_node_bookmarked_status(&id.0, value)
    }
    pub fn get_active_content(&mut self) -> Vec<u8> {
        let active_thread: Vec<u128> = self.weave.get_active_thread().rev().collect();

        active_thread
            .into_iter()
            .filter_map(|id| self.weave.get_node(&id))
            .flat_map(|node| node.contents.content.as_bytes().to_vec())
            .collect()
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
    pub fn is_mergeable_with_parent(&self, id: &Ulid) -> bool {
        if let Some(node) = self.weave.get_node(&id.0) {
            if node.from.len() == 1
                && let Some(parent) = node.from.first().and_then(|id| self.weave.get_node(id))
            {
                parent.to.len() == 1 && parent.contents.is_mergeable_with(&node.contents)
            } else {
                false
            }
        } else {
            false
        }
    }
    pub fn remove_node(&mut self, id: &Ulid) -> Option<TapestryNode> {
        self.weave.remove_node(&id.0)
    }
}

pub struct ArchivedTapestryWeave {
    pub weave: <TapestryWeaveInner as Archive>::Archived,
}

impl ArchivedTapestryWeave {
    pub fn len(&self) -> usize {
        self.weave.len()
    }
    pub fn is_empty(&self) -> bool {
        self.weave.is_empty()
    }
    pub fn contains(&self, id: &u128_le) -> bool {
        self.weave.contains(id)
    }
    pub fn get_node(&self, id: &u128_le) -> Option<&ArchivedTapestryNode> {
        self.weave.get_node(id)
    }
    pub fn get_roots(&self) -> impl ExactSizeIterator<Item = u128_le> {
        self.weave.get_roots().iter().copied()
    }
    pub fn get_bookmarks(&self) -> impl ExactSizeIterator<Item = u128_le> {
        self.weave.get_bookmarks().iter().copied()
    }
    pub fn get_active_thread(&mut self) -> impl DoubleEndedIterator<Item = &ArchivedTapestryNode> {
        self.weave
            .get_active_thread()
            .filter_map(|id| self.weave.get_node(&id))
    }
    pub fn get_active_content(&self) -> Vec<u8> {
        self.weave
            .get_active_thread()
            .rev()
            .filter_map(|id| self.weave.get_node(&id))
            .flat_map(|node| node.contents.content.as_bytes())
            .collect()
    }
}
