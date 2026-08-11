//! [`DependentWeave`] is a tree-based [`Weave`] where each [`Node`] depends on the contents of the previous Node.

use alloc::vec::Vec;
use core::{
    cmp::Ordering,
    hash::{BuildHasher, Hash},
    mem,
};

use hashbrown::{HashMap, HashSet, hash_map::Entry};
use indexmap::IndexSet;
use scratchpads::{Scratchpad, ScratchpadVec};

#[cfg(debug_assertions)]
use contracts::contract;

#[cfg(feature = "rkyv")]
use rkyv::{
    Archive, Deserialize, Serialize,
    bytecheck::Verify,
    collections::swiss_table::{ArchivedHashMap, ArchivedIndexSet},
    option::ArchivedOption,
    rancor::{Fallible, Source, fail},
    with::Skip,
};

#[cfg(feature = "serde")]
use serde::{
    Deserialize as SerdeDeserialize, Deserializer as SerdeDeserializer,
    Serialize as SerdeSerialize, de::Error as _,
};

use crate::{
    ActiveSingularWeave, BookmarkableWeave, DiscreteContentResult, DiscreteContents, DiscreteWeave,
    IndependentContents, MetadataWeave, Node, SemiIndependentWeave, SortableBookmarkableWeave,
    SortableWeave, Weave,
};

#[cfg(debug_assertions)]
use crate::contract::{lacks_duplicates, valid_path, valid_topological_sort};

#[cfg(feature = "rkyv")]
use crate::{
    ImmutableActiveSingularWeave, ImmutableBookmarkableWeave, ImmutableMetadataWeave,
    ImmutableWeave, archived_set_reverse_order,
};

#[cfg(any(feature = "serde", feature = "rkyv"))]
use crate::contract::ValidationError;

#[cfg(feature = "loro")]
pub mod loro;

#[cfg(feature = "legacy")]
#[deprecated]
pub mod legacy_dependent;

#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
/// A [`Node`] in a [`DependentWeave`] document.
#[must_use]
pub struct DependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// The node's unique identifier.
    pub id: K,
    /// The identifier corresponding to the node's parent.
    pub from: Option<K>,
    /// The identifiers corresponding to the node's children.
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "IndexSet<K, S>: SerdeSerialize",
            deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    pub to: IndexSet<K, S>,
    /// If the node should be considered active.
    ///
    /// [`DependentWeave`] only considers the node at the start of an active path to be active.
    pub active: bool,
    /// If the node is bookmarked.
    pub bookmarked: bool,
    /// The node's contents.
    pub contents: T,
}

#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, S> PartialEq for DependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord,
    T: PartialEq,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.from == other.from
            && self.to.len() == other.to.len()
            && self.to.iter().zip(other.to.iter()).all(|(a, b)| a == b)
            && self.active == other.active
            && self.bookmarked == other.bookmarked
            && self.contents == other.contents
    }
}

#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, S> Eq for DependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord,
    T: Eq,
    S: BuildHasher + Default + Clone,
{
}

impl<K, T, S> DependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn validate(&self) -> bool {
        self.from.is_none_or(|from| !self.to.contains(&from))
            && self.from != Some(self.id)
            && !self.to.contains(&self.id)
    }
}

impl<K, T, S> Node<K, T> for DependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    type From = Option<K>;
    type To = IndexSet<K, S>;

    #[inline]
    fn id(&self) -> K {
        self.id
    }
    #[inline]
    fn from(&self) -> &Self::From {
        &self.from
    }
    #[inline]
    fn to(&self) -> &Self::To {
        &self.to
    }
    #[inline]
    fn is_active(&self) -> bool {
        self.active
    }
    #[inline]
    fn contents(&self) -> &T {
        &self.contents
    }
}

/// A tree-based [`Weave`] where each [`Node`] depends on the contents of the previous Node.
#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize))]
#[cfg_attr(feature = "rkyv", rkyv(bytecheck(verify)))]
#[must_use]
pub struct DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "HashMap<K, DependentNode<K, T, S>, S>: SerdeSerialize",
            deserialize = "HashMap<K, DependentNode<K, T, S>, S>: SerdeDeserialize<'de>"
        ))
    )]
    pub(super) nodes: HashMap<K, DependentNode<K, T, S>, S>,
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "IndexSet<K, S>: SerdeSerialize",
            deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    pub(super) roots: IndexSet<K, S>,
    pub(super) active: Option<K>,
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "IndexSet<K, S>: SerdeSerialize",
            deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    pub(super) bookmarked: IndexSet<K, S>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    pub(super) scratchpad: Scratchpad,

    /// The metadata associated with the weave.
    pub metadata: M,
}

#[cfg(feature = "serde")]
#[derive(SerdeDeserialize)]
#[serde(rename = "DependentWeave")]
struct ProxyDependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    #[serde(bound(
        serialize = "HashMap<K, DependentNode<K, T, S>, S>: SerdeSerialize",
        deserialize = "HashMap<K, DependentNode<K, T, S>, S>: SerdeDeserialize<'de>"
    ))]
    nodes: HashMap<K, DependentNode<K, T, S>, S>,
    #[serde(bound(
        serialize = "IndexSet<K, S>: SerdeSerialize",
        deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
    ))]
    roots: IndexSet<K, S>,
    active: Option<K>,
    #[serde(bound(
        serialize = "IndexSet<K, S>: SerdeSerialize",
        deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
    ))]
    bookmarked: IndexSet<K, S>,
    metadata: M,
}

#[cfg(feature = "serde")]
#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<'de, K, T, M, S> SerdeDeserialize<'de> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + SerdeDeserialize<'de>,
    T: SerdeDeserialize<'de>,
    M: SerdeDeserialize<'de>,
    S: BuildHasher + Default + Clone,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: SerdeDeserializer<'de>,
    {
        let proxy = ProxyDependentWeave::deserialize(deserializer)?;
        let weave = Self {
            nodes: proxy.nodes,
            roots: proxy.roots,
            active: proxy.active,
            bookmarked: proxy.bookmarked,
            scratchpad: Scratchpad::new(),
            metadata: proxy.metadata,
        };

        if weave.validate() {
            Ok(weave)
        } else {
            Err(D::Error::custom(ValidationError))
        }
    }
}

#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, M, S> PartialEq for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: PartialEq,
    M: PartialEq,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.roots.len() == other.roots.len()
            && self.bookmarked.len() == other.bookmarked.len()
            && self.active == other.active
            && self
                .roots
                .iter()
                .zip(other.roots.iter())
                .all(|(a, b)| a == b)
            && self
                .bookmarked
                .iter()
                .zip(other.bookmarked.iter())
                .all(|(a, b)| a == b)
            && self.nodes == other.nodes
            && self.metadata == other.metadata
    }
}

#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, M, S> Eq for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: Eq,
    M: Eq,
    S: BuildHasher + Default + Clone,
{
}

impl<K, T, M, S> DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// Creates a new, empty [`DependentWeave`] with at least the specified capacity.
    #[cfg_attr(debug_assertions, contract(
        ensures(ret.nodes.is_empty()),
        ensures(ret.validate())
    ))]
    pub fn with_capacity(capacity: usize, metadata: M) -> Self {
        Self {
            nodes: HashMap::with_capacity_and_hasher(capacity, S::default()),
            roots: IndexSet::with_capacity_and_hasher(capacity, S::default()),
            active: None,
            bookmarked: IndexSet::with_capacity_and_hasher(capacity, S::default()),
            scratchpad: Scratchpad::new(),
            metadata,
        }
    }
    /// Returns the number of nodes the weave can hold without reallocating.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.nodes.capacity()
    }
    /// Reserves capacity for at least `additional` more nodes.
    #[cfg_attr(debug_assertions, contract(
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    pub fn reserve(&mut self, additional: usize) {
        self.nodes.reserve(additional);
        self.roots
            .reserve(self.nodes.capacity().saturating_sub(self.roots.len()));
        self.bookmarked
            .reserve(self.nodes.capacity().saturating_sub(self.bookmarked.len()));
    }
    /// Shrinks the capacity of the weave with a lower limit.
    #[cfg_attr(debug_assertions, contract(
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.nodes.shrink_to(min_capacity);
        self.roots.shrink_to(min_capacity);
        self.bookmarked.shrink_to(min_capacity);
    }
}

impl<K, T, M, S> Weave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    type Nodes = HashMap<K, DependentNode<K, T, S>, S>;
    type Roots = IndexSet<K, S>;

    #[inline]
    fn len(&self) -> usize {
        self.nodes.len()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    #[inline]
    fn nodes(&self) -> &Self::Nodes {
        &self.nodes
    }
    #[inline]
    fn roots(&self) -> &Self::Roots {
        &self.roots
    }
    #[inline]
    fn contains(&self, id: &K) -> bool {
        self.nodes.contains_key(id)
    }
    #[inline]
    fn contains_active(&self, id: &K) -> bool {
        self.active == Some(*id)
    }
    #[inline]
    fn get(&self, id: &K) -> Option<&DependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    #[inline]
    fn get_parents(&self, id: &K) -> Option<&Option<K>> {
        self.nodes.get(id).map(|node| &node.from)
    }
    #[inline]
    fn get_children(&self, id: &K) -> Option<&IndexSet<K, S>> {
        self.nodes.get(id).map(|node| &node.to)
    }
    #[inline]
    fn get_contents(&self, id: &K) -> Option<&T> {
        self.nodes.get(id).map(|node| &node.contents)
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(output.len() == self.nodes.len()),
        ensures(valid_topological_sort(&self.nodes, output)),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn get_ordered_identifiers(&mut self, output: &mut Vec<K>) {
        output.clear();
        output.reserve(self.nodes.len());

        let guard = self.scratchpad.guard();
        let mut stack = guard.vec();

        for root in &self.roots {
            topological_sort(&self.nodes, *root, &mut stack, output);
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(lacks_duplicates(output)),
        ensures(!self.nodes.contains_key(id) || output.first() == Some(id)),
        ensures(self.nodes.contains_key(id) || output.is_empty()),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn get_ordered_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();

        if self.nodes.contains_key(id) {
            let guard = self.scratchpad.guard();

            topological_sort(&self.nodes, *id, &mut guard.vec(), output);
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(self.active == output.first().copied()),
        ensures(lacks_duplicates(output)),
        ensures(valid_path(&self.nodes, output)),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn get_active_path(&mut self, output: &mut Vec<K>) {
        output.clear();

        if let Some(active) = self.active {
            path_to_root(&self.nodes, active, output);
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(!self.nodes.contains_key(id) || output.first() == Some(id)),
        ensures(self.nodes.contains_key(id) || output.is_empty()),
        ensures(lacks_duplicates(output)),
        ensures(valid_path(&self.nodes, output)),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn get_path_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();

        if self.nodes.contains_key(id) {
            path_to_root(&self.nodes, *id, output);
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(!ret || old(self.nodes.len()) + 1 == self.nodes.len()),
        ensures(!ret || old(!self.nodes.contains_key(&node.id))),
        ensures(!ret || self.nodes.contains_key(&old(node.id))),
        ensures(!ret || old(node.active) == (self.active == Some(old(node.id)))),
        ensures(!ret || old(node.bookmarked) == self.bookmarked.contains(&old(node.id))),
        ensures(!ret || old(node.from.is_some()) || self.roots.contains(&old(node.id))),
        ensures(!ret || old(node.from.is_none()) || old(self.roots.clone()) == self.roots),
        ensures(ret || old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(ret || old(self.roots.clone()) == self.roots),
        ensures(ret || old(self.active) == self.active),
        ensures(ret || old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn insert(&mut self, node: DependentNode<K, T, S>) -> bool {
        if !node.validate() || !node.to.is_empty() {
            return false;
        }

        match self.nodes.entry(node.id) {
            Entry::Occupied(_) => return false,
            Entry::Vacant(entry) => {
                let (id, from, active, bookmarked) =
                    (node.id, node.from, node.active, node.bookmarked);

                entry.insert(node);

                if let Some(from) = from {
                    if let Some(parent) = self.nodes.get_mut(&from) {
                        parent.to.insert(id);
                    } else {
                        self.nodes.remove(&id);
                        return false;
                    }
                } else {
                    self.roots.insert(id);
                }

                if active {
                    if let Some(active) = self.active.and_then(|id| self.nodes.get_mut(&id)) {
                        active.active = false;
                    }

                    self.active = Some(id);
                }

                if bookmarked {
                    self.bookmarked.insert(id);
                }
            }
        }

        true
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(!ret || value == self.contains_active(id)),
        ensures(ret || old(self.active) == self.active),
        ensures(ret == self.nodes.contains_key(id)),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn set_active(&mut self, id: &K, value: bool) -> bool {
        match self.nodes.get_mut(id) {
            Some(node) => {
                node.active = value;

                if value {
                    if self.active != Some(node.id)
                        && let Some(active) = self.active.and_then(|id| self.nodes.get_mut(&id))
                    {
                        active.active = false;
                    }

                    self.active = Some(*id);
                } else if self.active == Some(node.id) {
                    self.active = node.from;

                    if let Some(active) = self.active.and_then(|id| self.nodes.get_mut(&id)) {
                        active.active = true;
                    }
                }

                true
            }
            None => false,
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(!self.nodes.contains_key(id)),
        ensures(ret.is_some() == old(self.nodes.contains_key(id))),
        ensures(ret.as_ref().is_none_or(|node| &node.id == id)),
        ensures(ret.is_none() || old(self.nodes.len()) > self.nodes.len()),
        ensures(ret.is_none() || old(self.bookmarked.len()) >= self.bookmarked.len()),
        ensures(ret.is_some() || old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(ret.is_some() || old(self.roots.clone()) == self.roots),
        ensures(ret.is_some() || old(self.active) == self.active),
        ensures(ret.is_some() || old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn remove(&mut self, id: &K) -> Option<DependentNode<K, T, S>> {
        let mut removed_node = None;
        let mut removed_active = false;

        let guard = self.scratchpad.guard();
        let mut stack = guard.vec();
        let mut removed_bookmarks = guard.set(S::default());

        stack.push(*id);

        while let Some(id) = stack.pop() {
            if let Some(node) = self.nodes.remove(&id) {
                if node.bookmarked {
                    removed_bookmarks.insert(id);
                }
                if node.active {
                    self.active = None;
                    removed_active = true;
                }

                stack.extend(node.to.iter().copied());

                if removed_node.is_none() {
                    if node.from.is_none() {
                        self.roots.shift_remove(&id);
                    }
                    removed_node = Some(node);
                }
            }
        }

        if let Some(removed) = removed_node {
            if let Some(parent_node) = removed.from.as_ref().and_then(|id| self.nodes.get_mut(id)) {
                parent_node.to.shift_remove(id);
            }
            if !removed_bookmarks.is_empty() {
                self.bookmarked.retain(|id| !removed_bookmarks.contains(id));
            }
            if removed_active {
                self.active = removed.from;

                if let Some(node) = self.active.and_then(|id| self.nodes.get_mut(&id)) {
                    node.active = true;
                }
            }
            Some(removed)
        } else {
            None
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(!self.nodes.contains_key(id)),
        ensures(ret == old(self.nodes.contains_key(id))),
        ensures(!ret || old(self.nodes.len()) > self.nodes.len()),
        ensures(!ret || old(self.bookmarked.len()) >= self.bookmarked.len()),
        ensures(ret || old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(ret || old(self.roots.clone()) == self.roots),
        ensures(ret || old(self.active) == self.active),
        ensures(ret || old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn remove_tracked(
        &mut self,
        id: &K,
        mut on_removal: impl FnMut(DependentNode<K, T, S>),
    ) -> bool {
        let mut removed_node_parent = None;
        let mut removed_active = false;

        let guard = self.scratchpad.guard();
        let mut stack = guard.vec();
        let mut removed_bookmarks = guard.set(S::default());

        stack.push(*id);

        while let Some(id) = stack.pop() {
            if let Some(node) = self.nodes.remove(&id) {
                if node.bookmarked {
                    removed_bookmarks.insert(id);
                }
                if node.active {
                    self.active = None;
                    removed_active = true;
                }

                stack.extend(node.to.iter().rev().copied());

                if removed_node_parent.is_none() {
                    if node.from.is_none() {
                        self.roots.shift_remove(&id);
                    }
                    removed_node_parent = Some(node.from);
                }

                on_removal(node);
            }
        }

        if let Some(parent) = removed_node_parent {
            if let Some(parent_node) = parent.as_ref().and_then(|id| self.nodes.get_mut(id)) {
                parent_node.to.shift_remove(id);
            }
            if !removed_bookmarks.is_empty() {
                self.bookmarked.retain(|id| !removed_bookmarks.contains(id));
            }
            if removed_active {
                self.active = parent;

                if let Some(node) = self.active.and_then(|id| self.nodes.get_mut(&id)) {
                    node.active = true;
                }
            }
            true
        } else {
            false
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(self.nodes.is_empty()),
        ensures(self.validate())
    ))]
    fn clear(&mut self) {
        self.nodes.clear();
        self.roots.clear();
        self.active = None;
        self.bookmarked.clear();
    }
}

impl<K, T, M, S> DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// Validates that the weave is internally consistent.
    pub fn validate(&self) -> bool {
        self.roots
            .iter()
            .all(move |value| self.nodes.contains_key(value))
            && self
                .active
                .as_ref()
                .is_none_or(|active| self.nodes.contains_key(active))
            && self
                .bookmarked
                .iter()
                .all(move |value| self.nodes.contains_key(value))
            && self.nodes.iter().all(|(key, value)| {
                value.validate()
                    && value.id == *key
                    && value
                        .from
                        .as_ref()
                        .is_none_or(|v| self.nodes.get(v).is_some_and(|p| p.to.contains(key)))
                    && value.to.iter().all(|v| {
                        self.nodes
                            .get(v)
                            .is_some_and(|p| p.from.as_ref() == Some(key))
                    })
                    && value.from.is_none() == self.roots.contains(key)
                    && value.active == (self.active == Some(*key))
                    && value.bookmarked == self.bookmarked.contains(key)
            })
            && !detect_cycles(
                &self.nodes,
                self.roots.iter().copied(),
                &mut Vec::with_capacity(self.roots.len()),
                &mut HashSet::with_capacity_and_hasher(self.nodes.len(), S::default()),
            )
    }
}

impl<K, T, M, S> MetadataWeave<K, DependentNode<K, T, S>, T, M> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn metadata(&self) -> &M {
        &self.metadata
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    #[inline]
    fn metadata_mut<O>(&mut self, callback: impl FnOnce(&mut M) -> O) -> O {
        callback(&mut self.metadata)
    }
}

impl<K, T, M, S> BookmarkableWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    type Bookmarks = IndexSet<K, S>;

    #[inline]
    fn bookmarks(&self) -> &Self::Bookmarks {
        &self.bookmarked
    }
    #[inline]
    fn contains_bookmark(&self, id: &K) -> bool {
        self.bookmarked.contains(id)
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(!ret || value == self.bookmarked.contains(id)),
        ensures(ret || old(self.bookmarked.clone()) == self.bookmarked),
        ensures(ret == self.nodes.contains_key(id)),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active) == self.active),
        invariant(self.validate())
    ))]
    fn set_bookmarked(&mut self, id: &K, value: bool) -> bool {
        match self.nodes.get_mut(id) {
            Some(node) => {
                node.bookmarked = value;
                if value {
                    self.bookmarked.insert(node.id);
                } else {
                    self.bookmarked.shift_remove(id);
                }

                true
            }
            None => false,
        }
    }
}

impl<K, T, M, S> SortableWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    #[cfg_attr(debug_assertions, contract(
        ensures(ret == self.nodes.contains_key(id)),
        ensures(old(self.nodes.get(id).map(|n| n.to.clone())) == self.nodes.get(id).map(|n| n.to.clone())),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn sort_children_by(
        &mut self,
        id: &K,
        mut cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) -> bool {
        if let Some(node) = self.nodes.get_mut(id) {
            let mut set = mem::take(&mut node.to);

            {
                let guard = self.scratchpad.guard();
                let mut nodes = guard
                    .arena()
                    .alloc_iter_exact(set.drain(..).map(|id| &self.nodes[&id]));
                nodes.sort_by(|a, b| cmp(*a, *b));

                set.extend(nodes.into_iter().map(|node| node.id));
            }

            self.nodes.get_mut(id).unwrap().to = set;

            true
        } else {
            false
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(ret == self.nodes.contains_key(id)),
        ensures(old(self.nodes.get(id).map(|n| n.to.clone())) == self.nodes.get(id).map(|n| n.to.clone())),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn sort_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool {
        if let Some(node) = self.nodes.get_mut(id) {
            node.to.sort_by(cmp);

            true
        } else {
            false
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn sort_roots_by(
        &mut self,
        mut cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) {
        let guard = self.scratchpad.guard();

        let mut nodes = guard
            .arena()
            .alloc_iter_exact(self.roots.iter().map(|id| &self.nodes[id]));
        nodes.sort_by(|a, b| cmp(*a, *b));

        self.roots.clear();
        self.roots.extend(nodes.into_iter().map(|node| node.id));
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.roots.sort_by(cmp);
    }
}

impl<K, T, M, S> SortableBookmarkableWeave<K, DependentNode<K, T, S>, T>
    for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    #[cfg_attr(debug_assertions, contract(
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn sort_bookmarks_by(
        &mut self,
        mut cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) {
        let guard = self.scratchpad.guard();

        let mut nodes = guard
            .arena()
            .alloc_iter_exact(self.bookmarked.iter().map(|id| &self.nodes[id]));
        nodes.sort_by(|a, b| cmp(*a, *b));

        self.bookmarked.clear();
        self.bookmarked
            .extend(nodes.into_iter().map(|node| node.id));
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn sort_bookmarks_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.bookmarked.sort_by(cmp);
    }
}

impl<K, T, M, S> ActiveSingularWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn active(&self) -> Option<K> {
        self.active
    }
}

impl<K, T, M, S> DiscreteWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    #[cfg_attr(debug_assertions, contract(
        ensures(!ret || old(self.nodes.len()) + 1 == self.nodes.len()),
        ensures(!ret || self.nodes.contains_key(id)),
        ensures(!ret || self.nodes.contains_key(&new_id)),
        ensures(!ret || old(!self.nodes.contains_key(&new_id))),
        ensures(!ret || self.nodes[id].to.contains(&new_id) && self.nodes[id].to.len() == 1),
        ensures(!ret || self.nodes[&new_id].from == Some(*id)),
        ensures(!ret || old(self.nodes.get(id).map(|n| n.to.clone())).unwrap() == self.nodes[&new_id].to),
        ensures(ret || old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn split(&mut self, id: &K, at: usize, new_id: K) -> bool {
        if self.nodes.contains_key(&new_id) || *id == new_id {
            return false;
        }

        if let Some(mut node) = self.nodes.remove(id) {
            match node.contents.split(at) {
                DiscreteContentResult::Two(left, right) => {
                    let left_node = DependentNode {
                        id: node.id,
                        from: node.from,
                        to: IndexSet::from_iter([new_id]),
                        active: node.active,
                        bookmarked: node.bookmarked,
                        contents: left,
                    };

                    node.from = Some(node.id);
                    node.id = new_id;
                    node.contents = right;
                    node.active = false;
                    node.bookmarked = false;

                    for child in &node.to {
                        let child = self.nodes.get_mut(child).unwrap();
                        child.from = Some(node.id);
                    }

                    self.nodes.insert(left_node.id, left_node);
                    self.nodes.insert(node.id, node);

                    true
                }
                DiscreteContentResult::One(content) => {
                    node.contents = content;
                    self.nodes.insert(node.id, node);
                    false
                }
            }
        } else {
            false
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(ret.is_none() || old(self.nodes.len()) - 1 == self.nodes.len()),
        ensures(ret.is_none() || !self.nodes.contains_key(id)),
        ensures(ret.is_none() || old(self.nodes.contains_key(id))),
        ensures(ret.is_none() || !old(self.contains_active(id)) || old(self.contains_active(id)) && self.contains_active(&ret.unwrap())),
        ensures(ret.is_none() || old(self.contains_active(id)) || old(self.nodes.get(id).and_then(|n| n.from).and_then(|p| self.nodes.get(&p)).map(|p| p.active)).unwrap() == self.nodes[&ret.unwrap()].active),
        ensures(ret.is_none() || old(self.nodes.get(id).and_then(|n| n.from).and_then(|p| self.nodes.get(&p)).map(|p| p.from)).unwrap() == self.nodes[&ret.unwrap()].from),
        ensures(ret.is_none() || old(self.nodes.get(id).map(|node| node.to.clone())).unwrap() == self.nodes[&ret.unwrap()].to),
        ensures(ret.is_none() || ret.unwrap() == old(self.nodes.get(id).and_then(|node| node.from)).unwrap()),
        ensures(ret.is_some() || old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(ret.is_some() || old(self.active) == self.active),
        ensures(ret.is_some() || old(self.bookmarked.clone()) == self.bookmarked),
        ensures(old(self.roots.clone()) == self.roots),
        invariant(self.validate())
    ))]
    fn merge_with_parent(&mut self, id: &K) -> Option<K> {
        if let Some(mut node) = self.nodes.remove(id) {
            if let Some(mut parent) = node.from.as_ref().and_then(|id| self.nodes.remove(id)) {
                if parent.to.len() > 1 {
                    self.nodes.insert(parent.id, parent);
                    self.nodes.insert(node.id, node);
                    return None;
                }

                match parent.contents.merge(node.contents) {
                    DiscreteContentResult::Two(left, right) => {
                        parent.contents = left;
                        node.contents = right;
                        self.nodes.insert(parent.id, parent);
                        self.nodes.insert(node.id, node);
                        None
                    }
                    DiscreteContentResult::One(content) => {
                        parent.contents = content;
                        parent.to = node.to;

                        for child in &parent.to {
                            let child = self.nodes.get_mut(child).unwrap();
                            child.from = Some(parent.id);
                        }

                        if node.active {
                            parent.active = true;
                            self.active = Some(parent.id);
                        }

                        let parent_id = parent.id;

                        if node.bookmarked && !parent.bookmarked {
                            parent.bookmarked = true;
                            assert!(
                                self.bookmarked
                                    .replace_index(
                                        self.bookmarked.get_index_of(&node.id).unwrap(),
                                        parent.id,
                                    )
                                    .is_ok(),
                                "Should be unreachable"
                            );
                        } else if node.bookmarked {
                            self.bookmarked.shift_remove(&node.id);
                        }

                        self.nodes.insert(parent.id, parent);

                        Some(parent_id)
                    }
                }
            } else {
                self.nodes.insert(node.id, node);
                None
            }
        } else {
            None
        }
    }
}

impl<K, T, M, S> SemiIndependentWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[cfg_attr(debug_assertions, contract(
        ensures(ret.is_some() == old(self.nodes.contains_key(id))),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    #[inline]
    fn get_contents_mut<O>(&mut self, id: &K, callback: impl FnOnce(&mut T) -> O) -> Option<O> {
        self.nodes
            .get_mut(id)
            .map(|node| callback(&mut node.contents))
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, S> ArchivedDependentNode<K, T, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn validate(&self) -> bool {
        (if let ArchivedOption::Some(from) = &self.from {
            !self.to.contains(from)
        } else {
            true
        }) && self.from != Some(self.id)
            && !self.to.contains(&self.id)
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, S> Node<K::Archived, T::Archived> for ArchivedDependentNode<K, T, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
    S: BuildHasher + Default + Clone,
{
    type From = ArchivedOption<K::Archived>;
    type To = ArchivedIndexSet<K::Archived>;

    #[inline]
    fn id(&self) -> K::Archived {
        self.id
    }
    #[inline]
    fn from(&self) -> &Self::From {
        &self.from
    }
    #[inline]
    fn to(&self) -> &Self::To {
        &self.to
    }
    #[inline]
    fn is_active(&self) -> bool {
        self.active
    }
    #[inline]
    fn contents(&self) -> &T::Archived {
        &self.contents
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, M, S> ArchivedDependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
    M: Archive,
    S: BuildHasher + Default + Clone,
{
    fn validate(&self) -> bool {
        self.roots
            .iter()
            .all(move |value| self.nodes.contains_key(value))
            && self
                .active
                .as_ref()
                .is_none_or(|active| self.nodes.contains_key(active))
            && self
                .bookmarked
                .iter()
                .all(move |value| self.nodes.contains_key(value))
            && self.nodes.iter().all(|(key, value)| {
                value.validate()
                    && value.id == *key
                    && value
                        .from
                        .as_ref()
                        .is_none_or(|v| self.nodes.get(v).is_some_and(|p| p.to.contains(key)))
                    && value.to.iter().all(|v| {
                        self.nodes
                            .get(v)
                            .is_some_and(|p| p.from.as_ref() == Some(key))
                    })
                    && value.from.is_none() == self.roots.contains(key)
                    && value.active == (self.active == Some(*key))
                    && value.bookmarked == self.bookmarked.contains(key)
            })
            && !archived_detect_cycles(
                &self.nodes,
                self.roots.iter().copied(),
                &mut Vec::with_capacity(self.roots.len()),
                &mut HashSet::with_capacity_and_hasher(self.nodes.len(), S::default()),
            )
    }
}

#[cfg(feature = "rkyv")]
// SAFETY:
// All fields are safe to access and no unsafe functions are called
unsafe impl<K, T, M, S, C> Verify<C> for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
    M: Archive,
    S: BuildHasher + Default + Clone,
    C: Fallible + ?Sized,
    C::Error: Source,
{
    fn verify(&self, _context: &mut C) -> Result<(), C::Error> {
        if !self.validate() {
            fail!(ValidationError)
        }

        Ok(())
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, M, S> ImmutableWeave<K::Archived, ArchivedDependentNode<K, T, S>, T::Archived>
    for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
    M: Archive,
    S: BuildHasher + Default + Clone,
{
    type Nodes = ArchivedHashMap<K::Archived, ArchivedDependentNode<K, T, S>>;
    type Roots = ArchivedIndexSet<K::Archived>;

    #[inline]
    fn len(&self) -> usize {
        self.nodes.len()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    #[inline]
    fn nodes(&self) -> &Self::Nodes {
        &self.nodes
    }
    #[inline]
    fn roots(&self) -> &Self::Roots {
        &self.roots
    }
    #[inline]
    fn contains(&self, id: &K::Archived) -> bool {
        self.nodes.contains_key(id)
    }
    #[inline]
    fn contains_active(&self, id: &K::Archived) -> bool {
        self.active == Some(*id)
    }
    #[inline]
    fn get(&self, id: &K::Archived) -> Option<&ArchivedDependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    #[inline]
    fn get_parents(&self, id: &K::Archived) -> Option<&ArchivedOption<K::Archived>> {
        self.nodes.get(id).map(|node| &node.from)
    }
    #[inline]
    fn get_children(&self, id: &K::Archived) -> Option<&ArchivedIndexSet<K::Archived>> {
        self.nodes.get(id).map(|node| &node.to)
    }
    #[inline]
    fn get_contents(&self, id: &K::Archived) -> Option<&T::Archived> {
        self.nodes.get(id).map(|node| &node.contents)
    }
    fn get_ordered_identifiers(&self, output: &mut Vec<K::Archived>) {
        output.clear();
        output.reserve(self.nodes.len());

        let mut scratchpad = Vec::new();

        for root in self.roots.iter() {
            archived_topological_sort(&self.nodes, *root, &mut scratchpad, output);
        }
    }
    fn get_ordered_identifiers_from(&self, id: &K::Archived, output: &mut Vec<K::Archived>) {
        output.clear();

        if self.nodes.contains_key(id) {
            let mut scratchpad = Vec::new();

            archived_topological_sort(&self.nodes, *id, &mut scratchpad, output);
        }
    }
    fn get_active_path(&self, output: &mut Vec<K::Archived>) {
        output.clear();

        if let ArchivedOption::Some(active) = self.active {
            archived_path_to_root(&self.nodes, active, output);
        }
    }
    fn get_path_from(&self, id: &K::Archived, output: &mut Vec<K::Archived>) {
        output.clear();

        if self.nodes.contains_key(id) {
            archived_path_to_root(&self.nodes, *id, output);
        }
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, M, S>
    ImmutableMetadataWeave<K::Archived, ArchivedDependentNode<K, T, S>, T::Archived, M::Archived>
    for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
    M: Archive,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn metadata(&self) -> &M::Archived {
        &self.metadata
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, M, S>
    ImmutableBookmarkableWeave<K::Archived, ArchivedDependentNode<K, T, S>, T::Archived>
    for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
    M: Archive,
    S: BuildHasher + Default + Clone,
{
    type Bookmarks = ArchivedIndexSet<K::Archived>;

    #[inline]
    fn bookmarks(&self) -> &Self::Bookmarks {
        &self.bookmarked
    }
    #[inline]
    fn contains_bookmark(&self, id: &K::Archived) -> bool {
        self.bookmarked.contains(id)
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, M, S>
    ImmutableActiveSingularWeave<K::Archived, ArchivedDependentNode<K, T, S>, T::Archived>
    for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
    M: Archive,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn active(&self) -> Option<K::Archived> {
        match self.active {
            ArchivedOption::Some(active) => Some(active),
            ArchivedOption::None => None,
        }
    }
}

fn path_to_root<K, T, S>(
    nodes: &HashMap<K, DependentNode<K, T, S>, S>,
    mut id: K,
    thread: &mut Vec<K>,
) where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    thread.push(id);

    while let Some(parent) = nodes[&id].from {
        thread.push(parent);
        id = parent;
    }
}

#[cfg(feature = "rkyv")]
fn archived_path_to_root<K, T, S>(
    nodes: &ArchivedHashMap<K::Archived, ArchivedDependentNode<K, T, S>>,
    mut id: K::Archived,
    thread: &mut Vec<K::Archived>,
) where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord,
    T: Archive,
    S: BuildHasher + Default + Clone,
{
    thread.push(id);

    while let ArchivedOption::Some(parent) = nodes[&id].from {
        thread.push(parent);
        id = parent;
    }
}

fn topological_sort<K, N, T, S>(
    nodes: &HashMap<K, N, S>,
    id: K,
    stack: &mut ScratchpadVec<'_, K>,
    identifiers: &mut Vec<K>,
) where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T, From = Option<K>, To = IndexSet<K, S>>,
    S: BuildHasher + Default + Clone,
{
    stack.push(id);

    while let Some(id) = stack.pop() {
        identifiers.push(id);
        stack.extend(nodes[&id].to().into_iter().rev().copied());
    }
}

#[cfg(feature = "rkyv")]
fn archived_topological_sort<K, N, T>(
    nodes: &ArchivedHashMap<K, N>,
    id: K,
    stack: &mut Vec<K>,
    identifiers: &mut Vec<K>,
) where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T, From = ArchivedOption<K>, To = ArchivedIndexSet<K>>,
{
    stack.push(id);

    while let Some(id) = stack.pop() {
        identifiers.push(id);
        stack.extend(archived_set_reverse_order(nodes[&id].to()).copied());
    }
}

fn detect_cycles<K, N, T, S>(
    nodes: &HashMap<K, N, S>,
    roots: impl Iterator<Item = K>,
    stack: &mut Vec<K>,
    scratchpad_set: &mut HashSet<K, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T, From = Option<K>, To = IndexSet<K, S>>,
    S: BuildHasher + Default + Clone,
{
    stack.extend(roots);

    while let Some(id) = stack.pop() {
        if !scratchpad_set.insert(id) {
            return true;
        }
        stack.extend(nodes[&id].to().into_iter().rev().copied());
    }

    scratchpad_set.len() != nodes.len()
}

#[cfg(feature = "rkyv")]
fn archived_detect_cycles<K, N, T, S>(
    nodes: &ArchivedHashMap<K, N>,
    roots: impl Iterator<Item = K>,
    stack: &mut Vec<K>,
    scratchpad_set: &mut HashSet<K, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T, From = ArchivedOption<K>, To = ArchivedIndexSet<K>>,
    S: BuildHasher + Default + Clone,
{
    stack.extend(roots);

    while let Some(id) = stack.pop() {
        if !scratchpad_set.insert(id) {
            return true;
        }
        stack.extend(archived_set_reverse_order(nodes[&id].to()).copied());
    }

    scratchpad_set.len() != nodes.len()
}
