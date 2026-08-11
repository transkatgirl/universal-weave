//! A legacy version of `DependentWeave` used by `tapestry-weave`'s v0 format; Please don't use this!

use alloc::vec::Vec;
use core::{
    cmp::Ordering,
    hash::{BuildHasher, Hash},
};

use hashbrown::{HashMap, HashSet};
use indexmap::IndexSet;
use scratchpads::Scratchpad;

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
    IndependentContents, MetadataWeave, SemiIndependentWeave, SortableBookmarkableWeave,
    SortableWeave, Step, Weave,
    dependent::{
        DependentNode, DependentWeave as NewDependentWeave, detect_cycles, path_to_root,
        topological_sort,
    },
};

#[cfg(feature = "rkyv")]
use crate::{
    ImmutableActiveSingularWeave, ImmutableBookmarkableWeave, ImmutableMetadataWeave,
    ImmutableWeave,
    dependent::{
        ArchivedDependentNode, archived_detect_cycles, archived_path_to_root,
        archived_topological_sort,
    },
};

#[cfg(any(feature = "serde", feature = "rkyv"))]
use crate::contract::ValidationError;

#[cfg(doc)]
use crate::Node;

impl<K, T, M, S> From<DependentWeave<K, T, M, S>> for NewDependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    fn from(value: DependentWeave<K, T, M, S>) -> Self {
        Self {
            nodes: value.nodes,
            roots: value.roots,
            active: value.active,
            bookmarked: value.bookmarked,
            scratchpad: value.scratchpad,
            metadata: value.metadata,
        }
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
    nodes: HashMap<K, DependentNode<K, T, S>, S>,
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "IndexSet<K, S>: SerdeSerialize",
            deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    roots: IndexSet<K, S>,
    active: Option<K>,
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "IndexSet<K, S>: SerdeSerialize",
            deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    bookmarked: IndexSet<K, S>,

    // Legacy field required for deserialization
    #[allow(dead_code)]
    thread: Vec<K>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad: Scratchpad,

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
    thread: Vec<K>,
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
            thread: proxy.thread,
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
    pub fn with_capacity(capacity: usize, metadata: M) -> Self {
        Self {
            nodes: HashMap::with_capacity_and_hasher(capacity, S::default()),
            roots: IndexSet::with_capacity_and_hasher(capacity, S::default()),
            active: None,
            bookmarked: IndexSet::with_capacity_and_hasher(capacity, S::default()),
            thread: Vec::new(),
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
    pub fn reserve(&mut self, additional: usize) {
        self.nodes.reserve(additional);
        self.roots
            .reserve(self.nodes.capacity().saturating_sub(self.roots.len()));
        self.bookmarked
            .reserve(self.nodes.capacity().saturating_sub(self.bookmarked.len()));
    }
    /// Shrinks the capacity of the weave with a lower limit.
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
    fn get_ordered_identifiers(&mut self, output: &mut Vec<K>) {
        output.clear();

        let guard = self.scratchpad.guard();

        for root in &self.roots {
            topological_sort(&self.nodes, *root, &mut guard.vec(), output);
        }
    }
    fn get_ordered_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();

        if self.nodes.contains_key(id) {
            let guard = self.scratchpad.guard();

            topological_sort(&self.nodes, *id, &mut guard.vec(), output);
        }
    }
    fn get_active_path(&mut self, output: &mut Vec<K>) {
        output.clear();

        if let Some(active) = self.active {
            path_to_root(&self.nodes, active, output);
        }
    }
    fn get_path_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();

        if self.nodes.contains_key(id) {
            path_to_root(&self.nodes, *id, output);
        }
    }
    fn insert(&mut self, node: DependentNode<K, T, S>) -> bool {
        if self.nodes.contains_key(&node.id) || !node.validate() || !node.to.is_empty() {
            return false;
        }

        if let Some(from) = node.from {
            match self.nodes.get_mut(&from) {
                Some(parent) => {
                    parent.to.insert(node.id);
                }
                None => return false,
            }
        } else {
            self.roots.insert(node.id);
        }

        if node.active {
            if let Some(active) = self.active.and_then(|id| self.nodes.get_mut(&id)) {
                active.active = false;
            }

            self.active = Some(node.id);
        }

        if node.bookmarked {
            self.bookmarked.insert(node.id);
        }

        self.nodes.insert(node.id, node);

        true
    }
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
                    self.active = None;
                }

                true
            }
            None => false,
        }
    }
    fn remove(&mut self, id: &K) -> Option<DependentNode<K, T, S>> {
        let guard = self.scratchpad.guard();
        let mut stack = guard.vec();

        stack.push(Step::Enter(*id));

        while let Some(step) = stack.pop() {
            match step {
                Step::Enter(id) => {
                    if let Some(node) = self.nodes.get(&id) {
                        if node.from.is_none() {
                            self.roots.shift_remove(&id);
                        }
                        if node.bookmarked {
                            self.bookmarked.shift_remove(&id);
                        }

                        stack.push(Step::Exit(id));
                        stack.extend(node.to.iter().rev().copied().map(Step::Enter));
                    }
                }
                Step::Exit(id) => {
                    if let Some(node) = self.nodes.remove(&id) {
                        if node.active {
                            self.active = node.from;
                            if let Some(parent) = node.from.and_then(|id| self.nodes.get_mut(&id)) {
                                parent.active = true;
                            }
                        }
                        if let Some(parent) = node.from.and_then(|id| self.nodes.get_mut(&id)) {
                            parent.to.shift_remove(&id);
                        }

                        if stack.is_empty() {
                            return Some(node);
                        }
                    }
                }
            }
        }

        None
    }
    fn remove_tracked(
        &mut self,
        id: &K,
        mut on_removal: impl FnMut(DependentNode<K, T, S>),
    ) -> bool {
        let guard = self.scratchpad.guard();
        let mut stack = guard.vec();

        stack.push(Step::Enter(*id));

        while let Some(step) = stack.pop() {
            match step {
                Step::Enter(id) => {
                    if let Some(node) = self.nodes.get(&id) {
                        if node.from.is_none() {
                            self.roots.shift_remove(&id);
                        }
                        if node.bookmarked {
                            self.bookmarked.shift_remove(&id);
                        }

                        stack.push(Step::Exit(id));
                        stack.extend(node.to.iter().rev().copied().map(Step::Enter));
                    }
                }
                Step::Exit(id) => {
                    if let Some(node) = self.nodes.remove(&id) {
                        if node.active {
                            self.active = node.from;
                            if let Some(parent) = node.from.and_then(|id| self.nodes.get_mut(&id)) {
                                parent.active = true;
                            }
                        }
                        if let Some(parent) = node.from.and_then(|id| self.nodes.get_mut(&id)) {
                            parent.to.shift_remove(&id);
                        }

                        on_removal(node);
                        if stack.is_empty() {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }
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
    fn sort_children_by(
        &mut self,
        id: &K,
        mut cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) -> bool {
        if let Some(mut node) = self.nodes.remove(id) {
            node.to.sort_by(|a, b| cmp(&self.nodes[a], &self.nodes[b]));
            self.nodes.insert(node.id, node);

            true
        } else {
            false
        }
    }
    fn sort_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool {
        if let Some(node) = self.nodes.get_mut(id) {
            node.to.sort_by(cmp);

            true
        } else {
            false
        }
    }
    fn sort_roots_by(
        &mut self,
        mut cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) {
        self.roots
            .sort_by(|a, b| cmp(&self.nodes[a], &self.nodes[b]));
    }
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
    fn sort_bookmarks_by(
        &mut self,
        mut cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) {
        self.bookmarked
            .sort_by(|a, b| cmp(&self.nodes[a], &self.nodes[b]));
    }
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
    fn merge_with_parent(&mut self, id: &K) -> Option<K> {
        if let Some(mut node) = self.nodes.remove(id) {
            if let Some(mut parent) = node.from.and_then(|id| self.nodes.remove(&id)) {
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

                        self.nodes.insert(parent.id, parent);

                        self.bookmarked.shift_remove(&node.id);

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
    #[inline]
    fn get_contents_mut<O>(&mut self, id: &K, callback: impl FnOnce(&mut T) -> O) -> Option<O> {
        self.nodes
            .get_mut(id)
            .map(|node| callback(&mut node.contents))
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
                &mut HashSet::with_capacity(self.nodes.len()),
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
