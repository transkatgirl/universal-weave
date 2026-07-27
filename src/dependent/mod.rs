//! [`DependentWeave`] is a tree-based [`Weave`] where each [`Node`] depends on the contents of the previous Node.

use std::{
    cmp::Ordering,
    collections::HashMap,
    hash::{BuildHasher, Hash},
    iter,
};

#[allow(unused_imports, reason = "False positive")]
use contracts::{ensures, invariant};
use indexmap::IndexSet;
use stacksafe::stacksafe;

#[cfg(feature = "rkyv")]
use rkyv::{
    Archive, Deserialize, Serialize,
    collections::swiss_table::{ArchivedHashMap, ArchivedIndexSet},
    option::ArchivedOption,
    with::Skip,
};

#[cfg(feature = "wincode")]
use wincode::{SchemaRead, SchemaWrite};

#[cfg(feature = "serde")]
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

#[cfg(feature = "rkyv")]
use crate::{
    ArchivedActiveSingularWeave, ArchivedBookmarkableWeave, ArchivedMetadataWeave,
    ArchivedSortableWeave, ArchivedWeave,
};

use crate::{
    ActiveSingularWeave, BookmarkableWeave, DeduplicatableContents, DeduplicatableWeave,
    DiscreteContentResult, DiscreteContents, DiscreteWeave, IndependentContents, MetadataWeave,
    Node, SemiIndependentWeave, SortableBookmarkableWeave, SortableWeave, ValidatableWeave, Weave,
    contract::{
        lacks_duplicates, matches_topological_sort, matches_topological_sort_rev, valid_path,
        valid_topological_sort,
    },
};

#[cfg(feature = "loro")]
pub mod loro;

#[cfg(feature = "legacy")]
#[deprecated]
pub mod legacy_dependent;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
/// A [`Node`] in a [`DependentWeave`] document.
#[must_use]
pub struct DependentNode<K, T, S>
where
    K: Hash + Copy + Eq,
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
    /// If the node should be considered "active".
    ///
    /// [`DependentWeave`] only considers the node at the start of an active thread to be "active".
    pub active: bool,
    /// If the node is bookmarked.
    pub bookmarked: bool,
    /// The node's contents.
    pub contents: T,
}

#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, S> PartialEq for DependentNode<K, T, S>
where
    K: Hash + Copy + Eq,
    T: PartialEq,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
            && self.from.eq(&other.from)
            && self.to.eq(&other.to)
            && self.active.eq(&other.active)
            && self.bookmarked.eq(&other.bookmarked)
            && self.contents.eq(&other.contents)
    }
}

#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, S> Eq for DependentNode<K, T, S>
where
    K: Hash + Copy + Eq,
    T: Eq,
    S: BuildHasher + Default + Clone,
{
}

impl<K, T, S> DependentNode<K, T, S>
where
    K: Hash + Copy + Eq,
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
    K: Hash + Copy + Eq,
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
#[derive(Debug, Clone)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[must_use]
pub struct DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
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

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "wincode", wincode(skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad: Vec<K>,

    pub metadata: M,
}

impl<K, T, M, S> DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    pub fn with_capacity(capacity: usize, metadata: M) -> Self {
        Self {
            nodes: HashMap::with_capacity_and_hasher(capacity, S::default()),
            roots: IndexSet::with_capacity_and_hasher(capacity, S::default()),
            active: None,
            bookmarked: IndexSet::with_capacity_and_hasher(capacity, S::default()),
            scratchpad: Vec::with_capacity(capacity),
            metadata,
        }
    }
    #[inline]
    pub fn capacity(&self) -> usize {
        self.nodes.capacity()
    }
    pub fn reserve(&mut self, additional: usize) {
        self.nodes.reserve(additional);
        self.roots
            .reserve(self.nodes.capacity().saturating_sub(self.roots.len()));
        self.bookmarked
            .reserve(self.nodes.capacity().saturating_sub(self.bookmarked.len()));
        self.scratchpad
            .reserve(self.nodes.capacity().saturating_sub(self.scratchpad.len()));
    }
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.nodes.shrink_to(min_capacity);
        self.roots.shrink_to(min_capacity);
        self.bookmarked.shrink_to(min_capacity);
        self.scratchpad.shrink_to(min_capacity);
    }
    fn siblings<'a>(
        &'a self,
        node: &'a DependentNode<K, T, S>,
    ) -> Box<dyn Iterator<Item = &'a DependentNode<K, T, S>> + 'a> {
        match &node.from {
            Some(parent) => Box::new(self.nodes.get(parent).into_iter().flat_map(|parent| {
                parent
                    .to
                    .iter()
                    .copied()
                    .filter(|id| *id != node.id)
                    .filter_map(|id| self.nodes.get(&id))
            })),
            None => Box::new(
                self.roots
                    .iter()
                    .copied()
                    .filter(|id| *id != node.id)
                    .filter_map(|id| self.nodes.get(&id)),
            ),
        }
    }
    #[ensures(!self.nodes.contains_key(id))]
    #[stacksafe]
    fn remove_node_unverified(&mut self, id: &K) -> Option<DependentNode<K, T, S>> {
        if let Some(node) = self.nodes.remove(id) {
            self.roots.shift_remove(id);
            self.bookmarked.shift_remove(id);
            for child in &node.to {
                self.remove_node_unverified(child);
            }
            if node.active {
                self.active = node.from;
                if let Some(parent) = node.from.and_then(|id| self.nodes.get_mut(&id)) {
                    parent.active = true;
                } else {
                    self.active = None;
                }
            }
            if let Some(parent) = node.from.and_then(|id| self.nodes.get_mut(&id)) {
                parent.to.shift_remove(id);
            }
            Some(node)
        } else {
            None
        }
    }
    #[ensures(!self.nodes.contains_key(id))]
    #[stacksafe]
    fn remove_node_unverified_tracked(
        &mut self,
        id: &K,
        callback: &mut impl FnMut(DependentNode<K, T, S>),
    ) -> bool {
        if let Some(node) = self.nodes.remove(id) {
            self.roots.shift_remove(id);
            self.bookmarked.shift_remove(id);
            for child in &node.to {
                self.remove_node_unverified_tracked(child, callback);
            }
            if node.active {
                self.active = node.from;
                if let Some(parent) = node.from.and_then(|id| self.nodes.get_mut(&id)) {
                    parent.active = true;
                } else {
                    self.active = None;
                }
            }
            if let Some(parent) = node.from.and_then(|id| self.nodes.get_mut(&id)) {
                parent.to.shift_remove(id);
            }
            callback(node);
            true
        } else {
            false
        }
    }
}

impl<K, T, M, S> Weave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
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
    fn get_node(&self, id: &K) -> Option<&DependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    #[ensures(output.len() == self.nodes.len())]
    #[ensures(valid_topological_sort(&self.nodes, output))]
    #[ensures(matches_topological_sort(&self.nodes, &self.roots, output))]
    fn get_ordered_node_identifiers(&mut self, output: &mut Vec<K>) {
        output.clear();

        for root in &self.roots {
            topological_sort(&self.nodes, *root, &mut self.scratchpad, output);
        }
    }
    #[ensures(lacks_duplicates(output))]
    #[ensures(matches_topological_sort(&self.nodes, iter::once(id).filter(|id| self.nodes.contains_key(id)), output))]
    fn get_ordered_node_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();

        if self.nodes.contains_key(id) {
            topological_sort(&self.nodes, *id, &mut self.scratchpad, output);
        }
    }
    #[ensures(self.active == output.first().copied())]
    #[ensures(lacks_duplicates(output))]
    #[ensures(valid_path(&self.nodes, output))]
    fn get_active_thread(&mut self, output: &mut Vec<K>) {
        output.clear();

        if let Some(active) = self.active {
            path_to_root(&self.nodes, active, output);
        }
    }
    #[ensures(!self.nodes.contains_key(id) || output.first() == Some(id))]
    #[ensures(self.nodes.contains_key(id) || output.is_empty())]
    #[ensures(lacks_duplicates(output))]
    #[ensures(valid_path(&self.nodes, output))]
    fn get_thread_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();

        if self.nodes.contains_key(id) {
            path_to_root(&self.nodes, *id, output);
        }
    }
    #[ensures(!ret || old(self.nodes.len()) + 1 == self.nodes.len())]
    #[ensures(!ret || old(!self.nodes.contains_key(&node.id)))]
    #[ensures(!ret || self.nodes.contains_key(&old(node.id)))]
    #[ensures(!ret || old(node.active) == (self.active == Some(old(node.id))))]
    #[ensures(!ret || old(node.bookmarked) == self.bookmarked.contains(&old(node.id)))]
    #[ensures(ret || old(self.nodes.len()) == self.nodes.len())]
    #[ensures(ret || old(self.active) == self.active)]
    #[ensures(ret || old(self.bookmarked.clone()) == self.bookmarked)]
    #[invariant(self.validate())]
    fn add_node(&mut self, node: DependentNode<K, T, S>) -> bool {
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
    #[ensures(!ret || value == (self.active == Some(*id)))]
    #[ensures(ret || old(self.active) == self.active)]
    #[ensures(ret == self.nodes.contains_key(id))]
    #[invariant(self.validate())]
    fn set_node_active_status(&mut self, id: &K, value: bool) -> bool {
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
    #[ensures(!self.nodes.contains_key(id))]
    #[ensures(ret.is_none() || old(self.nodes.len()) > self.nodes.len())]
    #[ensures(ret.is_none() || old(self.bookmarked.len()) >= self.bookmarked.len())]
    #[ensures(ret.is_some() || old(self.nodes.len()) == self.nodes.len())]
    #[ensures(ret.is_some() || old(self.active) == self.active)]
    #[ensures(ret.is_some() || old(self.bookmarked.clone()) == self.bookmarked)]
    #[invariant(self.validate())]
    fn remove_node(&mut self, id: &K) -> Option<DependentNode<K, T, S>> {
        self.remove_node_unverified(id)
    }
    #[ensures(!self.nodes.contains_key(id))]
    #[ensures(!ret || old(self.nodes.len()) > self.nodes.len())]
    #[ensures(!ret || old(self.bookmarked.len()) >= self.bookmarked.len())]
    #[ensures(ret || old(self.nodes.len()) == self.nodes.len())]
    #[ensures(ret || old(self.active) == self.active)]
    #[ensures(ret || old(self.bookmarked.clone()) == self.bookmarked)]
    #[invariant(self.validate())]
    fn remove_node_tracked(
        &mut self,
        id: &K,
        mut on_removal: impl FnMut(DependentNode<K, T, S>),
    ) -> bool {
        self.remove_node_unverified_tracked(id, &mut on_removal)
    }
    #[ensures(self.nodes.is_empty())]
    #[invariant(self.validate())]
    fn remove_all_nodes(&mut self) {
        self.nodes.clear();
        self.roots.clear();
        self.active = None;
        self.bookmarked.clear();
    }
}

impl<K, T, M, S> ValidatableWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    fn validate(&self) -> bool {
        let nodes: IndexSet<_, _> = self.nodes.keys().copied().collect();

        self.scratchpad.is_empty()
            && self.roots.is_subset::<S>(&nodes)
            && self
                .active
                .is_none_or(|active| self.nodes.contains_key(&active))
            && self.bookmarked.is_subset(&nodes)
            && self.nodes.iter().all(|(key, value)| {
                value.validate()
                    && value.id == *key
                    && value.from.is_none_or(|from| self.nodes.contains_key(&from))
                    && value.to.is_subset(&nodes)
                    && value.from.is_none() == self.roots.contains(key)
                    && value.active == (self.active == Some(*key))
                    && value.bookmarked == self.bookmarked.contains(key)
                    && value
                        .from
                        .iter()
                        .all(|v| self.nodes.get(v).is_some_and(|p| p.to.contains(key)))
                    && value
                        .to
                        .iter()
                        .all(|v| self.nodes.get(v).is_some_and(|p| p.from == Some(*key)))
            })
    }
}

impl<K, T, M, S> MetadataWeave<K, DependentNode<K, T, S>, T, M> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
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
    K: Hash + Copy + Eq,
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
    #[ensures(!ret || value == self.bookmarked.contains(id))]
    #[ensures(ret || old(self.bookmarked.clone()) == self.bookmarked)]
    #[ensures(ret == self.nodes.contains_key(id))]
    #[invariant(self.validate())]
    fn set_node_bookmarked_status(&mut self, id: &K, value: bool) -> bool {
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
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    #[ensures(output.len() == self.nodes.len())]
    #[ensures(valid_topological_sort(&self.nodes, output))]
    #[ensures(matches_topological_sort_rev(&self.nodes, &self.roots, output))]
    fn get_ordered_node_identifiers_reversed_children(&mut self, output: &mut Vec<K>) {
        output.clear();

        for root in &self.roots {
            topological_sort_rev(&self.nodes, *root, &mut self.scratchpad, output);
        }
    }
    #[ensures(lacks_duplicates(output))]
    #[ensures(matches_topological_sort_rev(&self.nodes, iter::once(id).filter(|id| self.nodes.contains_key(id)), output))]
    fn get_ordered_node_identifiers_from_reversed_children(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();

        if self.nodes.contains_key(id) {
            topological_sort_rev(&self.nodes, *id, &mut self.scratchpad, output);
        }
    }
    #[ensures(old(self.nodes.len()) == self.nodes.len())]
    #[ensures(ret == self.nodes.contains_key(id))]
    #[invariant(self.validate())]
    fn sort_node_children_by(
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
    #[ensures(old(self.nodes.len()) == self.nodes.len())]
    #[ensures(ret == self.nodes.contains_key(id))]
    #[invariant(self.validate())]
    fn sort_node_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool {
        if let Some(node) = self.nodes.get_mut(id) {
            node.to.sort_by(cmp);

            true
        } else {
            false
        }
    }
    #[ensures(old(self.nodes.len()) == self.nodes.len())]
    #[ensures(old(self.roots.len()) == self.roots.len())]
    #[invariant(self.validate())]
    fn sort_roots_by(
        &mut self,
        mut cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) {
        self.roots
            .sort_by(|a, b| cmp(&self.nodes[a], &self.nodes[b]));
    }
    #[ensures(old(self.nodes.len()) == self.nodes.len())]
    #[ensures(old(self.roots.len()) == self.roots.len())]
    #[invariant(self.validate())]
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.roots.sort_by(cmp);
    }
}

impl<K, T, M, S> SortableBookmarkableWeave<K, DependentNode<K, T, S>, T>
    for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    #[ensures(old(self.bookmarked.len()) == self.bookmarked.len())]
    #[invariant(self.validate())]
    fn sort_bookmarks_by(
        &mut self,
        mut cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) {
        self.bookmarked
            .sort_by(|a, b| cmp(&self.nodes[a], &self.nodes[b]));
    }
    #[ensures(old(self.bookmarked.len()) == self.bookmarked.len())]
    #[invariant(self.validate())]
    fn sort_bookmarks_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.bookmarked.sort_by(cmp);
    }
}

impl<K, T, M, S> ActiveSingularWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn active(&self) -> Option<K> {
        self.active
    }
}

impl<K, T, M, S> DiscreteWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    #[ensures(!ret || old(self.nodes.len()) + 1 == self.nodes.len())]
    #[ensures(!ret || self.nodes.contains_key(id))]
    #[ensures(!ret || self.nodes.contains_key(&new_id))]
    #[ensures(!ret || old(!self.nodes.contains_key(&new_id)))]
    #[ensures(ret || old(self.nodes.len()) == self.nodes.len())]
    #[ensures(ret || old(self.active) == self.active)]
    #[ensures(ret || old(self.bookmarked.clone()) == self.bookmarked)]
    #[invariant(self.validate())]
    fn split_node(&mut self, id: &K, at: usize, new_id: K) -> bool {
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
    #[ensures(ret.is_none() || old(self.nodes.len()) - 1 == self.nodes.len())]
    #[ensures(ret.is_none() || !self.nodes.contains_key(id))]
    #[ensures(ret.is_none() || old(self.nodes.contains_key(id)))]
    #[ensures(ret.is_none() || self.nodes.contains_key(&ret.unwrap()))]
    #[ensures(ret.is_none() || ret == old(self.nodes.get(id).and_then(|node| node.from)))]
    #[ensures(ret.is_some() || old(self.nodes.len()) == self.nodes.len())]
    #[ensures(ret.is_some() || old(self.active) == self.active)]
    #[ensures(ret.is_some() || old(self.bookmarked.clone()) == self.bookmarked)]
    #[invariant(self.validate())]
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
    K: Hash + Copy + Eq,
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

impl<K, T, M, S> DeduplicatableWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: DeduplicatableContents,
    S: BuildHasher + Default + Clone,
{
    fn find_duplicates(&self, id: &K) -> impl Iterator<Item = K> {
        self.nodes.get(id).into_iter().flat_map(|node| {
            self.siblings(node).filter_map(|sibling| {
                if node.contents.is_duplicate_of(&sibling.contents) {
                    Some(sibling.id)
                } else {
                    None
                }
            })
        })
    }
}

#[cfg(feature = "rkyv")]
impl<K, K2, T, T2, S> Node<K::Archived, T::Archived> for ArchivedDependentNode<K, T, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2>,
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
    fn contents(&self) -> &<T as Archive>::Archived {
        &self.contents
    }
}

#[cfg(feature = "rkyv")]
impl<K, K2, T, T2, M, M2, S> ArchivedWeave<K::Archived, ArchivedDependentNode<K, T, S>, T::Archived>
    for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2>,
    M: Archive<Archived = M2>,
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
    fn get_node(&self, id: &K::Archived) -> Option<&ArchivedDependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    fn get_ordered_node_identifiers(&self, output: &mut Vec<K::Archived>) {
        output.clear();

        let mut scratchpad = Vec::with_capacity(self.len());
        let mut scratchpad_2 = Vec::with_capacity(self.len());

        for root in self.roots().iter() {
            archived_topological_sort(
                &self.nodes,
                *root,
                &mut scratchpad,
                &mut scratchpad_2,
                output,
            );
        }
    }
    fn get_ordered_node_identifiers_from(&self, id: &K::Archived, output: &mut Vec<K::Archived>) {
        output.clear();

        if self.nodes.contains_key(id) {
            let mut scratchpad = Vec::with_capacity(self.len());
            let mut scratchpad_2 = Vec::with_capacity(self.len());

            archived_topological_sort(&self.nodes, *id, &mut scratchpad, &mut scratchpad_2, output);
        }
    }
    fn get_active_thread(&self, output: &mut Vec<K::Archived>) {
        output.clear();

        if let ArchivedOption::Some(active) = self.active {
            archived_path_to_root(&self.nodes, active, output);
        }
    }
    fn get_thread_from(&self, id: &K::Archived, output: &mut Vec<K::Archived>) {
        output.clear();

        if self.nodes.contains_key(id) {
            archived_path_to_root(&self.nodes, *id, output);
        }
    }
}

#[cfg(feature = "rkyv")]
impl<K, K2, T, T2, M, M2, S>
    ArchivedMetadataWeave<K::Archived, ArchivedDependentNode<K, T, S>, T::Archived, M::Archived>
    for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2>,
    M: Archive<Archived = M2>,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn metadata(&self) -> &M::Archived {
        &self.metadata
    }
}

#[cfg(feature = "rkyv")]
impl<K, K2, T, T2, M, M2, S>
    ArchivedBookmarkableWeave<K::Archived, ArchivedDependentNode<K, T, S>, T::Archived>
    for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2>,
    M: Archive<Archived = M2>,
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
impl<K, K2, T, T2, M, M2, S>
    ArchivedSortableWeave<K::Archived, ArchivedDependentNode<K, T, S>, T::Archived>
    for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2>,
    M: Archive<Archived = M2>,
    S: BuildHasher + Default + Clone,
{
    fn get_ordered_node_identifiers_reversed_children(&self, output: &mut Vec<K::Archived>) {
        output.clear();
        let mut scratchpad = Vec::with_capacity(self.len());

        for root in self.roots().iter() {
            archived_topological_sort_rev(&self.nodes, *root, &mut scratchpad, output);
        }
    }
    fn get_ordered_node_identifiers_from_reversed_children(
        &mut self,
        id: &K::Archived,
        output: &mut Vec<K::Archived>,
    ) {
        output.clear();

        if self.nodes.contains_key(id) {
            let mut scratchpad = Vec::with_capacity(self.len());

            archived_topological_sort_rev(&self.nodes, *id, &mut scratchpad, output);
        }
    }
}

#[cfg(feature = "rkyv")]
impl<K, K2, T, T2, M, M2, S>
    ArchivedActiveSingularWeave<K::Archived, ArchivedDependentNode<K, T, S>, T::Archived>
    for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2>,
    M: Archive<Archived = M2>,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn active(&self) -> ArchivedOption<K::Archived> {
        self.active
    }
}

fn path_to_root<K, T, S>(
    nodes: &HashMap<K, DependentNode<K, T, S>, S>,
    mut id: K,
    thread: &mut Vec<K>,
) where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    thread.push(id);

    while let Some(parent) = nodes[&id].from {
        thread.push(parent);
        id = parent;
    }
}

fn topological_sort<K, N, T, S>(
    nodes: &HashMap<K, N, S>,
    id: K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut Vec<K>,
) where
    K: Hash + Copy + Eq,
    N: Node<K, T, From = Option<K>, To = IndexSet<K, S>>,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push(id);

    while let Some(id) = scratchpad.pop() {
        identifiers.push(id);
        scratchpad.extend(nodes[&id].to().into_iter().rev().copied());
    }
}

fn topological_sort_rev<K, N, T, S>(
    nodes: &HashMap<K, N, S>,
    id: K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut Vec<K>,
) where
    K: Hash + Copy + Eq,
    N: Node<K, T, From = Option<K>, To = IndexSet<K, S>>,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push(id);

    while let Some(id) = scratchpad.pop() {
        identifiers.push(id);
        scratchpad.extend(nodes[&id].to().into_iter().copied());
    }
}

#[cfg(feature = "rkyv")]
fn archived_path_to_root<K, K2, T, T2, S>(
    nodes: &ArchivedHashMap<K::Archived, ArchivedDependentNode<K, T, S>>,
    mut id: K::Archived,
    thread: &mut Vec<K::Archived>,
) where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq,
    T: Archive<Archived = T2>,
    S: BuildHasher + Default + Clone,
{
    thread.push(id);

    while let ArchivedOption::Some(parent) = nodes[&id].from {
        thread.push(parent);
        id = parent;
    }
}

#[cfg(feature = "rkyv")]
fn archived_topological_sort<K, N, T>(
    nodes: &ArchivedHashMap<K, N>,
    id: K,
    scratchpad: &mut Vec<K>,
    scratchpad_2: &mut Vec<K>,
    identifiers: &mut Vec<K>,
) where
    K: Hash + Copy + Eq,
    N: Node<K, T, From = ArchivedOption<K>, To = ArchivedIndexSet<K>>,
{
    scratchpad.push(id);

    while let Some(id) = scratchpad.pop() {
        identifiers.push(id);
        scratchpad_2.extend(nodes[&id].to().iter().copied());
        scratchpad_2.reverse();
        scratchpad.append(scratchpad_2);
    }
}

#[cfg(feature = "rkyv")]
fn archived_topological_sort_rev<K, N, T>(
    nodes: &ArchivedHashMap<K, N>,
    id: K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut Vec<K>,
) where
    K: Hash + Copy + Eq,
    N: Node<K, T, From = ArchivedOption<K>, To = ArchivedIndexSet<K>>,
{
    scratchpad.push(id);

    while let Some(id) = scratchpad.pop() {
        identifiers.push(id);
        scratchpad.extend(nodes[&id].to().iter().copied());
    }
}
