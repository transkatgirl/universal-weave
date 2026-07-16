//! [`DependentWeave`] is a tree-based [`Weave`] where each [`Node`] depends on the contents of the previous Node.

use std::{
    cmp::Ordering,
    collections::HashMap,
    hash::{BuildHasher, Hash},
    iter,
};

#[allow(unused_imports)] // false positive warning
use ::contracts::{ensures, invariant};
use indexmap::IndexSet;
use stacksafe::stacksafe;

#[cfg(feature = "rkyv")]
use rkyv::{
    Archive, Deserialize, Serialize,
    collections::swiss_table::{ArchivedHashMap, ArchivedIndexSet},
    option::ArchivedOption,
};

#[cfg(feature = "wincode")]
use wincode::{SchemaRead, SchemaWrite};

#[cfg(feature = "serde")]
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

#[cfg(feature = "rkyv")]
use crate::{
    ArchivedActiveSingularWeave, ArchivedIntegratedNode, ArchivedMetadataWeave, ArchivedNode,
    ArchivedSortableWeave, ArchivedWeave,
};

use crate::{
    ActiveSingularWeave, DeduplicatableContents, DeduplicatableWeave, DiscreteContentResult,
    DiscreteContents, DiscreteWeave, IndependentContents, IntegratedNode, MetadataWeave, Node,
    SemiIndependentWeave, SortableWeave, Weave,
    contract::{
        lacks_duplicates, matches_add_node_identifiers, matches_add_node_identifiers_rev,
        valid_ordered_nodes, valid_thread,
    },
};

mod contracts;

#[cfg(feature = "loro")]
pub mod loro;

#[cfg(feature = "legacy")]
pub mod legacy_dependent;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
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

impl<K, T, S> PartialEq for DependentNode<K, T, S>
where
    K: Hash + Copy + Eq,
    T: PartialEq,
    S: BuildHasher + Default + Clone,
{
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
            && self.from.eq(&other.from)
            && self.to.eq(&other.to)
            && self.active.eq(&other.active)
            && self.bookmarked.eq(&other.bookmarked)
            && self.contents.eq(&other.contents)
    }
}

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
    fn validate(&self) -> bool {
        (if let Some(from) = self.from {
            !self.to.contains(&from)
        } else {
            true
        } && self.from != Some(self.id)
            && !self.to.contains(&self.id))
    }
}

impl<K, T, S> Node<K, T> for DependentNode<K, T, S>
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    type From = Option<K>;
    type To = IndexSet<K, S>;

    fn id(&self) -> K {
        self.id
    }
    fn from(&self) -> &Self::From {
        &self.from
    }
    fn to(&self) -> &Self::To {
        &self.to
    }
    fn contents(&self) -> &T {
        &self.contents
    }
}

impl<K, T, S> IntegratedNode<K, T> for DependentNode<K, T, S>
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    fn is_active(&self) -> bool {
        self.active
    }
    fn is_bookmarked(&self) -> bool {
        self.bookmarked
    }
}

/// A tree-based [`Weave`] where each [`Node`] depends on the contents of the previous Node.
///
/// In order to reduce the serialized size, this weave implementation cannot contain more than [`i32::MAX`] nodes.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
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
            metadata,
        }
    }
    pub fn capacity(&self) -> usize {
        self.nodes.capacity()
    }
    pub fn reserve(&mut self, additional: usize) {
        self.nodes.reserve(additional);
        self.roots
            .reserve(self.nodes.capacity().saturating_sub(self.roots.len()));
        self.bookmarked
            .reserve(self.nodes.capacity().saturating_sub(self.bookmarked.len()));
    }
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.nodes.shrink_to(min_capacity);
        self.roots.shrink_to(min_capacity);
        self.bookmarked.shrink_to(min_capacity);
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
            for child in node.to.iter() {
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
            for child in node.to.iter() {
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
    type Bookmarks = IndexSet<K, S>;

    fn len(&self) -> usize {
        self.nodes.len()
    }
    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    fn nodes(&self) -> &Self::Nodes {
        &self.nodes
    }
    fn roots(&self) -> &Self::Roots {
        &self.roots
    }
    fn bookmarks(&self) -> &Self::Bookmarks {
        &self.bookmarked
    }
    fn contains(&self, id: &K) -> bool {
        self.nodes.contains_key(id)
    }
    fn contains_active(&self, id: &K) -> bool {
        self.active == Some(*id)
    }
    fn contains_bookmark(&self, id: &K) -> bool {
        self.bookmarked.contains(id)
    }
    fn get_node(&self, id: &K) -> Option<&DependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    #[ensures(output.len() == self.nodes.len())]
    #[ensures(valid_ordered_nodes(&self.nodes, output))]
    #[ensures(matches_add_node_identifiers(&self.nodes, &self.roots, output))]
    fn get_ordered_node_identifiers(&mut self, output: &mut Vec<K>) {
        output.clear();

        for root in &self.roots {
            add_node_identifiers(&self.nodes, *root, output);
        }
    }
    #[ensures(lacks_duplicates(output))]
    #[ensures(matches_add_node_identifiers(&self.nodes, iter::once(id).filter(|id| self.nodes.contains_key(id)), output))]
    fn get_ordered_node_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();
        add_node_identifiers(&self.nodes, *id, output);
    }
    #[ensures(output.is_empty() == self.active.is_none())]
    #[ensures(lacks_duplicates(output))]
    #[ensures(valid_thread(&self.nodes, output))]
    fn get_active_thread(&mut self, output: &mut Vec<K>) {
        output.clear();

        if let Some(active) = self.active {
            build_thread(&self.nodes, active, output);
        }
    }
    #[ensures(lacks_duplicates(output))]
    #[ensures(valid_thread(&self.nodes, output))]
    fn get_thread_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();

        build_thread(&self.nodes, *id, output);
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
        if self.nodes.contains_key(&node.id)
            || !node.validate()
            || !node.to.is_empty()
            || !self.under_max_size()
        {
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
    fn set_node_active_status(&mut self, id: &K, value: bool, _alternate: bool) -> bool {
        self.set_node_active_status_in_place(id, value)
    }
    #[ensures(!ret || value == (self.active == Some(*id)))]
    #[ensures(ret || old(self.active) == self.active)]
    #[ensures(ret == self.nodes.contains_key(id))]
    #[invariant(self.validate())]
    fn set_node_active_status_in_place(&mut self, id: &K, value: bool) -> bool {
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

impl<K, T, M, S> MetadataWeave<K, DependentNode<K, T, S>, T, M> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    fn metadata(&self) -> &M {
        &self.metadata
    }
    fn metadata_mut<O>(&mut self, callback: impl FnOnce(&mut M) -> O) -> O {
        callback(&mut self.metadata)
    }
}

impl<K, T, M, S> SortableWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    #[ensures(output.len() == self.nodes.len())]
    #[ensures(valid_ordered_nodes(&self.nodes, output))]
    #[ensures(matches_add_node_identifiers_rev(&self.nodes, &self.roots, output))]
    fn get_ordered_node_identifiers_reversed_children(&mut self, output: &mut Vec<K>) {
        output.clear();

        for root in &self.roots {
            add_node_identifiers_rev::<K, DependentNode<K, T, S>, T, S>(&self.nodes, *root, output); // Compiler limitation
        }
    }
    #[ensures(lacks_duplicates(output))]
    #[ensures(matches_add_node_identifiers_rev(&self.nodes, iter::once(id).filter(|id| self.nodes.contains_key(id)), output))]
    fn get_ordered_node_identifiers_from_reversed_children(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();
        add_node_identifiers_rev::<K, DependentNode<K, T, S>, T, S>(&self.nodes, *id, output); // Compiler limitation
    }
    #[ensures(old(self.nodes.len()) == self.nodes.len())]
    #[ensures(ret == self.nodes.contains_key(id))]
    #[invariant(self.validate())]
    fn sort_node_children_by(
        &mut self,
        id: &K,
        mut compare: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) -> bool {
        if let Some(mut node) = self.nodes.remove(id) {
            node.to
                .sort_by(|a, b| compare(self.nodes.get(a).unwrap(), self.nodes.get(b).unwrap()));
            self.nodes.insert(node.id, node);

            true
        } else {
            false
        }
    }
    #[ensures(old(self.nodes.len()) == self.nodes.len())]
    #[ensures(ret == self.nodes.contains_key(id))]
    #[invariant(self.validate())]
    fn sort_node_children_by_id(
        &mut self,
        id: &K,
        compare: impl FnMut(&K, &K) -> Ordering,
    ) -> bool {
        if let Some(node) = self.nodes.get_mut(id) {
            node.to.sort_by(compare);

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
        mut compare: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) {
        self.roots
            .sort_by(|a, b| compare(self.nodes.get(a).unwrap(), self.nodes.get(b).unwrap()));
    }
    #[ensures(old(self.nodes.len()) == self.nodes.len())]
    #[ensures(old(self.roots.len()) == self.roots.len())]
    #[invariant(self.validate())]
    fn sort_roots_by_id(&mut self, compare: impl FnMut(&K, &K) -> Ordering) {
        self.roots.sort_by(compare);
    }
    #[ensures(old(self.bookmarked.len()) == self.bookmarked.len())]
    #[invariant(self.validate())]
    fn sort_bookmarks_by(
        &mut self,
        mut compare: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) {
        self.bookmarked
            .sort_by(|a, b| compare(self.nodes.get(a).unwrap(), self.nodes.get(b).unwrap()));
    }
    #[ensures(old(self.bookmarked.len()) == self.bookmarked.len())]
    #[invariant(self.validate())]
    fn sort_bookmarks_by_id(&mut self, compare: impl FnMut(&K, &K) -> Ordering) {
        self.bookmarked.sort_by(compare);
    }
}

impl<K, T, M, S> ActiveSingularWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
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
        if self.nodes.contains_key(&new_id) || *id == new_id || !self.under_max_size() {
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

                    for child in node.to.iter() {
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

                        for child in parent.to.iter() {
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
impl<K, K2, T, T2, S> ArchivedNode<K::Archived, T::Archived> for ArchivedDependentNode<K, T, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2>,
    S: BuildHasher + Default + Clone,
{
    type From = ArchivedOption<K::Archived>;
    type To = ArchivedIndexSet<K::Archived>;

    fn id(&self) -> K::Archived {
        self.id
    }
    fn from(&self) -> &Self::From {
        &self.from
    }
    fn to(&self) -> &Self::To {
        &self.to
    }
    fn contents(&self) -> &<T as Archive>::Archived {
        &self.contents
    }
}

#[cfg(feature = "rkyv")]
impl<K, K2, T, T2, S> ArchivedIntegratedNode<K::Archived, T::Archived>
    for ArchivedDependentNode<K, T, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2>,
    S: BuildHasher + Default + Clone,
{
    fn is_active(&self) -> bool {
        self.active
    }
    fn is_bookmarked(&self) -> bool {
        self.bookmarked
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
    type Bookmarks = ArchivedIndexSet<K::Archived>;

    fn len(&self) -> usize {
        self.nodes.len()
    }
    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    fn nodes(&self) -> &Self::Nodes {
        &self.nodes
    }
    fn roots(&self) -> &Self::Roots {
        &self.roots
    }
    fn bookmarks(&self) -> &Self::Bookmarks {
        &self.bookmarked
    }
    fn contains(&self, id: &K::Archived) -> bool {
        self.nodes.contains_key(id)
    }
    fn contains_active(&self, id: &K::Archived) -> bool {
        self.active == Some(*id)
    }
    fn contains_bookmark(&self, id: &K::Archived) -> bool {
        self.bookmarked.contains(id)
    }
    fn get_node(&self, id: &K::Archived) -> Option<&ArchivedDependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    fn get_ordered_node_identifiers(&self, output: &mut Vec<K::Archived>) {
        output.clear();

        for root in self.roots().iter() {
            add_archived_node_identifiers(&self.nodes, *root, output);
        }
    }
    fn get_ordered_node_identifiers_from(&self, id: &K::Archived, output: &mut Vec<K::Archived>) {
        output.clear();
        add_archived_node_identifiers(&self.nodes, *id, output);
    }
    fn get_active_thread(&self, output: &mut Vec<K::Archived>) {
        output.clear();

        if let ArchivedOption::Some(active) = self.active {
            build_thread_archived(&self.nodes, active, output);
        }
    }
    fn get_thread_from(&self, id: &K::Archived, output: &mut Vec<K::Archived>) {
        output.clear();

        build_thread_archived(&self.nodes, *id, output);
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
    fn metadata(&self) -> &M::Archived {
        &self.metadata
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

        for root in self.roots().iter() {
            add_archived_node_identifiers_rev(&self.nodes, *root, output);
        }
    }
    fn get_ordered_node_identifiers_from_reversed_children(
        &mut self,
        id: &K::Archived,
        output: &mut Vec<K::Archived>,
    ) {
        output.clear();
        add_archived_node_identifiers_rev(&self.nodes, *id, output);
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
    fn active(&self) -> ArchivedOption<K::Archived> {
        self.active
    }
}

#[stacksafe]
fn build_thread<K, T, S>(nodes: &HashMap<K, DependentNode<K, T, S>, S>, id: K, thread: &mut Vec<K>)
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(&id) {
        thread.push(id);
        if let Some(parent) = node.from {
            build_thread(nodes, parent, thread);
        }
    }
}

#[cfg(feature = "rkyv")]
#[stacksafe]
fn build_thread_archived<K, K2, T, T2, S>(
    nodes: &ArchivedHashMap<K::Archived, ArchivedDependentNode<K, T, S>>,
    id: K::Archived,
    thread: &mut Vec<K::Archived>,
) where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq,
    T: Archive<Archived = T2>,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(&id) {
        thread.push(id);
        if let ArchivedOption::Some(parent) = node.from {
            build_thread_archived(nodes, parent, thread);
        }
    }
}

#[stacksafe]
fn add_node_identifiers<K, N, T, S>(nodes: &HashMap<K, N, S>, id: K, identifiers: &mut Vec<K>)
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(&id) {
        identifiers.push(id);
        for child in node.to().into_iter() {
            add_node_identifiers(nodes, *child, identifiers);
        }
    }
}

#[stacksafe]
fn add_node_identifiers_rev<K, N, T, S>(nodes: &HashMap<K, N, S>, id: K, identifiers: &mut Vec<K>)
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(&id) {
        identifiers.push(id);
        for child in node.to().into_iter().rev() {
            add_node_identifiers_rev(nodes, *child, identifiers);
        }
    }
}

#[cfg(feature = "rkyv")]
#[stacksafe]
fn add_archived_node_identifiers<K, N, T>(
    nodes: &ArchivedHashMap<K, N>,
    id: K,
    identifiers: &mut Vec<K>,
) where
    K: Hash + Copy + Eq,
    N: ArchivedNode<K, T, To = ArchivedIndexSet<K>>,
{
    if let Some(node) = nodes.get(&id) {
        identifiers.push(id);
        for child in node.to().iter() {
            add_archived_node_identifiers(nodes, *child, identifiers);
        }
    }
}

#[cfg(feature = "rkyv")]
#[stacksafe]
fn add_archived_node_identifiers_rev<K, N, T>(
    nodes: &ArchivedHashMap<K, N>,
    id: K,
    identifiers: &mut Vec<K>,
) where
    K: Hash + Copy + Eq,
    N: ArchivedNode<K, T, To = ArchivedIndexSet<K>>,
{
    if let Some(node) = nodes.get(&id) {
        identifiers.push(id);
        for child in node.to().iter().collect::<Vec<_>>().into_iter().rev() {
            add_archived_node_identifiers_rev(nodes, *child, identifiers);
        }
    }
}
