//! [`IndependentWeave`] is a DAG-based [`Weave`] where each [`Node`] does *not* depend on the contents of the previous Node.

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    hash::{BuildHasher, Hash},
    mem,
};

use ::contracts::{ensures, invariant};
use indexmap::IndexSet;
use stacksafe::stacksafe;

#[cfg(feature = "rkyv")]
use rkyv::{
    Archive, Deserialize, Serialize,
    collections::swiss_table::{ArchivedHashMap, ArchivedHashSet, ArchivedIndexSet},
    with::Skip,
};

#[cfg(feature = "wincode")]
use wincode::{SchemaRead, SchemaWrite};

#[cfg(feature = "serde")]
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

#[cfg(feature = "rkyv")]
use crate::{
    ArchivedActivePathWeave, ArchivedIntegratedNode, ArchivedMetadataWeave, ArchivedNode,
    ArchivedSortableWeave, ArchivedWeave,
};

use crate::{
    ActivePathWeave, DeduplicatableContents, DeduplicatableWeave, DiscreteContentResult,
    DiscreteContents, DiscreteWeave, IndependentContents, IntegratedNode, MetadataWeave, Node,
    SortableWeave, Weave, add_node_identifiers, add_node_identifiers_rev,
    contract::{lacks_duplicates, valid_ordered_nodes, valid_thread},
    dependent::DependentWeave,
};

mod contracts;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub struct IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    /// The node's unique identifier.
    pub id: K,
    /// The identifiers corresponding to the node's parents.
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "IndexSet<K, S>: SerdeSerialize",
            deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    pub from: IndexSet<K, S>,
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
    /// Unlike [`DependentWeave`], [`IndependentWeave`] considers all nodes within an active thread to be active.
    pub active: bool,
    /// If the node is bookmarked.
    pub bookmarked: bool,
    /// The node's contents.
    pub contents: T,
}

impl<K, T, S> PartialEq for IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents + PartialEq,
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

impl<K, T, S> Eq for IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents + Eq,
    S: BuildHasher + Default + Clone,
{
}

impl<K, T, S> IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    fn validate(&self) -> bool {
        self.from.is_disjoint(&self.to)
            && !self.from.contains(&self.id)
            && !self.to.contains(&self.id)
    }
}

impl<K, T, S> Node<K, T> for IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type From = IndexSet<K, S>;
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

impl<K, T, S> IntegratedNode<K, T> for IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    fn is_active(&self) -> bool {
        self.active
    }
    fn is_bookmarked(&self) -> bool {
        self.bookmarked
    }
}

/// A DAG-based [`Weave`] where each [`Node`] does *not* depend on the contents of the previous Node.
///
/// However, this additional flexibility results in worse performance and memory usage characteristics overall.
///
/// In order to reduce the serialized size, this weave implementation cannot contain more than [`i32::MAX`] nodes.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub struct IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "HashMap<K, IndependentNode<K, T, S>, S>: SerdeSerialize",
            deserialize = "HashMap<K, IndependentNode<K, T, S>, S>: SerdeDeserialize<'de>"
        ))
    )]
    nodes: HashMap<K, IndependentNode<K, T, S>, S>,
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "IndexSet<K, S>: SerdeSerialize",
            deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    roots: IndexSet<K, S>,
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "HashSet<K, S>: SerdeSerialize",
            deserialize = "HashSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    active: HashSet<K, S>,
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
    scratchpad_list: Vec<K>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "wincode", wincode(skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad_set: HashSet<K, S>,

    pub metadata: M,
}

impl<K, T, M, S> IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pub fn with_capacity(capacity: usize, metadata: M) -> Self {
        Self {
            nodes: HashMap::with_capacity_and_hasher(capacity, S::default()),
            roots: IndexSet::with_capacity_and_hasher(capacity, S::default()),
            active: HashSet::with_capacity_and_hasher(capacity, S::default()),
            bookmarked: IndexSet::with_capacity_and_hasher(capacity, S::default()),
            scratchpad_list: Vec::with_capacity(capacity),
            scratchpad_set: HashSet::with_capacity_and_hasher(capacity, S::default()),
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
        self.active
            .reserve(self.nodes.capacity().saturating_sub(self.active.len()));
        self.bookmarked
            .reserve(self.nodes.capacity().saturating_sub(self.bookmarked.len()));
        self.scratchpad_list.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_list.len()),
        );
        self.scratchpad_set.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_set.len()),
        );
    }
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.nodes.shrink_to(min_capacity);
        self.roots.shrink_to(min_capacity);
        self.active.shrink_to(min_capacity);
        self.bookmarked.shrink_to(min_capacity);
        self.scratchpad_list.shrink_to(min_capacity);
        self.scratchpad_set.shrink_to(min_capacity);
    }
    fn all_parents(
        &self,
        node: &IndependentNode<K, T, S>,
    ) -> impl Iterator<Item = &IndependentNode<K, T, S>> {
        node.from.iter().filter_map(|id| self.nodes.get(id))
    }
    fn sibling_ids_from_all_parents_including_roots<'a>(
        &'a self,
        node: &'a IndependentNode<K, T, S>,
    ) -> Box<dyn Iterator<Item = K> + 'a> {
        if node.from.is_empty() {
            Box::new(self.roots.iter().copied().filter(|id| *id != node.id))
        } else {
            Box::new(
                IndexSet::<K, S>::from_iter(self.all_parents(node).flat_map(|parent| {
                    {
                        parent.to.iter().copied().filter(|id| {
                            *id != node.id && !node.from.contains(id) && !node.to.contains(id)
                        })
                    }
                }))
                .into_iter(),
            )
        }
    }
    fn update_node_activity_in_place(&mut self, id: &K, value: bool) -> bool {
        self.update_node_activity_in_place_inner(id, value, true)
    }
    #[stacksafe]
    fn update_node_activity_in_place_inner(&mut self, id: &K, value: bool, start: bool) -> bool {
        if let Some(node) = self.nodes.get(id) {
            if node.active == value {
                return true;
            }

            if start {
                self.scratchpad_list.clear();
            }

            if value {
                let has_active_parents = node
                    .from
                    .iter()
                    .copied()
                    .any(|parent| self.active.contains(&parent));
                let active_siblings: Vec<_> = self
                    .sibling_ids_from_all_parents_including_roots(node)
                    .filter(|sibling| self.active.contains(sibling))
                    .collect();

                if !has_active_parents && let Some(parent) = node.from.first().copied() {
                    self.update_node_activity_in_place_inner(&parent, true, false);
                }
                for sibling in active_siblings {
                    self.update_node_activity_in_place_inner(&sibling, false, false);
                }
            } else {
                self.scratchpad_list.extend(node.to.iter().copied());
            }
        }
        match self.nodes.get_mut(id) {
            Some(node) => {
                node.active = value;
                if value {
                    self.active.insert(node.id);
                } else {
                    self.active.remove(&node.id);
                }
            }
            None => return false,
        }
        if start {
            for item in self.scratchpad_list.drain(..) {
                update_removed_child_activity(&mut self.nodes, &mut self.active, &item);
            }
        }
        true
    }
    #[ensures(!self.nodes.contains_key(id))]
    #[stacksafe]
    fn remove_node_unverified(&mut self, id: &K) -> Option<IndependentNode<K, T, S>> {
        if let Some(node) = self.nodes.remove(id) {
            self.roots.shift_remove(id);
            self.bookmarked.shift_remove(id);
            self.active.remove(id);
            for parent in &node.from {
                if let Some(parent) = self.nodes.get_mut(parent) {
                    parent.to.shift_remove(&node.id);
                }
            }
            for child in &node.to {
                if let Some(child) = self.nodes.get_mut(child) {
                    child.from.shift_remove(&node.id);

                    let identifier = child.id;
                    if child.from.is_empty() {
                        self.remove_node_unverified(&identifier);
                    } else if node.active && child.active {
                        update_removed_child_activity(
                            &mut self.nodes,
                            &mut self.active,
                            &identifier,
                        );
                    }
                }
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
        callback: &mut impl FnMut(IndependentNode<K, T, S>),
    ) -> bool {
        if let Some(node) = self.nodes.remove(id) {
            self.roots.shift_remove(id);
            self.bookmarked.shift_remove(id);
            self.active.remove(id);
            for parent in &node.from {
                if let Some(parent) = self.nodes.get_mut(parent) {
                    parent.to.shift_remove(&node.id);
                }
            }
            for child in &node.to {
                if let Some(child) = self.nodes.get_mut(child) {
                    child.from.shift_remove(&node.id);

                    let identifier = child.id;
                    if child.from.is_empty() {
                        self.remove_node_unverified_tracked(&identifier, callback);
                    } else if node.active && child.active {
                        update_removed_child_activity(
                            &mut self.nodes,
                            &mut self.active,
                            &identifier,
                        );
                    }
                }
            }
            callback(node);
            true
        } else {
            false
        }
    }
}

#[stacksafe]
fn update_removed_child_activity<K, T, S>(
    nodes: &mut HashMap<K, IndependentNode<K, T, S>, S>,
    active: &mut HashSet<K, S>,
    id: &K,
) -> bool
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(id) {
        if !node.active {
            return true;
        }

        let has_active_parents = node.from.iter().any(|parent| active.contains(parent));

        if has_active_parents {
            return true;
        }
    }
    if let Some(node) = nodes.get_mut(id) {
        node.active = false;
        active.remove(&node.id);

        let children: Vec<_> = node.to.iter().copied().collect();
        for child in &children {
            update_removed_child_activity(nodes, active, child);
        }

        true
    } else {
        false
    }
}

impl<K, T, M, S> From<DependentWeave<K, T, M, S>> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents + Clone,
    M: Clone,
    S: BuildHasher + Default + Clone,
{
    fn from(mut value: DependentWeave<K, T, M, S>) -> Self {
        let mut identifiers = Vec::with_capacity(value.len());
        value.get_ordered_node_identifiers(&mut identifiers);

        let mut output = Self::with_capacity(value.capacity(), value.metadata);

        for identifier in identifiers {
            let node = value.nodes.remove(&identifier).unwrap();

            assert!(output.add_node(IndependentNode {
                id: node.id,
                from: IndexSet::from_iter(node.from.into_iter()),
                to: IndexSet::with_capacity_and_hasher(node.to.len(), S::default()),
                active: node.active,
                bookmarked: node.bookmarked,
                contents: node.contents,
            }));
        }

        output
    }
}

impl<K, T, M, S> Weave<K, IndependentNode<K, T, S>, T> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type Nodes = HashMap<K, IndependentNode<K, T, S>, S>;
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
        self.active.contains(id)
    }
    fn contains_bookmark(&self, id: &K) -> bool {
        self.bookmarked.contains(id)
    }
    fn get_node(&self, id: &K) -> Option<&IndependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    #[ensures(output.len() == self.nodes.len() && valid_ordered_nodes(&self.nodes, output))]
    fn get_ordered_node_identifiers(&mut self, output: &mut Vec<K>) {
        output.clear();
        self.scratchpad_set.clear();

        for root in &self.roots {
            add_node_identifiers::<K, IndependentNode<K, T, S>, T, S>(
                &self.nodes,
                root,
                output,
                &mut self.scratchpad_set,
            ); // Compiler limitation
        }
    }
    #[ensures(lacks_duplicates(output))]
    fn get_ordered_node_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();
        self.scratchpad_set.clear();

        if self.nodes.contains_key(id) {
            if let Some(node) = self.nodes.get(id) {
                for parent in &node.from {
                    self.scratchpad_set.insert(*parent);
                }
            }
            add_node_identifiers::<K, IndependentNode<K, T, S>, T, S>(
                &self.nodes,
                id,
                output,
                &mut self.scratchpad_set,
            ); // Compiler limitation
        }
    }
    #[ensures(output.len() == self.active.len() && lacks_duplicates(output) && valid_thread(&self.nodes, output) && output.iter().all(|item| self.active.contains(item)))]
    fn get_active_thread(&mut self, output: &mut Vec<K>) {
        output.clear();
        self.scratchpad_list.clear();
        self.scratchpad_set.clear();

        for active_root in self
            .roots
            .iter()
            .copied()
            .filter(|root| self.active.contains(root))
        {
            build_thread(
                &self.nodes,
                &self.active,
                active_root,
                &mut self.scratchpad_list,
                &mut self.scratchpad_set,
                output,
            );
        }

        output.reverse();
    }
    #[ensures(lacks_duplicates(output) && valid_thread(&self.nodes, output))]
    fn get_thread_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();
        self.scratchpad_set.clear();

        build_thread_from(
            &self.nodes,
            &self.active,
            *id,
            output,
            &mut self.scratchpad_set,
        );

        if let Some(last_thread_node) = output.last()
            && !self.roots.contains(last_thread_node)
        {
            self.scratchpad_set.clear();
            self.scratchpad_list.clear();

            for active_root in self
                .roots
                .iter()
                .copied()
                .filter(|root| self.active.contains(root))
            {
                if build_thread_until(
                    &self.nodes,
                    &self.active,
                    active_root,
                    &HashSet::from_iter(
                        self.nodes
                            .get(last_thread_node)
                            .unwrap()
                            .from
                            .iter()
                            .copied()
                            .filter(|parent| self.active.contains(parent)),
                    ),
                    &mut self.scratchpad_list,
                    &mut self.scratchpad_set,
                ) {
                    break;
                }
            }

            output.extend(self.scratchpad_list.drain(..).rev());
        }
    }
    #[ensures((ret && (old(self.nodes.len()) + 1 == self.nodes.len())) || (!ret && (old(self.nodes.len()) == self.nodes.len())))]
    #[invariant(self.validate())]
    fn add_node(&mut self, mut node: IndependentNode<K, T, S>) -> bool {
        if self.nodes.contains_key(&node.id)
            || !node.validate()
            || !node.from.iter().all(|id| self.nodes.contains_key(id))
            || !node.to.iter().all(|id| self.nodes.contains_key(id))
            || !self.under_max_size()
        {
            return false;
        }

        for child in &node.to {
            let child = self.nodes.get(child).unwrap();
            if child.from.is_empty() {
                if child.active {
                    node.active = true;
                }
                self.roots.shift_remove(&child.id);
            }
        }

        if node.from.is_empty() {
            self.roots.insert(node.id);
        } else {
            for parent in &node.from {
                let parent = self.nodes.get_mut(parent).unwrap();
                parent.to.insert(node.id);
            }
        }

        for child in &node.to {
            let child = self.nodes.get_mut(child).unwrap();
            child.from.insert(node.id);
        }

        if node.bookmarked {
            self.bookmarked.insert(node.id);
        }

        let id = node.id;
        let active = node.active;
        node.active = false;

        self.nodes.insert(node.id, node);

        if active {
            self.update_node_activity_in_place(&id, true);
        }

        true
    }
    #[ensures((ret && value == self.active.contains(id)) || !ret)]
    #[invariant(self.validate())]
    fn set_node_active_status(&mut self, id: &K, value: bool, alternate: bool) -> bool {
        if value
            && let Some(node) = self.nodes.get(id)
            && let Some(active_child) = node
                .to
                .iter()
                .filter_map(|child| self.nodes.get(child))
                .find(|child| child.active)
        {
            let child_id = active_child.id;

            if (!alternate && active_child.from.len() == 1)
                || (alternate && active_child.from.len() > 1)
            {
                let result = self.update_node_activity_in_place(id, true);
                self.update_node_activity_in_place(&child_id, false);

                result
            } else {
                self.update_node_activity_in_place(id, value)
            }
        } else {
            self.update_node_activity_in_place(id, value)
        }
    }
    #[ensures((ret && value == self.active.contains(id)) || !ret)]
    #[invariant(self.validate())]
    fn set_node_active_status_in_place(&mut self, id: &K, value: bool) -> bool {
        self.update_node_activity_in_place(id, value)
    }
    #[ensures((ret && value == self.bookmarked.contains(id)) || !ret)]
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
    #[invariant(self.validate())]
    fn remove_node(&mut self, id: &K) -> Option<IndependentNode<K, T, S>> {
        self.remove_node_unverified(id)
    }
    #[ensures(!self.nodes.contains_key(id))]
    #[invariant(self.validate())]
    fn remove_node_tracked(
        &mut self,
        id: &K,
        mut on_removal: impl FnMut(IndependentNode<K, T, S>),
    ) -> bool {
        self.remove_node_unverified_tracked(id, &mut on_removal)
    }
    #[ensures(self.nodes.is_empty())]
    #[invariant(self.validate())]
    fn remove_all_nodes(&mut self) {
        self.nodes.clear();
        self.roots.clear();
        self.active.clear();
        self.bookmarked.clear();
    }
}

impl<K, T, M, S> MetadataWeave<K, IndependentNode<K, T, S>, T, M> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    fn metadata(&self) -> &M {
        &self.metadata
    }
    fn metadata_mut<O>(&mut self, callback: impl FnOnce(&mut M) -> O) -> O {
        callback(&mut self.metadata)
    }
}

impl<K, T, M, S> SortableWeave<K, IndependentNode<K, T, S>, T> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[ensures(output.len() == self.nodes.len() && valid_ordered_nodes(&self.nodes, output))]
    fn get_ordered_node_identifiers_reversed_children(&mut self, output: &mut Vec<K>) {
        output.clear();
        self.scratchpad_set.clear();

        for root in &self.roots {
            add_node_identifiers_rev::<K, IndependentNode<K, T, S>, T, S>(
                &self.nodes,
                root,
                output,
                &mut self.scratchpad_set,
            ); // Compiler limitation
        }
    }
    #[ensures(lacks_duplicates(output))]
    fn get_ordered_node_identifiers_from_reversed_children(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();
        self.scratchpad_set.clear();

        if self.nodes.contains_key(id) {
            if let Some(node) = self.nodes.get(id) {
                for parent in &node.from {
                    self.scratchpad_set.insert(*parent);
                }
            }
            add_node_identifiers_rev::<K, IndependentNode<K, T, S>, T, S>(
                &self.nodes,
                id,
                output,
                &mut self.scratchpad_set,
            ); // Compiler limitation
        }
    }
    #[ensures(old(self.nodes.len()) == self.nodes.len())]
    #[invariant(self.validate())]
    fn sort_node_children_by(
        &mut self,
        id: &K,
        mut compare: impl FnMut(&IndependentNode<K, T, S>, &IndependentNode<K, T, S>) -> Ordering,
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
    #[ensures(old(self.nodes.len()) == self.nodes.len() && old(self.roots.len()) == self.roots.len())]
    #[invariant(self.validate())]
    fn sort_roots_by(
        &mut self,
        mut compare: impl FnMut(&IndependentNode<K, T, S>, &IndependentNode<K, T, S>) -> Ordering,
    ) {
        self.roots
            .sort_by(|a, b| compare(self.nodes.get(a).unwrap(), self.nodes.get(b).unwrap()));
    }
    #[ensures(old(self.nodes.len()) == self.nodes.len() && old(self.roots.len()) == self.roots.len())]
    #[invariant(self.validate())]
    fn sort_roots_by_id(&mut self, compare: impl FnMut(&K, &K) -> Ordering) {
        self.roots.sort_by(compare);
    }
    #[ensures(old(self.bookmarked.len()) == self.bookmarked.len())]
    #[invariant(self.validate())]
    fn sort_bookmarks_by(
        &mut self,
        mut compare: impl FnMut(&IndependentNode<K, T, S>, &IndependentNode<K, T, S>) -> Ordering,
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

impl<K, T, M, S> ActivePathWeave<K, IndependentNode<K, T, S>, T> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type Active = HashSet<K, S>;

    fn active(&self) -> &Self::Active {
        &self.active
    }
}

impl<K, T, M, S> DiscreteWeave<K, IndependentNode<K, T, S>, T> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    #[invariant(self.validate())]
    fn split_node(&mut self, id: &K, at: usize, new_id: K) -> bool {
        if self.nodes.contains_key(&new_id) || *id == new_id || !self.under_max_size() {
            return false;
        }

        if let Some(mut node) = self.nodes.remove(id) {
            match node.contents.split(at) {
                DiscreteContentResult::Two(left, right) => {
                    let left_node = IndependentNode {
                        id: node.id,
                        from: node.from,
                        to: IndexSet::from_iter([new_id]),
                        active: node.active,
                        bookmarked: node.bookmarked,
                        contents: left,
                    };

                    node.from = IndexSet::from_iter([node.id]);
                    node.id = new_id;
                    node.contents = right;
                    node.active = false;
                    node.bookmarked = false;

                    for child in node.to.iter() {
                        let child = self.nodes.get_mut(child).unwrap();

                        if let Some(index) = child.from.get_index_of(&left_node.id) {
                            if child.from.replace_index(index, node.id).is_err() {
                                child.from.shift_remove_index(index);
                            }
                        } else {
                            child.from.insert(node.id);
                        }
                        if child.active && left_node.active {
                            node.active = true;
                            self.active.insert(node.id);
                        }
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
    #[invariant(self.validate())]
    fn merge_with_parent(&mut self, id: &K) -> Option<K> {
        if let Some(mut node) = self.nodes.remove(id) {
            if node.from.len() != 1 {
                self.nodes.insert(node.id, node);
                return None;
            }

            if let Some(mut parent) = node.from.first().and_then(|id| self.nodes.remove(id)) {
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

                            if let Some(index) = child.from.get_index_of(&node.id) {
                                if child.from.replace_index(index, parent.id).is_err() {
                                    child.from.shift_remove_index(index);
                                }
                            } else {
                                child.from.insert(parent.id);
                            }
                        }

                        let parent_id = parent.id;

                        self.nodes.insert(parent.id, parent);

                        self.bookmarked.shift_remove(&node.id);
                        self.active.remove(&node.id);

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

impl<K, T, M, S> crate::SemiIndependentWeave<K, IndependentNode<K, T, S>, T>
    for IndependentWeave<K, T, M, S>
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

impl<K, T, M, S> DeduplicatableWeave<K, IndependentNode<K, T, S>, T>
    for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents + DeduplicatableContents,
    S: BuildHasher + Default + Clone,
{
    fn find_duplicates(&self, id: &K) -> impl Iterator<Item = K> {
        self.nodes.get(id).into_iter().flat_map(|node| {
            self.sibling_ids_from_all_parents_including_roots(node)
                .filter_map(|id| self.nodes.get(&id))
                .filter_map(|sibling| {
                    if node.contents.is_duplicate_of(&sibling.contents) {
                        Some(sibling.id)
                    } else {
                        None
                    }
                })
        })
    }
}

impl<K, T, M, S> crate::IndependentWeave<K, IndependentNode<K, T, S>, T>
    for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[invariant(self.validate())]
    fn move_node(&mut self, id: &K, new_parents: &[K]) -> bool {
        if new_parents
            .iter()
            .any(|new_parent| !self.nodes.contains_key(new_parent))
        {
            return false;
        }

        let new_parents = IndexSet::from_iter(new_parents.iter().copied());

        if new_parents.contains(id) {
            return false;
        }

        let old_parents_ex_new = if let Some(node) = self.nodes.get_mut(id) {
            for child in &node.to {
                if new_parents.contains(child) {
                    return false;
                }
            }

            let mut old_parents = mem::take(&mut node.from);

            for old_parent in &old_parents {
                if !new_parents.contains(old_parent)
                    && let Some(old_parent) = self.nodes.get_mut(old_parent)
                {
                    old_parent.to.shift_remove(id);
                }
            }

            for new_parent in &new_parents {
                if !old_parents.contains(new_parent)
                    && let Some(new_parent) = self.nodes.get_mut(new_parent)
                {
                    new_parent.to.insert(*id);
                } else {
                    old_parents.swap_remove(new_parent);
                }
            }

            old_parents
        } else {
            return false;
        };

        let node = self.nodes.get_mut(id).unwrap();
        node.from = new_parents;

        if node.from.is_empty() {
            self.roots.insert(node.id);
        } else {
            self.roots.shift_remove(&node.id);
        }

        if node.active {
            node.active = false;
            assert!(self.update_node_activity_in_place(id, true));

            for old_parent in old_parents_ex_new {
                update_removed_child_activity(&mut self.nodes, &mut self.active, &old_parent);
            }
        }

        true
    }
}

#[cfg(feature = "rkyv")]
impl<K, K2, T, T2, S> ArchivedNode<K::Archived, T::Archived> for ArchivedIndependentNode<K, T, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2> + IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type From = ArchivedIndexSet<K::Archived>;
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
    fn contents(&self) -> &T::Archived {
        &self.contents
    }
}

#[cfg(feature = "rkyv")]
impl<K, K2, T, T2, S> ArchivedIntegratedNode<K::Archived, T::Archived>
    for ArchivedIndependentNode<K, T, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2> + IndependentContents,
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
impl<K, K2, T, T2, M, M2, S>
    ArchivedWeave<K::Archived, ArchivedIndependentNode<K, T, S>, T::Archived>
    for ArchivedIndependentWeave<K, T, M, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2> + IndependentContents,
    M: Archive<Archived = M2>,
    S: BuildHasher + Default + Clone,
{
    type Nodes = ArchivedHashMap<K::Archived, ArchivedIndependentNode<K, T, S>>;
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
        self.active.contains(id)
    }
    fn contains_bookmark(&self, id: &K::Archived) -> bool {
        self.bookmarked.contains(id)
    }
    fn get_node(&self, id: &K::Archived) -> Option<&ArchivedIndependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    fn get_ordered_node_identifiers(&self, output: &mut Vec<K::Archived>) {
        output.clear();
        let mut identifier_set = HashSet::with_capacity(self.len());

        for root in self.roots().iter() {
            add_archived_node_identifiers(&self.nodes, *root, output, &mut identifier_set);
        }
    }
    fn get_ordered_node_identifiers_from(&self, id: &K::Archived, output: &mut Vec<K::Archived>) {
        output.clear();
        let mut identifier_set = HashSet::with_capacity(self.len());

        if self.nodes.contains_key(id) {
            add_archived_node_identifiers(&self.nodes, *id, output, &mut identifier_set);
        }
    }
    fn get_active_thread(&self, output: &mut Vec<K::Archived>) {
        output.clear();
        let mut thread_list = Vec::with_capacity(self.len());
        let mut thread_set = HashSet::with_capacity(self.len());

        for active_root in self
            .roots
            .iter()
            .copied()
            .filter(|root| self.active.contains(root))
        {
            build_thread_archived(
                &self.nodes,
                &self.active,
                active_root,
                &mut thread_list,
                &mut thread_set,
                output,
            );
        }

        output.reverse();
    }
    fn get_thread_from(&self, id: &K::Archived, output: &mut Vec<K::Archived>) {
        output.clear();
        let mut thread_set = HashSet::with_capacity(self.len());

        build_thread_from_archived(&self.nodes, &self.active, *id, output, &mut thread_set);

        if let Some(last_thread_node) = output.last()
            && !self.roots.contains(last_thread_node)
        {
            thread_set.clear();

            let mut alternate_thread_list = Vec::with_capacity(self.len() - output.len());

            for active_root in self
                .roots
                .iter()
                .copied()
                .filter(|root| self.active.contains(root))
            {
                if build_thread_archived_until(
                    &self.nodes,
                    &self.active,
                    active_root,
                    &self
                        .nodes
                        .get(last_thread_node)
                        .unwrap()
                        .from
                        .iter()
                        .copied()
                        .filter(|parent| self.active.contains(parent))
                        .collect::<HashSet<K2>>(),
                    &mut alternate_thread_list,
                    &mut thread_set,
                ) {
                    break;
                }
            }

            output.extend(alternate_thread_list.into_iter().rev());
        }
    }
}

#[cfg(feature = "rkyv")]
impl<K, K2, T, T2, M, M2, S>
    ArchivedMetadataWeave<K::Archived, ArchivedIndependentNode<K, T, S>, T::Archived, M::Archived>
    for ArchivedIndependentWeave<K, T, M, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2> + IndependentContents,
    M: Archive<Archived = M2>,
    S: BuildHasher + Default + Clone,
{
    fn metadata(&self) -> &M::Archived {
        &self.metadata
    }
}

#[cfg(feature = "rkyv")]
impl<K, K2, T, T2, M, M2, S>
    ArchivedSortableWeave<K::Archived, ArchivedIndependentNode<K, T, S>, T::Archived>
    for ArchivedIndependentWeave<K, T, M, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2> + IndependentContents,
    M: Archive<Archived = M2>,
    S: BuildHasher + Default + Clone,
{
    fn get_ordered_node_identifiers_reversed_children(&self, output: &mut Vec<K::Archived>) {
        output.clear();
        let mut identifier_set = HashSet::with_capacity(self.len());

        for root in self.roots().iter() {
            add_archived_node_identifiers_rev(&self.nodes, *root, output, &mut identifier_set);
        }
    }
    fn get_ordered_node_identifiers_from_reversed_children(
        &mut self,
        id: &K::Archived,
        output: &mut Vec<K::Archived>,
    ) {
        output.clear();
        let mut identifier_set = HashSet::with_capacity(self.len());

        if self.nodes.contains_key(id) {
            add_archived_node_identifiers_rev(&self.nodes, *id, output, &mut identifier_set);
        }
    }
}

#[cfg(feature = "rkyv")]
impl<K, K2, T, T2, M, M2, S>
    ArchivedActivePathWeave<K::Archived, ArchivedIndependentNode<K, T, S>, T::Archived>
    for ArchivedIndependentWeave<K, T, M, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2> + IndependentContents,
    M: Archive<Archived = M2>,
    S: BuildHasher + Default + Clone,
{
    type Active = ArchivedHashSet<K::Archived>;

    fn active(&self) -> &Self::Active {
        &self.active
    }
}

#[stacksafe]
fn build_thread<K, T, S>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    active: &HashSet<K, S>,
    id: K,
    scratchpad_list: &mut Vec<K>,
    thread_set: &mut HashSet<K, S>,
    thread_list: &mut Vec<K>,
) where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(&id)
        && node
            .from
            .iter()
            .filter(|parent| active.contains(*parent))
            .all(|parent| thread_set.contains(parent))
        && thread_set.insert(id)
    {
        scratchpad_list.push(id);

        if scratchpad_list.len() > thread_list.len() {
            thread_list.clone_from(scratchpad_list);
        }

        for child in node.to.iter().copied() {
            if active.contains(&child) {
                build_thread(
                    nodes,
                    active,
                    child,
                    scratchpad_list,
                    thread_set,
                    thread_list,
                );
            }
        }

        scratchpad_list.pop();
        thread_set.remove(&id);
    }
}

#[stacksafe]
fn build_thread_until<K, T, S>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    active: &HashSet<K, S>,
    id: K,
    stop_at: &HashSet<K, S>,
    thread_list: &mut Vec<K>,
    thread_set: &mut HashSet<K, S>,
) -> bool
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(&id)
        && node
            .from
            .iter()
            .filter(|parent| active.contains(*parent))
            .all(|parent| thread_set.contains(parent))
        && thread_set.insert(id)
    {
        thread_list.push(id);

        if !stop_at.contains(&id) {
            for child in node.to.iter().copied() {
                if active.contains(&child)
                    && build_thread_until(nodes, active, child, stop_at, thread_list, thread_set)
                {
                    return true;
                }
            }

            thread_list.pop();
            thread_set.remove(&id);
            false
        } else {
            true
        }
    } else {
        false
    }
}

#[stacksafe]
fn build_thread_from<K, T, S>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    active: &HashSet<K, S>,
    id: K,
    thread_list: &mut Vec<K>,
    thread_set: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(&id) {
        thread_list.push(id);
        thread_set.insert(id);

        if node.from.iter().any(|parent| active.contains(parent)) {
            return;
        }

        if let Some(parent) = node.from.first().copied() {
            build_thread_from(nodes, active, parent, thread_list, thread_set);
        }
    }
}

#[cfg(feature = "rkyv")]
#[stacksafe]
fn build_thread_archived<K, K2, T, T2, S>(
    nodes: &ArchivedHashMap<K::Archived, ArchivedIndependentNode<K, T, S>>,
    active: &ArchivedHashSet<K::Archived>,
    id: K::Archived,
    scratchpad_list: &mut Vec<K::Archived>,
    thread_set: &mut HashSet<K::Archived>,
    thread_list: &mut Vec<K::Archived>,
) where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq,
    T: Archive<Archived = T2> + IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(&id)
        && node
            .from
            .iter()
            .filter(|parent| active.contains(*parent))
            .all(|parent| thread_set.contains(parent))
        && thread_set.insert(id)
    {
        scratchpad_list.push(id);

        if scratchpad_list.len() > thread_list.len() {
            thread_list.clone_from(scratchpad_list);
        }

        for child in node.to.iter().copied() {
            if active.contains(&child) {
                build_thread_archived(
                    nodes,
                    active,
                    child,
                    scratchpad_list,
                    thread_set,
                    thread_list,
                );
            }
        }

        scratchpad_list.pop();
        thread_set.remove(&id);
    }
}

#[cfg(feature = "rkyv")]
#[stacksafe]
fn build_thread_archived_until<K, K2, T, T2, S>(
    nodes: &ArchivedHashMap<K::Archived, ArchivedIndependentNode<K, T, S>>,
    active: &ArchivedHashSet<K::Archived>,
    id: K::Archived,
    stop_at: &HashSet<K::Archived>,
    thread_list: &mut Vec<K::Archived>,
    thread_set: &mut HashSet<K::Archived>,
) -> bool
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq,
    T: Archive<Archived = T2> + IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(&id)
        && node
            .from
            .iter()
            .filter(|parent| active.contains(*parent))
            .all(|parent| thread_set.contains(parent))
        && thread_set.insert(id)
    {
        thread_list.push(id);

        if !stop_at.contains(&id) {
            for child in node.to.iter().copied() {
                if active.contains(&child)
                    && build_thread_archived_until(
                        nodes,
                        active,
                        child,
                        stop_at,
                        thread_list,
                        thread_set,
                    )
                {
                    return true;
                }
            }

            thread_list.pop();
            thread_set.remove(&id);
            false
        } else {
            true
        }
    } else {
        false
    }
}

#[cfg(feature = "rkyv")]
#[stacksafe]
fn build_thread_from_archived<K, K2, T, T2, S>(
    nodes: &ArchivedHashMap<K::Archived, ArchivedIndependentNode<K, T, S>>,
    active: &ArchivedHashSet<K::Archived>,
    id: K::Archived,
    thread_list: &mut Vec<K::Archived>,
    thread_set: &mut HashSet<K::Archived>,
) where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq,
    T: Archive<Archived = T2> + IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(&id) {
        thread_list.push(id);
        thread_set.insert(id);

        if node.from.iter().any(|parent| active.contains(parent)) {
            return;
        }

        if let Some(parent) = node.from.get_index(0).copied() {
            build_thread_from_archived(nodes, active, parent, thread_list, thread_set);
        }
    }
}

#[cfg(feature = "rkyv")]
#[stacksafe]
fn add_archived_node_identifiers<K, N, T>(
    nodes: &ArchivedHashMap<K, N>,
    id: K,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K>,
) where
    K: Hash + Copy + Eq,
    N: ArchivedNode<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>>,
{
    if let Some(node) = nodes.get(&id)
        && !identifier_set.contains(&id)
        && node
            .from()
            .iter()
            .all(|parent| identifier_set.contains(parent))
    {
        identifiers.push(id);
        identifier_set.insert(id);
        for child in node.to().iter() {
            add_archived_node_identifiers(nodes, *child, identifiers, identifier_set);
        }
    }
}

#[cfg(feature = "rkyv")]
#[stacksafe]
fn add_archived_node_identifiers_rev<K, N, T>(
    nodes: &ArchivedHashMap<K, N>,
    id: K,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K>,
) where
    K: Hash + Copy + Eq,
    N: ArchivedNode<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>>,
{
    if let Some(node) = nodes.get(&id)
        && !identifier_set.contains(&id)
        && node
            .from()
            .iter()
            .all(|parent| identifier_set.contains(parent))
    {
        identifiers.push(id);
        identifier_set.insert(id);
        for child in node.to().iter().collect::<Vec<_>>().into_iter().rev() {
            add_archived_node_identifiers_rev(nodes, *child, identifiers, identifier_set);
        }
    }
}
