//! [`IndependentWeave`] is a DAG-based [`Weave`] where each [`Node`] does *not* depend on the contents of the previous Node.

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
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
    ArchivedActivePathWeave, ArchivedBookmarkableWeave, ArchivedMetadataWeave,
    ArchivedSortableWeave, ArchivedWeave,
};

use crate::{
    ActivePathWeave, BookmarkableWeave, DeduplicatableContents, DeduplicatableWeave,
    DiscreteContentResult, DiscreteContents, DiscreteWeave, IndependentContents, MetadataWeave,
    Node, SortableBookmarkableWeave, SortableWeave, Weave, ancestor_subgraph,
    contract::{lacks_duplicates, valid_path, valid_topological_sort},
    dependent::DependentWeave,
    descendant_subgraph, longest_path_to_root, shortest_path_to_ancestor,
    shortest_path_to_descendant, topological_sort, topological_sort_rev, topological_sort_subgraph,
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

#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, S> PartialEq for IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents + PartialEq,
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

/// A DAG-based [`Weave`] where each [`Node`] does *not* depend on the contents of the previous Node.
///
/// However, this additional flexibility results in worse performance and memory usage characteristics overall.
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
    scratchpad_list_2: Vec<K>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "wincode", wincode(skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad_set: HashSet<K, S>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "wincode", wincode(skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad_set_2: HashSet<K, S>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "wincode", wincode(skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad_map: HashMap<K, usize, S>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "wincode", wincode(skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad_queue: VecDeque<K>,

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
            scratchpad_list_2: Vec::with_capacity(capacity),
            scratchpad_set: HashSet::with_capacity_and_hasher(capacity, S::default()),
            scratchpad_set_2: HashSet::with_capacity_and_hasher(capacity, S::default()),
            scratchpad_map: HashMap::with_capacity_and_hasher(capacity, S::default()),
            scratchpad_queue: VecDeque::with_capacity(capacity),
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
        self.active
            .reserve(self.nodes.capacity().saturating_sub(self.active.len()));
        self.bookmarked
            .reserve(self.nodes.capacity().saturating_sub(self.bookmarked.len()));
        self.scratchpad_list.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_list.len()),
        );
        self.scratchpad_list_2.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_list_2.len()),
        );
        self.scratchpad_set.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_set.len()),
        );
        self.scratchpad_set_2.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_set_2.len()),
        );
        self.scratchpad_map.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_map.len()),
        );
        self.scratchpad_queue.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_queue.len()),
        );
    }
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.nodes.shrink_to(min_capacity);
        self.roots.shrink_to(min_capacity);
        self.active.shrink_to(min_capacity);
        self.bookmarked.shrink_to(min_capacity);
        self.scratchpad_list.shrink_to(min_capacity);
        self.scratchpad_list_2.shrink_to(min_capacity);
        self.scratchpad_set.shrink_to(min_capacity);
        self.scratchpad_set_2.shrink_to(min_capacity);
        self.scratchpad_map.shrink_to(min_capacity);
        self.scratchpad_queue.shrink_to(min_capacity);
    }
    fn sibling_ids_from_all_parents_including_roots<'a>(
        &'a self,
        node: &'a IndependentNode<K, T, S>,
    ) -> Box<dyn Iterator<Item = K> + 'a> {
        if node.from.is_empty() {
            Box::new(self.roots.iter().copied().filter(|id| *id != node.id))
        } else {
            Box::new(
                node.from
                    .iter()
                    .filter_map(|id| self.nodes.get(id))
                    .flat_map(|parent| {
                        {
                            parent.to.iter().copied().filter(|id| {
                                *id != node.id && !node.from.contains(id) && !node.to.contains(id)
                            })
                        }
                    })
                    .collect::<IndexSet<K, S>>()
                    .into_iter(),
            )
        }
    }
    #[allow(
        clippy::too_many_lines,
        reason = "Cannot be split into smaller functions"
    )]
    pub(super) fn update_node_activity_in_place(&mut self, id: &K, value: bool) -> bool {
        if let Some(node) = self.nodes.get_mut(id) {
            if node.active == value {
                return true;
            }

            node.active = value;
            if value {
                self.active.insert(node.id);
            } else {
                self.active.remove(id);
            }
        } else {
            return false;
        }

        if value {
            self.scratchpad_list.clear();
            self.scratchpad_list_2.clear();
            self.scratchpad_set.clear();
            self.scratchpad_set_2.clear();
            self.scratchpad_map.clear();

            ancestor_subgraph(
                &self.nodes,
                *id,
                &mut self.scratchpad_queue,
                &mut self.scratchpad_set,
            ); // ancestors

            for active_root in self
                .roots
                .iter()
                .copied()
                .filter(|root| self.active.contains(root) && self.scratchpad_set.contains(root))
            {
                topological_sort_subgraph(
                    &self.nodes,
                    &|id| self.active.contains(id) && self.scratchpad_set.contains(id),
                    &active_root,
                    &mut self.scratchpad_queue,
                    &mut self.scratchpad_list,
                    &mut self.scratchpad_set_2,
                );
            }

            longest_path_to_root(
                &self.nodes,
                &self.scratchpad_list,
                &mut self.scratchpad_map,
                &mut self.scratchpad_list_2,
            );

            let target = self.scratchpad_list_2.first().copied();

            self.scratchpad_list.clear();
            self.scratchpad_set_2.clear();

            self.scratchpad_set_2
                .extend(self.scratchpad_list_2.drain(..));

            self.scratchpad_list
                .extend(self.active.intersection(&self.scratchpad_set));

            for active_ancestor in self.scratchpad_list.drain(..) {
                if !self.scratchpad_set_2.contains(&active_ancestor) && &active_ancestor != id {
                    self.active.remove(&active_ancestor);
                    if let Some(node) = self.nodes.get_mut(&active_ancestor) {
                        node.active = false;
                    }
                }
            }

            self.scratchpad_set_2.clear();

            if let Some(target) = target {
                shortest_path_to_ancestor(
                    &self.nodes,
                    id,
                    &|node| node.id == target,
                    &mut self.scratchpad_list,
                    &mut self.scratchpad_set_2,
                    &mut self.scratchpad_list_2, // shortest path
                );
            } else {
                shortest_path_to_ancestor(
                    &self.nodes,
                    id,
                    &|node| node.from.is_empty(),
                    &mut self.scratchpad_list,
                    &mut self.scratchpad_set_2,
                    &mut self.scratchpad_list_2, // shortest path
                );
            }

            for path_item in self.scratchpad_list_2.drain(..) {
                if let Some(node) = self.nodes.get_mut(&path_item) {
                    node.active = true;
                }
                self.active.insert(path_item);
            }

            self.scratchpad_list.clear();
            self.scratchpad_set_2.clear();

            for parent in &self.nodes[id].from {
                for sibling in self.nodes[parent].to.iter().copied() {
                    if self.scratchpad_set.insert(sibling) {
                        self.scratchpad_list_2.push(sibling); // siblings
                    }
                }
            }

            self.scratchpad_set.remove(id);
            descendant_subgraph(
                &self.nodes,
                *id,
                &mut self.scratchpad_queue,
                &mut self.scratchpad_set,
            ); // decendants

            for item in self.scratchpad_list_2.drain(..) {
                self.scratchpad_set.remove(&item);
            }

            self.scratchpad_list
                .extend(self.active.difference(&self.scratchpad_set).copied());

            for orphan in self.scratchpad_list.drain(..) {
                self.active.remove(&orphan);
                if let Some(node) = self.nodes.get_mut(&orphan) {
                    node.active = false;
                }
            }

            shortest_path_to_descendant(
                &self.nodes,
                id,
                &|node| node.active && &node.id != id,
                &mut self.scratchpad_list,
                &mut self.scratchpad_set_2,
                &mut self.scratchpad_list_2,
            );

            for path_item in self.scratchpad_list_2.drain(..) {
                if let Some(node) = self.nodes.get_mut(&path_item) {
                    node.active = true;
                }
                self.active.insert(path_item);
            }

            self.scratchpad_set.clear();
            self.scratchpad_set_2.clear();
            self.scratchpad_list.clear();

            descendant_subgraph(
                &self.nodes,
                *id,
                &mut self.scratchpad_queue,
                &mut self.scratchpad_set,
            ); // decendants

            self.scratchpad_set_2
                .extend(self.active.intersection(&self.scratchpad_set));
            self.scratchpad_set.clear();

            topological_sort_subgraph(
                &self.nodes,
                &|id| self.scratchpad_set_2.contains(id),
                id,
                &mut self.scratchpad_queue,
                &mut self.scratchpad_list_2,
                &mut self.scratchpad_set,
            );

            self.scratchpad_list
                .extend(self.scratchpad_set_2.difference(&self.scratchpad_set));

            for orphan in self.scratchpad_list.drain(..) {
                self.active.remove(&orphan);
                if let Some(node) = self.nodes.get_mut(&orphan) {
                    node.active = false;
                }
            }
        }

        self.fix_orphaned_activations();

        true
    }
    pub(super) fn fix_orphaned_activations(&mut self) {
        self.scratchpad_list.clear();
        self.scratchpad_list_2.clear();
        self.scratchpad_set.clear();
        self.scratchpad_map.clear();

        for active_root in self
            .roots
            .iter()
            .copied()
            .filter(|root| self.active.contains(root))
        {
            topological_sort_subgraph(
                &self.nodes,
                &|id| self.active.contains(id),
                &active_root,
                &mut self.scratchpad_queue,
                &mut self.scratchpad_list,
                &mut self.scratchpad_set,
            );
        }

        longest_path_to_root(
            &self.nodes,
            &self.scratchpad_list,
            &mut self.scratchpad_map,
            &mut self.scratchpad_list_2,
        );

        self.scratchpad_list.clear();
        self.scratchpad_set.clear();

        self.scratchpad_set.extend(self.scratchpad_list_2.drain(..));
        self.scratchpad_list
            .extend(self.active.difference(&self.scratchpad_set).copied());

        for orphan in self.scratchpad_list.drain(..) {
            self.active.remove(&orphan);
            if let Some(node) = self.nodes.get_mut(&orphan) {
                node.active = false;
            }
        }
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

#[allow(clippy::fallible_impl_from, reason = "Should never fail")]
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

            assert!(
                output.add_node(IndependentNode {
                    id: node.id,
                    from: node.from.into_iter().collect(),
                    to: IndexSet::with_capacity_and_hasher(node.to.len(), S::default()),
                    active: node.active,
                    bookmarked: node.bookmarked,
                    contents: node.contents,
                }),
                "Failed to add node"
            );
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
        self.active.contains(id)
    }
    #[inline]
    fn get_node(&self, id: &K) -> Option<&IndependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    #[ensures(output.len() == self.nodes.len())]
    #[ensures(valid_topological_sort(&self.nodes, output))]
    fn get_ordered_node_identifiers(&mut self, output: &mut Vec<K>) {
        output.clear();
        self.scratchpad_set.clear();

        for root in &self.roots {
            topological_sort::<K, IndependentNode<K, T, S>, T, S>(
                &self.nodes,
                root,
                &mut self.scratchpad_queue,
                output,
                &mut self.scratchpad_set,
            ); // Compiler limitation
        }
    }
    #[ensures(lacks_duplicates(output))]
    fn get_ordered_node_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        // TODO

        output.clear();
        self.scratchpad_set.clear();

        if self.nodes.contains_key(id) {
            if let Some(node) = self.nodes.get(id) {
                for parent in &node.from {
                    self.scratchpad_set.insert(*parent);
                }
            }
            topological_sort::<K, IndependentNode<K, T, S>, T, S>(
                &self.nodes,
                id,
                &mut self.scratchpad_queue,
                output,
                &mut self.scratchpad_set,
            ); // Compiler limitation
        }
    }
    #[ensures(output.len() == self.active.len())]
    #[ensures(output.iter().all(|i| self.active.contains(i)))]
    #[ensures(lacks_duplicates(output))]
    #[ensures(valid_path(&self.nodes, output))]
    fn get_active_thread(&mut self, output: &mut Vec<K>) {
        output.clear();
        self.scratchpad_list.clear();
        self.scratchpad_set.clear();
        self.scratchpad_map.clear();

        for active_root in self
            .roots
            .iter()
            .copied()
            .filter(|root| self.active.contains(root))
        {
            topological_sort_subgraph(
                &self.nodes,
                &|id| self.active.contains(id),
                &active_root,
                &mut self.scratchpad_queue,
                &mut self.scratchpad_list,
                &mut self.scratchpad_set,
            );

            longest_path_to_root(
                &self.nodes,
                &self.scratchpad_list,
                &mut self.scratchpad_map,
                output,
            );
        }
    }
    #[ensures(!self.nodes.contains_key(id) || output.first() == Some(id))]
    #[ensures(self.nodes.contains_key(id) || output.is_empty())]
    #[ensures(lacks_duplicates(output))]
    #[ensures(valid_path(&self.nodes, output))]
    fn get_thread_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();
        if !self.nodes.contains_key(id) {
            return;
        }

        self.scratchpad_list.clear();
        self.scratchpad_set.clear();
        self.scratchpad_set_2.clear();
        self.scratchpad_map.clear();

        ancestor_subgraph(
            &self.nodes,
            *id,
            &mut self.scratchpad_queue,
            &mut self.scratchpad_set,
        );

        for active_root in self
            .roots
            .iter()
            .copied()
            .filter(|root| self.active.contains(root) && self.scratchpad_set.contains(root))
        {
            topological_sort_subgraph(
                &self.nodes,
                &|id| self.active.contains(id) && self.scratchpad_set.contains(id),
                &active_root,
                &mut self.scratchpad_queue,
                &mut self.scratchpad_list,
                &mut self.scratchpad_set_2,
            );

            longest_path_to_root(
                &self.nodes,
                &self.scratchpad_list,
                &mut self.scratchpad_map,
                &mut self.scratchpad_list_2,
            );
        }

        self.scratchpad_list.clear();
        self.scratchpad_set_2.clear();

        if let Some(target) = self.scratchpad_list_2.first().copied() {
            shortest_path_to_ancestor(
                &self.nodes,
                id,
                &|node| node.id == target,
                &mut self.scratchpad_list,
                &mut self.scratchpad_set_2,
                output,
            );

            output.pop();
            output.append(&mut self.scratchpad_list_2);
        } else {
            shortest_path_to_ancestor(
                &self.nodes,
                id,
                &|node| node.from.is_empty(),
                &mut self.scratchpad_list,
                &mut self.scratchpad_set_2,
                output,
            );
        }
    }
    #[ensures(!ret || old(self.nodes.len()) + 1 == self.nodes.len())]
    #[ensures(!ret || old(!self.nodes.contains_key(&node.id)))]
    #[ensures(!ret || self.nodes.contains_key(&old(node.id)))]
    #[ensures(!ret || old(node.active) == self.active.contains(&old(node.id)) || (!old(node.active) && self.active.contains(&old(node.id)) && old(node.to.iter().any(|c| self.active.contains(c)))))]
    #[ensures(!ret || old(node.bookmarked) == self.bookmarked.contains(&old(node.id)))]
    #[ensures(ret || old(self.nodes.len()) == self.nodes.len())]
    #[ensures(ret || old(self.active.clone()) == self.active)]
    #[ensures(ret || old(self.bookmarked.clone()) == self.bookmarked)]
    #[invariant(self.validate())]
    fn add_node(&mut self, mut node: IndependentNode<K, T, S>) -> bool {
        if self.nodes.contains_key(&node.id)
            || !node.validate()
            || !node.from.iter().all(|id| self.nodes.contains_key(id))
            || !node.to.iter().all(|id| self.nodes.contains_key(id))
        {
            return false;
        }

        if !node.to.is_empty() && !node.from.is_empty() {
            self.scratchpad_set.clear();

            for parent in node.from.iter().copied() {
                ancestor_subgraph(
                    &self.nodes,
                    parent,
                    &mut self.scratchpad_queue,
                    &mut self.scratchpad_set,
                );
            }

            if node
                .to
                .iter()
                .any(|child| self.scratchpad_set.contains(child))
            {
                return false;
            }
        }

        for child in &node.to {
            let child = &self.nodes[child];
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
    #[ensures(!ret || value == self.active.contains(id))]
    #[ensures(ret || old(self.active.clone()) == self.active)]
    #[ensures(ret == self.nodes.contains_key(id))]
    #[invariant(self.validate())]
    fn set_node_active_status(&mut self, id: &K, value: bool) -> bool {
        self.update_node_activity_in_place(id, value)
    }
    #[ensures(!self.nodes.contains_key(id))]
    #[ensures(ret.is_none() || old(self.nodes.len()) > self.nodes.len())]
    #[ensures(ret.is_none() || old(self.active.len()) >= self.active.len())]
    #[ensures(ret.is_none() || old(self.bookmarked.len()) >= self.bookmarked.len())]
    #[ensures(ret.is_some() || old(self.nodes.len()) == self.nodes.len())]
    #[ensures(ret.is_some() || old(self.active.clone()) == self.active)]
    #[ensures(ret.is_some() || old(self.bookmarked.clone()) == self.bookmarked)]
    #[invariant(self.validate())]
    fn remove_node(&mut self, id: &K) -> Option<IndependentNode<K, T, S>> {
        let result = self.remove_node_unverified(id);
        if result.is_some() {
            self.fix_orphaned_activations();
        }
        result
    }
    #[ensures(!self.nodes.contains_key(id))]
    #[ensures(!ret || old(self.nodes.len()) > self.nodes.len())]
    #[ensures(!ret || old(self.active.len()) >= self.active.len())]
    #[ensures(!ret || old(self.bookmarked.len()) >= self.bookmarked.len())]
    #[ensures(ret || old(self.nodes.len()) == self.nodes.len())]
    #[ensures(ret || old(self.active.clone()) == self.active)]
    #[ensures(ret || old(self.bookmarked.clone()) == self.bookmarked)]
    #[invariant(self.validate())]
    fn remove_node_tracked(
        &mut self,
        id: &K,
        mut on_removal: impl FnMut(IndependentNode<K, T, S>),
    ) -> bool {
        if self.remove_node_unverified_tracked(id, &mut on_removal) {
            self.fix_orphaned_activations();
            true
        } else {
            false
        }
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
    #[inline]
    fn metadata(&self) -> &M {
        &self.metadata
    }
    #[inline]
    fn metadata_mut<O>(&mut self, callback: impl FnOnce(&mut M) -> O) -> O {
        callback(&mut self.metadata)
    }
}

impl<K, T, M, S> BookmarkableWeave<K, IndependentNode<K, T, S>, T> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
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

impl<K, T, M, S> SortableWeave<K, IndependentNode<K, T, S>, T> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[ensures(output.len() == self.nodes.len())]
    #[ensures(valid_topological_sort(&self.nodes, output))]
    fn get_ordered_node_identifiers_reversed_children(&mut self, output: &mut Vec<K>) {
        output.clear();
        self.scratchpad_set.clear();

        for root in &self.roots {
            topological_sort_rev::<K, IndependentNode<K, T, S>, T, S>(
                &self.nodes,
                root,
                &mut self.scratchpad_queue,
                output,
                &mut self.scratchpad_set,
            ); // Compiler limitation
        }
    }
    #[ensures(lacks_duplicates(output))]
    fn get_ordered_node_identifiers_from_reversed_children(&mut self, id: &K, output: &mut Vec<K>) {
        // TODO

        output.clear();
        self.scratchpad_set.clear();

        if self.nodes.contains_key(id) {
            if let Some(node) = self.nodes.get(id) {
                for parent in &node.from {
                    self.scratchpad_set.insert(*parent);
                }
            }
            topological_sort_rev::<K, IndependentNode<K, T, S>, T, S>(
                &self.nodes,
                id,
                &mut self.scratchpad_queue,
                output,
                &mut self.scratchpad_set,
            ); // Compiler limitation
        }
    }
    #[ensures(old(self.nodes.len()) == self.nodes.len())]
    #[ensures(ret == self.nodes.contains_key(id))]
    #[invariant(self.validate())]
    fn sort_node_children_by(
        &mut self,
        id: &K,
        mut cmp: impl FnMut(&IndependentNode<K, T, S>, &IndependentNode<K, T, S>) -> Ordering,
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
        mut cmp: impl FnMut(&IndependentNode<K, T, S>, &IndependentNode<K, T, S>) -> Ordering,
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

impl<K, T, M, S> SortableBookmarkableWeave<K, IndependentNode<K, T, S>, T>
    for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[ensures(old(self.bookmarked.len()) == self.bookmarked.len())]
    #[invariant(self.validate())]
    fn sort_bookmarks_by(
        &mut self,
        mut cmp: impl FnMut(&IndependentNode<K, T, S>, &IndependentNode<K, T, S>) -> Ordering,
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

impl<K, T, M, S> ActivePathWeave<K, IndependentNode<K, T, S>, T> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type Active = HashSet<K, S>;

    #[inline]
    fn active(&self) -> &Self::Active {
        &self.active
    }
    #[invariant(self.validate())]
    fn set_active_path(&mut self, active: impl Iterator<Item = K>) {
        self.active.iter().for_each(|active| {
            self.nodes.get_mut(active).unwrap().active = false;
        });
        self.active.clear();
        self.active
            .extend(active.filter(|id| self.nodes.contains_key(id)));
        self.active.iter().for_each(|active| {
            self.nodes.get_mut(active).unwrap().active = true;
        });
        self.fix_orphaned_activations();
    }
}

impl<K, T, M, S> DiscreteWeave<K, IndependentNode<K, T, S>, T> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    #[ensures(!ret || old(self.nodes.len()) + 1 == self.nodes.len())]
    #[ensures(!ret || self.nodes.contains_key(id))]
    #[ensures(!ret || self.nodes.contains_key(&new_id))]
    #[ensures(!ret || old(!self.nodes.contains_key(&new_id)))]
    #[ensures(ret || old(self.nodes.len()) == self.nodes.len())]
    #[ensures(ret || old(self.active.clone()) == self.active)]
    #[ensures(ret || old(self.bookmarked.clone()) == self.bookmarked)]
    #[invariant(self.validate())]
    fn split_node(&mut self, id: &K, at: usize, new_id: K) -> bool {
        if self.nodes.contains_key(&new_id) || *id == new_id {
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

                    for child in &node.to {
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
    #[ensures(ret.is_none() || old(self.nodes.len()) - 1 == self.nodes.len())]
    #[ensures(ret.is_none() || !self.nodes.contains_key(id))]
    #[ensures(ret.is_none() || old(self.nodes.contains_key(id)))]
    #[ensures(ret.is_none() || self.nodes.contains_key(&ret.unwrap()))]
    #[ensures(ret.is_none() || ret == old(self.nodes.get(id).and_then(|node| node.from.first().copied())))]
    #[ensures(ret.is_some() || old(self.nodes.len()) == self.nodes.len())]
    #[ensures(ret.is_some() || old(self.active.clone()) == self.active)]
    #[ensures(ret.is_some() || old(self.bookmarked.clone()) == self.bookmarked)]
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

                        for child in &parent.to {
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
    #[inline]
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
    #[ensures(old(self.nodes.len()) == self.nodes.len())]
    #[ensures(old(self.bookmarked.clone()) == self.bookmarked)]
    #[ensures(!ret || self.nodes().get(id).unwrap().from.iter().copied().collect::<HashSet<_>>() == new_parents.iter().copied().collect::<HashSet<_>>())]
    #[ensures(ret || old(self.nodes().get(id).map(|node| node.from.clone())) == self.nodes().get(id).map(|node| node.from.clone()))]
    #[ensures(ret || old(self.active.len()) == self.active.len())]
    fn move_node(&mut self, id: &K, new_parents: &[K]) -> bool {
        if new_parents
            .iter()
            .any(|new_parent| !self.nodes.contains_key(new_parent))
        {
            return false;
        }

        if let Some(node) = self.nodes.get(id)
            && !node.to.is_empty()
            && !new_parents.is_empty()
        {
            self.scratchpad_set.clear();

            for child in node.to.iter().copied() {
                descendant_subgraph(
                    &self.nodes,
                    child,
                    &mut self.scratchpad_queue,
                    &mut self.scratchpad_set,
                );
            }

            if new_parents
                .iter()
                .any(|new_parent| self.scratchpad_set.contains(new_parent))
            {
                return false;
            }
        }

        let new_parents: IndexSet<K, S> = new_parents.iter().copied().collect();

        if new_parents.contains(id) {
            return false;
        }

        if let Some(node) = self.nodes.get_mut(id) {
            for child in &node.to {
                if new_parents.contains(child) {
                    return false;
                }
            }

            let old_parents = mem::take(&mut node.from);

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
                }
            }
        } else {
            return false;
        }

        let node = self.nodes.get_mut(id).unwrap();
        node.from = new_parents;

        if node.from.is_empty() {
            self.roots.insert(node.id);
        } else {
            self.roots.shift_remove(&node.id);
        }

        if node.active {
            self.fix_orphaned_activations();
        }

        true
    }
}

#[cfg(feature = "rkyv")]
impl<K, K2, T, T2, S> Node<K::Archived, T::Archived> for ArchivedIndependentNode<K, T, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2> + IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type From = ArchivedIndexSet<K::Archived>;
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
        self.active.contains(id)
    }
    #[inline]
    fn get_node(&self, id: &K::Archived) -> Option<&ArchivedIndependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    fn get_ordered_node_identifiers(&self, output: &mut Vec<K::Archived>) {
        output.clear();
        let mut scratchpad = Vec::with_capacity(self.len());
        let mut scratchpad_2 = Vec::with_capacity(self.len());
        let mut identifier_set = HashSet::with_capacity(self.len());

        for root in self.roots.iter() {
            archived_topological_sort(
                &self.nodes,
                root,
                &mut scratchpad,
                &mut scratchpad_2,
                output,
                &mut identifier_set,
            );
        }
    }
    fn get_ordered_node_identifiers_from(&self, id: &K::Archived, output: &mut Vec<K::Archived>) {
        output.clear();

        if self.nodes.contains_key(id) {
            let mut scratchpad = Vec::with_capacity(self.len());
            let mut scratchpad_2 = Vec::with_capacity(self.len());
            let mut identifier_set = HashSet::with_capacity(self.len());

            archived_topological_sort(
                &self.nodes,
                id,
                &mut scratchpad,
                &mut scratchpad_2,
                output,
                &mut identifier_set,
            );
        }
    }
    fn get_active_thread(&self, output: &mut Vec<K::Archived>) {
        output.clear();
        let mut scratchpad_list = Vec::with_capacity(self.len());
        let mut scratchpad_list_2 = Vec::with_capacity(self.len());
        let mut scratchpad_list_3 = Vec::with_capacity(self.len());
        let mut scratchpad_set = HashSet::with_capacity(self.len());
        let mut scratchpad_map = HashMap::with_capacity(self.len());

        for active_root in self
            .roots
            .iter()
            .copied()
            .filter(|root| self.active.contains(root))
        {
            archived_topological_sort_subgraph(
                &self.nodes,
                &|id| self.active.contains(id),
                &active_root,
                &mut scratchpad_list,
                &mut scratchpad_list_2,
                &mut scratchpad_list_3,
                &mut scratchpad_set,
            );

            archived_longest_path_to_root(
                &self.nodes,
                &scratchpad_list_3,
                &mut scratchpad_map,
                output,
            );
        }
    }
    fn get_thread_from(&self, id: &K::Archived, output: &mut Vec<K::Archived>) {
        output.clear();
        if !self.nodes.contains_key(id) {
            return;
        }

        let mut scratchpad_list = Vec::with_capacity(self.len());
        let mut scratchpad_list_2 = Vec::with_capacity(self.len());
        let mut scratchpad_list_3 = Vec::with_capacity(self.len());
        let mut scratchpad_list_4 = Vec::with_capacity(self.len());
        let mut scratchpad_set = HashSet::with_capacity(self.len());
        let mut scratchpad_set_2 = HashSet::with_capacity(self.len());
        let mut scratchpad_map = HashMap::with_capacity(self.len());

        archived_ancestor_subgraph(
            &self.nodes,
            *id,
            &mut scratchpad_list_3,
            &mut scratchpad_set,
        );

        for active_root in self
            .roots
            .iter()
            .copied()
            .filter(|root| self.active.contains(root) && scratchpad_set.contains(root))
        {
            archived_topological_sort_subgraph(
                &self.nodes,
                &|id| self.active.contains(id) && scratchpad_set.contains(id),
                &active_root,
                &mut scratchpad_list_3,
                &mut scratchpad_list_4,
                &mut scratchpad_list,
                &mut scratchpad_set_2,
            );

            archived_longest_path_to_root(
                &self.nodes,
                &scratchpad_list,
                &mut scratchpad_map,
                &mut scratchpad_list_2,
            );
        }

        scratchpad_list.clear();
        scratchpad_set_2.clear();

        if let Some(target) = scratchpad_list_2.first().copied() {
            archived_shortest_path_to_ancestor(
                &self.nodes,
                id,
                &|node| node.id == target,
                &mut scratchpad_list,
                &mut scratchpad_set_2,
                output,
            );

            output.pop();
            output.append(&mut scratchpad_list_2);
        } else {
            archived_shortest_path_to_ancestor(
                &self.nodes,
                id,
                &|node| node.from.is_empty(),
                &mut scratchpad_list,
                &mut scratchpad_set_2,
                output,
            );
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
    #[inline]
    fn metadata(&self) -> &M::Archived {
        &self.metadata
    }
}

#[cfg(feature = "rkyv")]
impl<K, K2, T, T2, M, M2, S>
    ArchivedBookmarkableWeave<K::Archived, ArchivedIndependentNode<K, T, S>, T::Archived>
    for ArchivedIndependentWeave<K, T, M, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2> + IndependentContents,
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
        let mut scratchpad = Vec::with_capacity(self.len());
        let mut identifier_set = HashSet::with_capacity(self.len());

        for root in self.roots.iter() {
            archived_topological_sort_rev(
                &self.nodes,
                root,
                &mut scratchpad,
                output,
                &mut identifier_set,
            );
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
            let mut identifier_set = HashSet::with_capacity(self.len());

            archived_topological_sort_rev(
                &self.nodes,
                id,
                &mut scratchpad,
                output,
                &mut identifier_set,
            );
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

    #[inline]
    fn active(&self) -> &Self::Active {
        &self.active
    }
}

#[cfg(feature = "rkyv")]
fn archived_topological_sort<'a, K, N, T>(
    nodes: &'a ArchivedHashMap<K, N>,
    id: &'a K,
    scratchpad: &mut Vec<K>,
    scratchpad_2: &mut Vec<K>,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
{
    scratchpad.push(*id);

    while let Some(id) = scratchpad.pop() {
        let node = &nodes[&id];

        if !identifier_set.contains(&id)
            && node
                .from()
                .iter()
                .all(|parent| identifier_set.contains(parent))
        {
            identifiers.push(id);
            identifier_set.insert(id);
            scratchpad_2.extend(nodes[&id].to().iter().copied());
            scratchpad_2.reverse();
            scratchpad.append(scratchpad_2);
        }
    }
}

#[cfg(feature = "rkyv")]
fn archived_topological_sort_subgraph<'a, K, N, T>(
    nodes: &'a ArchivedHashMap<K, N>,
    filter: &impl Fn(&K) -> bool,
    id: &'a K,
    scratchpad: &mut Vec<K>,
    scratchpad_2: &mut Vec<K>,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
{
    scratchpad.push(*id);

    while let Some(id) = scratchpad.pop() {
        let node = &nodes[&id];

        if filter(&id)
            && !identifier_set.contains(&id)
            && node
                .from()
                .iter()
                .all(|parent| identifier_set.contains(parent) || !filter(parent))
        {
            identifiers.push(id);
            identifier_set.insert(id);
            scratchpad_2.extend(nodes[&id].to().iter().copied());
            scratchpad_2.reverse();
            scratchpad.append(scratchpad_2);
        }
    }
}

#[cfg(feature = "rkyv")]
fn archived_topological_sort_rev<'a, K, N, T>(
    nodes: &'a ArchivedHashMap<K, N>,
    id: &'a K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
{
    scratchpad.push(*id);

    while let Some(id) = scratchpad.pop() {
        let node = &nodes[&id];

        if !identifier_set.contains(&id)
            && node
                .from()
                .iter()
                .all(|parent| identifier_set.contains(parent))
        {
            identifiers.push(id);
            identifier_set.insert(id);
            scratchpad.extend(node.to().iter().copied());
        }
    }
}

#[cfg(feature = "rkyv")]
#[stacksafe::stacksafe]
fn archived_shortest_path_to_ancestor<'a, K, N, T>(
    nodes: &'a ArchivedHashMap<K, N>,
    id: &'a K,
    target: &impl Fn(&'a N) -> bool,
    scratchpad_list: &mut Vec<K>,
    scratchpad_set: &mut HashSet<K>,
    path: &mut Vec<K>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
{
    let node = nodes.get(id).unwrap();

    if scratchpad_set.insert(*id) {
        scratchpad_list.push(*id);

        if target(node) {
            if path.is_empty() || path.len() > scratchpad_list.len() {
                path.clone_from(scratchpad_list);
            }
        } else {
            for parent in node.from().iter() {
                archived_shortest_path_to_ancestor(
                    nodes,
                    parent,
                    target,
                    scratchpad_list,
                    scratchpad_set,
                    path,
                );
            }
        }

        scratchpad_list.pop();
        scratchpad_set.remove(id);
    }
}

#[cfg(feature = "rkyv")]
fn archived_longest_path_to_root<'a, K, N, T>(
    nodes: &'a ArchivedHashMap<K, N>,
    topological_order: &'a [K],
    scratchpad_map: &mut HashMap<K, usize>,
    reversed_path: &mut Vec<K>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
{
    let mut longest_global_distance = None;

    for id in topological_order {
        let longest_distance = nodes[id]
            .from()
            .iter()
            .map(|parent| scratchpad_map.get(parent).copied().unwrap_or_default())
            .max()
            .map(|l| l.strict_add(1))
            .unwrap_or_default();

        scratchpad_map.insert(*id, longest_distance);

        match longest_global_distance {
            Some((value, _)) => {
                if longest_distance > value {
                    longest_global_distance = Some((longest_distance, id));
                }
            }
            None => {
                longest_global_distance = Some((longest_distance, id));
            }
        }
    }

    if let Some((_, id)) = longest_global_distance {
        let mut current_id = Some(id);

        while let Some(id) = current_id {
            reversed_path.push(*id);

            current_id = nodes[id]
                .from()
                .iter()
                .max_by_key(|id| scratchpad_map.get(*id).copied());
        }
    }
}

#[cfg(feature = "rkyv")]
fn archived_ancestor_subgraph<'a, K, N, T>(
    nodes: &'a ArchivedHashMap<K, N>,
    id: K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut HashSet<K>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
{
    scratchpad.push(id);

    while let Some(id) = scratchpad.pop() {
        if identifiers.insert(id) {
            scratchpad.extend(nodes[&id].from().iter().copied());
        }
    }
}
