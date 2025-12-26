//! [`DependentWeave`] is a tree-based [`Weave`] where each [`Node`] depends on the contents of the previous Node.

use std::{
    cmp::Ordering,
    collections::HashMap,
    hash::{BuildHasher, Hash},
};

use contracts::*;
use indexmap::IndexSet;
use rkyv::{
    Archive, Deserialize, Serialize,
    collections::swiss_table::{ArchivedHashMap, ArchivedIndexSet},
    option::ArchivedOption,
    with::Skip,
};

#[cfg(feature = "serde")]
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

use crate::{
    ArchivedNode, ArchivedWeave, DeduplicatableContents, DeduplicatableWeave,
    DiscreteContentResult, DiscreteContents, DiscreteWeave, IndependentContents, Node,
    SemiIndependentWeave, Weave,
};

#[cfg(feature = "legacy")]
pub mod legacy_dependent;

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
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

impl<K, T, S> Node<K, T, S> for DependentNode<K, T, S>
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    fn id(&self) -> K {
        self.id
    }
    fn from(&self) -> impl ExactSizeIterator<Item = K> + DoubleEndedIterator<Item = K> {
        self.from.into_iter()
    }
    fn to(&self) -> impl ExactSizeIterator<Item = K> + DoubleEndedIterator<Item = K> {
        self.to.iter().copied()
    }
    fn is_active(&self) -> bool {
        self.active
    }
    fn is_bookmarked(&self) -> bool {
        self.bookmarked
    }
    fn contents(&self) -> &T {
        &self.contents
    }
}

/// A tree-based [`Weave`] where each [`Node`] depends on the contents of the previous Node.
///
/// In order to reduce the serialized size, this weave implementation cannot contain more than [`i32::MAX`] nodes.
#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub struct DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    nodes: HashMap<K, DependentNode<K, T, S>, S>,
    roots: IndexSet<K, S>,
    active: Option<K>,
    bookmarked: IndexSet<K, S>,

    #[rkyv(with = Skip)]
    thread: Vec<K>,

    pub metadata: M,
}

impl<K, T, M, S> DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    /// Validates that the weave is internally consistent.
    ///
    /// If this returns `false`, further actions on the weave will result in unexpected behavior, including but not limited to panics. However, since this function is fairly slow, it should only be called occasionally (such as when saving the weave to disk).
    ///
    /// This function will be removed in the future once this [`Weave`] implementation has undergone formal verification.
    pub fn validate(&self) -> bool {
        let nodes: IndexSet<_, _> = self.nodes.keys().copied().collect();

        self.roots.is_subset::<S>(&nodes)
            && if let Some(active) = self.active {
                self.nodes.contains_key(&active)
            } else {
                true
            }
            && self.bookmarked.is_subset(&nodes)
            && self.nodes.iter().all(|(key, value)| {
                value.validate()
                    && value.id == *key
                    && if let Some(from) = value.from {
                        self.nodes.contains_key(&from)
                    } else {
                        true
                    }
                    && value.to.is_subset(&nodes)
                    && value.from.is_none() == self.roots.contains(key)
                    && value.active == (self.active == Some(*key))
                    && value.bookmarked == self.bookmarked.contains(key)
                    && value
                        .from
                        .iter()
                        .map(|v| self.nodes.get(v).unwrap())
                        .all(|p| p.to.contains(key))
                    && value
                        .to
                        .iter()
                        .map(|v| self.nodes.get(v).unwrap())
                        .all(|p| p.from == Some(*key))
            })
    }
    fn under_max_size(&self) -> bool {
        (self.nodes.len() as u64) < (i32::MAX as u64)
    }
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
            thread: Vec::with_capacity(capacity),
            metadata,
        }
    }
    pub fn capacity(&self) -> usize {
        self.nodes.capacity()
    }
    pub fn reserve(&mut self, additional: usize) {
        self.nodes.reserve(additional);
        self.roots
            .reserve(self.nodes.capacity().saturating_sub(self.roots.capacity()));
        self.bookmarked.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.bookmarked.capacity()),
        );
        self.thread
            .reserve(self.nodes.capacity().saturating_sub(self.thread.capacity()));
    }
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.nodes.shrink_to(min_capacity);
        self.roots.shrink_to(min_capacity);
        self.bookmarked.shrink_to(min_capacity);
        self.thread.shrink_to(min_capacity);
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
    #[debug_ensures(!self.nodes.contains_key(id))]
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
}

impl<K, T, M, S> Weave<K, DependentNode<K, T, S>, T, S> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    fn len(&self) -> usize {
        self.nodes.len()
    }
    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    fn nodes(&self) -> &HashMap<K, DependentNode<K, T, S>, S> {
        &self.nodes
    }
    fn roots(&self) -> &IndexSet<K, S> {
        &self.roots
    }
    fn bookmarks(&self) -> &IndexSet<K, S> {
        &self.bookmarked
    }
    fn contains(&self, id: &K) -> bool {
        self.nodes.contains_key(id)
    }
    fn get_node(&self, id: &K) -> Option<&DependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    fn get_active_thread(
        &mut self,
    ) -> impl ExactSizeIterator<Item = K> + DoubleEndedIterator<Item = K> {
        self.thread.clear();

        if let Some(active) = self.active {
            build_thread(&self.nodes, &active, &mut self.thread);
        }

        self.thread.iter().copied()
    }
    fn get_thread_from(
        &mut self,
        id: &K,
    ) -> impl ExactSizeIterator<Item = K> + DoubleEndedIterator<Item = K> {
        self.thread.clear();

        build_thread(&self.nodes, id, &mut self.thread);

        self.thread.iter().copied()
    }
    #[debug_ensures(self.validate())]
    #[requires(self.under_max_size())]
    fn add_node(&mut self, node: DependentNode<K, T, S>) -> bool {
        let is_invalid =
            self.nodes.contains_key(&node.id) || !node.validate() || !node.to.is_empty();

        if is_invalid {
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
    /*#[debug_ensures((ret && value == (self.active == Some(*id))) || !ret)]
    #[debug_ensures(self.validate())]*/
    fn set_node_active_status(&mut self, id: &K, value: bool, _alternate: bool) -> bool {
        self.set_node_active_status_in_place(id, value)
    }
    #[debug_ensures((ret && value == (self.active == Some(*id))) || !ret)]
    #[debug_ensures(self.validate())]
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
    #[debug_ensures((ret && value == self.bookmarked.contains(id)) || !ret)]
    #[debug_ensures(self.validate())]
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
    #[debug_ensures(ret == self.contains(id))]
    #[debug_ensures(self.validate())]
    fn sort_node_children_by(
        &mut self,
        id: &K,
        mut compare: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) -> bool {
        if let Some(node) = self.nodes.get(id) {
            let mut children: Vec<_> = node.to.iter().filter_map(|id| self.nodes.get(id)).collect();
            children.sort_by(|a, b| compare(a, b));

            let children: IndexSet<_, _> = children.into_iter().map(|node| node.id).collect();

            if let Some(node) = self.nodes.get_mut(id) {
                node.to = children;

                true
            } else {
                false
            }
        } else {
            false
        }
    }
    #[debug_ensures(self.validate())]
    fn sort_roots_by(
        &mut self,
        mut compare: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) {
        let mut roots: Vec<_> = self
            .roots
            .iter()
            .filter_map(|id| self.nodes.get(id))
            .collect();
        roots.sort_by(|a, b| compare(a, b));

        self.roots.clear();
        self.roots.extend(roots.into_iter().map(|node| node.id));
    }
    #[debug_ensures(!self.nodes.contains_key(id))]
    #[debug_ensures(self.validate())]
    fn remove_node(&mut self, id: &K) -> Option<DependentNode<K, T, S>> {
        self.remove_node_unverified(id)
    }
}

impl<K, T, M, S> DiscreteWeave<K, DependentNode<K, T, S>, T, S> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    #[debug_ensures(self.validate())]
    #[requires(self.under_max_size())]
    fn split_node(&mut self, id: &K, at: usize, new_id: K) -> bool {
        if self.nodes.contains_key(&new_id) || *id == new_id {
            return false;
        }

        if let Some(mut node) = self.nodes.remove(id) {
            match node.contents.split(at) {
                DiscreteContentResult::Two((left, right)) => {
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
    #[debug_ensures(self.validate())]
    fn merge_with_parent(&mut self, id: &K) -> Option<K> {
        if let Some(mut node) = self.nodes.remove(id) {
            if let Some(mut parent) = node.from.and_then(|id| self.nodes.remove(&id)) {
                if parent.to.len() > 1 {
                    self.nodes.insert(parent.id, parent);
                    self.nodes.insert(node.id, node);
                    return None;
                }

                match parent.contents.merge(node.contents) {
                    DiscreteContentResult::Two((left, right)) => {
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

impl<K, T, M, S> SemiIndependentWeave<K, DependentNode<K, T, S>, T, S>
    for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    fn get_contents_mut(&mut self, id: &K) -> Option<&mut T> {
        self.nodes.get_mut(id).map(|node| &mut node.contents)
    }
}

impl<K, T, M, S> DeduplicatableWeave<K, DependentNode<K, T, S>, T, S> for DependentWeave<K, T, M, S>
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

impl<K, K2, T, T2, S> ArchivedNode<K::Archived, T::Archived> for ArchivedDependentNode<K, T, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2>,
    S: BuildHasher + Default + Clone,
{
    fn id(&self) -> K::Archived {
        self.id
    }
    fn from(&self) -> impl Iterator<Item = K::Archived> {
        self.from.into_iter().copied()
    }
    fn to(&self) -> impl Iterator<Item = K::Archived> {
        self.to.iter().copied()
    }
    fn is_active(&self) -> bool {
        self.active
    }
    fn is_bookmarked(&self) -> bool {
        self.bookmarked
    }
    fn contents(&self) -> &<T as Archive>::Archived {
        &self.contents
    }
}

impl<K, K2, T, T2, M, M2, S> ArchivedWeave<K::Archived, ArchivedDependentNode<K, T, S>, T::Archived>
    for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq + 'static,
    T: Archive<Archived = T2>,
    M: Archive<Archived = M2>,
    S: BuildHasher + Default + Clone,
{
    fn len(&self) -> usize {
        self.nodes.len()
    }
    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    fn nodes(&self) -> &ArchivedHashMap<K::Archived, ArchivedDependentNode<K, T, S>> {
        &self.nodes
    }
    fn roots(&self) -> &ArchivedIndexSet<K::Archived> {
        &self.roots
    }
    fn bookmarks(&self) -> &ArchivedIndexSet<K::Archived> {
        &self.bookmarked
    }
    fn contains(&self, id: &K::Archived) -> bool {
        self.nodes.contains_key(id)
    }
    fn get_node(&self, id: &K::Archived) -> Option<&ArchivedDependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    fn get_active_thread(
        &self,
    ) -> impl ExactSizeIterator<Item = K::Archived> + DoubleEndedIterator<Item = K::Archived> {
        let mut thread =
            Vec::with_capacity((self.nodes.len() as f32).sqrt().max(16.0).round() as usize);

        if let ArchivedOption::Some(active) = self.active {
            build_thread_archived(&self.nodes, &active, &mut thread);
        }

        thread.into_iter()
    }
    fn get_thread_from(
        &self,
        id: &K::Archived,
    ) -> impl ExactSizeIterator<Item = K::Archived> + DoubleEndedIterator<Item = K::Archived> {
        let mut thread =
            Vec::with_capacity((self.nodes.len() as f32).sqrt().max(16.0).round() as usize);

        build_thread_archived(&self.nodes, id, &mut thread);

        thread.into_iter()
    }
}

fn build_thread<K, T, S>(nodes: &HashMap<K, DependentNode<K, T, S>, S>, id: &K, thread: &mut Vec<K>)
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(id) {
        thread.push(*id);
        if let Some(parent) = node.from {
            build_thread(nodes, &parent, thread);
        }
    }
}

fn build_thread_archived<K, K2, T, T2, S>(
    nodes: &ArchivedHashMap<K::Archived, ArchivedDependentNode<K, T, S>>,
    id: &K::Archived,
    thread: &mut Vec<K::Archived>,
) where
    K: Archive<Archived = K2> + Hash + Copy + Eq,
    <K as Archive>::Archived: Hash + Copy + Eq,
    T: Archive<Archived = T2>,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(id) {
        thread.push(*id);
        if let ArchivedOption::Some(parent) = node.from {
            build_thread_archived(nodes, &parent, thread);
        }
    }
}
