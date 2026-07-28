//! A legacy version of `DependentWeave` used by `tapestry-weave`'s v0 format; Please don't use this!

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    hash::{BuildHasher, Hash},
};

use indexmap::IndexSet;

#[cfg(feature = "rkyv")]
use rkyv::{
    Archive, Deserialize, Serialize,
    collections::swiss_table::{ArchivedHashMap, ArchivedIndexSet},
    option::ArchivedOption,
    with::Skip,
};

#[cfg(feature = "serde")]
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

#[cfg(feature = "rkyv")]
use crate::{
    ArchivedActiveSingularWeave, ArchivedBookmarkableWeave, ArchivedMetadataWeave,
    ArchivedSortableWeave, ArchivedWeave,
    dependent::{
        ArchivedDependentNode, archived_path_to_root, archived_topological_sort,
        archived_topological_sort_rev,
    },
};

use crate::{
    ActiveSingularWeave, BookmarkableWeave, DeduplicatableContents, DeduplicatableWeave,
    DiscreteContentResult, DiscreteContents, DiscreteWeave, IndependentContents, MetadataWeave,
    SemiIndependentWeave, SortableBookmarkableWeave, SortableWeave, Step, ValidatableWeave, Weave,
    dependent::{
        DependentNode, DependentWeave as NewDependentWeave, detect_cycles, path_to_root,
        topological_sort, topological_sort_rev,
    },
};

#[cfg(doc)]
use crate::Node;

impl<K, T, M, S> From<DependentWeave<K, T, M, S>> for NewDependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    fn from(mut value: DependentWeave<K, T, M, S>) -> Self {
        value.thread.clear();

        Self {
            scratchpad: value.thread,
            scratchpad_step_stack: value.scratchpad_step_stack,
            nodes: value.nodes,
            roots: value.roots,
            active: value.active,
            bookmarked: value.bookmarked,
            metadata: value.metadata,
        }
    }
}

/// A tree-based [`Weave`] where each [`Node`] depends on the contents of the previous Node.
#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
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
    thread: Vec<K>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad_step_stack: Vec<Step<K, K>>,

    /// The metadata associated with the weave.
    pub metadata: M,
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
        self.nodes.eq(&other.nodes)
            && self.roots.eq(&other.roots)
            && self.active.eq(&other.active)
            && self.bookmarked.eq(&other.bookmarked)
            && self.metadata.eq(&other.metadata)
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
            thread: Vec::with_capacity(capacity),
            scratchpad_step_stack: Vec::with_capacity(capacity),
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
        self.thread
            .reserve(self.nodes.capacity().saturating_sub(self.thread.len()));
        self.scratchpad_step_stack.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_step_stack.len()),
        );
    }
    /// Shrinks the capacity of the weave with a lower limit.
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.nodes.shrink_to(min_capacity);
        self.roots.shrink_to(min_capacity);
        self.bookmarked.shrink_to(min_capacity);
        self.thread.shrink_to(min_capacity);
        self.scratchpad_step_stack.shrink_to(min_capacity);
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
    fn get_node(&self, id: &K) -> Option<&DependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    fn get_ordered_node_identifiers(&mut self, output: &mut Vec<K>) {
        output.clear();
        self.thread.clear();

        for root in &self.roots {
            topological_sort(&self.nodes, *root, &mut self.thread, output);
        }
    }
    fn get_ordered_node_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();

        if self.nodes.contains_key(id) {
            self.thread.clear();
            topological_sort(&self.nodes, *id, &mut self.thread, output);
        }
    }
    fn get_active_thread(&mut self, output: &mut Vec<K>) {
        output.clear();

        if let Some(active) = self.active {
            path_to_root(&self.nodes, active, output);
        }
    }
    fn get_thread_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();

        if self.nodes.contains_key(id) {
            path_to_root(&self.nodes, *id, output);
        }
    }
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
    fn remove_node(&mut self, id: &K) -> Option<DependentNode<K, T, S>> {
        self.scratchpad_step_stack.push(Step::Enter(*id));

        while let Some(step) = self.scratchpad_step_stack.pop() {
            match step {
                Step::Enter(id) => {
                    if let Some(node) = self.nodes.get(&id) {
                        if node.from.is_none() {
                            self.roots.shift_remove(&id);
                        }
                        if node.bookmarked {
                            self.bookmarked.shift_remove(&id);
                        }

                        self.scratchpad_step_stack.push(Step::Exit(id));
                        self.scratchpad_step_stack
                            .extend(node.to.iter().rev().copied().map(Step::Enter));
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

                        if self.scratchpad_step_stack.is_empty() {
                            return Some(node);
                        }
                    }
                }
            }
        }

        None
    }
    fn remove_node_tracked(
        &mut self,
        id: &K,
        mut on_removal: impl FnMut(DependentNode<K, T, S>),
    ) -> bool {
        self.scratchpad_step_stack.push(Step::Enter(*id));

        while let Some(step) = self.scratchpad_step_stack.pop() {
            match step {
                Step::Enter(id) => {
                    if let Some(node) = self.nodes.get(&id) {
                        if node.from.is_none() {
                            self.roots.shift_remove(&id);
                        }
                        if node.bookmarked {
                            self.bookmarked.shift_remove(&id);
                        }

                        self.scratchpad_step_stack.push(Step::Exit(id));
                        self.scratchpad_step_stack
                            .extend(node.to.iter().rev().copied().map(Step::Enter));
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
                        if self.scratchpad_step_stack.is_empty() {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }
    fn remove_all_nodes(&mut self) {
        self.nodes.clear();
        self.roots.clear();
        self.active = None;
        self.bookmarked.clear();
    }
}

impl<K, T, M, S> ValidatableWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    fn validate(&self) -> bool {
        let nodes: IndexSet<_, _> = self.nodes.keys().copied().collect();
        let mut scratchpad = Vec::with_capacity(self.nodes.len());
        let mut scratchpad_set = HashSet::with_capacity_and_hasher(self.nodes.len(), S::default());

        self.scratchpad_step_stack.is_empty()
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
            && !detect_cycles(
                &self.nodes,
                self.roots.iter().copied(),
                &mut scratchpad,
                &mut scratchpad_set,
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
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    fn get_ordered_node_identifiers_reversed_children(&mut self, output: &mut Vec<K>) {
        output.clear();
        self.thread.clear();

        for root in &self.roots {
            topological_sort_rev(&self.nodes, *root, &mut self.thread, output);
        }
    }
    fn get_ordered_node_identifiers_from_reversed_children(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();

        if self.nodes.contains_key(id) {
            self.thread.clear();
            topological_sort_rev(&self.nodes, *id, &mut self.thread, output);
        }
    }
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
    fn sort_node_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool {
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

impl<K, T, M, S> DeduplicatableWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
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
impl<K, K2, T, T2, M, M2, S> ArchivedWeave<K::Archived, ArchivedDependentNode<K, T, S>, T::Archived>
    for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive<Archived = K2> + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
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
    K: Archive<Archived = K2> + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
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
    K: Archive<Archived = K2> + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
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
    K: Archive<Archived = K2> + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
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
    K: Archive<Archived = K2> + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive<Archived = T2>,
    M: Archive<Archived = M2>,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn active(&self) -> ArchivedOption<K::Archived> {
        self.active
    }
}
