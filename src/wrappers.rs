//! Wrappers which add additional functionality to [`Weave`] implementations

use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    hash::{BuildHasher, Hash},
    marker::PhantomData,
};

use crate::{
    ActivePathWeave, ActiveSingularWeave, DeduplicatableContents, DeduplicatableWeave,
    DiscreteContents, DiscreteWeave, IndependentContents, IndependentWeave, MetadataWeave, Node,
    SemiIndependentWeave, SortableWeave, Weave, dependent, independent,
};

#[cfg(feature = "rkyv")]
use rkyv::{Archive, Deserialize, Serialize};

#[cfg(feature = "wincode")]
use wincode::{SchemaRead, SchemaWrite};

#[cfg(feature = "serde")]
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

/// A [`Weave`] wrapper which logs actions successfully performed on the inner [`Weave`] in the order that they are performed.
///
/// See [`WeaveAction`] for the complete list of loggable actions.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub struct LoggedWeave<W, K, N, T, M>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    /// The [`Weave`] being wrapped.
    ///
    /// Actions performed directly on the inner [`Weave`] (without using the wrapper's functions) are not logged.
    pub weave: W,

    /// The list of actions that were performed on the attached [`Weave`] in the order they were performed.
    pub actions: VecDeque<WeaveAction<K, N, T, M>>,
}

impl<W, K, N, T, M> AsRef<W> for LoggedWeave<W, K, N, T, M>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    fn as_ref(&self) -> &W {
        &self.weave
    }
}

impl<W, K, N, T, M> From<W> for LoggedWeave<W, K, N, T, M>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    fn from(value: W) -> Self {
        Self {
            weave: value,
            actions: VecDeque::new(),
        }
    }
}

impl<W, K, N, T, M> LoggedWeave<W, K, N, T, M>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    pub fn new(weave: W, actions: VecDeque<WeaveAction<K, N, T, M>>) -> Self {
        Self { weave, actions }
    }
    pub fn into_weave(self) -> W {
        self.weave
    }
    pub fn clear_actions(&mut self) {
        self.actions.clear();
    }
    pub fn count_actions(&self) -> WeaveActionCount {
        let mut count = WeaveActionCount::new();

        for action in &self.actions {
            count.increment(action);
        }

        count
    }
    fn push_action(&mut self, action: WeaveAction<K, N, T, M>) {
        self.actions.push_back(action);
    }
}

/// An action performed on a [`Weave`] which changes its outwardly facing state.
///
/// When possible, actions map to a function of the [`Weave`] trait (or its supertraits), and use the same argument ordering as their corresponding function.
///
/// Some actions not logged here may change the [`Weave`]'s inner state but not its outwardly facing state.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub enum WeaveAction<K, N, T, M>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    /// [`Weave::add_node()`]
    AddNode(N),
    /// [`Weave::set_node_active_status()`]
    SetNodeActiveStatus(K, bool, bool),
    /// [`Weave::set_node_active_status_in_place()`]
    SetNodeActiveStatusInPlace(K, bool),
    /// [`Weave::set_node_bookmarked_status()`]
    SetNodeBookmarkedStatus(K, bool),
    /// [`Weave::remove_node()`]
    RemoveNode(K),
    /// [`Weave::remove_all_nodes()`]
    RemoveAllNodes,
    /// Caused by [`MetadataWeave::metadata_mut()`] or [`LoggedWeave::set_metadata()`]
    SetMetadata(M),
    /// (parent, children)
    /// Caused by [`SortableWeave::sort_node_children_by()`], [`SortableWeave::sort_node_children_by_id()`], [`SortableWeave::sort_roots_by()`], and [`SortableWeave::sort_roots_by_id()`]
    SetNodeChildOrdering(Option<K>, Vec<K>),
    /// Caused by [`SortableWeave::sort_bookmarks_by()`] and [`SortableWeave::sort_bookmarks_by_id()`]
    SetBookmarkOrdering(Vec<K>),
    /// [`IndependentWeave::move_node()`]
    MoveNode(K, Vec<K>),
    /// (id, contents)
    /// Caused by [`SemiIndependentWeave::get_contents_mut()`] or [`LoggedWeave::set_contents()`]
    SetNodeContent(K, T),
    /// [`DiscreteWeave::split_node()`]
    SplitNode(K, usize, K),
    /// [`DiscreteWeave::merge_with_parent()`]
    MergeNodeWithParent(K),
}

/// A [`Weave`] wrapper which logs the number of actions successfully performed on the inner [`Weave`].
///
/// See [`WeaveActionCount`] for the complete list of loggable actions.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub struct CountedWeave<W, K, N, T>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    /// The [`Weave`] being wrapped.
    ///
    /// Actions performed directly on the inner [`Weave`] (without using the wrapper's functions) are not logged.
    pub weave: W,

    /// The number of actions that were performed on the attached [`Weave`].
    pub count: WeaveActionCount,

    _phantom_k: PhantomData<K>,
    _phantom_n: PhantomData<N>,
    _phantom_t: PhantomData<T>,
}

impl<W, K, N, T> AsRef<W> for CountedWeave<W, K, N, T>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    fn as_ref(&self) -> &W {
        &self.weave
    }
}

impl<W, K, N, T> From<W> for CountedWeave<W, K, N, T>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    fn from(value: W) -> Self {
        Self {
            weave: value,
            count: WeaveActionCount::default(),
            _phantom_k: PhantomData,
            _phantom_n: PhantomData,
            _phantom_t: PhantomData,
        }
    }
}

impl<W, K, N, T> CountedWeave<W, K, N, T>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    pub fn new(weave: W, count: WeaveActionCount) -> Self {
        Self {
            weave,
            count,
            _phantom_k: PhantomData,
            _phantom_n: PhantomData,
            _phantom_t: PhantomData,
        }
    }
    pub fn into_weave(self) -> W {
        self.weave
    }
    pub fn reset_count(&mut self) {
        self.count.reset();
    }
}

/// The number of times actions changing the outwardly facing state of a [`Weave`] were performed.
///
/// When possible, actions map to a function of the [`Weave`] trait or its supertraits.
/// Some actions not logged here may change the [`Weave`]'s inner state but not its outwardly facing state.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub struct WeaveActionCount {
    /// [`Weave::add_node()`]
    pub add_node: usize,
    /// [`Weave::set_node_active_status()`]
    pub set_node_active_status: usize,
    /// [`Weave::set_node_active_status_in_place()`]
    pub set_node_active_status_in_place: usize,
    /// [`Weave::set_node_bookmarked_status()`]
    pub set_node_bookmarked_status: usize,
    /// [`Weave::remove_node()`]
    pub remove_node: usize,
    /// [`Weave::remove_all_nodes()`]
    pub remove_all_nodes: usize,
    /// [`MetadataWeave::metadata_mut()`]
    pub metadata_mut: usize,
    /// [`SortableWeave::sort_node_children_by()`] or [`SortableWeave::sort_node_children_by_id()`]
    pub sort_node_children: usize,
    /// [`SortableWeave::sort_roots_by()`] or [`SortableWeave::sort_roots_by_id()`]
    pub sort_roots: usize,
    /// [`SortableWeave::sort_bookmarks_by()`] or [`SortableWeave::sort_bookmarks_by_id()`]
    pub sort_bookmarks: usize,
    /// [`IndependentWeave::move_node()`]
    pub move_node: usize,
    /// [`SemiIndependentWeave::get_contents_mut()`]
    pub get_contents_mut: usize,
    /// [`DiscreteWeave::split_node()`]
    pub split_node: usize,
    /// [`DiscreteWeave::merge_with_parent()`]
    pub merge_with_parent: usize,
    /// User defined; Not incremented/decremented by the [`CountedWeave`] wrapper or [`WeaveActionCount`] functions
    pub other: usize,
}

impl WeaveActionCount {
    pub fn new() -> Self {
        WeaveActionCount::default()
    }
    /// Resets all action counts to zero.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
    /// Returns the sum of all action counts.
    pub fn total_count(&self) -> usize {
        self.add_node
            .saturating_add(self.set_node_active_status)
            .saturating_add(self.set_node_active_status_in_place)
            .saturating_add(self.set_node_bookmarked_status)
            .saturating_add(self.remove_node)
            .saturating_add(self.remove_all_nodes)
            .saturating_add(self.metadata_mut)
            .saturating_add(self.sort_node_children)
            .saturating_add(self.sort_roots)
            .saturating_add(self.sort_bookmarks)
            .saturating_add(self.move_node)
            .saturating_add(self.get_contents_mut)
            .saturating_add(self.split_node)
            .saturating_add(self.merge_with_parent)
            .saturating_add(self.other)
    }
    /// Increments the action count corresponding to the [`WeaveAction`]'s type.
    pub fn increment<K, N, T, M>(&mut self, action: &WeaveAction<K, N, T, M>)
    where
        K: Hash + Copy + Eq,
        N: Node<K, T>,
    {
        match action {
            WeaveAction::AddNode(_node) => self.add_node = self.add_node.saturating_add(1),
            WeaveAction::SetNodeActiveStatus(_id, _value, _alternate) => {
                self.set_node_active_status = self.set_node_active_status.saturating_add(1)
            }
            WeaveAction::SetNodeActiveStatusInPlace(_id, _value) => {
                self.set_node_active_status_in_place =
                    self.set_node_active_status_in_place.saturating_add(1)
            }
            WeaveAction::SetNodeBookmarkedStatus(_id, _value) => {
                self.set_node_bookmarked_status = self.set_node_bookmarked_status.saturating_add(1)
            }
            WeaveAction::RemoveNode(_id) => self.remove_node = self.remove_node.saturating_add(1),
            WeaveAction::RemoveAllNodes => {
                self.remove_all_nodes = self.remove_all_nodes.saturating_add(1)
            }
            WeaveAction::SetMetadata(_metadata) => {
                self.metadata_mut = self.metadata_mut.saturating_add(1)
            }
            WeaveAction::SetNodeChildOrdering(parent_id, _children) => match parent_id {
                Some(_id) => self.sort_node_children = self.sort_node_children.saturating_add(1),
                None => self.sort_roots = self.sort_roots.saturating_add(1),
            },
            WeaveAction::SetBookmarkOrdering(_ids) => {
                self.sort_bookmarks = self.sort_bookmarks.saturating_add(1)
            }
            WeaveAction::MoveNode(_id, _new_parents) => {
                self.move_node = self.move_node.saturating_add(1)
            }
            WeaveAction::SetNodeContent(_id, _contents) => {
                self.get_contents_mut = self.get_contents_mut.saturating_add(1)
            }
            WeaveAction::SplitNode(_id, _at, _new_id) => {
                self.split_node = self.split_node.saturating_add(1)
            }
            WeaveAction::MergeNodeWithParent(_id) => {
                self.merge_with_parent = self.merge_with_parent.saturating_add(1)
            }
        };
    }
    /// Decrements the action count corresponding to the [`WeaveAction`]'s type.
    pub fn decrement<K, N, T, M>(&mut self, action: &WeaveAction<K, N, T, M>)
    where
        K: Hash + Copy + Eq,
        N: Node<K, T>,
    {
        match action {
            WeaveAction::AddNode(_node) => self.add_node = self.add_node.saturating_sub(1),
            WeaveAction::SetNodeActiveStatus(_id, _value, _alternate) => {
                self.set_node_active_status = self.set_node_active_status.saturating_sub(1)
            }
            WeaveAction::SetNodeActiveStatusInPlace(_id, _value) => {
                self.set_node_active_status_in_place =
                    self.set_node_active_status_in_place.saturating_sub(1)
            }
            WeaveAction::SetNodeBookmarkedStatus(_id, _value) => {
                self.set_node_bookmarked_status = self.set_node_bookmarked_status.saturating_sub(1)
            }
            WeaveAction::RemoveNode(_id) => self.remove_node = self.remove_node.saturating_sub(1),
            WeaveAction::RemoveAllNodes => {
                self.remove_all_nodes = self.remove_all_nodes.saturating_sub(1)
            }
            WeaveAction::SetMetadata(_metadata) => {
                self.metadata_mut = self.metadata_mut.saturating_sub(1)
            }
            WeaveAction::SetNodeChildOrdering(parent_id, _children) => match parent_id {
                Some(_id) => self.sort_node_children = self.sort_node_children.saturating_sub(1),
                None => self.sort_roots = self.sort_roots.saturating_sub(1),
            },
            WeaveAction::SetBookmarkOrdering(_ids) => {
                self.sort_bookmarks = self.sort_bookmarks.saturating_sub(1)
            }
            WeaveAction::MoveNode(_id, _new_parents) => {
                self.move_node = self.move_node.saturating_sub(1)
            }
            WeaveAction::SetNodeContent(_id, _contents) => {
                self.get_contents_mut = self.get_contents_mut.saturating_sub(1)
            }
            WeaveAction::SplitNode(_id, _at, _new_id) => {
                self.split_node = self.split_node.saturating_sub(1)
            }
            WeaveAction::MergeNodeWithParent(_id) => {
                self.merge_with_parent = self.merge_with_parent.saturating_sub(1)
            }
        };
    }
}

/// A [`Weave`] which can have [`WeaveAction`]s applied to it.
pub trait ActionableWeave<K, N, T, M, S>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
    S: BuildHasher + Default + Clone,
{
    /// Applies a [`WeaveAction`] to a [`Weave`], panicking on failure.
    fn apply(&mut self, action: WeaveAction<K, N, T, M>);
}

/*impl<W, K, N, T, M, S> ActionableWeave<K, N, T, M, S> for W
where
    W: Weave<K, N, T>
        + MetadataWeave<K, N, T, M>
        + SortableWeave<K, N, T>
        + IndependentWeave<K, N, T>
        + SemiIndependentWeave<K, N, T>
        + DiscreteWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    fn apply(&mut self, action: WeaveAction<K, N, T, M>) {
        match action {
            WeaveAction::AddNode(node) => assert!(self.add_node(node)),
            WeaveAction::SetNodeActiveStatus(id, value, alternate) => {
                assert!(self.set_node_active_status(&id, value, alternate))
            }
            WeaveAction::SetNodeActiveStatusInPlace(id, value) => {
                assert!(self.set_node_active_status_in_place(&id, value))
            }
            WeaveAction::SetNodeBookmarkedStatus(id, value) => {
                assert!(self.set_node_bookmarked_status(&id, value))
            }
            WeaveAction::RemoveNode(id) => assert!(self.remove_node(&id).is_some()),
            WeaveAction::RemoveAllNodes => self.remove_all_nodes(),
            WeaveAction::SetMetadata(metadata) => {
                *self.metadata_mut() = metadata;
            }
            WeaveAction::SetNodeChildOrdering(parent_id, children) => {
                let mut id_mapping =
                    HashMap::with_capacity_and_hasher(children.len(), S::default());
                id_mapping.extend(
                    children
                        .into_iter()
                        .enumerate()
                        .map(|(index, id)| (id, index)),
                );

                match parent_id {
                    Some(id) => {
                        assert!(self.sort_node_children_by_id(&id, |a, b| {
                            id_mapping.get(a).unwrap().cmp(id_mapping.get(b).unwrap())
                        }))
                    }
                    None => {
                        self.sort_roots_by_id(|a, b| {
                            id_mapping.get(a).unwrap().cmp(id_mapping.get(b).unwrap())
                        });
                    }
                }
            }
            WeaveAction::SetBookmarkOrdering(ids) => {
                let mut id_mapping = HashMap::with_capacity_and_hasher(ids.len(), S::default());
                id_mapping.extend(ids.into_iter().enumerate().map(|(index, id)| (id, index)));

                self.sort_bookmarks_by_id(|a, b| {
                    id_mapping.get(a).unwrap().cmp(id_mapping.get(b).unwrap())
                });
            }
            WeaveAction::MoveNode(id, new_parents) => assert!(self.move_node(&id, &new_parents)),
            WeaveAction::SetNodeContent(id, contents) => {
                *self.get_contents_mut(&id).unwrap() = contents;
            }
            WeaveAction::SplitNode(id, at, new_id) => assert!(self.split_node(&id, at, new_id)),
            WeaveAction::MergeNodeWithParent(id) => assert!(self.merge_with_parent(&id).is_some()),
        }
    }
}*/

// Replace this if/when specialization lands in stable
impl<K, T, M, S> ActionableWeave<K, dependent::DependentNode<K, T, S>, T, M, S>
    for dependent::DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    fn apply(&mut self, action: WeaveAction<K, dependent::DependentNode<K, T, S>, T, M>) {
        match action {
            WeaveAction::AddNode(node) => assert!(self.add_node(node)),
            WeaveAction::SetNodeActiveStatus(id, value, alternate) => {
                assert!(self.set_node_active_status(&id, value, alternate))
            }
            WeaveAction::SetNodeActiveStatusInPlace(id, value) => {
                assert!(self.set_node_active_status_in_place(&id, value))
            }
            WeaveAction::SetNodeBookmarkedStatus(id, value) => {
                assert!(self.set_node_bookmarked_status(&id, value))
            }
            WeaveAction::RemoveNode(id) => assert!(self.remove_node(&id).is_some()),
            WeaveAction::RemoveAllNodes => self.remove_all_nodes(),
            WeaveAction::SetMetadata(metadata) => {
                *self.metadata_mut() = metadata;
            }
            WeaveAction::SetNodeChildOrdering(parent_id, children) => {
                let mut id_mapping =
                    HashMap::with_capacity_and_hasher(children.len(), S::default());
                id_mapping.extend(
                    children
                        .into_iter()
                        .enumerate()
                        .map(|(index, id)| (id, index)),
                );

                match parent_id {
                    Some(id) => {
                        assert!(self.sort_node_children_by_id(&id, |a, b| {
                            id_mapping.get(a).unwrap().cmp(id_mapping.get(b).unwrap())
                        }))
                    }
                    None => {
                        self.sort_roots_by_id(|a, b| {
                            id_mapping.get(a).unwrap().cmp(id_mapping.get(b).unwrap())
                        });
                    }
                }
            }
            WeaveAction::SetBookmarkOrdering(ids) => {
                let mut id_mapping = HashMap::with_capacity_and_hasher(ids.len(), S::default());
                id_mapping.extend(ids.into_iter().enumerate().map(|(index, id)| (id, index)));

                self.sort_bookmarks_by_id(|a, b| {
                    id_mapping.get(a).unwrap().cmp(id_mapping.get(b).unwrap())
                });
            }
            WeaveAction::MoveNode(_id, _new_parents) => unimplemented!(),
            WeaveAction::SetNodeContent(id, contents) => {
                *self.get_contents_mut(&id).unwrap() = contents;
            }
            WeaveAction::SplitNode(id, at, new_id) => assert!(self.split_node(&id, at, new_id)),
            WeaveAction::MergeNodeWithParent(id) => assert!(self.merge_with_parent(&id).is_some()),
        }
    }
}

// Replace this if/when specialization lands in stable
impl<K, T, M, S> ActionableWeave<K, independent::IndependentNode<K, T, S>, T, M, S>
    for independent::IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    fn apply(&mut self, action: WeaveAction<K, independent::IndependentNode<K, T, S>, T, M>) {
        match action {
            WeaveAction::AddNode(node) => assert!(self.add_node(node)),
            WeaveAction::SetNodeActiveStatus(id, value, alternate) => {
                assert!(self.set_node_active_status(&id, value, alternate))
            }
            WeaveAction::SetNodeActiveStatusInPlace(id, value) => {
                assert!(self.set_node_active_status_in_place(&id, value))
            }
            WeaveAction::SetNodeBookmarkedStatus(id, value) => {
                assert!(self.set_node_bookmarked_status(&id, value))
            }
            WeaveAction::RemoveNode(id) => assert!(self.remove_node(&id).is_some()),
            WeaveAction::RemoveAllNodes => self.remove_all_nodes(),
            WeaveAction::SetMetadata(metadata) => {
                *self.metadata_mut() = metadata;
            }
            WeaveAction::SetNodeChildOrdering(parent_id, children) => {
                let mut id_mapping =
                    HashMap::with_capacity_and_hasher(children.len(), S::default());
                id_mapping.extend(
                    children
                        .into_iter()
                        .enumerate()
                        .map(|(index, id)| (id, index)),
                );

                match parent_id {
                    Some(id) => {
                        assert!(self.sort_node_children_by_id(&id, |a, b| {
                            id_mapping.get(a).unwrap().cmp(id_mapping.get(b).unwrap())
                        }))
                    }
                    None => {
                        self.sort_roots_by_id(|a, b| {
                            id_mapping.get(a).unwrap().cmp(id_mapping.get(b).unwrap())
                        });
                    }
                }
            }
            WeaveAction::SetBookmarkOrdering(ids) => {
                let mut id_mapping = HashMap::with_capacity_and_hasher(ids.len(), S::default());
                id_mapping.extend(ids.into_iter().enumerate().map(|(index, id)| (id, index)));

                self.sort_bookmarks_by_id(|a, b| {
                    id_mapping.get(a).unwrap().cmp(id_mapping.get(b).unwrap())
                });
            }
            WeaveAction::MoveNode(id, new_parents) => assert!(self.move_node(&id, &new_parents)),
            WeaveAction::SetNodeContent(id, contents) => {
                *self.get_contents_mut(&id).unwrap() = contents;
            }
            WeaveAction::SplitNode(id, at, new_id) => assert!(self.split_node(&id, at, new_id)),
            WeaveAction::MergeNodeWithParent(id) => assert!(self.merge_with_parent(&id).is_some()),
        }
    }
}

impl<W, K, N, T, M> Weave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T> + Clone,
{
    type Nodes = W::Nodes;
    type Roots = W::Roots;
    type Bookmarks = W::Bookmarks;

    fn len(&self) -> usize {
        self.weave.len()
    }
    fn is_empty(&self) -> bool {
        self.weave.is_empty()
    }
    fn nodes(&self) -> &Self::Nodes {
        self.weave.nodes()
    }
    fn roots(&self) -> &Self::Roots {
        self.weave.roots()
    }
    fn bookmarks(&self) -> &Self::Bookmarks {
        self.weave.bookmarks()
    }
    fn contains(&self, id: &K) -> bool {
        self.weave.contains(id)
    }
    fn contains_active(&self, id: &K) -> bool {
        self.weave.contains_active(id)
    }
    fn contains_bookmark(&self, id: &K) -> bool {
        self.weave.contains_bookmark(id)
    }
    fn get_node(&self, id: &K) -> Option<&N> {
        self.weave.get_node(id)
    }
    fn get_ordered_node_identifiers(&mut self, output: &mut Vec<K>) {
        self.weave.get_ordered_node_identifiers(output);
    }
    fn get_active_thread(&mut self, output: &mut Vec<K>) {
        self.weave.get_active_thread(output);
    }
    fn get_thread_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_thread_from(id, output);
    }
    fn add_node(&mut self, node: N) -> bool {
        if self.weave.add_node(node.clone()) {
            self.push_action(WeaveAction::AddNode(node));
            true
        } else {
            false
        }
    }
    fn set_node_active_status(&mut self, id: &K, value: bool, alternate: bool) -> bool {
        if self.weave.set_node_active_status(id, value, alternate) {
            self.push_action(WeaveAction::SetNodeActiveStatus(*id, value, alternate));
            true
        } else {
            false
        }
    }
    fn set_node_active_status_in_place(&mut self, id: &K, value: bool) -> bool {
        if self.weave.set_node_active_status_in_place(id, value) {
            self.push_action(WeaveAction::SetNodeActiveStatusInPlace(*id, value));
            true
        } else {
            false
        }
    }
    fn set_node_bookmarked_status(&mut self, id: &K, value: bool) -> bool {
        if self.weave.set_node_bookmarked_status(id, value) {
            self.push_action(WeaveAction::SetNodeBookmarkedStatus(*id, value));
            true
        } else {
            false
        }
    }
    fn remove_node(&mut self, id: &K) -> Option<N> {
        if let Some(removed) = self.weave.remove_node(id) {
            self.push_action(WeaveAction::RemoveNode(*id));
            Some(removed)
        } else {
            None
        }
    }
    fn remove_all_nodes(&mut self) {
        self.push_action(WeaveAction::RemoveAllNodes);
        self.weave.remove_all_nodes();
    }
}

impl<W, K, N, T, M> LoggedWeave<W, K, N, T, M>
where
    W: MetadataWeave<K, N, T, M>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
    M: Clone,
{
    pub fn metadata(&self) -> &M {
        self.weave.metadata()
    }
    /// Must be used instead of [`MetadataWeave::metadata()`]
    pub fn set_metadata(&mut self, metadata: M) {
        *self.weave.metadata_mut() = metadata;
        let metadata = self.weave.metadata().clone();
        self.push_action(WeaveAction::SetMetadata(metadata));
    }
}

impl<W, K, N, T, M> SortableWeave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: SortableWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T> + Clone,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a W::Bookmarks: IntoIterator<Item = &'a K>,
{
    fn get_ordered_node_identifiers_reversed_children(&mut self, output: &mut Vec<K>) {
        self.weave
            .get_ordered_node_identifiers_reversed_children(output);
    }
    fn sort_node_children_by(&mut self, id: &K, cmp: impl FnMut(&N, &N) -> Ordering) -> bool {
        if self.weave.sort_node_children_by(id, cmp) {
            self.push_action(WeaveAction::SetNodeChildOrdering(
                Some(*id),
                self.weave
                    .get_node(id)
                    .unwrap()
                    .to()
                    .into_iter()
                    .copied()
                    .collect(),
            ));
            true
        } else {
            false
        }
    }
    fn sort_node_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool {
        if self.weave.sort_node_children_by_id(id, cmp) {
            self.push_action(WeaveAction::SetNodeChildOrdering(
                Some(*id),
                self.weave
                    .get_node(id)
                    .unwrap()
                    .to()
                    .into_iter()
                    .copied()
                    .collect(),
            ));
            true
        } else {
            false
        }
    }
    fn sort_roots_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering) {
        self.weave.sort_roots_by(cmp);
        self.push_action(WeaveAction::SetNodeChildOrdering(
            None,
            self.weave.roots().into_iter().copied().collect(),
        ));
    }
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.weave.sort_roots_by_id(cmp);
        self.push_action(WeaveAction::SetNodeChildOrdering(
            None,
            self.weave.roots().into_iter().copied().collect(),
        ));
    }
    fn sort_bookmarks_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering) {
        self.weave.sort_bookmarks_by(cmp);
        self.push_action(WeaveAction::SetBookmarkOrdering(
            self.weave.bookmarks().into_iter().copied().collect(),
        ));
    }
    fn sort_bookmarks_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.weave.sort_bookmarks_by_id(cmp);
        self.push_action(WeaveAction::SetBookmarkOrdering(
            self.weave.bookmarks().into_iter().copied().collect(),
        ));
    }
}

impl<W, K, N, T, M> ActiveSingularWeave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: ActiveSingularWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T> + Clone,
{
    fn active(&self) -> Option<K> {
        self.weave.active()
    }
}

impl<W, K, N, T, M> ActivePathWeave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: ActivePathWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T> + Clone,
{
    type Active = W::Active;

    fn active(&self) -> &Self::Active {
        self.weave.active()
    }
}

impl<W, K, N, T, M> IndependentWeave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T> + Clone,
    T: IndependentContents,
{
    fn move_node(&mut self, id: &K, new_parents: &[K]) -> bool {
        if self.weave.move_node(id, new_parents) {
            self.push_action(WeaveAction::MoveNode(*id, new_parents.to_vec()));
            true
        } else {
            false
        }
    }
}

impl<W, K, N, T, M> SemiIndependentWeave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: SemiIndependentWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T> + Clone,
    T: IndependentContents,
{
    /// Intentionally unimplemented; Use [`LoggedWeave::set_contents()`] instead!
    fn get_contents_mut(&mut self, _id: &K) -> Option<&mut T> {
        unimplemented!("Intentionally unimplemented; Use LoggedWeave::set_contents() instead");
    }
}

impl<W, K, N, T, M> LoggedWeave<W, K, N, T, M>
where
    W: SemiIndependentWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
    T: IndependentContents + Clone,
{
    /// Must be used instead of [`SemiIndependentWeave::get_contents_mut()`]
    pub fn set_contents<O>(&mut self, id: &K, callback: impl FnOnce(&mut T) -> O) -> Option<O> {
        if let Some(contents) = self.weave.get_contents_mut(id) {
            let output = callback(contents);
            let contents = contents.clone();
            self.push_action(WeaveAction::SetNodeContent(*id, contents));
            Some(output)
        } else {
            None
        }
    }
}

impl<W, K, N, T, M> DiscreteWeave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: DiscreteWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T> + Clone,
    T: DiscreteContents,
{
    fn split_node(&mut self, id: &K, at: usize, new_id: K) -> bool {
        if self.weave.split_node(id, at, new_id) {
            self.push_action(WeaveAction::SplitNode(*id, at, new_id));
            true
        } else {
            false
        }
    }
    fn merge_with_parent(&mut self, id: &K) -> Option<K> {
        match self.weave.merge_with_parent(id) {
            Some(new_id) => {
                self.push_action(WeaveAction::MergeNodeWithParent(*id));
                Some(new_id)
            }
            None => None,
        }
    }
}

impl<W, K, N, T, M> DeduplicatableWeave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: DeduplicatableWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T> + Clone,
    T: DeduplicatableContents,
{
    fn find_duplicates(&self, id: &K) -> impl Iterator<Item = K> {
        self.weave.find_duplicates(id)
    }
}

impl<W, K, N, T> Weave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    type Nodes = W::Nodes;
    type Roots = W::Roots;
    type Bookmarks = W::Bookmarks;

    fn len(&self) -> usize {
        self.weave.len()
    }
    fn is_empty(&self) -> bool {
        self.weave.is_empty()
    }
    fn nodes(&self) -> &Self::Nodes {
        self.weave.nodes()
    }
    fn roots(&self) -> &Self::Roots {
        self.weave.roots()
    }
    fn bookmarks(&self) -> &Self::Bookmarks {
        self.weave.bookmarks()
    }
    fn contains(&self, id: &K) -> bool {
        self.weave.contains(id)
    }
    fn contains_active(&self, id: &K) -> bool {
        self.weave.contains_active(id)
    }
    fn contains_bookmark(&self, id: &K) -> bool {
        self.weave.contains_bookmark(id)
    }
    fn get_node(&self, id: &K) -> Option<&N> {
        self.weave.get_node(id)
    }
    fn get_ordered_node_identifiers(&mut self, output: &mut Vec<K>) {
        self.weave.get_ordered_node_identifiers(output);
    }
    fn get_active_thread(&mut self, output: &mut Vec<K>) {
        self.weave.get_active_thread(output);
    }
    fn get_thread_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_thread_from(id, output);
    }
    fn add_node(&mut self, node: N) -> bool {
        if self.weave.add_node(node) {
            self.count.add_node = self.count.add_node.saturating_add(1);
            true
        } else {
            false
        }
    }
    fn set_node_active_status(&mut self, id: &K, value: bool, alternate: bool) -> bool {
        if self.weave.set_node_active_status(id, value, alternate) {
            self.count.set_node_active_status = self.count.set_node_active_status.saturating_add(1);
            true
        } else {
            false
        }
    }
    fn set_node_active_status_in_place(&mut self, id: &K, value: bool) -> bool {
        if self.weave.set_node_active_status_in_place(id, value) {
            self.count.set_node_active_status_in_place =
                self.count.set_node_active_status_in_place.saturating_add(1);
            true
        } else {
            false
        }
    }
    fn set_node_bookmarked_status(&mut self, id: &K, value: bool) -> bool {
        if self.weave.set_node_bookmarked_status(id, value) {
            self.count.set_node_bookmarked_status =
                self.count.set_node_bookmarked_status.saturating_add(1);
            true
        } else {
            false
        }
    }
    fn remove_node(&mut self, id: &K) -> Option<N> {
        if let Some(removed) = self.weave.remove_node(id) {
            self.count.remove_node = self.count.remove_node.saturating_add(1);
            Some(removed)
        } else {
            None
        }
    }
    fn remove_all_nodes(&mut self) {
        self.count.remove_all_nodes = self.count.remove_all_nodes.saturating_add(1);
        self.weave.remove_all_nodes();
    }
}

impl<W, K, N, T, M> MetadataWeave<K, N, T, M> for CountedWeave<W, K, N, T>
where
    W: MetadataWeave<K, N, T, M>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    fn metadata(&self) -> &M {
        self.weave.metadata()
    }
    fn metadata_mut(&mut self) -> &mut M {
        self.count.metadata_mut = self.count.metadata_mut.saturating_add(1);
        self.weave.metadata_mut()
    }
}

impl<W, K, N, T> SortableWeave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: SortableWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    fn get_ordered_node_identifiers_reversed_children(&mut self, output: &mut Vec<K>) {
        self.weave
            .get_ordered_node_identifiers_reversed_children(output);
    }
    fn sort_node_children_by(&mut self, id: &K, cmp: impl FnMut(&N, &N) -> Ordering) -> bool {
        if self.weave.sort_node_children_by(id, cmp) {
            self.count.sort_node_children = self.count.sort_node_children.saturating_add(1);
            true
        } else {
            false
        }
    }
    fn sort_node_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool {
        if self.weave.sort_node_children_by_id(id, cmp) {
            self.count.sort_node_children = self.count.sort_node_children.saturating_add(1);
            true
        } else {
            false
        }
    }
    fn sort_roots_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering) {
        self.count.sort_roots = self.count.sort_roots.saturating_add(1);
        self.weave.sort_roots_by(cmp);
    }
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.count.sort_roots = self.count.sort_roots.saturating_add(1);
        self.weave.sort_roots_by_id(cmp);
    }
    fn sort_bookmarks_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering) {
        self.count.sort_bookmarks = self.count.sort_bookmarks.saturating_add(1);
        self.weave.sort_bookmarks_by(cmp);
    }
    fn sort_bookmarks_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.count.sort_bookmarks = self.count.sort_bookmarks.saturating_add(1);
        self.weave.sort_bookmarks_by_id(cmp);
    }
}

impl<W, K, N, T> ActiveSingularWeave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: ActiveSingularWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    fn active(&self) -> Option<K> {
        self.weave.active()
    }
}

impl<W, K, N, T> ActivePathWeave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: ActivePathWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    type Active = W::Active;

    fn active(&self) -> &Self::Active {
        self.weave.active()
    }
}

impl<W, K, N, T> IndependentWeave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
    T: IndependentContents,
{
    fn move_node(&mut self, id: &K, new_parents: &[K]) -> bool {
        if self.weave.move_node(id, new_parents) {
            self.count.move_node = self.count.move_node.saturating_add(1);
            true
        } else {
            false
        }
    }
}

impl<W, K, N, T> SemiIndependentWeave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: SemiIndependentWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
    T: IndependentContents,
{
    fn get_contents_mut(&mut self, id: &K) -> Option<&mut T> {
        self.count.get_contents_mut = self.count.get_contents_mut.saturating_add(1);
        self.weave.get_contents_mut(id)
    }
}

impl<W, K, N, T> DiscreteWeave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
    T: DiscreteContents,
{
    fn split_node(&mut self, id: &K, at: usize, new_id: K) -> bool {
        if self.weave.split_node(id, at, new_id) {
            self.count.split_node = self.count.split_node.saturating_add(1);
            true
        } else {
            false
        }
    }
    fn merge_with_parent(&mut self, id: &K) -> Option<K> {
        match self.weave.merge_with_parent(id) {
            Some(new_id) => {
                self.count.merge_with_parent = self.count.merge_with_parent.saturating_add(1);
                Some(new_id)
            }
            None => None,
        }
    }
}

impl<W, K, N, T> DeduplicatableWeave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: DeduplicatableWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
    T: DeduplicatableContents,
{
    fn find_duplicates(&self, id: &K) -> impl Iterator<Item = K> {
        self.weave.find_duplicates(id)
    }
}
