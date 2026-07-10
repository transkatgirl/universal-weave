//! Wrappers which add additional functionality to [`Weave`] implementations

use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    hash::{BuildHasher, Hash},
};

use crate::{
    ActivePathWeave, ActiveSingularWeave, DeduplicatableContents, DeduplicatableWeave,
    DiscreteContents, DiscreteWeave, IndependentContents, IndependentWeave, Node,
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
pub struct LoggedWeave<W, K, N, T>
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
    pub actions: VecDeque<WeaveAction<K, N, T>>,
}

impl<W, K, N, T> AsRef<W> for LoggedWeave<W, K, N, T>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    fn as_ref(&self) -> &W {
        &self.weave
    }
}

impl<W, K, N, T> From<W> for LoggedWeave<W, K, N, T>
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

impl<W, K, N, T> LoggedWeave<W, K, N, T>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    pub fn new(weave: W, actions: VecDeque<WeaveAction<K, N, T>>) -> Self {
        Self { weave, actions }
    }
    pub fn into_weave(self) -> W {
        self.weave
    }
    fn push_action(&mut self, action: WeaveAction<K, N, T>) {
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
pub enum WeaveAction<K, N, T>
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
    /// (parent, children)
    /// Caused by [`SortableWeave::sort_node_children_by()`], [`SortableWeave::sort_node_children_by_id()`], [`SortableWeave::sort_roots_by()`], and [`SortableWeave::sort_roots_by_id()`]
    SetNodeChildOrdering(Option<K>, Vec<K>),
    /// Caused by [`SortableWeave::sort_bookmarks_by()`] and [`SortableWeave::sort_bookmarks_by_id()`]
    SetBookmarkOrdering(Vec<K>),
    /// [`IndependentWeave::move_node()`]
    MoveNode(K, Vec<K>),
    /// (id, contents)
    /// Caused by [`SemiIndependentWeave::get_contents_mut()`]
    SetNodeContent(K, T),
    /// [`DiscreteWeave::split_node()`]
    SplitNode(K, usize, K),
    /// [`DiscreteWeave::merge_with_parent()`]
    MergeNodeWithParent(K),
}

/// A [`Weave`] which can have [`WeaveAction`]s applied to it.
pub trait ActionableWeave<K, N, T, S>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
    S: BuildHasher + Default + Clone,
{
    /// Applies a [`WeaveAction`] to a [`Weave`], panicking on failure.
    fn apply(&mut self, action: WeaveAction<K, N, T>);
}

/*impl<W, K, N, T, S> ActionableWeave<K, N, T, S> for W
where
    W: Weave<K, N, T>
        + SortableWeave<K, N, T>
        + IndependentWeave<K, N, T>
        + SemiIndependentWeave<K, N, T>
        + DiscreteWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T>,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    fn apply(&mut self, action: WeaveAction<K, N, T>) {
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
impl<K, T, M, S> ActionableWeave<K, dependent::DependentNode<K, T, S>, T, S>
    for dependent::DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    fn apply(&mut self, action: WeaveAction<K, dependent::DependentNode<K, T, S>, T>) {
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
impl<K, T, M, S> ActionableWeave<K, independent::IndependentNode<K, T, S>, T, S>
    for independent::IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    fn apply(&mut self, action: WeaveAction<K, independent::IndependentNode<K, T, S>, T>) {
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

impl<W, K, N, T> Weave<K, N, T> for LoggedWeave<W, K, N, T>
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
}

impl<W, K, N, T> SortableWeave<K, N, T> for LoggedWeave<W, K, N, T>
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

impl<W, K, N, T> ActiveSingularWeave<K, N, T> for LoggedWeave<W, K, N, T>
where
    W: ActiveSingularWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T> + Clone,
{
    fn active(&self) -> Option<K> {
        self.weave.active()
    }
}

impl<W, K, N, T> ActivePathWeave<K, N, T> for LoggedWeave<W, K, N, T>
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

impl<W, K, N, T> IndependentWeave<K, N, T> for LoggedWeave<W, K, N, T>
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

impl<W, K, N, T> SemiIndependentWeave<K, N, T> for LoggedWeave<W, K, N, T>
where
    W: SemiIndependentWeave<K, N, T>,
    K: Hash + Copy + Eq,
    N: Node<K, T> + Clone,
    T: IndependentContents,
{
    /// Intentionally unimplemented; Calling this function will panic!
    ///
    /// Creating a [`WeaveAction::SetNodeContent`] must be done manually after accessing the wrapper's inner [`Weave`]
    fn get_contents_mut(&mut self, _id: &K) -> Option<&mut T> {
        unimplemented!()
    }
}

impl<W, K, N, T> DiscreteWeave<K, N, T> for LoggedWeave<W, K, N, T>
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

impl<W, K, N, T> DeduplicatableWeave<K, N, T> for LoggedWeave<W, K, N, T>
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
