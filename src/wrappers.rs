//! Wrappers which add additional functionality to [`Weave`] implementations.

#![allow(missing_docs, reason = "False positives")]

use alloc::{collections::VecDeque, vec::Vec};
use core::{
    cmp::Ordering,
    hash::{BuildHasher, Hash},
    iter,
    marker::PhantomData,
    ops::Range,
};

use hashbrown::{HashMap, HashSet};

#[cfg(feature = "rkyv")]
use rkyv::{Archive, Deserialize, Serialize};

#[cfg(feature = "serde")]
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

use crate::{
    ActivePathWeave, ActiveSingularWeave, BookmarkableWeave, BuildableNode, DeduplicatableContents,
    DiscreteContentResult, DiscreteContents, DiscreteWeave, IndependentContents, IndependentWeave,
    MetadataWeave, Node, SemiIndependentWeave, SortableBookmarkableWeave, SortableWeave, Weave,
    dependent, independent,
};

/// A [`Weave`] wrapper which logs actions successfully performed on the inner [`Weave`] in the order that they are performed.
///
/// See [`WeaveAction`] for the complete list of loggable actions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[must_use]
pub struct LoggedWeave<W, K, N, T, M>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T> + Clone,
{
    /// The [`Weave`] being wrapped.
    ///
    /// Actions performed directly on the inner [`Weave`] (without using the wrapper's functions) are not logged.
    pub weave: W,

    /// The list of actions that were performed on the attached [`Weave`] in the order they were performed.
    pub actions: VecDeque<WeaveAction<K, N, T, M>>,
}

impl<W, K, N, T, M> Default for LoggedWeave<W, K, N, T, M>
where
    W: Weave<K, N, T> + Default,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T> + Clone,
{
    #[inline]
    fn default() -> Self {
        Self::new(W::default())
    }
}

impl<W, K, N, T, M> AsRef<W> for LoggedWeave<W, K, N, T, M>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T> + Clone,
{
    #[inline]
    fn as_ref(&self) -> &W {
        &self.weave
    }
}

impl<W, K, N, T, M> From<W> for LoggedWeave<W, K, N, T, M>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T> + Clone,
{
    #[inline]
    fn from(value: W) -> Self {
        Self::new(value)
    }
}

impl<W, K, N, T, M> LoggedWeave<W, K, N, T, M>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T> + Clone,
{
    /// Creates a new [`LoggedWeave`] from a [`Weave`].
    #[inline]
    pub const fn new(weave: W) -> Self {
        Self {
            actions: VecDeque::new(),
            weave,
        }
    }
    /// Creates a new [`LoggedWeave`] with at least the specified capacity from a [`Weave`].
    #[inline]
    pub fn with_capacity(weave: W, capacity: usize) -> Self {
        Self {
            actions: VecDeque::with_capacity(capacity),
            weave,
        }
    }
    /// Creates a new [`LoggedWeave`] from a [`Weave`].
    #[inline]
    pub const fn from_weave(weave: W) -> Self {
        Self::new(weave)
    }
    /// Converts a [`LoggedWeave`] into it's inner [`Weave`].
    #[inline]
    pub fn into_weave(self) -> W {
        self.weave
    }
    /// Returns a reference to the inner [`Weave`].
    #[inline]
    pub const fn as_weave(&self) -> &W {
        &self.weave
    }
    /// Returns a reference to the list of actions performed on the [`Weave`].
    #[inline]
    pub const fn as_actions(&self) -> &VecDeque<WeaveAction<K, N, T, M>> {
        &self.actions
    }
    /// Clears the inner list of actions performed on the [`Weave`].
    #[inline]
    pub fn clear_actions(&mut self) {
        self.actions.clear();
    }
    /// Returns a [`WeaveActionCount`] calculated from the inner list of actions performed on the [`Weave`].
    #[inline]
    pub fn count_actions(&self) -> WeaveActionCount {
        let mut count = WeaveActionCount::new();

        for action in &self.actions {
            count.increment(action);
        }

        count
    }
}

/// An action performed on a [`Weave`] which changes its outwardly facing state.
///
/// When possible, actions map to a function of the [`Weave`] trait (or its supertraits), and use the same argument ordering as their corresponding function.
///
/// Some actions not logged here may change the [`Weave`]'s inner state but not its outwardly facing state.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[allow(clippy::doc_paragraphs_missing_punctuation, reason = "False positive")]
#[non_exhaustive]
#[must_use]
pub enum WeaveAction<K, N, T, M>
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
{
    /// [`Weave::insert()`]
    Insert(N),
    /// [`Weave::set_active()`]
    SetActive { id: K, value: bool },
    /// [`BookmarkableWeave::set_bookmarked()`]
    SetBookmarked { id: K, value: bool },
    /// [`Weave::remove()`] or [`Weave::remove_tracked()`]
    Remove(K),
    /// [`Weave::clear()`]
    Clear,
    /// [`MetadataWeave::metadata_mut()`]
    SetMetadata(M),
    /// Caused by [`SortableWeave::sort_children_by()`], [`SortableWeave::sort_children_by_id()`], [`SortableWeave::sort_roots_by()`], and [`SortableWeave::sort_roots_by_id()`]
    SetChildOrdering { parent: Option<K>, children: Vec<K> },
    /// Caused by [`SortableBookmarkableWeave::sort_bookmarks_by()`] and [`SortableBookmarkableWeave::sort_bookmarks_by_id()`]
    SetBookmarkOrdering(Vec<K>),
    /// [`ActivePathWeave::set_active_path()`]
    SetActivePath(Vec<K>),
    /// [`IndependentWeave::move_to()`]
    MoveTo { id: K, new_parents: Vec<K> },
    /// Caused by [`SemiIndependentWeave::get_contents_mut()`]
    SetContents { id: K, contents: T },
    /// [`DiscreteWeave::split()`]
    Split { id: K, at: usize, new_id: K },
    /// [`DiscreteWeave::merge_with_parent()`]
    MergeWithParent(K),
}

/// A [`Weave`] wrapper which logs the number of actions successfully performed on the inner [`Weave`].
///
/// See [`WeaveActionCount`] for the complete list of loggable actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[must_use]
pub struct CountedWeave<W, K, N, T>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
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

impl<W, K, N, T> Default for CountedWeave<W, K, N, T>
where
    W: Weave<K, N, T> + Default,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
{
    #[inline]
    fn default() -> Self {
        Self::from_weave(W::default())
    }
}

impl<W, K, N, T> AsRef<W> for CountedWeave<W, K, N, T>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
{
    #[inline]
    fn as_ref(&self) -> &W {
        &self.weave
    }
}

impl<W, K, N, T> From<W> for CountedWeave<W, K, N, T>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
{
    #[inline]
    fn from(value: W) -> Self {
        Self::new(value, WeaveActionCount::new())
    }
}

impl<W, K, N, T> CountedWeave<W, K, N, T>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
{
    /// Creates a [`CountedWeave`] from a [`Weave`] and [`WeaveActionCount`] pair.
    #[inline]
    pub const fn new(weave: W, count: WeaveActionCount) -> Self {
        Self {
            weave,
            count,
            _phantom_k: PhantomData,
            _phantom_n: PhantomData,
            _phantom_t: PhantomData,
        }
    }
    /// Creates a new [`CountedWeave`] from a [`Weave`].
    #[inline]
    pub fn from_weave(weave: W) -> Self {
        Self::new(weave, WeaveActionCount::new())
    }
    /// Converts a [`CountedWeave`] into it's inner [`Weave`].
    #[inline]
    pub fn into_weave(self) -> W {
        self.weave
    }
    /// Returns a reference to the inner [`Weave`].
    #[inline]
    pub const fn as_weave(&self) -> &W {
        &self.weave
    }
    /// Returns a reference to the inner [`WeaveActionCount`].
    #[inline]
    pub const fn as_count(&self) -> &WeaveActionCount {
        &self.count
    }
    /// Resets the inner [`WeaveActionCount`] to zero.
    #[inline]
    pub fn reset_count(&mut self) {
        self.count.reset();
    }
}

/// The number of times actions changing the outwardly facing state of a [`Weave`] were performed.
///
/// When possible, actions map to a function of the [`Weave`] trait or its supertraits.
/// Some actions not logged here may change the [`Weave`]'s inner state but not its outwardly facing state.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[allow(clippy::doc_paragraphs_missing_punctuation, reason = "False positive")]
#[non_exhaustive]
#[must_use]
pub struct WeaveActionCount {
    /// [`Weave::insert()`]
    pub insert: usize,
    /// [`Weave::set_active()`]
    pub set_active: usize,
    /// [`BookmarkableWeave::set_bookmarked()`]
    pub set_bookmarked: usize,
    /// [`Weave::remove()`] or [`Weave::remove_tracked()`]
    pub remove: usize,
    /// [`Weave::clear()`]
    pub clear: usize,
    /// [`MetadataWeave::metadata_mut()`]
    pub metadata_mut: usize,
    /// [`SortableWeave::sort_children_by()`] or [`SortableWeave::sort_children_by_id()`]
    pub sort_children: usize,
    /// [`SortableWeave::sort_roots_by()`] or [`SortableWeave::sort_roots_by_id()`]
    pub sort_roots: usize,
    /// [`SortableBookmarkableWeave::sort_bookmarks_by()`] or [`SortableBookmarkableWeave::sort_bookmarks_by_id()`]
    pub sort_bookmarks: usize,
    /// [`ActivePathWeave::set_active_path()`]
    pub set_active_path: usize,
    /// [`IndependentWeave::move_to()`]
    pub move_to: usize,
    /// [`SemiIndependentWeave::get_contents_mut()`]
    pub get_contents_mut: usize,
    /// [`DiscreteWeave::split()`]
    pub split: usize,
    /// [`DiscreteWeave::merge_with_parent()`]
    pub merge_with_parent: usize,
    /// User defined; Not incremented/decremented by the [`CountedWeave`] wrapper or [`WeaveActionCount`] functions.
    pub other: usize,
}

impl WeaveActionCount {
    /// Creates a new action count initalized to zero.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
    /// Resets all action counts to zero.
    #[inline]
    pub fn reset(&mut self) {
        *self = Self::default();
    }
    /// Returns the sum of all action counts.
    #[must_use]
    pub const fn total_count(&self) -> usize {
        self.insert
            .saturating_add(self.set_active)
            .saturating_add(self.set_bookmarked)
            .saturating_add(self.remove)
            .saturating_add(self.clear)
            .saturating_add(self.metadata_mut)
            .saturating_add(self.sort_children)
            .saturating_add(self.sort_roots)
            .saturating_add(self.sort_bookmarks)
            .saturating_add(self.set_active_path)
            .saturating_add(self.move_to)
            .saturating_add(self.get_contents_mut)
            .saturating_add(self.split)
            .saturating_add(self.merge_with_parent)
            .saturating_add(self.other)
    }
    /// Increments the action count corresponding to the [`WeaveAction`]'s type.
    pub const fn increment<K, N, T, M>(&mut self, action: &WeaveAction<K, N, T, M>)
    where
        K: Hash + Copy + Eq + Ord,
        N: Node<K, T>,
    {
        match action {
            WeaveAction::Insert(_node) => self.insert = self.insert.saturating_add(1),
            WeaveAction::SetActive { .. } => {
                self.set_active = self.set_active.saturating_add(1);
            }
            WeaveAction::SetBookmarked { .. } => {
                self.set_bookmarked = self.set_bookmarked.saturating_add(1);
            }
            WeaveAction::Remove(_id) => self.remove = self.remove.saturating_add(1),
            WeaveAction::Clear => {
                self.clear = self.clear.saturating_add(1);
            }
            WeaveAction::SetMetadata(_metadata) => {
                self.metadata_mut = self.metadata_mut.saturating_add(1);
            }
            WeaveAction::SetChildOrdering { parent, .. } => match parent {
                Some(_id) => self.sort_children = self.sort_children.saturating_add(1),
                None => self.sort_roots = self.sort_roots.saturating_add(1),
            },
            WeaveAction::SetBookmarkOrdering(_ids) => {
                self.sort_bookmarks = self.sort_bookmarks.saturating_add(1);
            }
            WeaveAction::SetActivePath(_) => {
                self.set_active_path = self.set_active_path.saturating_add(1);
            }
            WeaveAction::MoveTo { .. } => self.move_to = self.move_to.saturating_add(1),
            WeaveAction::SetContents { .. } => {
                self.get_contents_mut = self.get_contents_mut.saturating_add(1);
            }
            WeaveAction::Split { .. } => self.split = self.split.saturating_add(1),
            WeaveAction::MergeWithParent(_id) => {
                self.merge_with_parent = self.merge_with_parent.saturating_add(1);
            }
        }
    }
    /// Decrements the action count corresponding to the [`WeaveAction`]'s type.
    pub const fn decrement<K, N, T, M>(&mut self, action: &WeaveAction<K, N, T, M>)
    where
        K: Hash + Copy + Eq + Ord,
        N: Node<K, T>,
    {
        match action {
            WeaveAction::Insert(_node) => self.insert = self.insert.saturating_sub(1),
            WeaveAction::SetActive { .. } => {
                self.set_active = self.set_active.saturating_sub(1);
            }
            WeaveAction::SetBookmarked { .. } => {
                self.set_bookmarked = self.set_bookmarked.saturating_sub(1);
            }
            WeaveAction::Remove(_id) => self.remove = self.remove.saturating_sub(1),
            WeaveAction::Clear => {
                self.clear = self.clear.saturating_sub(1);
            }
            WeaveAction::SetMetadata(_metadata) => {
                self.metadata_mut = self.metadata_mut.saturating_sub(1);
            }
            WeaveAction::SetChildOrdering { parent, .. } => match parent {
                Some(_id) => self.sort_children = self.sort_children.saturating_sub(1),
                None => self.sort_roots = self.sort_roots.saturating_sub(1),
            },
            WeaveAction::SetBookmarkOrdering(_ids) => {
                self.sort_bookmarks = self.sort_bookmarks.saturating_sub(1);
            }
            WeaveAction::SetActivePath(_) => {
                self.set_active_path = self.set_active_path.saturating_sub(1);
            }
            WeaveAction::MoveTo { .. } => self.move_to = self.move_to.saturating_sub(1),
            WeaveAction::SetContents { .. } => {
                self.get_contents_mut = self.get_contents_mut.saturating_sub(1);
            }
            WeaveAction::Split { .. } => self.split = self.split.saturating_sub(1),
            WeaveAction::MergeWithParent(_id) => {
                self.merge_with_parent = self.merge_with_parent.saturating_sub(1);
            }
        }
    }
}

/// A [`Weave`] which can have [`WeaveAction`]s applied to it.
pub trait ActionableWeave<K, N, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
    S: BuildHasher + Default + Clone,
{
    /// Applies a [`WeaveAction`] to a [`Weave`].
    ///
    /// # Panics
    ///
    /// May panic if applying the action fails.
    fn apply(&mut self, action: WeaveAction<K, N, T, M>);
}

/*impl<W, K, N, T, M, S> ActionableWeave<K, N, T, M, S> for W
where
    W: Weave<K, N, T>
        + MetadataWeave<K, N, T, M>
        + BookmarkableWeave<K, N, T>
        + SortableWeave<K, N, T>
        + SortableBookmarkableWeave<K, N, T>
        + ActivePathWeave<K, N, T>
        + IndependentWeave<K, N, T>
        + SemiIndependentWeave<K, N, T>
        + DiscreteWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    fn apply(&mut self, action: WeaveAction<K, N, T, M>) {
        match action {
            WeaveAction::Insert(node) => {
                assert!(self.insert(node), "Failed to apply Weave action");
            }
            WeaveAction::SetActive { id, value } => {
                assert!(self.set_active(&id, value), "Failed to apply Weave action");
            }
            WeaveAction::SetBookmarked { id, value } => {
                assert!(
                    self.set_bookmarked(&id, value),
                    "Failed to apply Weave action"
                );
            }
            WeaveAction::Remove(id) => {
                assert!(self.remove(&id).is_some(), "Failed to apply Weave action");
            }
            WeaveAction::Clear => self.clear(),
            WeaveAction::SetMetadata(metadata) => {
                self.metadata_mut(|m| *m = metadata);
            }
            WeaveAction::SetChildOrdering { parent, children } => {
                let mut id_mapping =
                    HashMap::with_capacity_and_hasher(children.len(), S::default());
                id_mapping.extend(
                    children
                        .into_iter()
                        .enumerate()
                        .map(|(index, id)| (id, index)),
                );

                match parent {
                    Some(id) => {
                        assert!(
                            self.sort_children_by_id(&id, |a, b| {
                                id_mapping[a].cmp(&id_mapping[b])
                            }),
                            "Failed to apply Weave action"
                        );
                    }
                    None => {
                        self.sort_roots_by_id(|a, b| id_mapping[a].cmp(&id_mapping[b]));
                    }
                }
            }
            WeaveAction::SetBookmarkOrdering(ids) => {
                let mut id_mapping = HashMap::with_capacity_and_hasher(ids.len(), S::default());
                id_mapping.extend(ids.into_iter().enumerate().map(|(index, id)| (id, index)));

                self.sort_bookmarks_by_id(|a, b| id_mapping[a].cmp(&id_mapping[b]));
            }
            WeaveAction::SetActivePath(active) => {
                self.set_active_path(active.into_iter());
            }
            WeaveAction::MoveTo { id, new_parents } => assert!(
                self.move_to(&id, &new_parents),
                "Failed to apply Weave action"
            ),
            WeaveAction::SetContents { id, contents } => {
                assert!(
                    self.get_contents_mut(&id, |c| *c = contents).is_some(),
                    "Failed to apply Weave action"
                );
            }
            WeaveAction::Split { id, at, new_id } => {
                assert!(self.split(&id, at, new_id), "Failed to apply Weave action");
            }
            WeaveAction::MergeWithParent(id) => assert!(
                self.merge_with_parent(&id).is_some(),
                "Failed to apply Weave action"
            ),
        }
    }
}*/

// Replace this if/when specialization lands in stable
impl<K, T, M, S> ActionableWeave<K, dependent::DependentNode<K, T, S>, T, M, S>
    for dependent::DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    #[allow(clippy::panic, reason = "Necessary due to API shape")]
    fn apply(&mut self, action: WeaveAction<K, dependent::DependentNode<K, T, S>, T, M>) {
        match action {
            WeaveAction::Insert(node) => {
                assert!(self.insert(node), "Failed to apply Weave action");
            }
            WeaveAction::SetActive { id, value } => {
                assert!(self.set_active(&id, value), "Failed to apply Weave action");
            }
            WeaveAction::SetBookmarked { id, value } => {
                assert!(
                    self.set_bookmarked(&id, value),
                    "Failed to apply Weave action"
                );
            }
            WeaveAction::Remove(id) => {
                assert!(self.remove(&id).is_some(), "Failed to apply Weave action");
            }
            WeaveAction::Clear => self.clear(),
            WeaveAction::SetMetadata(metadata) => {
                self.metadata_mut(|m| *m = metadata);
            }
            WeaveAction::SetChildOrdering { parent, children } => {
                let mut id_mapping =
                    HashMap::with_capacity_and_hasher(children.len(), S::default());
                id_mapping.extend(
                    children
                        .into_iter()
                        .enumerate()
                        .map(|(index, id)| (id, index)),
                );

                match parent {
                    Some(id) => {
                        assert!(
                            self.sort_children_by_id(&id, |a, b| {
                                id_mapping[a].cmp(&id_mapping[b])
                            }),
                            "Failed to apply Weave action"
                        );
                    }
                    None => {
                        self.sort_roots_by_id(|a, b| id_mapping[a].cmp(&id_mapping[b]));
                    }
                }
            }
            WeaveAction::SetBookmarkOrdering(ids) => {
                let mut id_mapping = HashMap::with_capacity_and_hasher(ids.len(), S::default());
                id_mapping.extend(ids.into_iter().enumerate().map(|(index, id)| (id, index)));

                self.sort_bookmarks_by_id(|a, b| id_mapping[a].cmp(&id_mapping[b]));
            }
            WeaveAction::SetActivePath(_) => {
                panic!("Weave does not implement set_active_path()");
            }
            WeaveAction::MoveTo { .. } => {
                panic!("Weave does not implement move_to()");
            }
            WeaveAction::SetContents { id, contents } => {
                assert!(
                    self.get_contents_mut(&id, |c| *c = contents).is_some(),
                    "Failed to apply Weave action"
                );
            }
            WeaveAction::Split { id, at, new_id } => {
                assert!(self.split(&id, at, new_id), "Failed to apply Weave action");
            }
            WeaveAction::MergeWithParent(id) => assert!(
                self.merge_with_parent(&id).is_some(),
                "Failed to apply Weave action"
            ),
        }
    }
}

// Replace this if/when specialization lands in stable
impl<K, T, M, S> ActionableWeave<K, independent::IndependentNode<K, T, S>, T, M, S>
    for independent::IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    fn apply(&mut self, action: WeaveAction<K, independent::IndependentNode<K, T, S>, T, M>) {
        match action {
            WeaveAction::Insert(node) => {
                assert!(self.insert(node), "Failed to apply Weave action");
            }
            WeaveAction::SetActive { id, value } => {
                assert!(self.set_active(&id, value), "Failed to apply Weave action");
            }
            WeaveAction::SetBookmarked { id, value } => {
                assert!(
                    self.set_bookmarked(&id, value),
                    "Failed to apply Weave action"
                );
            }
            WeaveAction::Remove(id) => {
                assert!(self.remove(&id).is_some(), "Failed to apply Weave action");
            }
            WeaveAction::Clear => self.clear(),
            WeaveAction::SetMetadata(metadata) => {
                self.metadata_mut(|m| *m = metadata);
            }
            WeaveAction::SetChildOrdering { parent, children } => {
                let mut id_mapping =
                    HashMap::with_capacity_and_hasher(children.len(), S::default());
                id_mapping.extend(
                    children
                        .into_iter()
                        .enumerate()
                        .map(|(index, id)| (id, index)),
                );

                match parent {
                    Some(id) => {
                        assert!(
                            self.sort_children_by_id(&id, |a, b| {
                                id_mapping[a].cmp(&id_mapping[b])
                            }),
                            "Failed to apply Weave action"
                        );
                    }
                    None => {
                        self.sort_roots_by_id(|a, b| id_mapping[a].cmp(&id_mapping[b]));
                    }
                }
            }
            WeaveAction::SetBookmarkOrdering(ids) => {
                let mut id_mapping = HashMap::with_capacity_and_hasher(ids.len(), S::default());
                id_mapping.extend(ids.into_iter().enumerate().map(|(index, id)| (id, index)));

                self.sort_bookmarks_by_id(|a, b| id_mapping[a].cmp(&id_mapping[b]));
            }
            WeaveAction::SetActivePath(active) => {
                self.set_active_path(active.into_iter());
            }
            WeaveAction::MoveTo { id, new_parents } => assert!(
                self.move_to(&id, &new_parents),
                "Failed to apply Weave action"
            ),
            WeaveAction::SetContents { id, contents } => {
                assert!(
                    self.get_contents_mut(&id, |c| *c = contents).is_some(),
                    "Failed to apply Weave action"
                );
            }
            WeaveAction::Split { id, at, new_id } => {
                assert!(self.split(&id, at, new_id), "Failed to apply Weave action");
            }
            WeaveAction::MergeWithParent(id) => assert!(
                self.merge_with_parent(&id).is_some(),
                "Failed to apply Weave action"
            ),
        }
    }
}

impl<W, K, N, T, M> Weave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T> + Clone,
{
    type Nodes = W::Nodes;
    type Roots = W::Roots;

    #[inline]
    fn len(&self) -> usize {
        self.weave.len()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.weave.is_empty()
    }
    #[inline]
    fn nodes(&self) -> &Self::Nodes {
        self.weave.nodes()
    }
    #[inline]
    fn roots(&self) -> &Self::Roots {
        self.weave.roots()
    }
    #[inline]
    fn contains(&self, id: &K) -> bool {
        self.weave.contains(id)
    }
    #[inline]
    fn contains_active(&self, id: &K) -> bool {
        self.weave.contains_active(id)
    }
    #[inline]
    fn get(&self, id: &K) -> Option<&N> {
        self.weave.get(id)
    }
    #[inline]
    fn get_parents(&self, id: &K) -> Option<&N::From> {
        self.weave.get_parents(id)
    }
    #[inline]
    fn get_children(&self, id: &K) -> Option<&N::To> {
        self.weave.get_children(id)
    }
    #[inline]
    fn get_contents(&self, id: &K) -> Option<&T> {
        self.weave.get_contents(id)
    }
    #[inline]
    fn get_ordered_identifiers(&mut self, output: &mut Vec<K>) {
        self.weave.get_ordered_identifiers(output);
    }
    #[inline]
    fn get_ordered_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_ordered_identifiers_from(id, output);
    }
    #[inline]
    fn get_active_path(&mut self, output: &mut Vec<K>) {
        self.weave.get_active_path(output);
    }
    #[inline]
    fn get_path_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_path_from(id, output);
    }
    fn insert(&mut self, node: N) -> bool {
        if self.weave.insert(node.clone()) {
            self.actions.push_back(WeaveAction::Insert(node));
            true
        } else {
            false
        }
    }
    fn set_active(&mut self, id: &K, value: bool) -> bool {
        if self.weave.set_active(id, value) {
            self.actions
                .push_back(WeaveAction::SetActive { id: *id, value });
            true
        } else {
            false
        }
    }
    fn remove(&mut self, id: &K) -> Option<N> {
        if let Some(removed) = self.weave.remove(id) {
            self.actions.push_back(WeaveAction::Remove(*id));
            Some(removed)
        } else {
            None
        }
    }
    fn remove_tracked(&mut self, id: &K, on_removal: impl FnMut(N)) -> bool {
        if self.weave.remove_tracked(id, on_removal) {
            self.actions.push_back(WeaveAction::Remove(*id));
            true
        } else {
            false
        }
    }
    fn clear(&mut self) {
        self.weave.clear();
        self.actions.push_back(WeaveAction::Clear);
    }
}

impl<W, K, N, T, M> MetadataWeave<K, N, T, M> for LoggedWeave<W, K, N, T, M>
where
    W: MetadataWeave<K, N, T, M>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T> + Clone,
    M: Clone,
{
    #[inline]
    fn metadata(&self) -> &M {
        self.weave.metadata()
    }
    fn metadata_mut<O>(&mut self, callback: impl FnOnce(&mut M) -> O) -> O {
        self.weave.metadata_mut(|metadata| {
            let output = callback(metadata);

            self.actions
                .push_back(WeaveAction::SetMetadata(metadata.clone()));

            output
        })
    }
}

impl<W, K, N, T, M> BookmarkableWeave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: BookmarkableWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T> + Clone,
{
    type Bookmarks = W::Bookmarks;

    #[inline]
    fn bookmarks(&self) -> &Self::Bookmarks {
        self.weave.bookmarks()
    }
    #[inline]
    fn contains_bookmark(&self, id: &K) -> bool {
        self.weave.contains_bookmark(id)
    }
    fn set_bookmarked(&mut self, id: &K, value: bool) -> bool {
        if self.weave.set_bookmarked(id, value) {
            self.actions
                .push_back(WeaveAction::SetBookmarked { id: *id, value });
            true
        } else {
            false
        }
    }
}

impl<W, K, N, T, M> SortableWeave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: SortableWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T> + Clone,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
{
    fn sort_children_by(&mut self, id: &K, cmp: impl FnMut(&N, &N) -> Ordering) -> bool {
        if self.weave.sort_children_by(id, cmp) {
            self.actions.push_back(WeaveAction::SetChildOrdering {
                parent: Some(*id),
                children: self
                    .weave
                    .get_children(id)
                    .unwrap()
                    .into_iter()
                    .copied()
                    .collect(),
            });
            true
        } else {
            false
        }
    }
    fn sort_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool {
        if self.weave.sort_children_by_id(id, cmp) {
            self.actions.push_back(WeaveAction::SetChildOrdering {
                parent: Some(*id),
                children: self
                    .weave
                    .get_children(id)
                    .unwrap()
                    .into_iter()
                    .copied()
                    .collect(),
            });
            true
        } else {
            false
        }
    }
    fn sort_roots_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering) {
        self.weave.sort_roots_by(cmp);
        self.actions.push_back(WeaveAction::SetChildOrdering {
            parent: None,
            children: self.weave.roots().into_iter().copied().collect(),
        });
    }
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.weave.sort_roots_by_id(cmp);
        self.actions.push_back(WeaveAction::SetChildOrdering {
            parent: None,
            children: self.weave.roots().into_iter().copied().collect(),
        });
    }
}

impl<W, K, N, T, M> SortableBookmarkableWeave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: SortableBookmarkableWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T> + Clone,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a W::Bookmarks: IntoIterator<Item = &'a K>,
{
    fn sort_bookmarks_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering) {
        self.weave.sort_bookmarks_by(cmp);
        self.actions.push_back(WeaveAction::SetBookmarkOrdering(
            self.weave.bookmarks().into_iter().copied().collect(),
        ));
    }
    fn sort_bookmarks_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.weave.sort_bookmarks_by_id(cmp);
        self.actions.push_back(WeaveAction::SetBookmarkOrdering(
            self.weave.bookmarks().into_iter().copied().collect(),
        ));
    }
}

impl<W, K, N, T, M> ActiveSingularWeave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: ActiveSingularWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T> + Clone,
{
    #[inline]
    fn active(&self) -> Option<K> {
        self.weave.active()
    }
}

impl<W, K, N, T, M> ActivePathWeave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: ActivePathWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T> + Clone,
{
    type Active = W::Active;

    #[inline]
    fn active(&self) -> &Self::Active {
        self.weave.active()
    }
    fn set_active_path(&mut self, active: impl Iterator<Item = K>) {
        let active: Vec<K> = active.collect();

        self.weave.set_active_path(active.iter().copied());
        self.actions.push_back(WeaveAction::SetActivePath(active));
    }
}

impl<W, K, N, T, M> IndependentWeave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T> + Clone,
    T: IndependentContents + Clone,
{
    fn move_to(&mut self, id: &K, new_parents: &[K]) -> bool {
        if self.weave.move_to(id, new_parents) {
            self.actions.push_back(WeaveAction::MoveTo {
                id: *id,
                new_parents: new_parents.to_vec(),
            });
            true
        } else {
            false
        }
    }
}

impl<W, K, N, T, M> SemiIndependentWeave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: SemiIndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T> + Clone,
    T: IndependentContents + Clone,
{
    fn get_contents_mut<O>(&mut self, id: &K, callback: impl FnOnce(&mut T) -> O) -> Option<O> {
        self.weave
            .get_contents_mut(id, |contents| (callback(contents), contents.clone()))
            .map(|(output, contents)| {
                self.actions
                    .push_back(WeaveAction::SetContents { id: *id, contents });

                output
            })
    }
}

impl<W, K, N, T, M> DiscreteWeave<K, N, T> for LoggedWeave<W, K, N, T, M>
where
    W: DiscreteWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T> + Clone,
    T: DiscreteContents,
{
    fn split(&mut self, id: &K, at: usize, new_id: K) -> bool {
        if self.weave.split(id, at, new_id) {
            self.actions.push_back(WeaveAction::Split {
                id: *id,
                at,
                new_id,
            });
            true
        } else {
            false
        }
    }
    fn merge_with_parent(&mut self, id: &K) -> Option<K> {
        match self.weave.merge_with_parent(id) {
            Some(new_id) => {
                self.actions.push_back(WeaveAction::MergeWithParent(*id));
                Some(new_id)
            }
            None => None,
        }
    }
}

impl<W, K, N, T> Weave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
{
    type Nodes = W::Nodes;
    type Roots = W::Roots;

    #[inline]
    fn len(&self) -> usize {
        self.weave.len()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.weave.is_empty()
    }
    #[inline]
    fn nodes(&self) -> &Self::Nodes {
        self.weave.nodes()
    }
    #[inline]
    fn roots(&self) -> &Self::Roots {
        self.weave.roots()
    }
    #[inline]
    fn contains(&self, id: &K) -> bool {
        self.weave.contains(id)
    }
    #[inline]
    fn contains_active(&self, id: &K) -> bool {
        self.weave.contains_active(id)
    }
    #[inline]
    fn get(&self, id: &K) -> Option<&N> {
        self.weave.get(id)
    }
    #[inline]
    fn get_parents(&self, id: &K) -> Option<&N::From> {
        self.weave.get_parents(id)
    }
    #[inline]
    fn get_children(&self, id: &K) -> Option<&N::To> {
        self.weave.get_children(id)
    }
    #[inline]
    fn get_contents(&self, id: &K) -> Option<&T> {
        self.weave.get_contents(id)
    }
    #[inline]
    fn get_ordered_identifiers(&mut self, output: &mut Vec<K>) {
        self.weave.get_ordered_identifiers(output);
    }
    #[inline]
    fn get_ordered_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_ordered_identifiers_from(id, output);
    }
    #[inline]
    fn get_active_path(&mut self, output: &mut Vec<K>) {
        self.weave.get_active_path(output);
    }
    #[inline]
    fn get_path_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_path_from(id, output);
    }
    #[inline]
    fn insert(&mut self, node: N) -> bool {
        if self.weave.insert(node) {
            self.count.insert = self.count.insert.saturating_add(1);
            true
        } else {
            false
        }
    }
    #[inline]
    fn set_active(&mut self, id: &K, value: bool) -> bool {
        if self.weave.set_active(id, value) {
            self.count.set_active = self.count.set_active.saturating_add(1);
            true
        } else {
            false
        }
    }
    #[inline]
    fn remove(&mut self, id: &K) -> Option<N> {
        if let Some(removed) = self.weave.remove(id) {
            self.count.remove = self.count.remove.saturating_add(1);
            Some(removed)
        } else {
            None
        }
    }
    #[inline]
    fn remove_tracked(&mut self, id: &K, on_removal: impl FnMut(N)) -> bool {
        if self.weave.remove_tracked(id, on_removal) {
            self.count.remove = self.count.remove.saturating_add(1);
            true
        } else {
            false
        }
    }
    #[inline]
    fn clear(&mut self) {
        self.weave.clear();
        self.count.clear = self.count.clear.saturating_add(1);
    }
}

impl<W, K, N, T, M> MetadataWeave<K, N, T, M> for CountedWeave<W, K, N, T>
where
    W: MetadataWeave<K, N, T, M>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
{
    #[inline]
    fn metadata(&self) -> &M {
        self.weave.metadata()
    }
    #[inline]
    fn metadata_mut<O>(&mut self, callback: impl FnOnce(&mut M) -> O) -> O {
        self.weave.metadata_mut(|metadata| {
            let output = callback(metadata);
            self.count.metadata_mut = self.count.metadata_mut.saturating_add(1);
            output
        })
    }
}

impl<W, K, N, T> BookmarkableWeave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: BookmarkableWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
{
    type Bookmarks = W::Bookmarks;

    #[inline]
    fn bookmarks(&self) -> &Self::Bookmarks {
        self.weave.bookmarks()
    }
    #[inline]
    fn contains_bookmark(&self, id: &K) -> bool {
        self.weave.contains_bookmark(id)
    }
    #[inline]
    fn set_bookmarked(&mut self, id: &K, value: bool) -> bool {
        if self.weave.set_bookmarked(id, value) {
            self.count.set_bookmarked = self.count.set_bookmarked.saturating_add(1);
            true
        } else {
            false
        }
    }
}

impl<W, K, N, T> SortableWeave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: SortableWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
{
    #[inline]
    fn sort_children_by(&mut self, id: &K, cmp: impl FnMut(&N, &N) -> Ordering) -> bool {
        if self.weave.sort_children_by(id, cmp) {
            self.count.sort_children = self.count.sort_children.saturating_add(1);
            true
        } else {
            false
        }
    }
    #[inline]
    fn sort_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool {
        if self.weave.sort_children_by_id(id, cmp) {
            self.count.sort_children = self.count.sort_children.saturating_add(1);
            true
        } else {
            false
        }
    }
    #[inline]
    fn sort_roots_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering) {
        self.weave.sort_roots_by(cmp);
        self.count.sort_roots = self.count.sort_roots.saturating_add(1);
    }
    #[inline]
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.weave.sort_roots_by_id(cmp);
        self.count.sort_roots = self.count.sort_roots.saturating_add(1);
    }
}

impl<W, K, N, T> SortableBookmarkableWeave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: SortableBookmarkableWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
{
    #[inline]
    fn sort_bookmarks_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering) {
        self.weave.sort_bookmarks_by(cmp);
        self.count.sort_bookmarks = self.count.sort_bookmarks.saturating_add(1);
    }
    #[inline]
    fn sort_bookmarks_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.weave.sort_bookmarks_by_id(cmp);
        self.count.sort_bookmarks = self.count.sort_bookmarks.saturating_add(1);
    }
}

impl<W, K, N, T> ActiveSingularWeave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: ActiveSingularWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
{
    #[inline]
    fn active(&self) -> Option<K> {
        self.weave.active()
    }
}

impl<W, K, N, T> ActivePathWeave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: ActivePathWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
{
    type Active = W::Active;

    #[inline]
    fn active(&self) -> &Self::Active {
        self.weave.active()
    }
    #[inline]
    fn set_active_path(&mut self, active: impl Iterator<Item = K>) {
        self.weave.set_active_path(active);
        self.count.set_active_path = self.count.set_active_path.saturating_add(1);
    }
}

impl<W, K, N, T> IndependentWeave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: IndependentContents,
{
    #[inline]
    fn move_to(&mut self, id: &K, new_parents: &[K]) -> bool {
        if self.weave.move_to(id, new_parents) {
            self.count.move_to = self.count.move_to.saturating_add(1);
            true
        } else {
            false
        }
    }
}

impl<W, K, N, T> SemiIndependentWeave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: SemiIndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: IndependentContents,
{
    #[inline]
    fn get_contents_mut<O>(&mut self, id: &K, callback: impl FnOnce(&mut T) -> O) -> Option<O> {
        self.weave.get_contents_mut(id, callback).inspect(|_| {
            self.count.get_contents_mut = self.count.get_contents_mut.saturating_add(1);
        })
    }
}

impl<W, K, N, T> DiscreteWeave<K, N, T> for CountedWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents,
{
    #[inline]
    fn split(&mut self, id: &K, at: usize, new_id: K) -> bool {
        if self.weave.split(id, at, new_id) {
            self.count.split = self.count.split.saturating_add(1);
            true
        } else {
            false
        }
    }
    #[inline]
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

/// A [`Weave`] wrapper which prevents actions from creating siblings with duplicate contents.
///
/// Siblings which are also parents or children of the target node are excluded.
///
/// # Limitations
///
/// It is possible for [`Weave::insert()`], [`Weave::remove()`], and [`Weave::remove_tracked()`] to create duplicate siblings under circumstances specified in the function's documentation.
#[derive(Debug, Clone)]
#[must_use]
pub struct DeduplicatedWeave<W, K, N, T, S>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    T: DeduplicatableContents,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
    /// The [`Weave`] being wrapped.
    ///
    /// Actions performed directly on the inner [`Weave`] (without using the wrapper's functions) are not checked.
    pub weave: W,

    scratchpad: HashSet<K, S>,
    _phantom_n: PhantomData<N>,
    _phantom_t: PhantomData<T>,
}

impl<W, K, N, T, S> Default for DeduplicatedWeave<W, K, N, T, S>
where
    W: Weave<K, N, T> + Default,
    K: Hash + Copy + Eq + Ord,
    T: DeduplicatableContents,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
    #[inline]
    fn default() -> Self {
        Self::new(W::default())
    }
}

#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<W, K, N, T, S> PartialEq for DeduplicatedWeave<W, K, N, T, S>
where
    W: Weave<K, N, T> + PartialEq,
    K: Hash + Copy + Eq + Ord,
    T: DeduplicatableContents,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.weave == other.weave
    }
}

#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<W, K, N, T, S> Eq for DeduplicatedWeave<W, K, N, T, S>
where
    W: Weave<K, N, T> + Eq,
    K: Hash + Copy + Eq + Ord,
    T: DeduplicatableContents,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
}

impl<W, K, N, T, S> AsRef<W> for DeduplicatedWeave<W, K, N, T, S>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    T: DeduplicatableContents,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
    #[inline]
    fn as_ref(&self) -> &W {
        &self.weave
    }
}

impl<W, K, N, T, S> From<W> for DeduplicatedWeave<W, K, N, T, S>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    T: DeduplicatableContents,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
    #[inline]
    fn from(value: W) -> Self {
        Self::new(value)
    }
}

impl<W, K, N, T, S> DeduplicatedWeave<W, K, N, T, S>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    T: DeduplicatableContents,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
    /// Creates a [`DeduplicatedWeave`] from a [`Weave`].
    pub fn new(weave: W) -> Self {
        Self {
            scratchpad: HashSet::default(),
            weave,
            _phantom_n: PhantomData,
            _phantom_t: PhantomData,
        }
    }
    /// Converts a [`DeduplicatedWeave`] into it's inner [`Weave`].
    #[inline]
    pub fn into_inner(self) -> W {
        self.weave
    }
    /// Returns a reference to the inner [`Weave`].
    #[inline]
    pub const fn as_inner(&self) -> &W {
        &self.weave
    }
}

fn has_duplicate_siblings<W, K, N, T, S, I, F, O>(
    weave: &W,
    ignored: &I,
    parents: &F,
    children: &O,
    contents: &T,
    scratchpad: &mut HashSet<K, S>,
) -> bool
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    T: DeduplicatableContents,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    for<'a> &'a I: IntoIterator<Item = &'a K>,
    for<'a> &'a F: IntoIterator<Item = &'a K>,
    for<'a> &'a O: IntoIterator<Item = &'a K>,
    F: ?Sized,
    O: ?Sized,
{
    if parents.into_iter().next().is_none() {
        scratchpad.extend(weave.roots().into_iter().copied());
    } else {
        for sibling in parents
            .into_iter()
            .filter_map(|id| weave.get_children(id))
            .flatten()
            .copied()
        {
            scratchpad.insert(sibling);
        }
    }

    for parent in parents {
        scratchpad.remove(parent);
    }

    for child in children {
        scratchpad.remove(child);
    }

    for ignore in ignored {
        scratchpad.remove(ignore);
    }

    scratchpad
        .drain()
        .filter_map(|id| weave.get_contents(&id))
        .any(|c| c.is_duplicate_of(contents))
}

impl<W, K, N, T, S> Weave<K, N, T> for DeduplicatedWeave<W, K, N, T, S>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    T: DeduplicatableContents,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
    type Nodes = W::Nodes;
    type Roots = W::Roots;

    #[inline]
    fn len(&self) -> usize {
        self.weave.len()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.weave.is_empty()
    }
    #[inline]
    fn nodes(&self) -> &Self::Nodes {
        self.weave.nodes()
    }
    #[inline]
    fn roots(&self) -> &Self::Roots {
        self.weave.roots()
    }
    #[inline]
    fn contains(&self, id: &K) -> bool {
        self.weave.contains(id)
    }
    #[inline]
    fn contains_active(&self, id: &K) -> bool {
        self.weave.contains_active(id)
    }
    #[inline]
    fn get(&self, id: &K) -> Option<&N> {
        self.weave.get(id)
    }
    #[inline]
    fn get_parents(&self, id: &K) -> Option<&N::From> {
        self.weave.get_parents(id)
    }
    #[inline]
    fn get_children(&self, id: &K) -> Option<&N::To> {
        self.weave.get_children(id)
    }
    #[inline]
    fn get_contents(&self, id: &K) -> Option<&T> {
        self.weave.get_contents(id)
    }
    #[inline]
    fn get_ordered_identifiers(&mut self, output: &mut Vec<K>) {
        self.weave.get_ordered_identifiers(output);
    }
    #[inline]
    fn get_ordered_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_ordered_identifiers_from(id, output);
    }
    #[inline]
    fn get_active_path(&mut self, output: &mut Vec<K>) {
        self.weave.get_active_path(output);
    }
    #[inline]
    fn get_path_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_path_from(id, output);
    }
    /// Inserts a node into the Weave, returning `true` if the insertion was successful.
    ///
    /// This function may change the active status of nodes if it is necessary to preserve internal consistency.
    ///
    /// # Deduplication
    ///
    /// This function does not deduplicate the node's children. As a result, it is possible (albeit uncommon) for this operation to create duplicate siblings.
    fn insert(&mut self, node: N) -> bool {
        if has_duplicate_siblings(
            &self.weave,
            &[node.id()],
            node.from(),
            node.to(),
            node.contents(),
            &mut self.scratchpad,
        ) {
            return false;
        }

        self.weave.insert(node)
    }
    #[inline]
    fn set_active(&mut self, id: &K, value: bool) -> bool {
        self.weave.set_active(id, value)
    }
    /// Removes a node with the specified identifier, returning its value if it was present within the Weave.
    ///
    /// This function may remove or update other nodes if it is necessary to preserve internal consistency.
    ///
    /// This function uses the same removal logic as [`Weave::remove_tracked`].
    ///
    /// # Deduplication
    ///
    /// If the underlying [`Weave::remove()`] implementation reparents nodes, this operation may create duplicate siblings.
    #[inline]
    fn remove(&mut self, id: &K) -> Option<N> {
        self.weave.remove(id)
    }
    /// Removes a node with the specified identifier, returning `true` if it was present within the Weave.
    ///
    /// This function may remove or update other nodes if it is necessary to preserve internal consistency. Every removed node will be returned by the `on_removal` call, with removal ordering being defined by the `Weave` implementation.
    ///
    /// # Panics
    ///
    /// May panic if `on_removal` panics.
    ///
    /// # Deduplication
    ///
    /// If the underlying [`Weave::remove_tracked()`] implementation reparents nodes, this operation may create duplicate siblings.
    #[inline]
    fn remove_tracked(&mut self, id: &K, on_removal: impl FnMut(N)) -> bool {
        self.weave.remove_tracked(id, on_removal)
    }
    #[inline]
    fn clear(&mut self) {
        self.weave.clear();
    }
}

impl<W, K, N, T, M, S> MetadataWeave<K, N, T, M> for DeduplicatedWeave<W, K, N, T, S>
where
    W: MetadataWeave<K, N, T, M>,
    K: Hash + Copy + Eq + Ord,
    T: DeduplicatableContents,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
    #[inline]
    fn metadata(&self) -> &M {
        self.weave.metadata()
    }
    #[inline]
    fn metadata_mut<O>(&mut self, callback: impl FnOnce(&mut M) -> O) -> O {
        self.weave.metadata_mut(callback)
    }
}

impl<W, K, N, T, S> BookmarkableWeave<K, N, T> for DeduplicatedWeave<W, K, N, T, S>
where
    W: BookmarkableWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    T: DeduplicatableContents,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
    type Bookmarks = W::Bookmarks;

    #[inline]
    fn bookmarks(&self) -> &Self::Bookmarks {
        self.weave.bookmarks()
    }
    #[inline]
    fn contains_bookmark(&self, id: &K) -> bool {
        self.weave.contains_bookmark(id)
    }
    #[inline]
    fn set_bookmarked(&mut self, id: &K, value: bool) -> bool {
        self.weave.set_bookmarked(id, value)
    }
}

impl<W, K, N, T, S> SortableWeave<K, N, T> for DeduplicatedWeave<W, K, N, T, S>
where
    W: SortableWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    T: DeduplicatableContents,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
    #[inline]
    fn sort_children_by(&mut self, id: &K, cmp: impl FnMut(&N, &N) -> Ordering) -> bool {
        self.weave.sort_children_by(id, cmp)
    }
    #[inline]
    fn sort_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool {
        self.weave.sort_children_by_id(id, cmp)
    }
    #[inline]
    fn sort_roots_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering) {
        self.weave.sort_roots_by(cmp);
    }
    #[inline]
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.weave.sort_roots_by_id(cmp);
    }
}

impl<W, K, N, T, S> SortableBookmarkableWeave<K, N, T> for DeduplicatedWeave<W, K, N, T, S>
where
    W: SortableBookmarkableWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    T: DeduplicatableContents,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
    #[inline]
    fn sort_bookmarks_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering) {
        self.weave.sort_bookmarks_by(cmp);
    }
    #[inline]
    fn sort_bookmarks_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.weave.sort_bookmarks_by_id(cmp);
    }
}

impl<W, K, N, T, S> ActiveSingularWeave<K, N, T> for DeduplicatedWeave<W, K, N, T, S>
where
    W: ActiveSingularWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    T: DeduplicatableContents,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
    #[inline]
    fn active(&self) -> Option<K> {
        self.weave.active()
    }
}

impl<W, K, N, T, S> ActivePathWeave<K, N, T> for DeduplicatedWeave<W, K, N, T, S>
where
    W: ActivePathWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    T: DeduplicatableContents,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
    type Active = W::Active;

    #[inline]
    fn active(&self) -> &Self::Active {
        self.weave.active()
    }
    #[inline]
    fn set_active_path(&mut self, active: impl Iterator<Item = K>) {
        self.weave.set_active_path(active);
    }
}

impl<W, K, N, T, S> IndependentWeave<K, N, T> for DeduplicatedWeave<W, K, N, T, S>
where
    W: IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents + DeduplicatableContents + Clone,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
    fn move_to(&mut self, id: &K, new_parents: &[K]) -> bool {
        if let Some(node) = self.weave.get(id) {
            if has_duplicate_siblings(
                &self.weave,
                &[node.id()],
                new_parents,
                node.to(),
                node.contents(),
                &mut self.scratchpad,
            ) {
                return false;
            }

            self.weave.move_to(id, new_parents)
        } else {
            false
        }
    }
}

impl<W, K, N, T, S> SemiIndependentWeave<K, N, T> for DeduplicatedWeave<W, K, N, T, S>
where
    W: SemiIndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents + DeduplicatableContents + Clone,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
    /// Mutable access to the contents of a node with the specified identifier.
    ///
    /// `callback` may be executed without updating the node's contents.
    ///
    /// # Panics
    ///
    /// May panic if `callback` panics.
    fn get_contents_mut<O>(&mut self, id: &K, callback: impl FnOnce(&mut T) -> O) -> Option<O> {
        if let Some(node) = self.weave.get(id) {
            let mut contents = node.contents().clone();
            let output = callback(&mut contents);

            if has_duplicate_siblings(
                &self.weave,
                &[node.id()],
                node.from(),
                node.to(),
                &contents,
                &mut self.scratchpad,
            ) {
                None
            } else if self.weave.get_contents_mut(id, |c| *c = contents).is_some() {
                Some(output)
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<W, K, N, T, S> DiscreteWeave<K, N, T> for DeduplicatedWeave<W, K, N, T, S>
where
    W: DiscreteWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    T: DiscreteContents + DeduplicatableContents + Clone,
    N: BuildableNode<K, T>,
    S: BuildHasher + Default + Clone,
    for<'a> &'a W::Roots: IntoIterator<Item = &'a K>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K, IntoIter: ExactSizeIterator>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
{
    fn split(&mut self, id: &K, at: usize, new_id: K) -> bool {
        if let Some(node) = self.weave.get(id)
            && let DiscreteContentResult::Two(left, _right) = node.contents().clone().split(at)
            && has_duplicate_siblings(
                &self.weave,
                &[node.id()],
                node.from(),
                &[],
                &left,
                &mut self.scratchpad,
            )
        {
            return false;
        }

        self.weave.split(id, at, new_id)
    }
    fn merge_with_parent(&mut self, id: &K) -> Option<K> {
        if let Some(node) = self.weave.get(id) {
            if node.from().into_iter().len() != 1 {
                return None;
            }

            if let Some(parent_id) = node.from().into_iter().next()
                && let Some(parent) = self.weave.get(parent_id)
                && let DiscreteContentResult::One(merged) =
                    parent.contents().clone().merge(node.contents().clone())
                && has_duplicate_siblings(
                    &self.weave,
                    &[node.id(), *parent_id],
                    parent.from(),
                    node.to(),
                    &merged,
                    &mut self.scratchpad,
                )
            {
                return None;
            }
        }

        self.weave.merge_with_parent(id)
    }
}

/// A [`Weave`] wrapper which adds content patch operations to the active path.
///
/// Unless documented otherwise, the wrapper's exact indexing behavior for zero-width nodes is an implementation detail which may be changed in the future. However, indexing behavior is consistent between functions.
///
/// # Requirements
///
/// The underlying [`Weave`] must meet all of the following requirements for patch operations to function properly:
///
/// 1. [`IndependentWeave`](crate::independent::IndependentWeave)-like activation semantics.
/// 2. [`Weave::insert`], [`DiscreteWeave::split`], and [`IndependentWeave::move_to`] must never refuse a structurally valid operation.
/// 3. `T::split()` must always split at the exact specified index and should never refuse a structurally valid operation.
/// 4. `T::default()` must have a length of zero.
///
/// # Panics
///
/// May panic if the underlying [`Weave`] violates any of the specified [requirements](#requirements).
///
/// All panics should be assumed to leave the underlying Weave in a malformed state.
#[derive(Debug, Clone)]
#[must_use]
pub struct PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    /// The [`Weave`] being wrapped.
    pub weave: W,

    scratchpad: Vec<K>,

    _phantom_n: PhantomData<N>,
    _phantom_t: PhantomData<T>,
}

impl<W, K, N, T> Default for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T> + Default,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn default() -> Self {
        Self::new(W::default())
    }
}

#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<W, K, N, T> PartialEq for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T> + PartialEq,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.weave == other.weave
    }
}

#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<W, K, N, T> Eq for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T> + Eq,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
}

impl<W, K, N, T> AsRef<W> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn as_ref(&self) -> &W {
        &self.weave
    }
}

impl<W, K, N, T> From<W> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn from(value: W) -> Self {
        Self::new(value)
    }
}

impl<W, K, N, T> PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    /// Creates a [`PatchablePathWeave`] from a [`Weave`].
    #[inline]
    pub const fn new(weave: W) -> Self {
        Self {
            weave,
            scratchpad: Vec::new(),
            _phantom_n: PhantomData,
            _phantom_t: PhantomData,
        }
    }
    /// Converts a [`PatchablePathWeave`] into it's inner [`Weave`].
    #[inline]
    pub fn into_inner(self) -> W {
        self.weave
    }
    /// Returns a reference to the inner [`Weave`].
    #[inline]
    pub const fn as_inner(&self) -> &W {
        &self.weave
    }
}

impl<W, K, N, T> Weave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    type Nodes = W::Nodes;
    type Roots = W::Roots;

    #[inline]
    fn len(&self) -> usize {
        self.weave.len()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.weave.is_empty()
    }
    #[inline]
    fn nodes(&self) -> &Self::Nodes {
        self.weave.nodes()
    }
    #[inline]
    fn roots(&self) -> &Self::Roots {
        self.weave.roots()
    }
    #[inline]
    fn contains(&self, id: &K) -> bool {
        self.weave.contains(id)
    }
    #[inline]
    fn contains_active(&self, id: &K) -> bool {
        self.weave.contains_active(id)
    }
    #[inline]
    fn get(&self, id: &K) -> Option<&N> {
        self.weave.get(id)
    }
    #[inline]
    fn get_parents(&self, id: &K) -> Option<&N::From> {
        self.weave.get_parents(id)
    }
    #[inline]
    fn get_children(&self, id: &K) -> Option<&N::To> {
        self.weave.get_children(id)
    }
    #[inline]
    fn get_contents(&self, id: &K) -> Option<&T> {
        self.weave.get_contents(id)
    }
    #[inline]
    fn get_ordered_identifiers(&mut self, output: &mut Vec<K>) {
        self.weave.get_ordered_identifiers(output);
    }
    #[inline]
    fn get_ordered_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_ordered_identifiers_from(id, output);
    }
    #[inline]
    fn get_active_path(&mut self, output: &mut Vec<K>) {
        self.weave.get_active_path(output);
    }
    #[inline]
    fn get_path_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_path_from(id, output);
    }
    #[inline]
    fn insert(&mut self, node: N) -> bool {
        self.weave.insert(node)
    }
    #[inline]
    fn set_active(&mut self, id: &K, value: bool) -> bool {
        self.weave.set_active(id, value)
    }
    #[inline]
    fn remove(&mut self, id: &K) -> Option<N> {
        self.weave.remove(id)
    }
    #[inline]
    fn remove_tracked(&mut self, id: &K, on_removal: impl FnMut(N)) -> bool {
        self.weave.remove_tracked(id, on_removal)
    }
    #[inline]
    fn clear(&mut self) {
        self.weave.clear();
    }
}

impl<W, K, N, T, M> MetadataWeave<K, N, T, M> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T>
        + ActivePathWeave<K, N, T>
        + IndependentWeave<K, N, T>
        + MetadataWeave<K, N, T, M>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn metadata(&self) -> &M {
        self.weave.metadata()
    }
    #[inline]
    fn metadata_mut<O>(&mut self, callback: impl FnOnce(&mut M) -> O) -> O {
        self.weave.metadata_mut(callback)
    }
}

impl<W, K, N, T> BookmarkableWeave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T>
        + ActivePathWeave<K, N, T>
        + IndependentWeave<K, N, T>
        + BookmarkableWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    type Bookmarks = W::Bookmarks;

    #[inline]
    fn bookmarks(&self) -> &Self::Bookmarks {
        self.weave.bookmarks()
    }
    #[inline]
    fn contains_bookmark(&self, id: &K) -> bool {
        self.weave.contains_bookmark(id)
    }
    #[inline]
    fn set_bookmarked(&mut self, id: &K, value: bool) -> bool {
        self.weave.set_bookmarked(id, value)
    }
}

impl<W, K, N, T> SortableWeave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T>
        + ActivePathWeave<K, N, T>
        + IndependentWeave<K, N, T>
        + SortableWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn sort_children_by(&mut self, id: &K, cmp: impl FnMut(&N, &N) -> Ordering) -> bool {
        self.weave.sort_children_by(id, cmp)
    }
    #[inline]
    fn sort_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool {
        self.weave.sort_children_by_id(id, cmp)
    }
    #[inline]
    fn sort_roots_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering) {
        self.weave.sort_roots_by(cmp);
    }
    #[inline]
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.weave.sort_roots_by_id(cmp);
    }
}

impl<W, K, N, T> SortableBookmarkableWeave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T>
        + ActivePathWeave<K, N, T>
        + IndependentWeave<K, N, T>
        + SortableBookmarkableWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn sort_bookmarks_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering) {
        self.weave.sort_bookmarks_by(cmp);
    }
    #[inline]
    fn sort_bookmarks_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.weave.sort_bookmarks_by_id(cmp);
    }
}

impl<W, K, N, T> ActiveSingularWeave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T>
        + ActivePathWeave<K, N, T>
        + IndependentWeave<K, N, T>
        + ActiveSingularWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn active(&self) -> Option<K> {
        ActiveSingularWeave::active(&self.weave)
    }
}

impl<W, K, N, T> ActivePathWeave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    type Active = W::Active;

    #[inline]
    fn active(&self) -> &Self::Active {
        self.weave.active()
    }
    #[inline]
    fn set_active_path(&mut self, active: impl Iterator<Item = K>) {
        self.weave.set_active_path(active);
    }
}

impl<W, K, N, T> IndependentWeave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn move_to(&mut self, id: &K, new_parents: &[K]) -> bool {
        self.weave.move_to(id, new_parents)
    }
}

impl<W, K, N, T> SemiIndependentWeave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn get_contents_mut<O>(&mut self, id: &K, callback: impl FnOnce(&mut T) -> O) -> Option<O> {
        self.weave.get_contents_mut(id, callback)
    }
}

impl<W, K, N, T> DiscreteWeave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn split(&mut self, id: &K, at: usize, new_id: K) -> bool {
        self.weave.split(id, at, new_id)
    }
    #[inline]
    fn merge_with_parent(&mut self, id: &K) -> Option<K> {
        self.weave.merge_with_parent(id)
    }
}

impl<W, K, N, T> PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    /// Convenience function which returns an iterator over the content corresponding to the active path.
    #[allow(
        clippy::missing_panics_doc,
        reason = "Should never panic when underlying Weave is correctly implemented"
    )]
    pub fn active_content(&mut self) -> impl Iterator<Item = &T> {
        self.scratchpad.clear();
        self.weave.get_active_path(&mut self.scratchpad);

        self.scratchpad
            .drain(..)
            .rev()
            .map(|id| self.weave.get_contents(&id).unwrap())
    }
    /// Removes the specified range from the active path without removing the content from the underlying Weave.
    ///
    /// If the range is empty or starts past the end of the active path, this function does nothing. If the range extends beyond the active path, its length is clamped to the active path's length.
    ///
    /// If the range starts at zero, an empty root node (`T::default()`) may be inserted at the start of the active path.
    ///
    /// This function may split up to 2 nodes, may move up to 1 node, and may insert up to 1 node if necessary to apply the operation.
    ///
    /// # Panics
    ///
    /// May panic if the underlying [`Weave`] violates any of the wrapper's [requirements](#requirements).
    ///
    /// May panic if `generate_id` panics or returns an identifier already in the Weave.
    pub fn split_out<F>(&mut self, range: Range<usize>, mut generate_id: F)
    where
        F: FnMut() -> K,
    {
        if range.is_empty() {
            return;
        }

        self.scratchpad.clear();
        self.weave.get_active_path(&mut self.scratchpad);
        self.scratchpad.reverse();

        if self.scratchpad.is_empty() {
            return;
        }

        #[allow(clippy::branches_sharing_code, reason = "Variable scoping")]
        let (prefix_len, start_split, end) = if range.start == 0 {
            let mut cursor: usize = 0;
            let mut prefix_len = 0;
            let mut end = None;

            for (index, id) in self.scratchpad.iter().enumerate() {
                let length = self.weave.get_contents(id).unwrap().len();
                let next = cursor.strict_add(length);

                if cursor == 0 {
                    prefix_len = index;
                }

                #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                if cursor == range.end || next > range.end {
                    end = Some((index, range.end - cursor, *id));
                    break;
                }

                cursor = next;
            }

            if cursor == 0 && end.is_none() {
                return;
            }

            (prefix_len, None, end)
        } else {
            let mut cursor: usize = 0;
            let mut prefix_len = 0;
            let mut start_split = None;
            let mut end = None;

            for (index, id) in self.scratchpad.iter().enumerate() {
                let length = self.weave.get_contents(id).unwrap().len();
                let next = cursor.strict_add(length);

                if prefix_len == 0 && next >= range.start {
                    prefix_len = index.strict_add(1);

                    #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                    let at = range.start - cursor;

                    if at != length {
                        start_split = Some((id, at));
                    }
                }

                #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                if cursor == range.end || next > range.end {
                    end = Some((index, range.end - cursor, *id));
                    break;
                }

                cursor = next;
            }

            if end.is_none()
                && (range.start > cursor
                    || (range.start == cursor && prefix_len == self.scratchpad.len()))
            {
                return;
            }

            (prefix_len, start_split, end)
        };

        let (end, suffix_index) = if let Some((index, at, left)) = end {
            let next = index.strict_add(1);

            if at == 0 {
                (Some(left), next)
            } else {
                let right = generate_id();

                assert!(self.weave.split(&left, at, right), "Splitting node failed");

                (Some(right), next)
            }
        } else {
            (None, self.scratchpad.len())
        };

        if let Some((left, at)) = start_split {
            assert!(
                self.weave.split(left, at, generate_id()),
                "Splitting node failed"
            );
        }

        let prefix = &self.scratchpad[..prefix_len];

        if let Some(prefix_tail) = prefix.last().copied() {
            if let Some(end) = end {
                let parents = self.weave.get_parents(&end).unwrap();

                if parents.into_iter().all(|id| id != &prefix_tail) {
                    let start = self.scratchpad.len();

                    self.scratchpad
                        .extend(parents.into_iter().copied().chain(iter::once(prefix_tail)));

                    assert!(
                        self.weave.move_to(&end, &self.scratchpad[start..]),
                        "Moving node failed"
                    );

                    self.scratchpad.truncate(start);
                }

                self.weave.set_active_path(
                    self.scratchpad[..prefix_len]
                        .iter()
                        .copied()
                        .chain(iter::once(end))
                        .chain(self.scratchpad[suffix_index..].iter().copied()),
                );
            } else if prefix_len != self.scratchpad.len() {
                self.weave.set_active_path(
                    prefix
                        .iter()
                        .copied()
                        .chain(self.scratchpad[suffix_index..].iter().copied()),
                );
            }
        } else {
            let id = generate_id();
            let contents = T::default();

            assert!(
                contents.is_empty(),
                "`T::default()` must have a length of zero"
            );

            assert!(
                self.weave.insert(N::new(
                    id,
                    N::From::from_iter(iter::empty()),
                    N::To::from_iter(end),
                    end.is_none(),
                    contents
                )),
                "Inserting node failed"
            );

            if let Some(end) = end {
                self.weave.set_active_path(
                    iter::once(id)
                        .chain(iter::once(end))
                        .chain(self.scratchpad[suffix_index..].iter().copied()),
                );
            }
        }
    }
    /// Splits the active path at the specified index without deactivating the right side of the split.
    ///
    /// If `at` is zero or beyond the active path's length, this function does nothing.
    ///
    /// Returns `false` if splitting the path failed or if `generate_id` returns an identifier already in the Weave.
    ///
    /// # Panics
    ///
    /// May panic if `T::split()` or `generate_id` panics.
    pub fn split_at<F>(&mut self, at: usize, mut generate_id: F) -> bool
    where
        F: FnMut() -> K,
    {
        if at == 0 {
            return true;
        }

        self.scratchpad.clear();
        self.weave.get_active_path(&mut self.scratchpad);

        let mut cursor: usize = 0;
        let mut target = None;

        for id in self.scratchpad.drain(..).rev() {
            let length = self.weave.get_contents(&id).unwrap().len();
            let next = cursor.strict_add(length);

            if next >= at {
                target = Some((id, length));
                break;
            }

            cursor = next;
        }

        if let Some((parent, length)) = target {
            #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
            let split_at = at - cursor;

            if split_at == length {
                true
            } else {
                let new_id = generate_id();

                if self.weave.split(&parent, split_at, new_id) {
                    assert!(
                        self.weave.set_active(&new_id, true),
                        "Setting node active status failed"
                    );

                    true
                } else {
                    false
                }
            }
        } else {
            true
        }
    }
    /// Inserts a new node into the active path at the specified index without removing existing node connections.
    ///
    /// If the index extends past the end of the active path, it is clamped to the active path's length.
    ///
    /// If the index is zero, an empty root node (`T::default()`) may be inserted at the start of the active path.
    ///
    /// This function may split up to 1 node and may insert up to 1 additional node if necessary to apply the operation.
    ///
    /// # Panics
    ///
    /// May panic if the underlying [`Weave`] violates any of the wrapper's [requirements](#requirements).
    ///
    /// May panic if `generate_id` panics or returns an identifier already in the Weave.
    pub fn insert_at<F>(&mut self, at: usize, contents: T, mut generate_id: F)
    where
        F: FnMut() -> K,
    {
        enum Action {
            InsertAnchor,
            SplitNode,
            Boundary,
        }

        self.scratchpad.clear();
        self.weave.get_active_path(&mut self.scratchpad);
        self.scratchpad.reverse();

        let (index, parent, child, action) = if at == 0 {
            let prefix_len = self
                .scratchpad
                .iter()
                .take_while(|id| self.weave.get_contents(id).unwrap().is_empty())
                .count();

            if prefix_len == 0
                && let Some(root) = self.scratchpad.first().copied()
            {
                let id = generate_id();
                let contents = T::default();

                assert!(
                    contents.is_empty(),
                    "`T::default()` must have a length of zero"
                );

                assert!(
                    self.weave.insert(N::new(
                        id,
                        N::From::from_iter(iter::empty()),
                        N::To::from_iter(iter::once(root)),
                        false,
                        contents
                    )),
                    "Inserting node failed"
                );

                (0, Some(id), Some(root), Action::InsertAnchor)
            } else {
                (
                    prefix_len,
                    self.scratchpad[..prefix_len].last().copied(),
                    self.scratchpad.get(prefix_len).copied(),
                    Action::Boundary,
                )
            }
        } else {
            let mut cursor: usize = 0;
            let mut target = None;

            for (index, id) in self.scratchpad.iter().enumerate() {
                let length = self.weave.get_contents(id).unwrap().len();
                let next = cursor.strict_add(length);

                if next >= at {
                    target = Some((index, length, *id));
                    break;
                }

                cursor = next;
            }

            if let Some((index, length, parent)) = target {
                #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                let split_at = at - cursor;
                let next = index.strict_add(1);

                if split_at == length {
                    (
                        next,
                        Some(parent),
                        self.scratchpad.get(next).copied(),
                        Action::Boundary,
                    )
                } else {
                    let right = generate_id();

                    assert!(
                        self.weave.split(&parent, split_at, right),
                        "Splitting node failed"
                    );

                    (next, Some(parent), Some(right), Action::SplitNode)
                }
            } else {
                (
                    self.scratchpad.len(),
                    self.scratchpad.last().copied(),
                    None,
                    Action::Boundary,
                )
            }
        };

        let id = generate_id();

        assert!(
            self.weave.insert(N::new(
                id,
                N::From::from_iter(parent),
                N::To::from_iter(child),
                matches!(action, Action::Boundary),
                contents
            )),
            "Inserting node failed"
        );

        match action {
            Action::InsertAnchor => {
                self.weave.set_active_path(
                    iter::once(parent.unwrap())
                        .chain(iter::once(id))
                        .chain(self.scratchpad.iter().copied()),
                );
            }
            Action::SplitNode => {
                self.weave.set_active_path(
                    self.scratchpad[..index]
                        .iter()
                        .copied()
                        .chain(iter::once(id))
                        .chain(iter::once(child.unwrap()))
                        .chain(self.scratchpad[index..].iter().copied()),
                );
            }
            Action::Boundary => {}
        }
    }
}
