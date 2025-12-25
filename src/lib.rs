//! General-purpose building blocks for [Loom](https://generative.ink/posts/loom-interface-to-the-multiverse/) implementations.

// TODO: Unit tests
// TODO: Use a formal verifier (such as Creusot, Kani, Verus, etc...) once one of them supports enough of the language features

pub mod dependent;
pub mod independent;

use std::{
    cmp::Ordering,
    collections::HashMap,
    hash::{BuildHasher, Hash},
};

pub use indexmap;
use indexmap::IndexSet;
pub use rkyv;
use rkyv::collections::swiss_table::{ArchivedHashMap, ArchivedIndexSet};

/// An item within a [`Weave`] which can be connected to other items.
pub trait Node<K, T, S>
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    /// The unique identifier of the node.
    fn id(&self) -> K;
    /// The identifiers corresponding to the parents of this node.
    fn from(&self) -> impl ExactSizeIterator<Item = K> + DoubleEndedIterator<Item = K>;
    /// The identifiers corresponding to the children of this node.
    fn to(&self) -> impl ExactSizeIterator<Item = K> + DoubleEndedIterator<Item = K>;
    /// If the node is considered "active".
    ///
    /// The meaning of this value can depend on the underlying [`Weave`] implementation.
    fn is_active(&self) -> bool;
    /// If the node is bookmarked.
    fn is_bookmarked(&self) -> bool;
    /// The contents of the node.
    fn contents(&self) -> &T;
}

/// [`Node`] contents which can be split apart or merged together.
pub trait DiscreteContents: Sized {
    /// Splits the item at specified index.
    fn split(self, at: usize) -> DiscreteContentResult<Self>;
    /// Merges two items together.
    fn merge(self, value: Self) -> DiscreteContentResult<Self>;
}

/// The result from an action on a [`DiscreteContents`] item.
///
/// Actions are fallible; If the action was not successful, the original contents are returned.
pub enum DiscreteContentResult<T> {
    Two((T, T)),
    One(T),
}

/// [`Node`] contents which can be meaningfully deduplicated.
pub trait DeduplicatableContents {
    /// Whether or not two items are considered duplicates of each-other.
    ///
    /// The result of this function must be symmetric (`a == b` implies `b == a` and `a != b` implies `!(a == b)`).
    fn is_duplicate_of(&self, value: &Self) -> bool;
}

pub trait IndependentContents {}

/// A document which links together multiple [`Node`] objects.
pub trait Weave<K, N, T, S>
where
    K: Hash + Copy + Eq,
    N: Node<K, T, S>,
    S: BuildHasher + Default + Clone,
{
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn contains(&self, id: &K) -> bool;
    fn get_node(&self, id: &K) -> Option<&N>;
    fn get_all_nodes(&self) -> &HashMap<K, N, S>;
    fn get_roots(&self) -> &IndexSet<K, S>;
    fn get_bookmarks(&self) -> &IndexSet<K, S>;
    fn get_active_thread(
        &mut self,
    ) -> impl ExactSizeIterator<Item = K> + DoubleEndedIterator<Item = K>;
    fn get_thread_from(
        &mut self,
        id: &K,
    ) -> impl ExactSizeIterator<Item = K> + DoubleEndedIterator<Item = K>;
    fn add_node(&mut self, node: N) -> bool;
    fn set_node_active_status(&mut self, id: &K, value: bool, alternate: bool) -> bool;
    fn set_node_active_status_in_place(&mut self, id: &K, value: bool) -> bool;
    fn set_node_bookmarked_status(&mut self, id: &K, value: bool) -> bool;
    fn sort_node_children_by(&mut self, id: &K, compare: impl FnMut(&N, &N) -> Ordering) -> bool;
    fn sort_roots_by(&mut self, compare: impl FnMut(&N, &N) -> Ordering);
    fn remove_node(&mut self, id: &K) -> Option<N>;
}

/// A [`Weave`] where [`Node`] objects do not depend on their parents in order to be meaningful.
pub trait IndependentWeave<K, N, T, S>:
    Weave<K, N, T, S> + SemiIndependentWeave<K, N, T, S>
where
    K: Hash + Copy + Eq,
    N: Node<K, T, S>,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    fn move_node(&mut self, id: &K, new_parents: &[K]) -> bool;
}

/// A [`Weave`] where [`Node`] objects do not depend on the *contents* of their parents in order to be meaningful.
pub trait SemiIndependentWeave<K, N, T, S>: Weave<K, N, T, S>
where
    K: Hash + Copy + Eq,
    N: Node<K, T, S>,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    fn get_contents_mut(&mut self, id: &K) -> Option<&mut T>;
}

/// A [`Weave`] where the contents of [`Node`] objects can be split and merged.
pub trait DiscreteWeave<K, N, T, S>: Weave<K, N, T, S>
where
    K: Hash + Copy + Eq,
    N: Node<K, T, S>,
    T: DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    fn split_node(&mut self, id: &K, at: usize, new_id: K) -> bool;
    fn merge_with_parent(&mut self, id: &K) -> Option<K>;
}

/// A [`Weave`] where [`Node`] objects can be meaningfully deduplicated by their contents.
pub trait DeduplicatableWeave<K, N, T, S>: Weave<K, N, T, S>
where
    K: Hash + Copy + Eq,
    N: Node<K, T, S>,
    T: DeduplicatableContents,
    S: BuildHasher + Default + Clone,
{
    fn find_duplicates(&self, id: &K) -> impl Iterator<Item = K>;
}

pub trait ArchivedNode<K, T>
where
    K: Hash + Copy + Eq,
{
    fn id(&self) -> K;
    fn from(&self) -> impl Iterator<Item = K>;
    fn to(&self) -> impl Iterator<Item = K>;
    fn is_active(&self) -> bool;
    fn is_bookmarked(&self) -> bool;
    fn contents(&self) -> &T;
}

pub trait ArchivedWeave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: ArchivedNode<K, T>,
{
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn contains(&self, id: &K) -> bool;
    fn get_node(&self, id: &K) -> Option<&N>;
    fn get_all_nodes(&self) -> &ArchivedHashMap<K, N>;
    fn get_roots(&self) -> &ArchivedIndexSet<K>;
    fn get_bookmarks(&self) -> &ArchivedIndexSet<K>;
    fn get_active_thread(&self)
    -> impl ExactSizeIterator<Item = K> + DoubleEndedIterator<Item = K>;
    fn get_thread_from(
        &self,
        id: &K,
    ) -> impl ExactSizeIterator<Item = K> + DoubleEndedIterator<Item = K>;
}
