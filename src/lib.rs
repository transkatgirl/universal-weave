// TODO: Unit tests
// TODO: Use a formal verifier (such as Creusot, Kani, Verus, etc...) once one of them supports enough of the language features

pub mod dependent;
pub mod independent;
pub mod legacy_dependent;

use std::{
    cmp::Ordering,
    hash::{BuildHasher, Hash},
};

pub use indexmap;
use indexmap::IndexSet;
pub use rkyv;
use rkyv::collections::swiss_table::ArchivedIndexSet;

pub trait Node<K, T, S>
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    fn id(&self) -> K;
    fn from(&self) -> impl ExactSizeIterator<Item = K> + DoubleEndedIterator<Item = K>;
    fn to(&self) -> impl ExactSizeIterator<Item = K> + DoubleEndedIterator<Item = K>;
    fn is_active(&self) -> bool;
    fn is_bookmarked(&self) -> bool;
    fn contents(&self) -> &T;
}

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
    fn get_all_nodes_unordered(&self) -> impl ExactSizeIterator<Item = K>;
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

pub trait IndependentWeave<K, N, T, S>
where
    K: Hash + Copy + Eq,
    N: Node<K, T, S>,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    fn move_node(&mut self, id: &K, new_parents: &[K]) -> bool;
    fn get_contents_mut(&mut self, id: &K) -> Option<&mut T>;
}

pub trait SemiIndependentWeave<K, N, T, S>
where
    K: Hash + Copy + Eq,
    N: Node<K, T, S>,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    fn get_contents_mut(&mut self, id: &K) -> Option<&mut T>;
}

pub trait DiscreteWeave<K, N, T, S>
where
    K: Hash + Copy + Eq,
    N: Node<K, T, S>,
    T: DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    fn split_node(&mut self, id: &K, at: usize, new_id: K) -> bool;
    fn merge_with_parent(&mut self, id: &K) -> bool;
}

pub trait DeduplicatableWeave<K, N, T, S>
where
    K: Hash + Copy + Eq,
    N: Node<K, T, S>,
    T: DeduplicatableContents,
    S: BuildHasher + Default + Clone,
{
    fn find_duplicates(&self, id: &K) -> impl Iterator<Item = K>;
}

pub enum DiscreteContentResult<T> {
    Two((T, T)),
    One(T),
}

pub trait DiscreteContents: Sized {
    fn split(self, at: usize) -> DiscreteContentResult<Self>;
    fn merge(self, value: Self) -> DiscreteContentResult<Self>;
}

pub trait DeduplicatableContents {
    fn is_duplicate_of(&self, value: &Self) -> bool;
}

pub trait IndependentContents {}

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
    fn get_all_nodes_unordered(&self) -> impl ExactSizeIterator<Item = K>;
    fn get_roots(&self) -> &ArchivedIndexSet<K>;
    fn get_bookmarks(&self) -> &ArchivedIndexSet<K>;
    fn get_active_thread(&self)
    -> impl ExactSizeIterator<Item = K> + DoubleEndedIterator<Item = K>;
    fn get_thread_from(
        &self,
        id: &K,
    ) -> impl ExactSizeIterator<Item = K> + DoubleEndedIterator<Item = K>;
}
