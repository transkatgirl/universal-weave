// TODO: Make key and hashing algo generic
// TODO: Unit tests
// TODO: Use a formal verifier (such as Creusot, Kani, Verus, etc...) once one of them supports enough of the language features

pub mod dependent;
pub mod independent;

use std::{cmp::Ordering, collections::VecDeque, hash::BuildHasherDefault};

pub use indexmap;
use indexmap::IndexSet;
pub use rkyv;
use rkyv::{collections::swiss_table::ArchivedIndexSet, hash::FxHasher64, rend::u128_le};

pub type IdentifierSet = IndexSet<u128, BuildHasherDefault<FxHasher64>>;

pub trait Node<T> {
    fn id(&self) -> u128;
    fn from(&self) -> impl Iterator<Item = u128>;
    fn to(&self) -> impl Iterator<Item = u128>;
    fn is_active(&self) -> bool;
    fn is_bookmarked(&self) -> bool;
    fn contents(&self) -> &T;
}

pub trait Weave<N, T>
where
    N: Node<T>,
{
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn contains(&self, id: &u128) -> bool;
    fn get_node(&self, id: &u128) -> Option<&N>;
    fn get_all_nodes_unordered(&self) -> impl ExactSizeIterator<Item = u128>;
    fn get_roots(&self) -> &IndexSet<u128, BuildHasherDefault<FxHasher64>>;
    fn get_bookmarks(&self) -> &IndexSet<u128, BuildHasherDefault<FxHasher64>>;
    fn get_active_thread(&mut self) -> &VecDeque<u128>; // TODO: Rewrite this to use Vec<T>
    fn get_thread_from(&mut self, id: &u128) -> &VecDeque<u128>; // TODO: Rewrite this to use Vec<T>
    fn add_node(&mut self, node: N) -> bool;
    fn set_node_active_status(&mut self, id: &u128, value: bool, alternate: bool) -> bool;
    fn set_node_bookmarked_status(&mut self, id: &u128, value: bool) -> bool;
    fn sort_node_children_by(&mut self, id: &u128, compare: impl FnMut(&N, &N) -> Ordering)
    -> bool;
    fn sort_roots_by(&mut self, compare: impl FnMut(&N, &N) -> Ordering);
    fn remove_node(&mut self, id: &u128) -> Option<N>;
}

pub trait IndependentWeave<N, T>
where
    N: Node<T>,
    T: IndependentContents,
{
    fn move_node(&mut self, id: &u128, new_parents: &[u128]) -> bool;
    fn get_contents_mut(&mut self, id: &u128) -> Option<&mut T>;
}

pub trait SemiIndependentWeave<N, T>
where
    N: Node<T>,
    T: IndependentContents,
{
    fn get_contents_mut(&mut self, id: &u128) -> Option<&mut T>;
}

pub trait DiscreteWeave<N, T>
where
    N: Node<T>,
    T: DiscreteContents,
{
    fn split_node(&mut self, id: &u128, at: usize, new_id: u128) -> bool;
    fn merge_with_parent(&mut self, id: &u128) -> bool;
}

pub trait DuplicatableWeave<N, T>
where
    N: Node<T>,
    T: DeduplicatableContents,
{
    fn find_duplicates(&self, id: &u128) -> impl Iterator<Item = u128>;
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

pub trait ArchivedNode<T> {
    fn id(&self) -> u128_le;
    fn from(&self) -> impl Iterator<Item = u128_le>;
    fn to(&self) -> impl Iterator<Item = u128_le>;
    fn is_active(&self) -> bool;
    fn is_bookmarked(&self) -> bool;
    fn contents(&self) -> &T;
}

pub trait ArchivedWeave<N, T>
where
    N: ArchivedNode<T>,
{
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn contains(&self, id: &u128_le) -> bool;
    fn get_node(&self, id: &u128_le) -> Option<&N>;
    fn get_all_nodes_unordered(&self) -> impl ExactSizeIterator<Item = u128_le>;
    fn get_roots(&self) -> &ArchivedIndexSet<u128_le>;
    fn get_bookmarks(&self) -> &ArchivedIndexSet<u128_le>;
    fn get_active_thread(&self) -> VecDeque<u128_le>;
    fn get_thread_from(&self, id: &u128_le) -> VecDeque<u128_le>;
}
