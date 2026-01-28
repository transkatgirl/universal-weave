//! General-purpose building blocks for [Loom](https://generative.ink/posts/loom-interface-to-the-multiverse/) implementations.

// TODO: Unit tests
// TODO: Use a formal verifier (such as Creusot, Kani, Verus, etc...) once one of them supports enough of the language features

pub mod dependent;
pub mod independent;

#[cfg(feature = "rkyv")]
pub mod versioning;

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    hash::{BuildHasher, Hash},
};

pub use indexmap;
use indexmap::IndexSet;

#[cfg(feature = "rkyv")]
pub use rkyv;

#[cfg(feature = "rkyv")]
use rkyv::{
    collections::swiss_table::{ArchivedHashMap, ArchivedHashSet, ArchivedIndexSet},
    option::ArchivedOption,
};

#[cfg(feature = "serde")]
pub use serde;

/// An item within a [`Weave`] which can be connected to other items.
pub trait Node<K, T, S>
where
    K: Hash + Copy + Eq,
    S: BuildHasher + Default + Clone,
{
    /// Returns the node's unique identifier.
    fn id(&self) -> K;
    /// An iterator over the identifiers corresponding to the node's children.
    fn from(&self) -> impl ExactSizeIterator<Item = K> + DoubleEndedIterator<Item = K>;
    /// An iterator over the identifiers corresponding to the node's parents.
    fn to(&self) -> impl ExactSizeIterator<Item = K> + DoubleEndedIterator<Item = K>;
    /// Returns `true` if the node is considered "active".
    ///
    /// The meaning of this value can depend on the underlying [`Weave`] implementation.
    fn is_active(&self) -> bool;
    /// Returns `true` if the node is bookmarked.
    fn is_bookmarked(&self) -> bool;
    /// Returns a reference to the node's contents.
    fn contents(&self) -> &T;
}

/// [`Node`] contents which can be split apart or merged together.
pub trait DiscreteContents: Sized {
    /// Splits the item at specified index.
    ///
    /// If splitting the item fails, the original contents are returned.
    fn split(self, at: usize) -> DiscreteContentResult<Self>;
    /// Merges two items together.
    ///
    /// If merging the two items fails, the original contents are returned in the order they were specified in.
    fn merge(self, value: Self) -> DiscreteContentResult<Self>;
}

/// A type representing the results of an action on a [`DiscreteContents`] item.
pub enum DiscreteContentResult<T> {
    One(T),
    Two((T, T)),
}

/// [`Node`] contents which do not depend on the contents of other [`Node`] objects in order to be meaningful.
pub trait IndependentContents {}

/// [`Node`] contents which can be meaningfully deduplicated.
///
/// Deduplication must be symmetric:
/// For all `a` and `b`, `a == b` implies `b == a` and `a != b` implies `!(a == b)`.
pub trait DeduplicatableContents {
    /// Tests if `self` and `other` should be considered duplicates of each other.
    fn is_duplicate_of(&self, other: &Self) -> bool;
}

/// A document linking together multiple [`Node`] objects without cyclical links.
pub trait Weave<K, N, T, S>
where
    K: Hash + Copy + Eq,
    N: Node<K, T, S>,
    S: BuildHasher + Default + Clone,
{
    /// Returns the number of nodes stored within the Weave.
    fn len(&self) -> usize;
    /// Returns `true` if the Weave does not contain any nodes.
    fn is_empty(&self) -> bool;
    /// Returns a reference to the HashMap used to map identifiers to nodes.
    fn nodes(&self) -> &HashMap<K, N, S>;
    /// Returns a reference to the IndexSet used to store the identifiers of "root" nodes (nodes which do not have any parents).
    fn roots(&self) -> &IndexSet<K, S>;
    /// Returns a reference to the IndexSet used to store the identifiers of bookmarked nodes.
    fn bookmarks(&self) -> &IndexSet<K, S>;
    /// Returns `true` if the Weave contains a node with the specified identifier.
    fn contains(&self, id: &K) -> bool;
    /// Returns `true` if the Weave contains an "active" node (`node.is_active() == true`) with the specified identifier.
    ///
    /// The meaning of this value can depend on the underlying Weave implementation.
    fn contains_active(&self, id: &K) -> bool;
    /// Returns a reference to the node corresponding to the identifier.
    fn get_node(&self, id: &K) -> Option<&N>;
    /// Builds a list of all node identifiers ordered by their positions in the Weave.
    fn get_ordered_node_identifiers(&mut self, output: &mut Vec<K>);
    /// Builds a list of all node identifiers ordered by their positions in the Weave.
    ///
    /// Unlike [`Weave::get_ordered_node_identifiers`], this function reverses the ordering of a node's children.
    fn get_ordered_node_identifiers_reversed_children(&mut self, output: &mut Vec<K>);
    /// Builds a thread starting at the deepest active node within the Weave.
    ///
    /// A thread is an identifier list of directly connected nodes which always ends at a root node.
    ///
    /// In Weave implementations where nodes can contain multiple parents, the thread always uses the active parent if one is present, falling back to the first parent if the node does not contain any active parents.
    fn get_active_thread(&mut self, output: &mut Vec<K>);
    /// Builds a thread starting at the specified node.
    ///
    /// A thread is an identifier list of directly connected nodes which always ends at a root node.
    ///
    /// In Weave implementations where nodes can contain multiple parents, the thread always uses the active parent if one is present, falling back to the first parent if the node does not contain any active parents.
    fn get_thread_from(&mut self, id: &K, output: &mut Vec<K>);
    /// Inserts a node into the Weave.
    ///
    /// Note: This function does not comprehensively check for cyclical connections; doing so must be done by the function caller. Creating a cyclical connection of nodes within a Weave will cause unexpected behavior including but not limited to infinite loops and stack overflows.
    ///
    /// This function may change the active status of other nodes if it is necessary to keep the Weave internally consistent.
    fn add_node(&mut self, node: N) -> bool;
    /// Sets the active status of a node with the specified identifier.
    ///
    /// This function is meant to be used in user interfaces and it's exact behavior is decided by the Weave implementation. The `alternate` argument should be used in cases where an alternative behavior is desired (such as when shift-clicking a button).
    ///
    /// This function uses [`Weave::set_node_active_status_in_place`] internally.
    fn set_node_active_status(&mut self, id: &K, value: bool, alternate: bool) -> bool;
    /// Sets the active status of a node with the specified identifier.
    ///
    /// Unlike [`Weave::set_node_active_status`], this function only changes the active status of other nodes if it is necessary to keep the Weave internally consistent.
    fn set_node_active_status_in_place(&mut self, id: &K, value: bool) -> bool;
    /// Sets the bookmarked status of a node with the specified identifier.
    fn set_node_bookmarked_status(&mut self, id: &K, value: bool) -> bool;
    /// Sorts the child nodes of a parent node with the specified identifier using the comparison function `cmp`.
    fn sort_node_children_by(&mut self, id: &K, cmp: impl FnMut(&N, &N) -> Ordering) -> bool;
    /// Sorts the identifiers of a parent node's children with the specified identifier using the comparison function `cmp`.
    fn sort_node_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool;
    /// Sorts "root" nodes (nodes which do not have any parents) using the comparison function `cmp`.
    fn sort_roots_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering);
    /// Sorts the identifiers of "root" nodes (nodes which do not have any parents) using the comparison function `cmp`.
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering);
    /// Removes a node with the specified identifier, returning its value if it was present within the Weave.
    ///
    /// This function may change the active status of other nodes if it is necessary to keep the Weave internally consistent.
    fn remove_node(&mut self, id: &K) -> Option<N>;
}

/// A [`Weave`] where only one [`Node`] object can be considered "active" at a time.
pub trait ActiveSingularWeave<K, N, T, S>: Weave<K, N, T, S>
where
    K: Hash + Copy + Eq,
    N: Node<K, T, S>,
    S: BuildHasher + Default + Clone,
{
    /// Returns the active node's identifier, if any.
    fn active(&self) -> Option<K>;
}

/// A [`Weave`] where every [`Node`] object in the active path is always considered "active".
pub trait ActivePathWeave<K, N, T, S>: Weave<K, N, T, S>
where
    K: Hash + Copy + Eq,
    N: Node<K, T, S>,
    S: BuildHasher + Default + Clone,
{
    /// Returns a reference to the HashSet used to store the identifiers of active nodes.
    fn active(&self) -> &HashSet<K, S>;
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
    /// Moves a node with the specified identifier to a new set of parent nodes.
    ///
    /// Note: This function does not comprehensively check for cyclical connections; doing so must be done by the function caller. Creating a cyclical connection of nodes within a Weave will cause unexpected behavior including but not limited to infinite loops and stack overflows.
    ///
    /// This function may change the active status of other nodes if it is necessary to keep the Weave internally consistent.
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
    /// Returns a mutable reference to the contents of a node with the specified identifier.
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
    /// Splits a node with the specified identifier at the given index, creating a new node with the identifier `new_id`.
    ///
    /// Returns `false` if splitting the node failed or the node could not be found.
    fn split_node(&mut self, id: &K, at: usize, new_id: K) -> bool;
    /// Merges a node with the specified identifier with its parent.
    ///
    /// Returns the identifier of the merged node if merging was successful.
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
    /// An iterator over the specified node's sibling identifiers which contain contents which are duplicates of the specified node's contents.
    fn find_duplicates(&self, id: &K) -> impl Iterator<Item = K>;
}

#[cfg(feature = "rkyv")]
/// A [`Node`] which has been decoded using zero-copy deserialization.
pub trait ArchivedNode<K, T>
where
    K: Hash + Copy + Eq,
{
    /// Returns the node's unique identifier.
    fn id(&self) -> K;
    /// An iterator over the identifiers corresponding to the node's children.
    fn from(&self) -> impl Iterator<Item = K>;
    /// An iterator over the identifiers corresponding to the node's parents.
    fn to(&self) -> impl Iterator<Item = K>;
    /// Returns `true` if the node is considered "active".
    ///
    /// The meaning of this value can depend on the underlying [`Weave`] implementation.
    fn is_active(&self) -> bool;
    /// Returns `true` if the node is bookmarked.
    fn is_bookmarked(&self) -> bool;
    /// Returns a reference to the node's contents.
    fn contents(&self) -> &T;
}

#[cfg(feature = "rkyv")]
/// A read-only [`Weave`] which has been decoded using zero-copy deserialization.
pub trait ArchivedWeave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: ArchivedNode<K, T>,
{
    /// Returns the number of nodes stored within the Weave.
    fn len(&self) -> usize;
    /// Returns `true` if the Weave does not contain any nodes.
    fn is_empty(&self) -> bool;
    /// Returns a reference to the HashMap used to map identifiers to nodes.
    fn nodes(&self) -> &ArchivedHashMap<K, N>;
    /// Returns a reference to the IndexSet used to store "root" nodes (nodes which do not have any parents).
    fn roots(&self) -> &ArchivedIndexSet<K>;
    /// Returns a reference to the IndexSet used to store bookmarked nodes.
    fn bookmarks(&self) -> &ArchivedIndexSet<K>;
    /// Returns `true` if the Weave contains a node with the specified identifier.
    fn contains(&self, id: &K) -> bool;
    /// Returns `true` if the Weave contains an "active" node (`node.is_active() == true`) with the specified identifier.
    ///
    /// The meaning of this value can depend on the underlying Weave implementation.
    fn contains_active(&self, id: &K) -> bool;
    /// Returns a reference to the node corresponding to the identifier.
    fn get_node(&self, id: &K) -> Option<&N>;
    /// Builds a list of all node identifiers ordered by their positions in the Weave.
    fn get_ordered_node_identifiers(&self, output: &mut Vec<K>);
    /// Builds a list of all node identifiers ordered by their positions in the Weave.
    ///
    /// Unlike [`ArchivedWeave::get_ordered_node_identifiers`], this function reverses the ordering of a node's children.
    fn get_ordered_node_identifiers_reversed_children(&self, output: &mut Vec<K>);
    /// Builds a thread starting at the deepest active node within the Weave.
    ///
    /// A thread is an identifier list of directly connected nodes which always ends at a root node.
    ///
    /// In Weave implementations where nodes can contain multiple parents, the thread always uses the active parent if one is present, falling back to the first parent if the node does not contain any active parents.
    fn get_active_thread(&self, output: &mut Vec<K>);
    /// Builds a thread starting at the specified node.
    ///
    /// A thread is an identifier list of directly connected nodes which always ends at a root node.
    ///
    /// In Weave implementations where nodes can contain multiple parents, the thread always uses the active parent if one is present, falling back to the first parent if the node does not contain any active parents.
    fn get_thread_from(&self, id: &K, output: &mut Vec<K>);
}

#[cfg(feature = "rkyv")]
/// An [`ArchivedWeave`] where only one [`ArchivedNode`] object can be considered "active" at a time.
pub trait ArchivedActiveSingularWeave<K, N, T>: ArchivedWeave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: ArchivedNode<K, T>,
{
    /// Returns the active node's identifier, if any.
    fn active(&self) -> ArchivedOption<K>;
}

#[cfg(feature = "rkyv")]
/// An [`ArchivedWeave`] where every [`ArchivedNode`] object in the active path is always considered "active".
pub trait ArchivedActivePathWeave<K, N, T>: ArchivedWeave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: ArchivedNode<K, T>,
{
    /// Returns a reference to the HashSet used to store the identifiers of active nodes.
    fn active(&self) -> &ArchivedHashSet<K>;
}

fn add_node_identifiers<K, N, T, S>(
    nodes: &HashMap<K, N, S>,
    id: K,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq,
    N: Node<K, T, S>,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(&id)
        && node.from().all(|parent| identifier_set.contains(&parent))
    {
        identifiers.push(id);
        identifier_set.insert(id);
        for child in node.to() {
            add_node_identifiers(nodes, child, identifiers, identifier_set);
        }
    }
}

fn add_node_identifiers_rev<K, N, T, S>(
    nodes: &HashMap<K, N, S>,
    id: K,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq,
    N: Node<K, T, S>,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(&id)
        && node.from().all(|parent| identifier_set.contains(&parent))
    {
        identifiers.push(id);
        identifier_set.insert(id);
        for child in node.to().rev() {
            add_node_identifiers_rev(nodes, child, identifiers, identifier_set);
        }
    }
}

#[cfg(feature = "rkyv")]
fn add_archived_node_identifiers<K, N, T, S>(
    nodes: &ArchivedHashMap<K, N>,
    id: K,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq,
    N: ArchivedNode<K, T>,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(&id)
        && node.from().all(|parent| identifier_set.contains(&parent))
    {
        identifiers.push(id);
        identifier_set.insert(id);
        for child in node.to() {
            add_archived_node_identifiers(nodes, child, identifiers, identifier_set);
        }
    }
}

#[cfg(feature = "rkyv")]
fn add_archived_node_identifiers_rev<K, N, T, S>(
    nodes: &ArchivedHashMap<K, N>,
    id: K,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq,
    N: ArchivedNode<K, T>,
    S: BuildHasher + Default + Clone,
{
    if let Some(node) = nodes.get(&id)
        && node.from().all(|parent| identifier_set.contains(&parent))
    {
        identifiers.push(id);
        identifier_set.insert(id);
        for child in node.to().collect::<Vec<_>>().into_iter().rev() {
            add_archived_node_identifiers_rev(nodes, child, identifiers, identifier_set);
        }
    }
}
