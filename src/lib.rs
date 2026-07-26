//! General-purpose building blocks for [Loom](https://generative.ink/posts/loom-interface-to-the-multiverse/) implementations.

/*

# 0.1.0 Checklist:
- [ ] Linting using all available clippy lints
- [ ] Rewrite all traversal logic to be non-recursive
- [ ] IMPORTANT - Review function contracts to ensure consistency with documentation & reasonable behavior
    - [ ] IMPORTANT - Review validate() behavior
- [ ] Add validate() to Weave
- [ ] Ensure crate is compliant with https://rust-lang.github.io/api-guidelines/checklist.html
- [ ] Full documentation review (including README)
    - [ ] Add crate examples
- [ ] Full code review
- [ ] Improve test coverage
    - [ ] Property tests for LoggedWeave
    - [ ] Property tests for DependentLoroWeave CRDT merging
    - [ ] Property tests for IndependentWeave::from(DependentWeave)
    - [ ] Property tests for IndependentWeave cycle detection
    - [ ] Property tests for IndependentWeave behavior parity with DependentWeave?
    - [ ] Property tests for Archived structs
    - [ ] Add unit tests until test coverage is 100%
- [ ] Publish to crates.io

# 0.2.0 Checklist:
- [ ] Separate bookmarking into a Weave wrapper?
- [ ] Remove all opportunities for internal inconsistency
    - [ ] Add validation at deserialization time

# Ideas for future releases:
- Formal verification using Verus once it supports enough of the language features

*/

#![forbid(unsafe_code)]
#![forbid(non_ascii_idents)]
#![warn(let_underscore)]
#![warn(clippy::pedantic)]
#![warn(clippy::cargo)]
#![allow(clippy::multiple_crate_versions, reason = "Unresolvable")]
#![warn(clippy::nursery)]

mod contract;
pub mod dependent;
pub mod independent;
pub mod wrappers;

#[cfg(feature = "rkyv")]
pub mod versioning;

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    hash::{BuildHasher, Hash},
    ops::Index,
};

pub use contracts;
pub use indexmap;
pub use stacksafe;

#[cfg(feature = "rkyv")]
pub use rkyv;

#[cfg(feature = "loro")]
pub use loro;

#[cfg(feature = "rkyv")]
use rkyv::option::ArchivedOption;

#[cfg(feature = "serde")]
pub use serde;

/// An item within a [`Weave`] which can be connected to other items.
pub trait Node<K, T>
where
    K: Hash + Copy + Eq,
{
    /// Identifiers corresponding to the node's parents.
    type From;
    /// Identifiers corresponding to the node's children.
    type To;

    /// Returns the node's unique identifier.
    fn id(&self) -> K;
    /// Returns a reference to the identifiers corresponding to the node's parents.
    fn from(&self) -> &Self::From;
    /// Returns a reference to the identifiers corresponding to the node's children.
    fn to(&self) -> &Self::To;
    /// Returns `true` if the node is considered "active".
    ///
    /// The meaning of this value can depend on the underlying [`Weave`] implementation.
    fn is_active(&self) -> bool;
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
    Two(T, T),
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
///
/// # Internal inconsistency and Panics
///
/// If a Weave is internally inconsistent, operations on it may panic, infinitely loop, or exhibit undocumented behavior. However, an internally inconsistent Weave will never result in unsafe behavior.
///
/// Operations on a Weave should never result in internal inconsistency, except in the following cases:
/// - The weave was already internally inconsistent.
/// - An operation resulted in a panic, and further operations were attempted on the same Weave through the use of [`std::panic::catch_unwind`].
///
/// However, Weave objects which have been deserialized from an untrusted source may be internally inconsistent.
pub trait Weave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    /// Mapping between identifiers and nodes.
    type Nodes;
    /// Identifiers of "root" nodes (nodes which do not have any parents).
    type Roots;

    /// Returns the number of nodes stored within the Weave.
    fn len(&self) -> usize;
    /// Returns `true` if the Weave does not contain any nodes.
    fn is_empty(&self) -> bool;
    /// Returns a reference to the identifier:node mapping.
    fn nodes(&self) -> &Self::Nodes;
    /// Returns a reference to the identifiers of "root" nodes (nodes which do not have any parents).
    fn roots(&self) -> &Self::Roots;
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
    /// Recursively builds a list of all children of the specified node ordered by their positions in the Weave.
    fn get_ordered_node_identifiers_from(&mut self, id: &K, output: &mut Vec<K>);
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
    /// Inserts a node into the Weave, returning `true` if the insertion was successful.
    ///
    /// This function may change the active status of nodes if it is necessary to preserve internal consistency.
    fn add_node(&mut self, node: N) -> bool;
    /// Sets the active status of a node with the specified identifier.
    ///
    /// This function may change the active status of other nodes in an implementation-specific manner if it is necessary to preserve internal consistency.
    fn set_node_active_status(&mut self, id: &K, value: bool) -> bool;
    /// Removes a node with the specified identifier, returning its value if it was present within the Weave.
    ///
    /// This function may update other nodes if it is necessary to preserve internal consistency.
    ///
    /// This function uses [`Weave::remove_node_tracked`] internally.
    fn remove_node(&mut self, id: &K) -> Option<N>;
    /// Removes a node with the specified identifier, returning `true` if it was present within the Weave.
    ///
    /// This function may update other nodes if it is necessary to preserve internal consistency. Every removed node will be returned by the `on_removal` call, with removal ordering being defined by the `Weave` implementation.
    ///
    /// # Panics
    ///
    /// May panic if `on_removal` panics.
    fn remove_node_tracked(&mut self, id: &K, on_removal: impl FnMut(N)) -> bool;
    /// Removes all nodes from the Weave.
    fn remove_all_nodes(&mut self);
}

/// A [`Weave`] containing document-wide metadata.
pub trait MetadataWeave<K, N, T, M>: Weave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    /// Returns a reference to the Weave's associated metadata.
    fn metadata(&self) -> &M;
    /// Mutable access to the Weave's associated metadata.
    ///
    /// # Panics
    ///
    /// May panic if `callback` panics.
    fn metadata_mut<O>(&mut self, callback: impl FnOnce(&mut M) -> O) -> O;
}

/// A [`Weave`] where nodes can be bookmarked.
pub trait BookmarkableWeave<K, N, T>: Weave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    /// Identifiers of bookmarked nodes.
    type Bookmarks;

    /// Returns a reference to the identifiers of bookmarked nodes.
    fn bookmarks(&self) -> &Self::Bookmarks;
    /// Returns `true` if the Weave contains a bookmarked node with the specified identifier.
    fn contains_bookmark(&self, id: &K) -> bool;
    /// Sets the bookmarked status of a node with the specified identifier.
    fn set_node_bookmarked_status(&mut self, id: &K, value: bool) -> bool;
}

/// A [`Weave`] where the ordering of nodes is stable and can be user-defined.
pub trait SortableWeave<K, N, T>: Weave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    /// Builds a list of all node identifiers ordered by their positions in the Weave.
    ///
    /// Unlike [`Weave::get_ordered_node_identifiers`], this function reverses the ordering of a node's children.
    fn get_ordered_node_identifiers_reversed_children(&mut self, output: &mut Vec<K>);
    /// Recursively builds a list of all children of the specified node ordered by their positions in the Weave.
    ///
    /// Unlike [`Weave::get_ordered_node_identifiers_from`], this function reverses the ordering of a node's children.
    fn get_ordered_node_identifiers_from_reversed_children(&mut self, id: &K, output: &mut Vec<K>);
    /// Sorts the child nodes of a parent node with the specified identifier using the comparison function `cmp`.
    ///
    /// # Panics
    ///
    /// May panic if `cmp` does not implement a [total order](https://en.wikipedia.org/wiki/Total_order), or if `cmp` itself panics.
    fn sort_node_children_by(&mut self, id: &K, cmp: impl FnMut(&N, &N) -> Ordering) -> bool;
    /// Sorts the identifiers of a parent node's children with the specified identifier using the comparison function `cmp`.
    ///
    /// # Panics
    ///
    /// May panic if `cmp` does not implement a [total order](https://en.wikipedia.org/wiki/Total_order), or if `cmp` itself panics.
    fn sort_node_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool;
    /// Sorts "root" nodes (nodes which do not have any parents) using the comparison function `cmp`.
    ///
    /// # Panics
    ///
    /// May panic if `cmp` does not implement a [total order](https://en.wikipedia.org/wiki/Total_order), or if `cmp` itself panics.
    fn sort_roots_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering);
    /// Sorts the identifiers of "root" nodes (nodes which do not have any parents) using the comparison function `cmp`.
    ///
    /// # Panics
    ///
    /// May panic if `cmp` does not implement a [total order](https://en.wikipedia.org/wiki/Total_order), or if `cmp` itself panics.
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering);
}

/// A [`Weave`] where the ordering of bookmarked nodes is stable and can be user-defined.
pub trait SortableBookmarkableWeave<K, N, T>:
    BookmarkableWeave<K, N, T> + SortableWeave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    /// Sorts bookmarked nodes using the comparison function `cmp`.
    ///
    /// # Panics
    ///
    /// May panic if `cmp` does not implement a [total order](https://en.wikipedia.org/wiki/Total_order), or if `cmp` itself panics.
    fn sort_bookmarks_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering);
    /// Sorts the identifiers of bookmarked nodes using the comparison function `cmp`.
    ///
    /// # Panics
    ///
    /// May panic if `cmp` does not implement a [total order](https://en.wikipedia.org/wiki/Total_order), or if `cmp` itself panics.
    fn sort_bookmarks_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering);
}

/// A [`Weave`] where only one [`Node`] can be considered "active" at a time.
pub trait ActiveSingularWeave<K, N, T>: Weave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    /// Returns the active node's identifier, if any.
    fn active(&self) -> Option<K>;
}

/// A [`Weave`] where every [`Node`] in the active path is always considered "active".
pub trait ActivePathWeave<K, N, T>: Weave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    /// Identifiers of active nodes.
    type Active;

    /// Returns a reference to the identifiers of active nodes.
    fn active(&self) -> &Self::Active;
}

/// A [`Weave`] where [`Node`] objects do not depend on their parents in order to be meaningful.
pub trait IndependentWeave<K, N, T>: Weave<K, N, T> + SemiIndependentWeave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
    T: IndependentContents,
{
    /// Moves a node with the specified identifier to a new set of parent nodes, returning `true` if the move was successful.
    ///
    /// This function may change the active status of other nodes if it is necessary to preserve internal consistency.
    fn move_node(&mut self, id: &K, new_parents: &[K]) -> bool;
}

/// A [`Weave`] where [`Node`] objects do not depend on the *contents* of their parents in order to be meaningful.
pub trait SemiIndependentWeave<K, N, T>: Weave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
    T: IndependentContents,
{
    /// Mutable access to the contents of a node with the specified identifier.
    ///
    /// # Panics
    ///
    /// May panic if `callback` panics.
    fn get_contents_mut<O>(&mut self, id: &K, callback: impl FnOnce(&mut T) -> O) -> Option<O>;
}

/// A [`Weave`] where the contents of [`Node`] objects can be split and merged.
pub trait DiscreteWeave<K, N, T>: Weave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
    T: DiscreteContents,
{
    /// Splits a node with the specified identifier at the given index, creating a new node with the identifier `new_id`.
    ///
    /// Returns `false` if splitting the node failed or the node could not be found.
    fn split_node(&mut self, id: &K, at: usize, new_id: K) -> bool;
    /// Merges a node with the specified identifier with its parent, with the newly merged node inheriting the parent's identifier.
    ///
    /// Returns the identifier of the merged node if merging was successful.
    fn merge_with_parent(&mut self, id: &K) -> Option<K>;
}

/// A [`Weave`] where [`Node`] objects can be meaningfully deduplicated by their contents.
pub trait DeduplicatableWeave<K, N, T>: Weave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
    T: DeduplicatableContents,
{
    /// An iterator over the specified node's sibling identifiers which contain contents which are duplicates of the specified node's contents.
    fn find_duplicates(&self, id: &K) -> impl Iterator<Item = K>;
}

#[cfg(feature = "rkyv")]
/// A read-only [`Weave`] which has been decoded using zero-copy deserialization.
pub trait ArchivedWeave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    /// Mapping between identifiers and nodes.
    type Nodes;
    /// Identifiers of "root" nodes (nodes which do not have any parents).
    type Roots;

    /// Returns the number of nodes stored within the Weave.
    fn len(&self) -> usize;
    /// Returns `true` if the Weave does not contain any nodes.
    fn is_empty(&self) -> bool;
    /// Returns a reference to the identifier:node mapping.
    fn nodes(&self) -> &Self::Nodes;
    /// Returns a reference to the identifiers of "root" nodes (nodes which do not have any parents).
    fn roots(&self) -> &Self::Roots;
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
    /// Recursively builds a list of all children of the specified node ordered by their positions in the Weave.
    fn get_ordered_node_identifiers_from(&self, id: &K, output: &mut Vec<K>);
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
/// An [`ArchivedWeave`] containing document-wide metadata.
pub trait ArchivedMetadataWeave<K, N, T, M>: ArchivedWeave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    /// Returns a reference to the Weave's associated metadata.
    fn metadata(&self) -> &M;
}

#[cfg(feature = "rkyv")]
/// An [`ArchivedWeave`] where nodes can be bookmarked.
pub trait ArchivedBookmarkableWeave<K, N, T>: ArchivedWeave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    /// Identifiers of bookmarked nodes.
    type Bookmarks;

    /// Returns a reference to the identifiers of bookmarked nodes.
    fn bookmarks(&self) -> &Self::Bookmarks;
    /// Returns `true` if the Weave contains a bookmarked node with the specified identifier.
    fn contains_bookmark(&self, id: &K) -> bool;
}

#[cfg(feature = "rkyv")]
/// An [`ArchivedWeave`] where the ordering of nodes is stable and can be user-defined.
pub trait ArchivedSortableWeave<K, N, T>: ArchivedWeave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    /// Builds a list of all node identifiers ordered by their positions in the Weave.
    ///
    /// Unlike [`ArchivedWeave::get_ordered_node_identifiers`], this function reverses the ordering of a node's children.
    fn get_ordered_node_identifiers_reversed_children(&self, output: &mut Vec<K>);
    /// Recursively builds a list of all children of the specified node ordered by their positions in the Weave.
    ///
    /// Unlike [`ArchivedWeave::get_ordered_node_identifiers_from`], this function reverses the ordering of a node's children.
    fn get_ordered_node_identifiers_from_reversed_children(&mut self, id: &K, output: &mut Vec<K>);
}

#[cfg(feature = "rkyv")]
/// An [`ArchivedWeave`] where only one [`Node`] can be considered "active" at a time.
pub trait ArchivedActiveSingularWeave<K, N, T>: ArchivedWeave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    /// Returns the active node's identifier, if any.
    fn active(&self) -> ArchivedOption<K>;
}

#[cfg(feature = "rkyv")]
/// An [`ArchivedWeave`] where every [`Node`] in the active path is always considered "active".
pub trait ArchivedActivePathWeave<K, N, T>: ArchivedWeave<K, N, T>
where
    K: Hash + Copy + Eq,
    N: Node<K, T>,
{
    /// Identifiers of active nodes.
    type Active;

    /// Returns a reference to the identifiers of active nodes.
    fn active(&self) -> &Self::Active;
}

fn topological_sort<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    id: &'a K,
    scratchpad: &mut VecDeque<K>,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push_back(*id);

    while let Some(id) = scratchpad.pop_back() {
        let node = &nodes[&id];

        if !identifier_set.contains(&id)
            && node
                .from()
                .into_iter()
                .all(|parent| identifier_set.contains(parent))
        {
            identifiers.push(id);
            identifier_set.insert(id);
            scratchpad.extend(node.to().into_iter().rev().copied());
        }
    }
}

fn topological_sort_subgraph<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    filter: &impl Fn(&K) -> bool,
    id: &'a K,
    scratchpad: &mut VecDeque<K>,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push_back(*id);

    while let Some(id) = scratchpad.pop_back() {
        let node = &nodes[&id];

        if filter(&id)
            && !identifier_set.contains(&id)
            && node
                .from()
                .into_iter()
                .all(|parent| identifier_set.contains(parent) || !filter(parent))
        {
            identifiers.push(id);
            identifier_set.insert(id);
            scratchpad.extend(node.to().into_iter().rev().copied());
        }
    }
}

fn topological_sort_rev<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    id: &'a K,
    scratchpad: &mut VecDeque<K>,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push_back(*id);

    while let Some(id) = scratchpad.pop_back() {
        let node = &nodes[&id];

        if !identifier_set.contains(&id)
            && node
                .from()
                .into_iter()
                .all(|parent| identifier_set.contains(parent))
        {
            identifiers.push(id);
            identifier_set.insert(id);
            scratchpad.extend(node.to().into_iter().copied());
        }
    }
}

#[stacksafe::stacksafe]
fn shortest_path_to_ancestor<'a, K, N, T, S>(
    nodes: &'a impl Index<&'a K, Output = N>,
    id: &'a K,
    target: &impl Fn(&'a N) -> bool,
    scratchpad_list: &mut Vec<K>,
    scratchpad_set: &mut HashSet<K, S>,
    path: &mut Vec<K>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    let node = nodes.index(id);

    if scratchpad_set.insert(*id) {
        scratchpad_list.push(*id);

        if target(node) {
            if path.is_empty() || path.len() > scratchpad_list.len() {
                path.clone_from(scratchpad_list);
            }
        } else {
            for parent in node.from() {
                shortest_path_to_ancestor(
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

#[stacksafe::stacksafe]
fn shortest_path_to_descendant<'a, K, N, T, S>(
    nodes: &'a impl Index<&'a K, Output = N>,
    id: &'a K,
    target: &impl Fn(&'a N) -> bool,
    scratchpad_list: &mut Vec<K>,
    scratchpad_set: &mut HashSet<K, S>,
    path: &mut Vec<K>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    let node = nodes.index(id);

    if scratchpad_set.insert(*id) {
        scratchpad_list.push(*id);

        if target(node) {
            if path.is_empty() || path.len() > scratchpad_list.len() {
                path.clone_from(scratchpad_list);
            }
        } else {
            for child in node.to() {
                shortest_path_to_descendant(
                    nodes,
                    child,
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

fn longest_path_to_root<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    topological_order: &'a [K],
    scratchpad_map: &mut HashMap<K, usize, S>,
    reversed_path: &mut Vec<K>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator + ExactSizeIterator>,
    S: BuildHasher + Default + Clone,
{
    let mut longest_global_distance = None;

    for id in topological_order {
        let longest_distance = nodes[id]
            .from()
            .into_iter()
            .map(|parent| scratchpad_map.get(parent).copied().unwrap_or_default())
            .max()
            .map(|l| l + 1)
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
                .into_iter()
                .max_by_key(|id| scratchpad_map.get(*id).copied());
        }
    }
}

fn ancestor_subgraph<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    id: K,
    scratchpad: &mut VecDeque<K>,
    identifiers: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T>,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push_back(id);

    while let Some(id) = scratchpad.pop_back() {
        if identifiers.insert(id) {
            scratchpad.extend(nodes[&id].from().into_iter().rev().copied());
        }
    }
}

fn descendant_subgraph<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    id: K,
    scratchpad: &mut VecDeque<K>,
    identifiers: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T>,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push_back(id);

    while let Some(id) = scratchpad.pop_back() {
        if identifiers.insert(id) {
            scratchpad.extend(nodes[&id].to().into_iter().rev().copied());
        }
    }
}
