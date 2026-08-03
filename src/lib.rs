//! General-purpose building blocks for [Loom](https://generative.ink/posts/loom-interface-to-the-multiverse/) implementations.
//!
//! This library aims to make building Loom implementations easier by providing the following primitives:
//! - [`dependent::DependentWeave`] - A tree-based [`Weave`] where each [`Node`] depends on the contents of the previous Node.
//!     - [`dependent::loro::DependentLoroWeave`] - A [`dependent::DependentWeave`] wrapper which adds collaborative editing using the [`loro`] CRDT library (requires `rkyv` and `loro` features to be enabled).
//! - [`independent::IndependentWeave`] - A DAG-based [`Weave`] where each [`Node`] does *not* depend on the contents of the previous Node.
//!
//! Efficient (de)serialization is supported using `rkyv` and `serde`. Basic functionality for versioning serialized data is provided by [`versioning::VersionedBytes`] (requires `rkyv` feature to be enabled).
//!

#![no_std]
#![forbid(non_ascii_idents)]
#![warn(missing_docs)]
#![warn(let_underscore)]
#![warn(clippy::pedantic)]
#![warn(clippy::cargo)]
#![allow(clippy::multiple_crate_versions, reason = "Unresolvable")]
#![warn(clippy::nursery)]
#![warn(clippy::restriction)]
#![allow(clippy::blanket_clippy_restriction_lints, reason = "Conflicting lint")]
#![allow(clippy::allow_attributes, reason = "Conflicting lint")]
#![allow(clippy::pattern_type_mismatch, reason = "Conflicting lint")]
#![allow(clippy::separated_literal_suffix, reason = "Conflicting lint")]
#![allow(
    clippy::field_scoped_visibility_modifiers,
    reason = "Used by IndependentWeave::from()"
)]
#![allow(
    clippy::missing_inline_in_public_items,
    reason = "Reasonable candidates have already been inlined"
)]
#![allow(clippy::exhaustive_enums, reason = "API")]
#![allow(clippy::exhaustive_structs, reason = "API")]
#![allow(clippy::little_endian_bytes, reason = "API")]
#![allow(clippy::partial_pub_fields, reason = "API")]
#![allow(clippy::pub_use, reason = "API")]
#![allow(clippy::arbitrary_source_item_ordering, reason = "Readability")]
#![allow(clippy::question_mark_used, reason = "Readability")]
#![allow(clippy::single_call_fn, reason = "Readability")]
#![allow(clippy::single_char_lifetime_names, reason = "Readability")]
#![allow(clippy::else_if_without_else, reason = "Style")]
#![allow(clippy::if_then_some_else_none, reason = "Style")]
#![allow(clippy::implicit_return, reason = "Style")]
#![allow(clippy::min_ident_chars, reason = "Style")]
#![allow(clippy::mod_module_files, reason = "Style")]
#![allow(clippy::module_name_repetitions, reason = "Style")]
#![allow(clippy::multiple_inherent_impl, reason = "Style")]
#![allow(clippy::try_err, reason = "Style")]
#![allow(clippy::allow_attributes_without_reason)] // TODO
#![allow(clippy::indexing_slicing)] // TODO
#![allow(clippy::unwrap_in_result)] // TODO
#![allow(clippy::unwrap_used)] // TODO
#![allow(clippy::missing_docs_in_private_items)] // TODO
#![allow(clippy::shadow_unrelated)] // TODO
#![allow(clippy::shadow_reuse)] // TODO

mod contract;
pub mod dependent;
pub mod independent;
pub mod wrappers;

#[cfg(feature = "rkyv")]
pub mod versioning;

pub use contracts;
pub use hashbrown;
pub use indexmap;

#[cfg(feature = "rkyv")]
pub use rkyv;

#[cfg(feature = "loro")]
pub use loro;

extern crate alloc;

use alloc::{collections::vec_deque::VecDeque, vec::Vec};
use core::{
    cmp::{Ordering, Reverse},
    hash::{BuildHasher, Hash},
};

use hashbrown::{HashMap, HashSet, hash_map::Entry};

#[cfg(feature = "serde")]
pub use serde;

/// An item within a [`Weave`] which can be connected to other items.
#[must_use]
pub trait Node<K, T>
where
    K: Hash + Copy + Eq + Ord,
{
    /// Identifiers corresponding to the node's parents.
    type From;
    /// Identifiers corresponding to the node's children.
    type To;

    /// Returns the node's unique identifier.
    #[must_use]
    fn id(&self) -> K;
    /// Returns a reference to the identifiers corresponding to the node's parents.
    #[must_use]
    fn from(&self) -> &Self::From;
    /// Returns a reference to the identifiers corresponding to the node's children.
    #[must_use]
    fn to(&self) -> &Self::To;
    /// Returns `true` if the node is considered active.
    ///
    /// The meaning of this value can depend on the underlying [`Weave`] implementation.
    #[must_use]
    fn is_active(&self) -> bool;
    /// Returns a reference to the node's contents.
    #[must_use]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(missing_docs, reason = "Enum items are self-explanatory")]
#[must_use]
pub enum DiscreteContentResult<T> {
    One(T),
    Two(T, T),
}

/// [`Node`] contents which do not depend on the contents of other [`Node`] objects in order to be meaningful.
pub trait IndependentContents {}

/// [`Node`] contents which can be meaningfully deduplicated.
///
/// Deduplication must be symmetric: `a.is_duplicate_of(b)` implies `b.is_duplicate_of(a)`.
pub trait DeduplicatableContents {
    /// Tests if `self` and `other` should be considered duplicates of each other.
    #[must_use]
    fn is_duplicate_of(&self, other: &Self) -> bool;
}

/// A document linking together multiple [`Node`] objects without cyclical links.
///
/// # Deserialization
///
/// If a Weave implementation supports deserialization, it must validate internal consistency during the deserialization process in a way which is robust to untrusted inputs.
///
/// # Panics
///
/// All panics should be assumed to leave the Weave in a malformed state unless otherwise specified by the implementation.
#[must_use]
pub trait Weave<K, N, T>
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
{
    /// Mapping between identifiers and nodes.
    type Nodes;
    /// Identifiers of root nodes (nodes which do not have any parents).
    type Roots;

    /// Returns the number of nodes stored within the Weave.
    #[must_use]
    fn len(&self) -> usize;
    /// Returns `true` if the Weave does not contain any nodes.
    #[must_use]
    fn is_empty(&self) -> bool;
    /// Returns a reference to the identifier:node mapping.
    #[must_use]
    fn nodes(&self) -> &Self::Nodes;
    /// Returns a reference to the identifiers of root nodes (nodes which do not have any parents).
    #[must_use]
    fn roots(&self) -> &Self::Roots;
    /// Returns `true` if the Weave contains a node with the specified identifier.
    #[must_use]
    fn contains(&self, id: &K) -> bool;
    /// Returns `true` if the Weave contains an active node (`node.is_active() == true`) with the specified identifier.
    ///
    /// The meaning of this value can depend on the underlying Weave implementation.
    #[must_use]
    fn contains_active(&self, id: &K) -> bool;
    /// Returns a reference to the node corresponding to the identifier.
    #[must_use]
    fn get_node(&self, id: &K) -> Option<&N>;
    /// Convenience method for `self.get_node(id).map(Node::from)`.
    #[must_use]
    fn get_node_parents(&self, id: &K) -> Option<&N::From>;
    /// Convenience method for `self.get_node(id).map(Node::to)`.
    #[must_use]
    fn get_node_children(&self, id: &K) -> Option<&N::To>;
    /// Convenience method for `self.get_node(id).map(Node::contents)`.
    #[must_use]
    fn get_node_contents(&self, id: &K) -> Option<&T>;
    /// Builds a list of all node identifiers ordered by their positions in the Weave.
    fn get_ordered_node_identifiers(&mut self, output: &mut Vec<K>);
    /// Recursively builds a list of all children of the specified node ordered by their positions in the Weave.
    fn get_ordered_node_identifiers_from(&mut self, id: &K, output: &mut Vec<K>);
    /// Builds a path through the Weave starting at the deepest active node and ending at a root node.
    ///
    /// In an [`ActivePathWeave`], this path will be the longest contiguous path of active nodes.
    fn get_active_path(&mut self, output: &mut Vec<K>);
    /// Builds a path through the Weave starting at the specified node and ending at a root node.
    ///
    /// In an [`ActivePathWeave`], this path will preferentially route through the active path.
    fn get_path_from(&mut self, id: &K, output: &mut Vec<K>);
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
    /// This function may remove or update other nodes if it is necessary to preserve internal consistency.
    ///
    /// This function uses the same removal logic as [`Weave::remove_node_tracked`].
    fn remove_node(&mut self, id: &K) -> Option<N>;
    /// Removes a node with the specified identifier, returning `true` if it was present within the Weave.
    ///
    /// This function may remove or update other nodes if it is necessary to preserve internal consistency. Every removed node will be returned by the `on_removal` call, with removal ordering being defined by the `Weave` implementation.
    ///
    /// # Panics
    ///
    /// May panic if `on_removal` panics.
    fn remove_node_tracked(&mut self, id: &K, on_removal: impl FnMut(N)) -> bool;
    /// Removes all nodes from the Weave.
    fn remove_all_nodes(&mut self);
}

/// A [`Weave`] containing document-wide metadata.
///
/// # Panics
///
/// All panics should be assumed to leave the Weave in a malformed state unless otherwise specified by the implementation.
pub trait MetadataWeave<K, N, T, M>: Weave<K, N, T>
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
{
    /// Returns a reference to the Weave's associated metadata.
    #[must_use]
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
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
{
    /// Identifiers of bookmarked nodes.
    type Bookmarks;

    /// Returns a reference to the identifiers of bookmarked nodes.
    #[must_use]
    fn bookmarks(&self) -> &Self::Bookmarks;
    /// Returns `true` if the Weave contains a bookmarked node with the specified identifier.
    #[must_use]
    fn contains_bookmark(&self, id: &K) -> bool;
    /// Sets the bookmarked status of a node with the specified identifier.
    fn set_node_bookmarked_status(&mut self, id: &K, value: bool) -> bool;
}

/// A [`Weave`] where the ordering of nodes is stable and can be user-defined.
///
/// # Panics
///
/// All panics should be assumed to leave the Weave in a malformed state unless otherwise specified by the implementation.
pub trait SortableWeave<K, N, T>: Weave<K, N, T>
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
{
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
    /// Sorts root nodes (nodes which do not have any parents) using the comparison function `cmp`.
    ///
    /// # Panics
    ///
    /// May panic if `cmp` does not implement a [total order](https://en.wikipedia.org/wiki/Total_order), or if `cmp` itself panics.
    fn sort_roots_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering);
    /// Sorts the identifiers of root nodes (nodes which do not have any parents) using the comparison function `cmp`.
    ///
    /// # Panics
    ///
    /// May panic if `cmp` does not implement a [total order](https://en.wikipedia.org/wiki/Total_order), or if `cmp` itself panics.
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering);
}

/// A [`Weave`] where the ordering of bookmarked nodes is stable and can be user-defined.
///
/// # Panics
///
/// All panics should be assumed to leave the Weave in a malformed state unless otherwise specified by the implementation.
pub trait SortableBookmarkableWeave<K, N, T>:
    BookmarkableWeave<K, N, T> + SortableWeave<K, N, T>
where
    K: Hash + Copy + Eq + Ord,
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

/// A [`Weave`] where only one [`Node`] can be considered active at a time.
pub trait ActiveSingularWeave<K, N, T>: Weave<K, N, T>
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
{
    /// Returns the active node's identifier, if any.
    #[must_use]
    fn active(&self) -> Option<K>;
}

/// A [`Weave`] where every [`Node`] in the active path is always considered active.
pub trait ActivePathWeave<K, N, T>: Weave<K, N, T>
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
{
    /// Identifiers of active nodes.
    type Active;

    /// Returns a reference to the identifiers of active nodes.
    #[must_use]
    fn active(&self) -> &Self::Active;
    /// Replaces the currently active path with the specified set of node IDs.
    ///
    /// If the new active path would result in internal inconsistency, this function will correct the path in an implementation-specific manner.
    fn set_active_path(&mut self, active: impl Iterator<Item = K>);
}

/// A [`Weave`] where [`Node`] objects do not depend on their parents in order to be meaningful.
pub trait IndependentWeave<K, N, T>: Weave<K, N, T> + SemiIndependentWeave<K, N, T>
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
    T: IndependentContents,
{
    /// Moves a node with the specified identifier to a new set of parent nodes, returning `true` if the move was successful.
    ///
    /// This function may change the active status of other nodes if it is necessary to preserve internal consistency.
    fn move_node(&mut self, id: &K, new_parents: &[K]) -> bool;
}

/// A [`Weave`] where [`Node`] objects do not depend on the *contents* of their parents in order to be meaningful.
///
/// # Panics
///
/// All panics should be assumed to leave the Weave in a malformed state unless otherwise specified by the implementation.
pub trait SemiIndependentWeave<K, N, T>: Weave<K, N, T>
where
    K: Hash + Copy + Eq + Ord,
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
///
/// # Panics
///
/// All panics should be assumed to leave the Weave in a malformed state unless otherwise specified by the implementation.
pub trait DiscreteWeave<K, N, T>: Weave<K, N, T>
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
    T: DiscreteContents,
{
    /// Splits a node with the specified identifier at the given index, creating a new child node with the identifier `new_id`.
    ///
    /// Returns `false` if splitting the node failed or the node could not be found.
    ///
    /// # Panics
    ///
    /// May panic if `T::split` panics.
    fn split_node(&mut self, id: &K, at: usize, new_id: K) -> bool;
    /// Merges a node with the specified identifier with its parent, with the newly merged node inheriting the parent's identifier.
    ///
    /// Returns the identifier of the merged node if merging was successful.
    ///
    /// # Panics
    ///
    /// May panic if `T::merge` panics.
    fn merge_with_parent(&mut self, id: &K) -> Option<K>;
}

/// A read-only [`Weave`].
#[must_use]
pub trait ImmutableWeave<K, N, T>
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
{
    /// Mapping between identifiers and nodes.
    type Nodes;
    /// Identifiers of root nodes (nodes which do not have any parents).
    type Roots;

    /// Returns the number of nodes stored within the Weave.
    #[must_use]
    fn len(&self) -> usize;
    /// Returns `true` if the Weave does not contain any nodes.
    #[must_use]
    fn is_empty(&self) -> bool;
    /// Returns a reference to the identifier:node mapping.
    #[must_use]
    fn nodes(&self) -> &Self::Nodes;
    /// Returns a reference to the identifiers of root nodes (nodes which do not have any parents).
    #[must_use]
    fn roots(&self) -> &Self::Roots;
    /// Returns `true` if the Weave contains a node with the specified identifier.
    #[must_use]
    fn contains(&self, id: &K) -> bool;
    /// Returns `true` if the Weave contains an active node (`node.is_active() == true`) with the specified identifier.
    ///
    /// The meaning of this value can depend on the underlying Weave implementation.
    #[must_use]
    fn contains_active(&self, id: &K) -> bool;
    /// Returns a reference to the node corresponding to the identifier.
    #[must_use]
    fn get_node(&self, id: &K) -> Option<&N>;
    /// Convenience method for `self.get_node(id).map(Node::from)`.
    #[must_use]
    fn get_node_parents(&self, id: &K) -> Option<&N::From>;
    /// Convenience method for `self.get_node(id).map(Node::to)`.
    #[must_use]
    fn get_node_children(&self, id: &K) -> Option<&N::To>;
    /// Convenience method for `self.get_node(id).map(Node::contents)`.
    #[must_use]
    fn get_node_contents(&self, id: &K) -> Option<&T>;
    /// Builds a list of all node identifiers ordered by their positions in the Weave.
    fn get_ordered_node_identifiers(&self, output: &mut Vec<K>);
    /// Recursively builds a list of all children of the specified node ordered by their positions in the Weave.
    fn get_ordered_node_identifiers_from(&self, id: &K, output: &mut Vec<K>);
    /// Builds a path through the Weave starting at the deepest active node and ending at a root node.
    ///
    /// In an [`ImmutableActivePathWeave`], this path will be the longest contiguous path of active nodes.
    fn get_active_path(&self, output: &mut Vec<K>);
    /// Builds a path through the Weave starting at the specified node and ending at a root node.
    ///
    /// In an [`ImmutableActivePathWeave`], this path will preferentially route through the active path.
    fn get_path_from(&self, id: &K, output: &mut Vec<K>);
}

/// An [`ImmutableWeave`] containing document-wide metadata.
pub trait ImmutableMetadataWeave<K, N, T, M>: ImmutableWeave<K, N, T>
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
{
    /// Returns a reference to the Weave's associated metadata.
    #[must_use]
    fn metadata(&self) -> &M;
}

/// An [`ImmutableWeave`] where nodes can be bookmarked.
pub trait ImmutableBookmarkableWeave<K, N, T>: ImmutableWeave<K, N, T>
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
{
    /// Identifiers of bookmarked nodes.
    type Bookmarks;

    /// Returns a reference to the identifiers of bookmarked nodes.
    #[must_use]
    fn bookmarks(&self) -> &Self::Bookmarks;
    /// Returns `true` if the Weave contains a bookmarked node with the specified identifier.
    #[must_use]
    fn contains_bookmark(&self, id: &K) -> bool;
}

/// An [`ImmutableWeave`] where only one [`Node`] can be considered active at a time.
pub trait ImmutableActiveSingularWeave<K, N, T>: ImmutableWeave<K, N, T>
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
{
    /// Returns the active node's identifier, if any.
    #[must_use]
    fn active(&self) -> Option<K>;
}

/// An [`ImmutableWeave`] where every [`Node`] in the active path is always considered active.
pub trait ImmutableActivePathWeave<K, N, T>: ImmutableWeave<K, N, T>
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
{
    /// Identifiers of active nodes.
    type Active;

    /// Returns a reference to the identifiers of active nodes.
    #[must_use]
    fn active(&self) -> &Self::Active;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Step<A, B> {
    Enter(A),
    Exit(B),
}

fn topological_sort<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    id: &'a K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K, S>,
    identifier_map: &mut HashMap<K, usize, S>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator + ExactSizeIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push(*id);

    while let Some(id) = scratchpad.pop() {
        let node = &nodes[&id];

        if identifier_set.contains(&id)
            || identifier_map
                .get(&id)
                .copied()
                .unwrap_or_else(|| node.from().into_iter().len())
                != 0
        {
            continue;
        }

        identifiers.push(id);
        identifier_set.insert(id);

        for child in node.to().into_iter().rev().copied() {
            let remaining = identifier_map
                .entry(child)
                .or_insert_with(|| nodes[&child].from().into_iter().len());
            *remaining = remaining.strict_sub(1);

            scratchpad.push(child);
        }
    }
}

fn topological_sort_subgraph<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    filter: &impl Fn(&K) -> bool,
    id: &'a K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K, S>,
    identifier_map: &mut HashMap<K, usize, S>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push(*id);

    while let Some(id) = scratchpad.pop() {
        let node = &nodes[&id];

        if !filter(&id)
            || identifier_set.contains(&id)
            || identifier_map.get(&id).copied().unwrap_or_else(|| {
                node.from()
                    .into_iter()
                    .filter(|&parent| filter(parent))
                    .count()
            }) != 0
        {
            continue;
        }

        identifiers.push(id);
        identifier_set.insert(id);

        for child in node.to().into_iter().rev().copied() {
            let remaining = identifier_map.entry(child).or_insert_with(|| {
                nodes[&child]
                    .from()
                    .into_iter()
                    .filter(|&parent| filter(parent))
                    .count()
            });
            *remaining = remaining.strict_sub(1);

            scratchpad.push(child);
        }
    }
}

fn detect_cycles<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    roots: impl Iterator<Item = K>,
    scratchpad: &mut Vec<Step<K, K>>,
    scratchpad_map: &mut HashMap<K, bool, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator + ExactSizeIterator>,
    S: BuildHasher + Default + Clone,
{
    for root in roots {
        if scratchpad_map.contains_key(&root) {
            continue;
        }

        scratchpad.push(Step::Enter(root));

        while let Some(step) = scratchpad.pop() {
            match step {
                Step::Enter(id) => {
                    scratchpad.push(Step::Exit(id));

                    match scratchpad_map.entry(id) {
                        Entry::Occupied(entry) => {
                            if !entry.get() {
                                return true;
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert_entry(false);

                            scratchpad.extend(
                                nodes[&id].to().into_iter().rev().copied().map(Step::Enter),
                            );
                        }
                    }
                }
                Step::Exit(id) => {
                    scratchpad_map.insert(id, true);
                }
            }
        }
    }

    scratchpad_map.len() != nodes.len()
}

fn shortest_path_to_ancestor<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    id: &'a K,
    target: &impl Fn(&'a N) -> bool,
    scratchpad: &mut VecDeque<K>,
    scratchpad_map: &mut HashMap<K, K, S>,
    scratchpad_set: &mut HashSet<K, S>,
    path: &mut Vec<K>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push_front(*id);
    scratchpad_set.insert(*id);

    while let Some(id) = scratchpad.pop_back() {
        let node = &nodes[&id];

        if target(node) {
            scratchpad.clear();

            path.push(id);

            while let Some(child) = scratchpad_map.remove(path.last().unwrap()) {
                path.push(child);
            }

            return;
        }

        for parent in node.from().into_iter().copied() {
            if scratchpad_set.insert(parent) {
                scratchpad.push_front(parent);
                scratchpad_map.insert(parent, id);
            }
        }
    }
}

fn longest_candidate_path_to_root<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    topological_order: &[K],
    is_candidate: &impl Fn(&K) -> bool,
    scratchpad_map: &mut HashMap<K, usize, S>,
    reversed_path: &mut Vec<K>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    let mut longest_distance = None;

    for id in topological_order {
        if !is_candidate(id) {
            continue;
        }

        let node = &nodes[id];
        let distance = if node.from().into_iter().next().is_none() {
            Some(0)
        } else {
            node.from()
                .into_iter()
                .filter_map(|parent| scratchpad_map.get(parent).copied())
                .max()
                .map(|l| l.strict_add(1))
        };

        if let Some(distance) = distance {
            scratchpad_map.insert(*id, distance);

            if longest_distance.is_none_or(|(value, _)| distance > value) {
                longest_distance = Some((distance, id));
            }
        }
    }

    let mut current = longest_distance.map(|(_, id)| id);

    while let Some(id) = current {
        reversed_path.push(*id);

        current = nodes[id]
            .from()
            .into_iter()
            .filter(|id| scratchpad_map.contains_key(*id))
            .min_by_key(|id| Reverse(scratchpad_map[*id]));
    }
}

fn ancestor_subgraph<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    id: K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T>,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push(id);

    while let Some(id) = scratchpad.pop() {
        if identifiers.insert(id) {
            scratchpad.extend(nodes[&id].from().into_iter().rev().copied());
        }
    }
}

fn descendant_subgraph<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    id: K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T>,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push(id);

    while let Some(id) = scratchpad.pop() {
        if identifiers.insert(id) {
            scratchpad.extend(nodes[&id].to().into_iter().rev().copied());
        }
    }
}
