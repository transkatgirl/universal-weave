//! The graph searches and topological sorts which [`IndependentWeave`](super::IndependentWeave) is
//! built on.
//!
//! - [`search`]: depth-first searches of a node's ancestors or descendants.
//! - [`kahn`]: topological sorts (Kahn's algorithm).
//! - [`paths`]: the longest path through some nodes to a root, and the shortest path to an
//!   ancestor.

mod kahn;
mod paths;
mod search;

pub(super) use kahn::{topological_sort, topological_sort_subgraph};
pub(super) use paths::{longest_candidate_path_to_root, shortest_path_to_ancestor};
pub(super) use search::{
    ancestor_subgraph, ancestor_subgraph_reaches, descendant_subgraph, descendant_subgraph_reaches,
};

use core::hash::{BuildHasher, Hash};

use creusot_std::{logic::Mapping, macros::*, snapshot::Snapshot};
use hashbrown::HashMap;
use indexmap::IndexSet;
use scratchpads::{ScratchpadMap, ScratchpadVec};

use super::IndependentNode;
use crate::{IndependentContents, Verifiable};

#[cfg(creusot)]
use crate::verification::{Id, Model, ids};

/// A predicate on identifiers which describes a filter closure, for verification.
#[cfg(creusot)]
pub(super) type Predicate<K> = Snapshot<Mapping<Id<K>, bool>>;

/// A predicate on identifiers which describes a filter closure, for verification.
#[cfg(not(creusot))]
pub(super) type Predicate<K> = Snapshot<Mapping<K, bool>>;

/// Sets `map` to the number of parents of each node.
#[trusted]
#[ensures(forall<k> (^map).model().contains(&k) == nodes.model().contains(&k))]
#[ensures(forall<k> nodes.model().contains(&k) ==> (^map).model()[k]@ == nodes.model()[k]@.from.len())]
pub(super) fn count_parents<K, T, S>(
    map: &mut ScratchpadMap<'_, K, usize, S>,
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
) where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    map.extend(nodes.iter().map(|(&k, n)| (k, n.from.len())));
}

/// Pushes the identifiers in `set` onto `stack`, in reverse order.
#[trusted]
#[ensures(ids((^stack).model()) == ids(stack.model()).concat(set.model().reverse()))]
pub(super) fn extend_from_set_rev<K, S>(stack: &mut ScratchpadVec<'_, K>, set: &IndexSet<K, S>)
where
    K: Copy + Verifiable,
{
    stack.extend(set.iter().rev().copied());
}
