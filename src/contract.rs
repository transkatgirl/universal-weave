#![allow(clippy::impl_trait_in_params, reason = "Readability")]

#[cfg(feature = "rkyv")]
use alloc::vec::Vec;
#[cfg(feature = "rkyv")]
use core::hash::Hash;

#[cfg(any(feature = "serde", feature = "rkyv"))]
use core::{error::Error, fmt};

#[cfg(feature = "rkyv")]
use hashbrown::{DefaultHashBuilder, hash_map::Entry};
#[cfg(feature = "rkyv")]
use scratchpads::Scratchpad;

#[cfg(feature = "rkyv")]
use rkyv::collections::swiss_table::{ArchivedHashMap, ArchivedHashSet, ArchivedIndexSet};

#[cfg(feature = "rkyv")]
use crate::{Node, Step, archived_longest_candidate_path_to_root, archived_set_reverse_order};

#[cfg(feature = "rkyv")]
pub fn archived_valid_topology<'a, K, N, T>(
    nodes: &'a ArchivedHashMap<K, N>,
    roots: &'a ArchivedIndexSet<K>,
    active: &'a ArchivedHashSet<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
{
    let mut topological = Vec::with_capacity(nodes.len());
    let mut scratchpad = Scratchpad::new();

    {
        let guard = scratchpad.guard();

        let mut stack = guard.vec_with_capacity(roots.len());
        let mut scratchpad_map =
            guard.map_with_capacity(nodes.len(), DefaultHashBuilder::default());

        for root in roots.iter().copied() {
            if scratchpad_map.contains_key(&root) {
                continue;
            }

            stack.push(Step::Enter(root));

            while let Some(step) = stack.pop() {
                match step {
                    Step::Enter(id) => match scratchpad_map.entry(id) {
                        Entry::Occupied(entry) => {
                            if !entry.get() {
                                return false;
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert_entry(false);

                            stack.push(Step::Exit(id));
                            stack.extend(
                                archived_set_reverse_order(nodes[&id].to())
                                    .copied()
                                    .map(Step::Enter),
                            );
                        }
                    },
                    Step::Exit(id) => {
                        scratchpad_map.insert(id, true);
                        topological.push(id);
                    }
                }
            }
        }

        if scratchpad_map.len() != nodes.len() {
            return false;
        }
    }

    topological.reverse();

    let guard = scratchpad.guard();

    let mut path = guard.vec_with_capacity(active.len());
    let mut scratchpad_map = guard.map_with_capacity(nodes.len(), DefaultHashBuilder::default());

    archived_longest_candidate_path_to_root(
        nodes,
        &topological,
        |id| active.contains(id),
        &mut scratchpad_map,
        |id| path.push(id),
    );

    path.len() == active.len() && path.into_iter().all(|id| active.contains(&id))
}

#[cfg(any(feature = "serde", feature = "rkyv"))]
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[must_use]
pub struct ValidationError;

#[cfg(any(feature = "serde", feature = "rkyv"))]
impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "validation failed")
    }
}

#[cfg(any(feature = "serde", feature = "rkyv"))]
#[allow(clippy::missing_trait_methods, reason = "API limitation")]
impl Error for ValidationError {}
