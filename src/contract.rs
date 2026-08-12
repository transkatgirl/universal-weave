#![allow(clippy::impl_trait_in_params, reason = "Readability")]

use alloc::vec::Vec;
use core::hash::{BuildHasher, Hash};

#[cfg(debug_assertions)]
use core::ops::Index;

#[cfg(any(feature = "serde", feature = "rkyv"))]
use core::{error::Error, fmt};

use hashbrown::{HashMap, HashSet, hash_map::Entry};
use indexmap::IndexSet;
use scratchpads::Scratchpad;

#[cfg(feature = "rkyv")]
use hashbrown::DefaultHashBuilder;

#[cfg(feature = "rkyv")]
use rkyv::collections::swiss_table::{ArchivedHashMap, ArchivedHashSet, ArchivedIndexSet};

use crate::{Node, Step, longest_candidate_path_to_root};

#[cfg(feature = "rkyv")]
use crate::{archived_longest_candidate_path_to_root, archived_set_reverse_order};

#[cfg(debug_assertions)]
pub fn lacks_duplicates<'a, I, T>(value: &'a I) -> bool
where
    &'a I: IntoIterator<Item = T, IntoIter: ExactSizeIterator>,
    T: Hash + Eq,
{
    let value = value.into_iter();

    let mut set = HashSet::with_capacity(value.len());

    for item in value {
        if !set.insert(item) {
            return false;
        }
    }

    true
}

#[cfg(debug_assertions)]
pub fn valid_topological_sort<'a, K, N, T, S>(nodes: &'a HashMap<K, N, S>, value: &'a [K]) -> bool
where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    let mut seen = HashSet::with_capacity_and_hasher(value.len(), S::default());

    for id in value {
        if !(nodes[id]
            .from()
            .into_iter()
            .all(|parent| seen.contains(parent))
            && seen.insert(id))
        {
            return false;
        }
    }

    true
}

#[cfg(debug_assertions)]
pub fn valid_path<'a, K, N, T>(nodes: &'a impl Index<&'a K, Output = N>, value: &'a [K]) -> bool
where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
{
    let mut last_id = None;

    for item in value.iter().rev() {
        let node = &nodes[item];

        if let Some(last) = last_id {
            if !node.from().into_iter().any(|a| a == &last) {
                return false;
            }
        } else if node.from().into_iter().next().is_some() {
            return false;
        }
        last_id = Some(*item);
    }

    true
}

pub fn valid_topology<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    roots: &'a IndexSet<K, S>,
    active: &'a HashSet<K, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T, From = IndexSet<K, S>, To = IndexSet<K, S>> + 'a,
    S: BuildHasher + Default + Clone,
{
    let mut topological = Vec::with_capacity(nodes.len());
    let mut scratchpad = Scratchpad::new();

    {
        let guard = scratchpad.guard();

        let mut stack = guard.vec_with_capacity(roots.len());
        let mut scratchpad_map = guard.map_with_capacity(nodes.len(), S::default());

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
                                nodes[&id].to().into_iter().copied().rev().map(Step::Enter),
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
    let mut scratchpad_map = guard.map_with_capacity(nodes.len(), S::default());

    longest_candidate_path_to_root(
        nodes,
        &topological,
        |id| active.contains(id),
        &mut scratchpad_map,
        |id| path.push(id),
    );

    path.len() == active.len() && path.into_iter().all(|id| active.contains(&id))
}

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
