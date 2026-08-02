#![allow(clippy::impl_trait_in_params, reason = "Readability")]

use alloc::vec::Vec;
use core::hash::{BuildHasher, Hash};

#[cfg(debug_assertions)]
use core::ops::Index;

#[cfg(any(feature = "serde", feature = "rkyv"))]
use core::{error::Error, fmt};

use hashbrown::{HashMap, HashSet};

use crate::{Node, longest_candidate_path_to_root, topological_sort};

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

pub fn active_path_is_valid<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    roots: impl Iterator<Item = &'a K>,
    active: &'a HashSet<K, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator + ExactSizeIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    let mut scratchpad = Vec::with_capacity(nodes.len());
    let mut scratchpad_list = Vec::with_capacity(nodes.len());
    let mut scratchpad_list_2 = Vec::with_capacity(nodes.len());
    let mut scratchpad_set = HashSet::with_capacity_and_hasher(nodes.len(), S::default());
    let mut scratchpad_map = HashMap::with_capacity_and_hasher(nodes.len(), S::default());

    for root in roots {
        topological_sort(
            nodes,
            root,
            &mut scratchpad,
            &mut scratchpad_list,
            &mut scratchpad_set,
            &mut scratchpad_map,
        );
    }

    scratchpad_set.clear();
    scratchpad_map.clear();

    longest_candidate_path_to_root(
        nodes,
        &scratchpad_list,
        &|id| active.contains(id),
        &mut scratchpad_map,
        &mut scratchpad_list_2,
    );

    scratchpad_set.extend(scratchpad_list_2);

    scratchpad_set.len() == active.len()
        && scratchpad_set.into_iter().all(|id| active.contains(&id))
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
