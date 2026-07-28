#![allow(clippy::impl_trait_in_params, reason = "Readability")]

use alloc::vec::Vec;
use core::{
    hash::{BuildHasher, Hash},
    ops::Index,
};

use hashbrown::{HashMap, HashSet};

use crate::{
    Node, longest_path_to_root, topological_sort, topological_sort_rev, topological_sort_subgraph,
};

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

pub fn matches_topological_sort<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    ids: impl IntoIterator<Item = &'a K>,
    value: &'a [K],
) -> bool
where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    let mut identifiers = Vec::with_capacity(value.len());
    let mut identifier_set = HashSet::with_capacity_and_hasher(value.len(), S::default());
    let mut scratchpad = Vec::with_capacity(value.len());

    for id in ids {
        let node = nodes.index(id);
        for parent in node.from() {
            identifier_set.insert(*parent);
        }
        topological_sort(
            nodes,
            id,
            &mut scratchpad,
            &mut identifiers,
            &mut identifier_set,
        );
    }

    identifiers == value
}

pub fn matches_topological_sort_rev<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    ids: impl IntoIterator<Item = &'a K>,
    value: &'a [K],
) -> bool
where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    let mut identifiers = Vec::with_capacity(value.len());
    let mut identifier_set = HashSet::with_capacity_and_hasher(value.len(), S::default());
    let mut scratchpad = Vec::with_capacity(value.len());

    for id in ids {
        let node = nodes.index(id);
        for parent in node.from() {
            identifier_set.insert(*parent);
        }
        topological_sort_rev(
            nodes,
            id,
            &mut scratchpad,
            &mut identifiers,
            &mut identifier_set,
        );
    }

    identifiers == value
}

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
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    let mut scratchpad = Vec::with_capacity(nodes.len());
    let mut scratchpad_list = Vec::with_capacity(nodes.len());
    let mut scratchpad_list_2 = Vec::with_capacity(nodes.len());
    let mut scratchpad_set = HashSet::with_capacity_and_hasher(nodes.len(), S::default());
    let mut scratchpad_map = HashMap::with_capacity_and_hasher(nodes.len(), S::default());

    for active_root in roots.filter(|root| active.contains(*root)) {
        topological_sort_subgraph(
            nodes,
            &|id| active.contains(id),
            active_root,
            &mut scratchpad,
            &mut scratchpad_list,
            &mut scratchpad_set,
        );
    }

    scratchpad_set.clear();

    longest_path_to_root(
        nodes,
        &scratchpad_list,
        &mut scratchpad_map,
        &mut scratchpad_list_2,
    );

    scratchpad_set.extend(scratchpad_list_2);

    scratchpad_set.len() == active.len()
        && scratchpad_set.into_iter().all(|id| active.contains(&id))
}
