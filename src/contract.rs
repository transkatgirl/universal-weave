use std::{collections::HashSet, hash::Hash, ops::Index};

use crate::{Node, add_node_identifiers, add_node_identifiers_rev};

pub(crate) fn lacks_duplicates<'a, I, T>(value: &'a I) -> bool
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

pub(crate) fn matches_add_node_identifiers<'a, K, N, T>(
    nodes: &'a impl Index<&'a K, Output = N>,
    ids: impl IntoIterator<Item = &'a K>,
    value: &'a [K],
) -> bool
where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
{
    let mut identifiers = Vec::with_capacity(value.len());
    let mut identifier_set = HashSet::with_capacity(value.len());

    for id in ids {
        let node = nodes.index(id);
        for parent in node.from() {
            identifier_set.insert(*parent);
        }
        add_node_identifiers(nodes, id, &mut identifiers, &mut identifier_set);
    }

    identifiers == value
}

pub(crate) fn matches_add_node_identifiers_rev<'a, K, N, T>(
    nodes: &'a impl Index<&'a K, Output = N>,
    ids: impl IntoIterator<Item = &'a K>,
    value: &'a [K],
) -> bool
where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
{
    let mut identifiers = Vec::with_capacity(value.len());
    let mut identifier_set = HashSet::with_capacity(value.len());

    for id in ids {
        let node = nodes.index(id);
        for parent in node.from() {
            identifier_set.insert(*parent);
        }
        add_node_identifiers_rev(nodes, id, &mut identifiers, &mut identifier_set);
    }

    identifiers == value
}

pub(crate) fn valid_ordered_nodes<'a, K, N, T>(
    nodes: &'a impl Index<&'a K, Output = N>,
    value: &'a [K],
) -> bool
where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
{
    let mut visited = HashSet::with_capacity(value.len());

    for item in value {
        let node = &nodes[item];

        if !((node
            .from()
            .into_iter()
            .any(|parent| visited.contains(parent))
            || node.from().into_iter().next().is_none())
            && visited.insert(node.id()))
        {
            return false;
        }
    }

    true
}

pub(crate) fn valid_thread<'a, K, N, T>(
    nodes: &'a impl Index<&'a K, Output = N>,
    value: &'a [K],
) -> bool
where
    K: Hash + Copy + Eq + 'a,
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
            if node.from().into_iter().find(|a| *a == &last).is_none() {
                return false;
            }
        } else if node.from().into_iter().next().is_some() {
            return false;
        }
        last_id = Some(*item);
    }

    true
}
