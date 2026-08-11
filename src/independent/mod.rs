//! [`IndependentWeave`] is a DAG-based [`Weave`] where each [`Node`] does *not* depend on the contents of the previous Node.

use alloc::vec::Vec;
use core::{
    cmp::Ordering,
    hash::{BuildHasher, Hash},
    mem,
};

use hashbrown::{HashMap, HashSet};
use indexmap::IndexSet;
use scratchpads::{Scratchpad, ScratchpadMap};

#[cfg(debug_assertions)]
use contracts::contract;

#[cfg(feature = "rkyv")]
use rkyv::{
    Archive, Deserialize, Serialize,
    bytecheck::Verify,
    collections::swiss_table::{ArchivedHashMap, ArchivedHashSet, ArchivedIndexSet},
    rancor::{Fallible, Source, fail},
    with::Skip,
};

#[cfg(feature = "serde")]
use serde::{
    Deserialize as SerdeDeserialize, Deserializer as SerdeDeserializer,
    Serialize as SerdeSerialize, de::Error as _,
};

use crate::{
    ActivePathWeave, BookmarkableWeave, DiscreteContentResult, DiscreteContents, DiscreteWeave,
    IndependentContents, MetadataWeave, Node, SemiIndependentWeave, SortableBookmarkableWeave,
    SortableWeave, Weave, ancestor_subgraph, ancestor_subgraph_reaches,
    contract::valid_topology,
    dependent::{DependentNode, DependentWeave},
    descendant_subgraph, descendant_subgraph_reaches, longest_candidate_path_to_root,
    shortest_path_to_ancestor, topological_sort, topological_sort_subgraph,
};

#[cfg(debug_assertions)]
use crate::contract::{lacks_duplicates, valid_path, valid_topological_sort};

#[cfg(feature = "rkyv")]
use crate::{
    ImmutableActivePathWeave, ImmutableBookmarkableWeave, ImmutableMetadataWeave, ImmutableWeave,
    archived_ancestor_subgraph, archived_descendant_subgraph,
    archived_longest_candidate_path_to_root, archived_shortest_path_to_ancestor,
    archived_topological_sort, archived_topological_sort_subgraph,
    contract::archived_valid_topology,
};

#[cfg(any(feature = "serde", feature = "rkyv"))]
use crate::contract::ValidationError;

#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
/// A [`Node`] in a [`IndependentWeave`] document.
#[must_use]
pub struct IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    /// The node's unique identifier.
    pub id: K,
    /// The identifiers corresponding to the node's parents.
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "IndexSet<K, S>: SerdeSerialize",
            deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    pub from: IndexSet<K, S>,
    /// The identifiers corresponding to the node's children.
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "IndexSet<K, S>: SerdeSerialize",
            deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    pub to: IndexSet<K, S>,
    /// If the node should be considered active.
    ///
    /// Unlike [`DependentWeave`], [`IndependentWeave`] considers all nodes within an active path to be active.
    pub active: bool,
    /// If the node is bookmarked.
    pub bookmarked: bool,
    /// The node's contents.
    pub contents: T,
}

#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, S> PartialEq for IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents + PartialEq,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.from.len() == other.from.len()
            && self.to.len() == other.to.len()
            && self.from.iter().zip(other.from.iter()).all(|(a, b)| a == b)
            && self.to.iter().zip(other.to.iter()).all(|(a, b)| a == b)
            && self.active == other.active
            && self.bookmarked == other.bookmarked
            && self.contents == other.contents
    }
}

#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, S> Eq for IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents + Eq,
    S: BuildHasher + Default + Clone,
{
}

impl<K, T, S> IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    fn validate(&self) -> bool {
        self.from.is_disjoint(&self.to)
            && !self.from.contains(&self.id)
            && !self.to.contains(&self.id)
    }
}

impl<K, T, S> Node<K, T> for IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type From = IndexSet<K, S>;
    type To = IndexSet<K, S>;

    #[inline]
    fn id(&self) -> K {
        self.id
    }
    #[inline]
    fn from(&self) -> &Self::From {
        &self.from
    }
    #[inline]
    fn to(&self) -> &Self::To {
        &self.to
    }
    #[inline]
    fn is_active(&self) -> bool {
        self.active
    }
    #[inline]
    fn contents(&self) -> &T {
        &self.contents
    }
}

impl<K, T, S> From<DependentNode<K, T, S>> for IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn from(value: DependentNode<K, T, S>) -> Self {
        Self {
            id: value.id,
            from: IndexSet::from_iter(value.from),
            to: value.to,
            active: value.active,
            bookmarked: value.bookmarked,
            contents: value.contents,
        }
    }
}

impl<K, T, S> TryFrom<IndependentNode<K, T, S>> for DependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type Error = IndependentNode<K, T, S>;

    #[inline]
    fn try_from(value: IndependentNode<K, T, S>) -> Result<Self, Self::Error> {
        if value.from.len() < 2 {
            Ok(Self {
                id: value.id,
                from: value.from.into_iter().next(),
                to: value.to,
                active: value.active,
                bookmarked: value.bookmarked,
                contents: value.contents,
            })
        } else {
            Err(value)
        }
    }
}

/// A DAG-based [`Weave`] where each [`Node`] does *not* depend on the contents of the previous Node.
///
/// However, this additional flexibility results in worse performance and memory usage characteristics overall.
#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize))]
#[cfg_attr(feature = "rkyv", rkyv(bytecheck(verify)))]
#[must_use]
pub struct IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "HashMap<K, IndependentNode<K, T, S>, S>: SerdeSerialize",
            deserialize = "HashMap<K, IndependentNode<K, T, S>, S>: SerdeDeserialize<'de>"
        ))
    )]
    nodes: HashMap<K, IndependentNode<K, T, S>, S>,
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "IndexSet<K, S>: SerdeSerialize",
            deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    roots: IndexSet<K, S>,
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "HashSet<K, S>: SerdeSerialize",
            deserialize = "HashSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    active: HashSet<K, S>,
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "IndexSet<K, S>: SerdeSerialize",
            deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    bookmarked: IndexSet<K, S>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad: Scratchpad,

    /// The metadata associated with the weave.
    pub metadata: M,
}

#[cfg(feature = "serde")]
#[derive(SerdeDeserialize)]
#[serde(rename = "IndependentWeave")]
struct ProxyIndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[serde(bound(
        serialize = "HashMap<K, IndependentNode<K, T, S>, S>: SerdeSerialize",
        deserialize = "HashMap<K, IndependentNode<K, T, S>, S>: SerdeDeserialize<'de>"
    ))]
    nodes: HashMap<K, IndependentNode<K, T, S>, S>,
    #[serde(bound(
        serialize = "IndexSet<K, S>: SerdeSerialize",
        deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
    ))]
    roots: IndexSet<K, S>,
    #[serde(bound(
        serialize = "HashSet<K, S>: SerdeSerialize",
        deserialize = "HashSet<K, S>: SerdeDeserialize<'de>"
    ))]
    active: HashSet<K, S>,
    #[serde(bound(
        serialize = "IndexSet<K, S>: SerdeSerialize",
        deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
    ))]
    bookmarked: IndexSet<K, S>,
    metadata: M,
}

#[cfg(feature = "serde")]
#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<'de, K, T, M, S> SerdeDeserialize<'de> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + SerdeDeserialize<'de>,
    T: IndependentContents + SerdeDeserialize<'de>,
    M: SerdeDeserialize<'de>,
    S: BuildHasher + Default + Clone,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: SerdeDeserializer<'de>,
    {
        let proxy = ProxyIndependentWeave::deserialize(deserializer)?;
        let weave = Self {
            nodes: proxy.nodes,
            roots: proxy.roots,
            active: proxy.active,
            bookmarked: proxy.bookmarked,
            scratchpad: Scratchpad::new(),
            metadata: proxy.metadata,
        };

        if weave.validate() {
            Ok(weave)
        } else {
            Err(D::Error::custom(ValidationError))
        }
    }
}

#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, M, S> PartialEq for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents + PartialEq,
    M: PartialEq,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.roots.len() == other.roots.len()
            && self.bookmarked.len() == other.bookmarked.len()
            && self.active == other.active
            && self
                .roots
                .iter()
                .zip(other.roots.iter())
                .all(|(a, b)| a == b)
            && self
                .bookmarked
                .iter()
                .zip(other.bookmarked.iter())
                .all(|(a, b)| a == b)
            && self.nodes == other.nodes
            && self.metadata == other.metadata
    }
}

#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, M, S> Eq for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents + Eq,
    M: Eq,
    S: BuildHasher + Default + Clone,
{
}

impl<K, T, M, S> IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    /// Creates a new, empty [`IndependentWeave`] with at least the specified capacity.
    #[cfg_attr(debug_assertions, contract(
        ensures(ret.nodes.is_empty()),
        ensures(ret.validate())
    ))]
    pub fn with_capacity(capacity: usize, metadata: M) -> Self {
        let nodes = HashMap::with_capacity_and_hasher(capacity, S::default());
        let capacity = nodes.capacity();

        Self {
            nodes,
            roots: IndexSet::with_capacity_and_hasher(capacity, S::default()),
            active: HashSet::with_capacity_and_hasher(capacity, S::default()),
            bookmarked: IndexSet::with_capacity_and_hasher(capacity, S::default()),
            scratchpad: Scratchpad::new(),
            metadata,
        }
    }
    /// Returns the worst-case number of nodes that the weave can hold without reallocating.
    ///
    /// May be lower than `self.len()`.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.nodes
            .capacity()
            .min(self.roots.capacity())
            .min(self.active.capacity())
            .min(self.bookmarked.capacity())
    }
    /// Reserves capacity for at least `additional` more nodes.
    #[cfg_attr(debug_assertions, contract(
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active.clone()) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    pub fn reserve(&mut self, additional: usize) {
        self.nodes.reserve(additional);
        self.roots
            .reserve(self.nodes.capacity().saturating_sub(self.roots.len()));
        self.active
            .reserve(self.nodes.capacity().saturating_sub(self.active.len()));
        self.bookmarked
            .reserve(self.nodes.capacity().saturating_sub(self.bookmarked.len()));
    }
    /// Shrinks the capacity of the weave with a lower limit.
    #[cfg_attr(debug_assertions, contract(
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active.clone()) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.nodes.shrink_to(min_capacity);
        self.roots.shrink_to(min_capacity);
        self.active.shrink_to(min_capacity);
        self.bookmarked.shrink_to(min_capacity);
    }
    #[allow(
        clippy::too_many_lines,
        reason = "Cannot be split into smaller functions"
    )]
    #[cfg_attr(debug_assertions, contract(
        ensures(ret == self.nodes.contains_key(id)),
        ensures(!ret || value == self.active.contains(id)),
        ensures(self.validate())
    ))]
    fn update_node_activity_in_place(&mut self, id: &K, value: bool) -> bool {
        let at_end = if let Some(node) = self.nodes.get(id) {
            if node.active == value {
                return true;
            }

            if value {
                (node.from.is_empty() && self.active.is_empty())
                    || node.from.iter().any(|parent| {
                        self.active.contains(parent)
                            && self.nodes[parent]
                                .to
                                .iter()
                                .all(|child| !self.active.contains(child))
                    })
            } else {
                node.to.iter().all(|child| !self.active.contains(child))
            }
        } else {
            return false;
        };

        let node = self.nodes.get_mut(id).unwrap();
        node.active = value;
        if value {
            self.active.insert(node.id);
        } else {
            self.active.remove(id);
        }

        if at_end {
            return true;
        }

        if value {
            let has_descendants = !node.to.is_empty();

            let guard = self.scratchpad.guard();

            let mut stack = guard.vec();
            let mut closure = guard.set(S::default());

            ancestor_subgraph(&self.nodes, *id, &mut stack, &mut closure);

            let mut topological = guard.vec_with_capacity(closure.len());
            let mut scratchpad_map = guard.map_with_capacity(closure.len(), S::default());
            let mut scratchpad_map_2 = guard.map_with_capacity(closure.len(), S::default());
            let mut scratchpad_map_3: ScratchpadMap<'_, K, (usize, usize), S> =
                guard.map_with_capacity(closure.len(), S::default());
            let mut scratchpad_set = guard.set(S::default());

            for root in self
                .roots
                .iter()
                .filter(|id| closure.contains(*id))
                .copied()
            {
                topological_sort_subgraph(
                    &self.nodes,
                    &|id| closure.contains(id),
                    root,
                    &mut stack,
                    |id| topological.push(id),
                    &mut scratchpad_map,
                );
            }

            for id in topological.iter().copied() {
                let node = &self.nodes[&id];

                let best_parent = node
                    .from
                    .iter()
                    .map(|id| (id, scratchpad_map_3[id])) // score: (connectors, active)
                    .min_by(|(_, a), (_, b)| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

                let (parent, score) = if let Some((parent, mut score)) = best_parent {
                    if node.active {
                        score.1 = score.1.strict_add(1);
                    } else {
                        score.0 = score.0.strict_add(1);
                    }

                    (Some(parent), score)
                } else {
                    (None, if node.active { (0, 1) } else { (1, 0) })
                };

                if let Some(parent) = parent {
                    scratchpad_map_2.insert(id, *parent); // predecessors
                }

                scratchpad_map_3.insert(id, score);
            }

            let mut current = Some(id);

            while let Some(id) = current {
                scratchpad_set.insert(*id);
                current = scratchpad_map_2.get(id);
            }

            closure.clear();
            topological.clear();
            scratchpad_map.clear();
            scratchpad_map_2.clear();
            scratchpad_map_3.clear();

            if self.active.len() != 1 && has_descendants {
                descendant_subgraph(&self.nodes, *id, &mut stack, &mut closure);

                let has_active_descendant = if closure.len() >= self.active.len() {
                    self.active.iter().any(|a| a != id && closure.contains(a))
                } else {
                    closure.iter().any(|d| d != id && self.active.contains(d))
                };

                if has_active_descendant {
                    topological_sort_subgraph(
                        &self.nodes,
                        &|id| closure.contains(id),
                        *id,
                        &mut stack,
                        |id| topological.push(id),
                        &mut scratchpad_map,
                    );

                    for id in topological.drain(..).rev() {
                        let node = &self.nodes[&id];

                        let best_child = node
                            .to
                            .iter()
                            .map(|id| (id, scratchpad_map_3[id])) // score: (connectors, active)
                            .min_by(|(_, a), (_, b)| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

                        let (child, score) = if let Some((child, mut score)) = best_child {
                            if node.active {
                                score.1 = score.1.strict_add(1);
                            } else {
                                score.0 = score.0.strict_add(1);
                            }

                            (Some(child), score)
                        } else {
                            (None, if node.active { (0, 1) } else { (1, 0) })
                        };

                        if let Some(child) = child {
                            scratchpad_map_2.insert(id, *child); // successors
                        }

                        scratchpad_map_3.insert(id, score);
                    }

                    let mut current = Some(id);

                    while let Some(id) = current {
                        scratchpad_set.insert(*id);

                        current = if scratchpad_map_3[id].1 > usize::from(self.nodes[id].active) {
                            scratchpad_map_2.get(id)
                        } else {
                            None
                        };
                    }
                }
            }

            let mut disjoint = topological;

            disjoint.extend(
                self.active
                    .iter()
                    .filter(|id| !scratchpad_set.contains(*id))
                    .copied(),
            );

            for id in disjoint.drain(..) {
                self.nodes.get_mut(&id).unwrap().active = false;
                self.active.remove(&id);
            }

            disjoint.extend(
                scratchpad_set
                    .iter()
                    .filter(|id| !self.active.contains(*id))
                    .copied(),
            );

            for id in disjoint.drain(..) {
                self.nodes.get_mut(&id).unwrap().active = true;
                self.active.insert(id);
            }
        } else {
            self.fix_orphaned_activations();
        }

        true
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(self.validate())
    ))]
    fn fix_orphaned_activations(&mut self) {
        let guard = self.scratchpad.guard();

        let mut stack = guard.vec();
        let mut closure = guard.set_with_capacity(self.active.len(), S::default());

        for id in self.active.iter().copied() {
            ancestor_subgraph(&self.nodes, id, &mut stack, &mut closure);
        }

        let mut topological = guard.vec_with_capacity(closure.len());
        let mut scratchpad_map = guard.map_with_capacity(closure.len(), S::default());

        for root in self
            .roots
            .iter()
            .filter(|id| closure.contains(*id))
            .copied()
        {
            topological_sort_subgraph(
                &self.nodes,
                &|id| closure.contains(id),
                root,
                &mut stack,
                |id| topological.push(id),
                &mut scratchpad_map,
            );
        }

        scratchpad_map.clear();

        let mut candidate_path = guard.vec_with_capacity(self.active.len());

        longest_candidate_path_to_root(
            &self.nodes,
            &topological,
            &|id| self.active.contains(id),
            &mut scratchpad_map,
            |id| candidate_path.push(id),
        );

        topological.clear();
        let mut disjoint = topological;

        closure.clear();
        let mut candidate_path_set = closure;

        candidate_path_set.extend(candidate_path.drain(..));

        disjoint.extend(
            self.active
                .iter()
                .filter(|id| !candidate_path_set.contains(*id))
                .copied(),
        );

        for orphan in disjoint.drain(..) {
            self.active.remove(&orphan);
            if let Some(node) = self.nodes.get_mut(&orphan) {
                node.active = false;
            }
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(!ret || value || !self.active.contains(id) || (old(self.active.clone()) == self.active && self.nodes[id].to.iter().any(|id| self.contains_active(id)))),
        ensures(!ret || !value || self.contains_active(id) && !self.nodes[id].to.iter().any(|id| self.active.contains(id))),
        ensures(ret || old(self.active.clone()) == self.active),
        ensures(ret == self.nodes.contains_key(id)),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    #[allow(
        clippy::too_many_lines,
        reason = "Cannot be split into smaller functions"
    )]
    #[allow(clippy::missing_panics_doc, reason = "Should never panic")]
    /// Sets the active status of a node with the specified identifier, using identical activation behavior to [`DependentWeave`].
    pub fn set_active_dependent_semantics(&mut self, id: &K, value: bool) -> bool {
        if value {
            if let Some(node) = self.nodes.get(id) {
                if node.active && !node.to.iter().any(|id| self.active.contains(id)) {
                    return true;
                }

                if !node.active
                    && ((node.from.is_empty() && self.active.is_empty())
                        || node.from.iter().any(|parent| {
                            self.active.contains(parent)
                                && self.nodes[parent]
                                    .to
                                    .iter()
                                    .all(|child| !self.active.contains(child))
                        }))
                {
                    self.nodes.get_mut(id).unwrap().active = true;
                    self.active.insert(*id);
                    return true;
                }
            } else {
                return false;
            }

            let guard = self.scratchpad.guard();

            let mut stack = guard.vec();
            let mut closure = guard.set(S::default());

            ancestor_subgraph(&self.nodes, *id, &mut stack, &mut closure);

            let mut topological = guard.vec_with_capacity(closure.len());
            let mut scratchpad_map = guard.map_with_capacity(closure.len(), S::default());
            let mut scratchpad_map_2 = guard.map_with_capacity(closure.len(), S::default());
            let mut scratchpad_map_3: ScratchpadMap<'_, K, (usize, usize), S> =
                guard.map_with_capacity(closure.len(), S::default());

            for root in self
                .roots
                .iter()
                .filter(|id| closure.contains(*id))
                .copied()
            {
                topological_sort_subgraph(
                    &self.nodes,
                    &|id| closure.contains(id),
                    root,
                    &mut stack,
                    |id| topological.push(id), // topological order
                    &mut scratchpad_map,
                );
            }

            for id in topological.drain(..) {
                let node = &self.nodes[&id];

                let best_parent = node
                    .from
                    .iter()
                    .map(|id| (id, scratchpad_map_3[id])) // score: (connectors, active)
                    .min_by(|(_, a), (_, b)| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

                let (parent, score) = if let Some((parent, mut score)) = best_parent {
                    if node.active {
                        score.1 = score.1.strict_add(1);
                    } else {
                        score.0 = score.0.strict_add(1);
                    }

                    (Some(parent), score)
                } else {
                    (None, if node.active { (0, 1) } else { (1, 0) })
                };

                if let Some(parent) = parent {
                    scratchpad_map_2.insert(id, *parent); // predecessors
                }

                scratchpad_map_3.insert(id, score);
            }

            closure.clear();
            let mut scratchpad_set = closure;

            let mut disjoint = topological;

            let mut current = Some(id);

            while let Some(id) = current {
                scratchpad_set.insert(*id);
                current = scratchpad_map_2.get(id);
            }

            disjoint.extend(
                self.active
                    .iter()
                    .filter(|id| !scratchpad_set.contains(*id))
                    .copied(),
            );

            for id in disjoint.drain(..) {
                self.nodes.get_mut(&id).unwrap().active = false;
                self.active.remove(&id);
            }

            disjoint.extend(
                scratchpad_set
                    .iter()
                    .filter(|id| !self.active.contains(*id))
                    .copied(),
            );

            for id in disjoint.drain(..) {
                self.nodes.get_mut(&id).unwrap().active = true;
                self.active.insert(id);
            }
        } else if let Some(node) = self.nodes.get_mut(id) {
            if !node.active || node.to.iter().any(|id| self.active.contains(id)) {
                return true;
            }

            node.active = false;
            self.active.remove(&node.id);
        } else {
            return false;
        }

        true
    }
}

impl<K, T, M, S> From<DependentWeave<K, T, M, S>> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    fn from(value: DependentWeave<K, T, M, S>) -> Self {
        let mut output = Self {
            active: HashSet::with_capacity_and_hasher(value.nodes.capacity(), S::default()),
            nodes: {
                let mut map =
                    HashMap::with_capacity_and_hasher(value.nodes.capacity(), S::default());
                map.extend(value.nodes.into_iter().map(|(id, mut node)| {
                    node.active = false;
                    (id, node.into())
                }));

                map
            },
            roots: value.roots,
            bookmarked: value.bookmarked,
            scratchpad: value.scratchpad,
            metadata: value.metadata,
        };

        if let Some(active) = value.active {
            output.set_active(&active, true);
        }

        debug_assert!(output.validate(), "Converted weave is malformed");

        output
    }
}

#[allow(clippy::panic_in_result_fn, reason = "Should never panic")]
#[allow(clippy::unreachable, reason = "Should never panic")]
impl<K, T, M, S> TryFrom<IndependentWeave<K, T, M, S>> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type Error = IndependentWeave<K, T, M, S>;

    fn try_from(value: IndependentWeave<K, T, M, S>) -> Result<Self, Self::Error> {
        if value.nodes.iter().all(|(_, node)| node.from.len() < 2) {
            let mut active = None;

            let output = Self {
                nodes: {
                    let mut map =
                        HashMap::with_capacity_and_hasher(value.nodes.capacity(), S::default());
                    map.extend(value.nodes.into_iter().map(|(id, mut node)| {
                        node.active =
                            node.active && !node.to.iter().any(|id| value.active.contains(id));
                        if node.active {
                            active = Some(id);
                        }

                        node.try_into()
                            .map_or_else(|_| unreachable!(), |node| (id, node))
                    }));

                    map
                },
                roots: value.roots,
                active,
                bookmarked: value.bookmarked,
                scratchpad: value.scratchpad,
                metadata: value.metadata,
            };

            debug_assert!(output.validate(), "Converted weave is malformed");

            Ok(output)
        } else {
            Err(value)
        }
    }
}

impl<K, T, M, S> Weave<K, IndependentNode<K, T, S>, T> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type Nodes = HashMap<K, IndependentNode<K, T, S>, S>;
    type Roots = IndexSet<K, S>;

    #[inline]
    fn len(&self) -> usize {
        self.nodes.len()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    #[inline]
    fn nodes(&self) -> &Self::Nodes {
        &self.nodes
    }
    #[inline]
    fn roots(&self) -> &Self::Roots {
        &self.roots
    }
    #[inline]
    fn contains(&self, id: &K) -> bool {
        self.nodes.contains_key(id)
    }
    #[inline]
    fn contains_active(&self, id: &K) -> bool {
        self.active.contains(id)
    }
    #[inline]
    fn get(&self, id: &K) -> Option<&IndependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    #[inline]
    fn get_parents(&self, id: &K) -> Option<&IndexSet<K, S>> {
        self.nodes.get(id).map(|node| &node.from)
    }
    #[inline]
    fn get_children(&self, id: &K) -> Option<&IndexSet<K, S>> {
        self.nodes.get(id).map(|node| &node.to)
    }
    #[inline]
    fn get_contents(&self, id: &K) -> Option<&T> {
        self.nodes.get(id).map(|node| &node.contents)
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(output.len() == self.nodes.len()),
        ensures(valid_topological_sort(&self.nodes, output)),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active.clone()) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn get_ordered_identifiers(&mut self, output: &mut Vec<K>) {
        output.clear();
        output.reserve(self.nodes.len());

        let guard = self.scratchpad.guard();

        topological_sort(
            &self.nodes,
            self.roots.iter().copied(),
            &mut guard.vec_with_capacity(self.roots.len()),
            |id| output.push(id),
            &mut guard.map_with_capacity(self.nodes.len(), S::default()),
        );
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(lacks_duplicates(output)),
        ensures(!self.nodes.contains_key(id) || output.first() == Some(id)),
        ensures(self.nodes.contains_key(id) || output.is_empty()),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active.clone()) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn get_ordered_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();

        if self.nodes.contains_key(id) {
            let guard = self.scratchpad.guard();

            let mut stack = guard.vec();
            let mut descendants = guard.set(S::default());

            descendant_subgraph(&self.nodes, *id, &mut stack, &mut descendants);

            output.reserve(descendants.len());

            topological_sort_subgraph(
                &self.nodes,
                &|id| descendants.contains(id),
                *id,
                &mut stack,
                |id| output.push(id),
                &mut guard.map_with_capacity(descendants.len(), S::default()),
            );
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(output.len() == self.active.len()),
        ensures(output.iter().all(|i| self.active.contains(i))),
        ensures(lacks_duplicates(output)),
        ensures(valid_path(&self.nodes, output)),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active.clone()) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn get_active_path(&mut self, output: &mut Vec<K>) {
        output.clear();
        output.reserve(self.active.len());

        let guard = self.scratchpad.guard();

        let mut stack = guard.vec();
        let mut topological_subgraph = guard.vec_with_capacity(self.active.len());
        let mut scratchpad_map = guard.map_with_capacity(self.active.len(), S::default());

        for root in self
            .roots
            .iter()
            .filter(|id| self.active.contains(*id))
            .copied()
        {
            topological_sort_subgraph(
                &self.nodes,
                &|id| self.active.contains(id),
                root,
                &mut stack,
                |id| topological_subgraph.push(id),
                &mut scratchpad_map,
            );
        }

        scratchpad_map.clear();

        longest_candidate_path_to_root(
            &self.nodes,
            &topological_subgraph,
            &|id| self.active.contains(id),
            &mut scratchpad_map,
            |id| output.push(id),
        );
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(!self.nodes.contains_key(id) || output.first() == Some(id)),
        ensures(self.nodes.contains_key(id) || output.is_empty()),
        ensures(lacks_duplicates(output)),
        ensures(valid_path(&self.nodes, output)),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active.clone()) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn get_path_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();
        if !self.nodes.contains_key(id) {
            return;
        }

        let guard = self.scratchpad.guard();
        let mut stack = guard.vec();
        let mut ancestors = guard.set(S::default());

        ancestor_subgraph(&self.nodes, *id, &mut stack, &mut ancestors);

        let mut active_topological_subgraph =
            guard.vec_with_capacity(self.active.len().min(ancestors.len()));
        let mut scratchpad_map =
            guard.map_with_capacity(self.active.len().min(ancestors.len()), S::default());

        for root in self
            .roots
            .iter()
            .filter(|id| self.active.contains(*id) && ancestors.contains(*id))
            .copied()
        {
            topological_sort_subgraph(
                &self.nodes,
                &|id| self.active.contains(id) && ancestors.contains(id),
                root,
                &mut stack,
                |id| active_topological_subgraph.push(id),
                &mut scratchpad_map,
            );
        }

        scratchpad_map.clear();

        let mut reversed_path = guard.vec();

        longest_candidate_path_to_root(
            &self.nodes,
            &active_topological_subgraph,
            &|id| self.active.contains(id) && ancestors.contains(id),
            &mut scratchpad_map,
            |id| reversed_path.push(id),
        );

        let mut scratchpad_map_2 = guard.map_with_capacity(ancestors.len(), S::default());

        ancestors.clear();
        let mut scratchpad_set = ancestors;

        if let Some(target) = reversed_path.first().copied() {
            shortest_path_to_ancestor(
                &self.nodes,
                id,
                &|node| node.id == target,
                &mut stack,
                &mut scratchpad_map_2,
                &mut scratchpad_set,
                output,
            );

            output.reverse();
            output.pop();
            output.extend_from_slice(&reversed_path);
        } else {
            shortest_path_to_ancestor(
                &self.nodes,
                id,
                &|node| node.from.is_empty(),
                &mut stack,
                &mut scratchpad_map_2,
                &mut scratchpad_set,
                output,
            );

            output.reverse();
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(!ret || old(self.nodes.len()) + 1 == self.nodes.len()),
        ensures(!ret || old(!self.nodes.contains_key(&node.id))),
        ensures(!ret || self.nodes.contains_key(&old(node.id))),
        ensures(!ret || old(node.active) == self.active.contains(&old(node.id)) || (!old(node.active) && self.active.contains(&old(node.id)) && old(node.to.iter().any(|c| self.active.contains(c))))),
        ensures(!ret || old(node.bookmarked) == self.bookmarked.contains(&old(node.id))),
        ensures(!ret || old(!node.from.is_empty()) || self.roots.contains(&old(node.id))),
        ensures(ret || old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(ret || old(self.roots.clone()) == self.roots),
        ensures(ret || old(self.active.clone()) == self.active),
        ensures(ret || old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn insert(&mut self, mut node: IndependentNode<K, T, S>) -> bool {
        if self.nodes.contains_key(&node.id)
            || !node.validate()
            || !node.from.iter().all(|id| self.nodes.contains_key(id))
            || !node.to.iter().all(|id| self.nodes.contains_key(id))
        {
            return false;
        }

        if !node.to.is_empty() && !node.from.is_empty() {
            let guard = self.scratchpad.guard();

            if ancestor_subgraph_reaches(
                &self.nodes,
                node.from.iter().copied(),
                &|id| node.to.contains(id),
                &mut guard.vec_with_capacity(node.from.len()),
                &mut guard.set_with_capacity(node.from.len(), S::default()),
            ) {
                return false;
            }
        }

        let root_index = if node.from.is_empty() {
            node.to
                .iter()
                .filter_map(|child| self.roots.get_index_of(child))
                .min()
        } else {
            None
        };

        let mut detached_root = false;

        for child in &node.to {
            let child = self.nodes.get_mut(child).unwrap();

            if child.from.is_empty() {
                node.active |= child.active;
                detached_root = true;
            }

            child.from.insert(node.id);
        }

        if detached_root {
            self.roots.retain(|id| !node.to.contains(id));
        }

        let extends_active = node.active
            && node.to.is_empty()
            && node.from.iter().map(|id| &self.nodes[id]).any(|parent| {
                parent.active && parent.to.iter().all(|child| !self.active.contains(child))
            });

        if node.from.is_empty() {
            if let Some(index) = root_index {
                self.roots.shift_insert(index, node.id);
            } else {
                self.roots.insert(node.id);
            }
        } else {
            for parent in &node.from {
                let parent = self.nodes.get_mut(parent).unwrap();
                parent.to.insert(node.id);
            }
        }

        if node.bookmarked {
            self.bookmarked.insert(node.id);
        }

        let id = node.id;
        let active = node.active;

        if !extends_active {
            node.active = false;
        }

        self.nodes.insert(node.id, node);

        if extends_active {
            self.active.insert(id);
        } else if active {
            self.update_node_activity_in_place(&id, true);
        }

        true
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(!ret || value == self.contains_active(id)),
        ensures(ret || old(self.active.clone()) == self.active),
        ensures(ret == self.nodes.contains_key(id)),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn set_active(&mut self, id: &K, value: bool) -> bool {
        self.update_node_activity_in_place(id, value)
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(!self.nodes.contains_key(id)),
        ensures(ret.is_some() == old(self.nodes.contains_key(id))),
        ensures(ret.as_ref().is_none_or(|node| &node.id == id)),
        ensures(ret.is_none() || old(self.nodes.len()) > self.nodes.len()),
        ensures(ret.is_none() || old(self.active.len()) >= self.active.len()),
        ensures(ret.is_none() || old(self.bookmarked.len()) >= self.bookmarked.len()),
        ensures(ret.is_some() || old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(ret.is_some() || old(self.roots.clone()) == self.roots),
        ensures(ret.is_some() || old(self.active.clone()) == self.active),
        ensures(ret.is_some() || old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn remove(&mut self, id: &K) -> Option<IndependentNode<K, T, S>> {
        let node = self.nodes.remove(id)?;

        if node.from.is_empty() {
            self.roots.shift_remove(id);
        } else {
            for parent in &node.from {
                if let Some(parent) = self.nodes.get_mut(parent) {
                    parent.to.shift_remove(id);
                }
            }
        }

        let mut removed_active = node.active;
        let mut removed_bookmark = node.bookmarked;

        if removed_active {
            self.active.remove(id);
        }

        if node.to.is_empty() {
            if removed_bookmark {
                self.bookmarked.shift_remove(id);
            }
            return Some(node);
        }

        {
            let guard = self.scratchpad.guard();
            let mut stack = guard.vec();
            let mut removed = guard.vec();
            let mut remaining_parents = guard.map_with_capacity(node.to.len(), S::default());

            removed.push(*id);

            for child in node.to.iter().rev().copied() {
                let remaining = remaining_parents
                    .entry(child)
                    .or_insert_with(|| self.nodes[&child].from.len());
                *remaining = remaining.strict_sub(1);

                if *remaining == 0 {
                    stack.push(child);
                }
            }

            while let Some(id) = stack.pop() {
                let node = self.nodes.remove(&id).unwrap();
                removed.push(id);

                removed_bookmark |= node.bookmarked;
                if node.active {
                    self.active.remove(&id);
                    removed_active = true;
                }

                for child in node.to.iter().rev().copied() {
                    let remaining = remaining_parents
                        .entry(child)
                        .or_insert_with(|| self.nodes[&child].from.len());
                    *remaining = remaining.strict_sub(1);

                    if *remaining == 0 {
                        stack.push(child);
                    }
                }
            }

            if removed_active
                && !remaining_parents
                    .iter()
                    .any(|(child, remaining)| *remaining > 0 && self.active.contains(child))
            {
                removed_active = false;
            }

            if removed.len() == 1 {
                for (child, _) in remaining_parents {
                    if let Some(child) = self.nodes.get_mut(&child) {
                        child.from.shift_remove(id);
                    }
                }
                if removed_bookmark {
                    self.bookmarked.shift_remove(id);
                }
            } else {
                let mut removed_set = guard.set_with_capacity(removed.len(), S::default());
                removed_set.extend(removed);

                for (child, remaining) in remaining_parents {
                    if remaining > 0
                        && let Some(child) = self.nodes.get_mut(&child)
                    {
                        child.from.retain(|parent| !removed_set.contains(parent));
                    }
                }

                if removed_bookmark {
                    self.bookmarked.retain(|id| !removed_set.contains(id));
                }
            }
        }

        if removed_active {
            self.fix_orphaned_activations();
        }

        Some(node)
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(!self.nodes.contains_key(id)),
        ensures(ret == old(self.nodes.contains_key(id))),
        ensures(!ret || old(self.nodes.len()) > self.nodes.len()),
        ensures(!ret || old(self.active.len()) >= self.active.len()),
        ensures(!ret || old(self.bookmarked.len()) >= self.bookmarked.len()),
        ensures(ret || old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(ret || old(self.roots.clone()) == self.roots),
        ensures(ret || old(self.active.clone()) == self.active),
        ensures(ret || old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn remove_tracked(
        &mut self,
        id: &K,
        mut on_removal: impl FnMut(IndependentNode<K, T, S>),
    ) -> bool {
        let Some(node) = self.nodes.remove(id) else {
            return false;
        };

        if node.from.is_empty() {
            self.roots.shift_remove(id);
        } else {
            for parent in &node.from {
                if let Some(parent) = self.nodes.get_mut(parent) {
                    parent.to.shift_remove(id);
                }
            }
        }

        let mut removed_active = node.active;
        let mut removed_bookmark = node.bookmarked;

        if removed_active {
            self.active.remove(id);
        }

        if node.to.is_empty() {
            if removed_bookmark {
                self.bookmarked.shift_remove(id);
            }
            on_removal(node);
            return true;
        }

        {
            let guard = self.scratchpad.guard();
            let mut stack = guard.vec();
            let mut removed = guard.vec();
            let mut remaining_parents = guard.map_with_capacity(node.to.len(), S::default());

            removed.push(*id);

            for child in node.to.iter().rev().copied() {
                let remaining = remaining_parents
                    .entry(child)
                    .or_insert_with(|| self.nodes[&child].from.len());
                *remaining = remaining.strict_sub(1);

                if *remaining == 0 {
                    stack.push(child);
                }
            }

            on_removal(node);

            while let Some(id) = stack.pop() {
                let node = self.nodes.remove(&id).unwrap();
                removed.push(id);

                removed_bookmark |= node.bookmarked;
                if node.active {
                    self.active.remove(&id);
                    removed_active = true;
                }

                for child in node.to.iter().rev().copied() {
                    let remaining = remaining_parents
                        .entry(child)
                        .or_insert_with(|| self.nodes[&child].from.len());
                    *remaining = remaining.strict_sub(1);

                    if *remaining == 0 {
                        stack.push(child);
                    }
                }

                on_removal(node);
            }

            if removed_active
                && !remaining_parents
                    .iter()
                    .any(|(child, remaining)| *remaining > 0 && self.active.contains(child))
            {
                removed_active = false;
            }

            if removed.len() == 1 {
                for (child, _) in remaining_parents {
                    if let Some(child) = self.nodes.get_mut(&child) {
                        child.from.shift_remove(id);
                    }
                }
                if removed_bookmark {
                    self.bookmarked.shift_remove(id);
                }
            } else {
                let mut removed_set = guard.set_with_capacity(removed.len(), S::default());
                removed_set.extend(removed);

                for (child, remaining) in remaining_parents {
                    if remaining > 0
                        && let Some(child) = self.nodes.get_mut(&child)
                    {
                        child.from.retain(|parent| !removed_set.contains(parent));
                    }
                }

                if removed_bookmark {
                    self.bookmarked.retain(|id| !removed_set.contains(id));
                }
            }
        }

        if removed_active {
            self.fix_orphaned_activations();
        }

        true
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(self.nodes.is_empty()),
        ensures(self.validate())
    ))]
    fn clear(&mut self) {
        self.nodes.clear();
        self.roots.clear();
        self.active.clear();
        self.bookmarked.clear();
    }
}

impl<K, T, M, S> IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    /// Validates that the weave is internally consistent.
    pub fn validate(&self) -> bool {
        self.roots
            .iter()
            .all(move |value| self.nodes.contains_key(value))
            && self
                .active
                .iter()
                .all(move |value| self.nodes.contains_key(value))
            && self
                .bookmarked
                .iter()
                .all(move |value| self.nodes.contains_key(value))
            && self.nodes.iter().all(|(key, value)| {
                value.validate()
                    && value.id == *key
                    && value
                        .from
                        .iter()
                        .all(|v| self.nodes.get(v).is_some_and(|p| p.to.contains(key)))
                    && value
                        .to
                        .iter()
                        .all(|v| self.nodes.get(v).is_some_and(|p| p.from.contains(key)))
                    && value.from.is_empty() == self.roots.contains(key)
                    && value.active == self.active.contains(key)
                    && value.bookmarked == self.bookmarked.contains(key)
            })
            && valid_topology(&self.nodes, &self.roots, &self.active)
    }
}

impl<K, T, M, S> MetadataWeave<K, IndependentNode<K, T, S>, T, M> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn metadata(&self) -> &M {
        &self.metadata
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active.clone()) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    #[inline]
    fn metadata_mut<O>(&mut self, callback: impl FnOnce(&mut M) -> O) -> O {
        callback(&mut self.metadata)
    }
}

impl<K, T, M, S> BookmarkableWeave<K, IndependentNode<K, T, S>, T> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type Bookmarks = IndexSet<K, S>;

    #[inline]
    fn bookmarks(&self) -> &Self::Bookmarks {
        &self.bookmarked
    }
    #[inline]
    fn contains_bookmark(&self, id: &K) -> bool {
        self.bookmarked.contains(id)
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(!ret || value == self.bookmarked.contains(id)),
        ensures(ret || old(self.bookmarked.clone()) == self.bookmarked),
        ensures(ret == self.nodes.contains_key(id)),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active.clone()) == self.active),
        invariant(self.validate())
    ))]
    fn set_bookmarked(&mut self, id: &K, value: bool) -> bool {
        match self.nodes.get_mut(id) {
            Some(node) => {
                node.bookmarked = value;
                if value {
                    self.bookmarked.insert(node.id);
                } else {
                    self.bookmarked.shift_remove(id);
                }

                true
            }
            None => false,
        }
    }
}

impl<K, T, M, S> SortableWeave<K, IndependentNode<K, T, S>, T> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[cfg_attr(debug_assertions, contract(
        ensures(ret == self.nodes.contains_key(id)),
        ensures(old(self.nodes.get(id).map(|n| n.to.clone())) == self.nodes.get(id).map(|n| n.to.clone())),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active.clone()) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn sort_children_by(
        &mut self,
        id: &K,
        mut cmp: impl FnMut(&IndependentNode<K, T, S>, &IndependentNode<K, T, S>) -> Ordering,
    ) -> bool {
        if let Some(node) = self.nodes.get_mut(id) {
            let mut set = mem::take(&mut node.to);

            if set.len() > 20 {
                let guard = self.scratchpad.guard();
                let mut nodes = guard
                    .arena()
                    .alloc_iter_exact(set.drain(..).map(|id| &self.nodes[&id]));
                nodes.sort_by(|a, b| cmp(*a, *b));

                set.extend(nodes.into_iter().map(|node| node.id));
            } else {
                set.sort_by(|a, b| cmp(&self.nodes[a], &self.nodes[b]));
            }

            self.nodes.get_mut(id).unwrap().to = set;

            true
        } else {
            false
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(ret == self.nodes.contains_key(id)),
        ensures(old(self.nodes.get(id).map(|n| n.to.clone())) == self.nodes.get(id).map(|n| n.to.clone())),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active.clone()) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn sort_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool {
        if let Some(node) = self.nodes.get_mut(id) {
            node.to.sort_by(cmp);

            true
        } else {
            false
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active.clone()) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn sort_roots_by(
        &mut self,
        mut cmp: impl FnMut(&IndependentNode<K, T, S>, &IndependentNode<K, T, S>) -> Ordering,
    ) {
        if self.roots.len() > 20 {
            let guard = self.scratchpad.guard();

            let mut nodes = guard
                .arena()
                .alloc_iter_exact(self.roots.iter().map(|id| &self.nodes[id]));
            nodes.sort_by(|a, b| cmp(*a, *b));

            self.roots.clear();
            self.roots.extend(nodes.into_iter().map(|node| node.id));
        } else {
            self.roots
                .sort_by(|a, b| cmp(&self.nodes[a], &self.nodes[b]));
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active.clone()) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.roots.sort_by(cmp);
    }
}

impl<K, T, M, S> SortableBookmarkableWeave<K, IndependentNode<K, T, S>, T>
    for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[cfg_attr(debug_assertions, contract(
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active.clone()) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn sort_bookmarks_by(
        &mut self,
        mut cmp: impl FnMut(&IndependentNode<K, T, S>, &IndependentNode<K, T, S>) -> Ordering,
    ) {
        if self.bookmarked.len() > 20 {
            let guard = self.scratchpad.guard();

            let mut nodes = guard
                .arena()
                .alloc_iter_exact(self.bookmarked.iter().map(|id| &self.nodes[id]));
            nodes.sort_by(|a, b| cmp(*a, *b));

            self.bookmarked.clear();
            self.bookmarked
                .extend(nodes.into_iter().map(|node| node.id));
        } else {
            self.bookmarked
                .sort_by(|a, b| cmp(&self.nodes[a], &self.nodes[b]));
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active.clone()) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn sort_bookmarks_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.bookmarked.sort_by(cmp);
    }
}

impl<K, T, M, S> ActivePathWeave<K, IndependentNode<K, T, S>, T> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type Active = HashSet<K, S>;

    #[inline]
    fn active(&self) -> &Self::Active {
        &self.active
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn set_active_path(&mut self, active: impl Iterator<Item = K>) {
        self.active.iter().for_each(|active| {
            self.nodes.get_mut(active).unwrap().active = false;
        });
        self.active.clear();
        self.active
            .extend(active.filter(|id| self.nodes.contains_key(id)));
        self.active.iter().for_each(|active| {
            self.nodes.get_mut(active).unwrap().active = true;
        });
        self.fix_orphaned_activations();
    }
}

impl<K, T, M, S> DiscreteWeave<K, IndependentNode<K, T, S>, T> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    #[cfg_attr(debug_assertions, contract(
        ensures(!ret || old(self.nodes.len()) + 1 == self.nodes.len()),
        ensures(!ret || self.nodes.contains_key(id)),
        ensures(!ret || self.nodes.contains_key(&new_id)),
        ensures(!ret || old(!self.nodes.contains_key(&new_id))),
        ensures(!ret || self.nodes[id].to.contains(&new_id) && self.nodes[id].to.len() == 1),
        ensures(!ret || self.nodes[&new_id].from.contains(id) && self.nodes[&new_id].from.len() == 1),
        ensures(!ret || old(self.nodes.get(id).map(|n| n.to.clone())).unwrap() == self.nodes[&new_id].to),
        ensures(ret || old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(ret || old(self.active.clone()) == self.active),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn split(&mut self, id: &K, at: usize, new_id: K) -> bool {
        if self.nodes.contains_key(&new_id) || *id == new_id {
            return false;
        }

        if let Some(mut node) = self.nodes.remove(id) {
            match node.contents.split(at) {
                DiscreteContentResult::Two(left, right) => {
                    let left_node = IndependentNode {
                        id: node.id,
                        from: node.from,
                        to: IndexSet::from_iter([new_id]),
                        active: node.active,
                        bookmarked: node.bookmarked,
                        contents: left,
                    };

                    node.from = IndexSet::from_iter([node.id]);
                    node.id = new_id;
                    node.contents = right;
                    node.active = false;
                    node.bookmarked = false;

                    let mut child_active = false;

                    for child in &node.to {
                        let child = self.nodes.get_mut(child).unwrap();
                        let index = child.from.get_index_of(&left_node.id).unwrap();

                        assert!(
                            child.from.replace_index(index, node.id).is_ok(),
                            "Should be unreachable"
                        );

                        child_active |= child.active;
                    }

                    if left_node.active && child_active {
                        node.active = true;
                        self.active.insert(node.id);
                    }

                    self.nodes.insert(left_node.id, left_node);
                    self.nodes.insert(node.id, node);

                    true
                }
                DiscreteContentResult::One(content) => {
                    node.contents = content;
                    self.nodes.insert(node.id, node);
                    false
                }
            }
        } else {
            false
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(ret.is_none() || old(self.nodes.len()) - 1 == self.nodes.len()),
        ensures(ret.is_none() || !self.nodes.contains_key(id)),
        ensures(ret.is_none() || old(self.nodes.contains_key(id))),
        ensures(ret.is_none() || !old(self.contains_active(id)) || old(self.contains_active(id)) && self.contains_active(&ret.unwrap())),
        ensures(ret.is_none() || old(self.nodes.get(id).and_then(|n| n.from.first()).and_then(|p| self.nodes.get(p)).map(|p| p.active)).unwrap() == self.nodes[&ret.unwrap()].active),
        ensures(ret.is_none() || old(self.nodes.get(id).and_then(|n| n.from.first()).and_then(|p| self.nodes.get(p)).map(|p| p.from.clone())).unwrap() == self.nodes[&ret.unwrap()].from),
        ensures(ret.is_none() || old(self.nodes.get(id).map(|node| node.to.clone())).unwrap() == self.nodes[&ret.unwrap()].to),
        ensures(ret.is_none() || old(self.nodes.get(id).map(|node| node.from.len() == 1)).unwrap()),
        ensures(ret.is_none() || ret.unwrap() == old(self.nodes.get(id).and_then(|node| node.from.first().copied())).unwrap()),
        ensures(ret.is_some() || old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(ret.is_some() || old(self.active.clone()) == self.active),
        ensures(ret.is_some() || old(self.bookmarked.clone()) == self.bookmarked),
        ensures(old(self.roots.clone()) == self.roots),
        invariant(self.validate())
    ))]
    fn merge_with_parent(&mut self, id: &K) -> Option<K> {
        if let Some(mut node) = self.nodes.remove(id) {
            if node.from.len() != 1 {
                self.nodes.insert(node.id, node);
                return None;
            }

            if let Some(mut parent) = node.from.first().and_then(|id| self.nodes.remove(id)) {
                if parent.to.len() > 1 {
                    self.nodes.insert(parent.id, parent);
                    self.nodes.insert(node.id, node);
                    return None;
                }

                match parent.contents.merge(node.contents) {
                    DiscreteContentResult::Two(left, right) => {
                        parent.contents = left;
                        node.contents = right;
                        self.nodes.insert(parent.id, parent);
                        self.nodes.insert(node.id, node);
                        None
                    }
                    DiscreteContentResult::One(content) => {
                        parent.contents = content;
                        parent.to = node.to;

                        for child in &parent.to {
                            let child = self.nodes.get_mut(child).unwrap();
                            let index = child.from.get_index_of(&node.id).unwrap();

                            assert!(
                                child.from.replace_index(index, parent.id).is_ok(),
                                "Should be unreachable"
                            );
                        }

                        let parent_id = parent.id;

                        if node.bookmarked && !parent.bookmarked {
                            parent.bookmarked = true;
                            assert!(
                                self.bookmarked
                                    .replace_index(
                                        self.bookmarked.get_index_of(&node.id).unwrap(),
                                        parent.id,
                                    )
                                    .is_ok(),
                                "Should be unreachable"
                            );
                        } else if node.bookmarked {
                            self.bookmarked.shift_remove(&node.id);
                        }

                        self.nodes.insert(parent.id, parent);
                        self.active.remove(&node.id);

                        Some(parent_id)
                    }
                }
            } else {
                self.nodes.insert(node.id, node);
                None
            }
        } else {
            None
        }
    }
}

impl<K, T, M, S> SemiIndependentWeave<K, IndependentNode<K, T, S>, T>
    for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[cfg_attr(debug_assertions, contract(
        ensures(ret.is_some() == old(self.nodes.contains_key(id))),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active.clone()) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    #[inline]
    fn get_contents_mut<O>(&mut self, id: &K, callback: impl FnOnce(&mut T) -> O) -> Option<O> {
        self.nodes
            .get_mut(id)
            .map(|node| callback(&mut node.contents))
    }
}

impl<K, T, M, S> crate::IndependentWeave<K, IndependentNode<K, T, S>, T>
    for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[cfg_attr(debug_assertions, contract(
        ensures(!ret || self.nodes[id].from.iter().copied().collect::<HashSet<_>>() == new_parents.iter().copied().collect::<HashSet<_>>()),
        ensures(ret || old(self.nodes.get(id).map(|node| node.from.clone())).as_ref() == self.nodes.get(id).map(|node| &node.from)),
        ensures(ret || old(self.roots.clone()) == self.roots),
        ensures(ret || old(self.active.clone()) == self.active),
        ensures(old(self.nodes.get(id).map(|node| node.to.clone())).as_ref() == self.nodes.get(id).map(|node| &node.to)),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        ensures(old(self.active.contains(id)) == self.active.contains(id)),
        invariant(self.validate())
    ))]
    fn move_to(&mut self, id: &K, new_parents: &[K]) -> bool {
        if new_parents
            .iter()
            .any(|new_parent| !self.nodes.contains_key(new_parent))
        {
            return false;
        }

        let Some(node) = self.nodes.get(id) else {
            return false;
        };

        let new_parents: IndexSet<K, S> = new_parents.iter().copied().collect();

        if new_parents.contains(id) {
            return false;
        }

        if !node.to.is_empty() && !new_parents.is_empty() {
            let guard = self.scratchpad.guard();

            if descendant_subgraph_reaches(
                &self.nodes,
                node.to.iter().copied(),
                &|id| new_parents.contains(id),
                &mut guard.vec_with_capacity(node.to.len()),
                &mut guard.set_with_capacity(node.to.len(), S::default()),
            ) {
                return false;
            }
        }

        if let Some(node) = self.nodes.get_mut(id) {
            let old_parents = mem::take(&mut node.from);

            for old_parent in &old_parents {
                if !new_parents.contains(old_parent)
                    && let Some(old_parent) = self.nodes.get_mut(old_parent)
                {
                    old_parent.to.shift_remove(id);
                }
            }

            for new_parent in &new_parents {
                if !old_parents.contains(new_parent)
                    && let Some(new_parent) = self.nodes.get_mut(new_parent)
                {
                    new_parent.to.insert(*id);
                }
            }
        }

        let node = self.nodes.get_mut(id).unwrap();
        node.from = new_parents;

        if node.from.is_empty() {
            self.roots.insert(node.id);
        } else {
            self.roots.shift_remove(&node.id);
        }

        if node.active {
            node.active = false;
            self.active.remove(id);
            self.update_node_activity_in_place(id, true);
        }

        true
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, S> ArchivedIndependentNode<K, T, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive + IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn validate(&self) -> bool {
        (if self.from.len() <= self.to.len() {
            self.from.iter().all(|v| !self.to.contains(v))
        } else {
            self.to.iter().all(|v| !self.from.contains(v))
        }) && !self.from.contains(&self.id)
            && !self.to.contains(&self.id)
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, S> Node<K::Archived, T::Archived> for ArchivedIndependentNode<K, T, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive + IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type From = ArchivedIndexSet<K::Archived>;
    type To = ArchivedIndexSet<K::Archived>;

    #[inline]
    fn id(&self) -> K::Archived {
        self.id
    }
    #[inline]
    fn from(&self) -> &Self::From {
        &self.from
    }
    #[inline]
    fn to(&self) -> &Self::To {
        &self.to
    }
    #[inline]
    fn is_active(&self) -> bool {
        self.active
    }
    #[inline]
    fn contents(&self) -> &T::Archived {
        &self.contents
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, M, S> ArchivedIndependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive + IndependentContents,
    M: Archive,
    S: BuildHasher + Default + Clone,
{
    fn validate(&self) -> bool {
        self.roots
            .iter()
            .all(move |value| self.nodes.contains_key(value))
            && self
                .active
                .iter()
                .all(move |value| self.nodes.contains_key(value))
            && self
                .bookmarked
                .iter()
                .all(move |value| self.nodes.contains_key(value))
            && self.nodes.iter().all(|(key, value)| {
                value.validate()
                    && value.id == *key
                    && value
                        .from
                        .iter()
                        .all(|v| self.nodes.get(v).is_some_and(|p| p.to.contains(key)))
                    && value
                        .to
                        .iter()
                        .all(|v| self.nodes.get(v).is_some_and(|p| p.from.contains(key)))
                    && value.from.is_empty() == self.roots.contains(key)
                    && value.active == self.active.contains(key)
                    && value.bookmarked == self.bookmarked.contains(key)
            })
            && archived_valid_topology(&self.nodes, &self.roots, &self.active)
    }
}

#[cfg(feature = "rkyv")]
// SAFETY:
// All fields are safe to access and no unsafe functions are called
unsafe impl<K, T, M, S, C> Verify<C> for ArchivedIndependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive + IndependentContents,
    M: Archive,
    S: BuildHasher + Default + Clone,
    C: Fallible + ?Sized,
    C::Error: Source,
{
    fn verify(&self, _context: &mut C) -> Result<(), C::Error> {
        if !self.validate() {
            fail!(ValidationError)
        }

        Ok(())
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, M, S> ImmutableWeave<K::Archived, ArchivedIndependentNode<K, T, S>, T::Archived>
    for ArchivedIndependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive + IndependentContents,
    M: Archive,
    S: BuildHasher + Default + Clone,
{
    type Nodes = ArchivedHashMap<K::Archived, ArchivedIndependentNode<K, T, S>>;
    type Roots = ArchivedIndexSet<K::Archived>;

    #[inline]
    fn len(&self) -> usize {
        self.nodes.len()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    #[inline]
    fn nodes(&self) -> &Self::Nodes {
        &self.nodes
    }
    #[inline]
    fn roots(&self) -> &Self::Roots {
        &self.roots
    }
    #[inline]
    fn contains(&self, id: &K::Archived) -> bool {
        self.nodes.contains_key(id)
    }
    #[inline]
    fn contains_active(&self, id: &K::Archived) -> bool {
        self.active.contains(id)
    }
    #[inline]
    fn get(&self, id: &K::Archived) -> Option<&ArchivedIndependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    #[inline]
    fn get_parents(&self, id: &K::Archived) -> Option<&ArchivedIndexSet<K::Archived>> {
        self.nodes.get(id).map(|node| &node.from)
    }
    #[inline]
    fn get_children(&self, id: &K::Archived) -> Option<&ArchivedIndexSet<K::Archived>> {
        self.nodes.get(id).map(|node| &node.to)
    }
    #[inline]
    fn get_contents(&self, id: &K::Archived) -> Option<&T::Archived> {
        self.nodes.get(id).map(|node| &node.contents)
    }
    fn get_ordered_identifiers(&self, output: &mut Vec<K::Archived>) {
        output.clear();
        output.reserve(self.nodes.len());

        let mut scratchpad = Scratchpad::new();
        let guard = scratchpad.guard();

        archived_topological_sort(
            &self.nodes,
            &self.roots,
            &mut guard.vec_with_capacity(self.roots.len()),
            |id| output.push(id),
            &mut guard.map_with_capacity(self.nodes.len(), S::default()),
        );
    }
    fn get_ordered_identifiers_from(&self, id: &K::Archived, output: &mut Vec<K::Archived>) {
        output.clear();

        if self.nodes.contains_key(id) {
            let mut scratchpad = Scratchpad::new();
            let guard = scratchpad.guard();

            let mut stack = guard.vec();
            let mut descendants = guard.set(S::default());

            archived_descendant_subgraph(&self.nodes, *id, &mut stack, &mut descendants);

            output.reserve(descendants.len());

            archived_topological_sort_subgraph(
                &self.nodes,
                &|id| descendants.contains(id),
                *id,
                &mut stack,
                |id| output.push(id),
                &mut guard.map_with_capacity(descendants.len(), S::default()),
            );
        }
    }
    fn get_active_path(&self, output: &mut Vec<K::Archived>) {
        output.clear();
        output.reserve(self.active.len());

        let mut scratchpad = Scratchpad::new();
        let guard = scratchpad.guard();

        let mut stack = guard.vec();
        let mut topological_subgraph = guard.vec_with_capacity(self.active.len());
        let mut scratchpad_map = guard.map_with_capacity(self.active.len(), S::default());

        for root in self
            .roots
            .iter()
            .filter(|id| self.active.contains(*id))
            .copied()
        {
            archived_topological_sort_subgraph(
                &self.nodes,
                &|id| self.active.contains(id),
                root,
                &mut stack,
                |id| topological_subgraph.push(id),
                &mut scratchpad_map,
            );
        }

        scratchpad_map.clear();

        archived_longest_candidate_path_to_root(
            &self.nodes,
            &topological_subgraph,
            &|id| self.active.contains(id),
            &mut scratchpad_map,
            |id| output.push(id),
        );
    }
    fn get_path_from(&self, id: &K::Archived, output: &mut Vec<K::Archived>) {
        output.clear();
        if !self.nodes.contains_key(id) {
            return;
        }

        let mut scratchpad = Scratchpad::new();
        let guard = scratchpad.guard();
        let mut stack = guard.vec();
        let mut ancestors = guard.set(S::default());

        archived_ancestor_subgraph(&self.nodes, *id, &mut stack, &mut ancestors);

        let mut active_topological_subgraph =
            guard.vec_with_capacity(self.active.len().min(ancestors.len()));
        let mut scratchpad_map =
            guard.map_with_capacity(self.active.len().min(ancestors.len()), S::default());

        for root in self
            .roots
            .iter()
            .filter(|id| self.active.contains(*id) && ancestors.contains(*id))
            .copied()
        {
            archived_topological_sort_subgraph(
                &self.nodes,
                &|id| self.active.contains(id) && ancestors.contains(id),
                root,
                &mut stack,
                |id| active_topological_subgraph.push(id),
                &mut scratchpad_map,
            );
        }

        scratchpad_map.clear();

        let mut reversed_path = guard.vec();

        archived_longest_candidate_path_to_root(
            &self.nodes,
            &active_topological_subgraph,
            &|id| self.active.contains(id) && ancestors.contains(id),
            &mut scratchpad_map,
            |id| reversed_path.push(id),
        );

        let mut scratchpad_map_2 = guard.map_with_capacity(ancestors.len(), S::default());

        ancestors.clear();
        let mut scratchpad_set = ancestors;

        if let Some(target) = reversed_path.first().copied() {
            archived_shortest_path_to_ancestor(
                &self.nodes,
                id,
                &|node| node.id == target,
                &mut stack,
                &mut scratchpad_map_2,
                &mut scratchpad_set,
                output,
            );

            output.reverse();
            output.pop();
            output.extend_from_slice(&reversed_path);
        } else {
            archived_shortest_path_to_ancestor(
                &self.nodes,
                id,
                &|node| node.from.is_empty(),
                &mut stack,
                &mut scratchpad_map_2,
                &mut scratchpad_set,
                output,
            );

            output.reverse();
        }
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, M, S>
    ImmutableMetadataWeave<K::Archived, ArchivedIndependentNode<K, T, S>, T::Archived, M::Archived>
    for ArchivedIndependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive + IndependentContents,
    M: Archive,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn metadata(&self) -> &M::Archived {
        &self.metadata
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, M, S>
    ImmutableBookmarkableWeave<K::Archived, ArchivedIndependentNode<K, T, S>, T::Archived>
    for ArchivedIndependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive + IndependentContents,
    M: Archive,
    S: BuildHasher + Default + Clone,
{
    type Bookmarks = ArchivedIndexSet<K::Archived>;

    #[inline]
    fn bookmarks(&self) -> &Self::Bookmarks {
        &self.bookmarked
    }
    #[inline]
    fn contains_bookmark(&self, id: &K::Archived) -> bool {
        self.bookmarked.contains(id)
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, M, S>
    ImmutableActivePathWeave<K::Archived, ArchivedIndependentNode<K, T, S>, T::Archived>
    for ArchivedIndependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive + IndependentContents,
    M: Archive,
    S: BuildHasher + Default + Clone,
{
    type Active = ArchivedHashSet<K::Archived>;

    #[inline]
    fn active(&self) -> &Self::Active {
        &self.active
    }
}
