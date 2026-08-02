//! [`IndependentWeave`] is a DAG-based [`Weave`] where each [`Node`] does *not* depend on the contents of the previous Node.

use alloc::{boxed::Box, collections::vec_deque::VecDeque, vec::Vec};
use core::{
    cmp::Ordering,
    hash::{BuildHasher, Hash},
    mem,
};

use hashbrown::{HashMap, HashSet};
use indexmap::IndexSet;

#[cfg(debug_assertions)]
use contracts::contract;

#[cfg(feature = "rkyv")]
use core::cmp::Reverse;

#[cfg(feature = "rkyv")]
use hashbrown::hash_map::Entry;

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
    ActivePathWeave, BookmarkableWeave, DeduplicatableContents, DeduplicatableWeave,
    DiscreteContentResult, DiscreteContents, DiscreteWeave, IndependentContents, MetadataWeave,
    Node, SortableBookmarkableWeave, SortableWeave, Weave, ancestor_subgraph,
    contract::active_path_is_valid,
    dependent::{DependentNode, DependentWeave},
    descendant_subgraph, detect_cycles, longest_candidate_path_to_root, shortest_path_to_ancestor,
    topological_sort, topological_sort_mirrored, topological_sort_subgraph,
    topological_sort_subgraph_mirrored,
};

#[cfg(debug_assertions)]
use crate::contract::{lacks_duplicates, valid_path, valid_topological_sort};

#[cfg(feature = "rkyv")]
use crate::{
    ImmutableActivePathWeave, ImmutableBookmarkableWeave, ImmutableMetadataWeave,
    ImmutableSortableWeave, ImmutableWeave, Step,
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
            && self.contents.eq(&other.contents)
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
    scratchpad_list: Vec<K>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad_list_2: Vec<K>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad_set: HashSet<K, S>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad_set_2: HashSet<K, S>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad_map: HashMap<K, usize, S>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad_map_2: HashMap<K, K, S>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad_map_3: HashMap<K, (usize, usize), S>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad_stack: Vec<K>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad_queue: VecDeque<K>,

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
            scratchpad_list: Vec::with_capacity(proxy.nodes.capacity()),
            scratchpad_list_2: Vec::with_capacity(proxy.nodes.capacity()),
            scratchpad_set: HashSet::with_capacity_and_hasher(proxy.nodes.capacity(), S::default()),
            scratchpad_set_2: HashSet::with_capacity_and_hasher(
                proxy.nodes.capacity(),
                S::default(),
            ),
            scratchpad_map: HashMap::with_capacity_and_hasher(proxy.nodes.capacity(), S::default()),
            scratchpad_map_2: HashMap::with_capacity_and_hasher(
                proxy.nodes.capacity(),
                S::default(),
            ),
            scratchpad_map_3: HashMap::with_capacity_and_hasher(
                proxy.nodes.capacity(),
                S::default(),
            ),
            scratchpad_stack: Vec::with_capacity(proxy.nodes.capacity()),
            scratchpad_queue: VecDeque::with_capacity(proxy.nodes.capacity()),
            nodes: proxy.nodes,
            roots: proxy.roots,
            active: proxy.active,
            bookmarked: proxy.bookmarked,
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
        Self {
            nodes: HashMap::with_capacity_and_hasher(capacity, S::default()),
            roots: IndexSet::with_capacity_and_hasher(capacity, S::default()),
            active: HashSet::with_capacity_and_hasher(capacity, S::default()),
            bookmarked: IndexSet::with_capacity_and_hasher(capacity, S::default()),
            scratchpad_list: Vec::with_capacity(capacity),
            scratchpad_list_2: Vec::with_capacity(capacity),
            scratchpad_set: HashSet::with_capacity_and_hasher(capacity, S::default()),
            scratchpad_set_2: HashSet::with_capacity_and_hasher(capacity, S::default()),
            scratchpad_map: HashMap::with_capacity_and_hasher(capacity, S::default()),
            scratchpad_map_2: HashMap::with_capacity_and_hasher(capacity, S::default()),
            scratchpad_map_3: HashMap::with_capacity_and_hasher(capacity, S::default()),
            scratchpad_stack: Vec::with_capacity(capacity),
            scratchpad_queue: VecDeque::with_capacity(capacity),
            metadata,
        }
    }
    /// Returns the number of nodes the weave can hold without reallocating.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.nodes.capacity()
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
        self.scratchpad_list.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_list.len()),
        );
        self.scratchpad_list_2.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_list_2.len()),
        );
        self.scratchpad_set.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_set.len()),
        );
        self.scratchpad_set_2.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_set_2.len()),
        );
        self.scratchpad_map.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_map.len()),
        );
        self.scratchpad_map_2.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_map_2.len()),
        );
        self.scratchpad_map_3.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_map_3.len()),
        );
        self.scratchpad_stack.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_stack.len()),
        );
        self.scratchpad_queue.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.scratchpad_queue.len()),
        );
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
        self.scratchpad_list.shrink_to(min_capacity);
        self.scratchpad_list_2.shrink_to(min_capacity);
        self.scratchpad_set.shrink_to(min_capacity);
        self.scratchpad_set_2.shrink_to(min_capacity);
        self.scratchpad_map.shrink_to(min_capacity);
        self.scratchpad_map_2.shrink_to(min_capacity);
        self.scratchpad_map_3.shrink_to(min_capacity);
        self.scratchpad_stack.shrink_to(min_capacity);
        self.scratchpad_queue.shrink_to(min_capacity);
    }
    fn sibling_ids_from_all_parents_including_roots<'a>(
        &'a self,
        node: &'a IndependentNode<K, T, S>,
    ) -> Box<dyn Iterator<Item = K> + 'a> {
        if node.from.is_empty() {
            Box::new(self.roots.iter().copied().filter(|id| *id != node.id))
        } else {
            Box::new(
                node.from
                    .iter()
                    .filter_map(|id| self.nodes.get(id))
                    .flat_map(|parent| {
                        {
                            parent.to.iter().copied().filter(|id| {
                                *id != node.id && !node.from.contains(id) && !node.to.contains(id)
                            })
                        }
                    })
                    .collect::<IndexSet<K, S>>()
                    .into_iter(),
            )
        }
    }
    #[allow(
        clippy::too_many_lines,
        reason = "Cannot be split into smaller functions"
    )]
    #[cfg_attr(debug_assertions, contract(
        requires(self.validate_scratchpads()),
        ensures(ret == self.nodes.contains_key(id)),
        ensures(!ret || value == self.active.contains(id)),
        ensures(self.validate())
    ))]
    pub(super) fn update_node_activity_in_place(&mut self, id: &K, value: bool) -> bool {
        if let Some(node) = self.nodes.get_mut(id) {
            if node.active == value {
                return true;
            }

            node.active = value;
            if value {
                self.active.insert(node.id);
            } else {
                self.active.remove(id);
            }
        } else {
            return false;
        }

        if value {
            for root in &self.roots {
                topological_sort(
                    &self.nodes,
                    root,
                    &mut self.scratchpad_stack,
                    &mut self.scratchpad_list, // topological order
                    &mut self.scratchpad_set,
                    &mut self.scratchpad_map,
                );
            }

            self.scratchpad_set.clear();
            self.scratchpad_map.clear();

            for id in self.scratchpad_list.iter().copied() {
                let node = &self.nodes[&id];

                let best_parent = node
                    .from
                    .iter()
                    .map(|id| (id, self.scratchpad_map_3[id])) // score: (connectors, active)
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
                    self.scratchpad_map_2.insert(id, *parent); // predecessors
                }

                self.scratchpad_map_3.insert(id, score);
            }

            let mut current = Some(id);

            while let Some(id) = current {
                self.scratchpad_set.insert(*id);
                current = self.scratchpad_map_2.get(id);
            }

            self.scratchpad_map_2.clear();
            self.scratchpad_map_3.clear();

            for id in self.scratchpad_list.drain(..).rev() {
                let node = &self.nodes[&id];

                let best_child = node
                    .to
                    .iter()
                    .map(|id| (id, self.scratchpad_map_3[id])) // score: (connectors, active)
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
                    self.scratchpad_map_2.insert(id, *child); // successors
                }

                self.scratchpad_map_3.insert(id, score);
            }

            let mut current = Some(id);

            while let Some(id) = current {
                self.scratchpad_set.insert(*id);

                current = if self.scratchpad_map_3[id].1 > usize::from(self.nodes[id].active) {
                    self.scratchpad_map_2.get(id)
                } else {
                    None
                };
            }

            self.scratchpad_map_2.clear();
            self.scratchpad_map_3.clear();

            self.scratchpad_list
                .extend(self.active.difference(&self.scratchpad_set).copied());

            for id in self.scratchpad_list.drain(..) {
                self.nodes.get_mut(&id).unwrap().active = false;
                self.active.remove(&id);
            }

            self.scratchpad_list
                .extend(self.scratchpad_set.difference(&self.active).copied());

            self.scratchpad_set.clear();

            for id in self.scratchpad_list.drain(..) {
                self.nodes.get_mut(&id).unwrap().active = true;
                self.active.insert(id);
            }
        } else {
            self.fix_orphaned_activations();
        }

        true
    }
    #[cfg_attr(debug_assertions, contract(
        requires(self.validate_scratchpads()),
        ensures(self.validate())
    ))]
    pub(super) fn fix_orphaned_activations(&mut self) {
        for root in &self.roots {
            topological_sort(
                &self.nodes,
                root,
                &mut self.scratchpad_stack,
                &mut self.scratchpad_list,
                &mut self.scratchpad_set,
                &mut self.scratchpad_map,
            );
        }

        self.scratchpad_map.clear();

        longest_candidate_path_to_root(
            &self.nodes,
            &self.scratchpad_list,
            &|id| self.active.contains(id),
            &mut self.scratchpad_map,
            &mut self.scratchpad_list_2,
        );

        self.scratchpad_list.clear();
        self.scratchpad_set.clear();
        self.scratchpad_map.clear();

        self.scratchpad_set.extend(self.scratchpad_list_2.drain(..));
        self.scratchpad_list
            .extend(self.active.difference(&self.scratchpad_set).copied());

        self.scratchpad_set.clear();

        for orphan in self.scratchpad_list.drain(..) {
            self.active.remove(&orphan);
            if let Some(node) = self.nodes.get_mut(&orphan) {
                node.active = false;
            }
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(!ret || value || self.active.is_empty() || (old(self.active.clone()) == self.active && (!self.active.contains(id) || self.nodes[id].to.iter().any(|id| self.contains_active(id))))),
        ensures(!ret || !value || self.contains_active(id) && !self.nodes[id].to.iter().any(|id| self.active.contains(id))),
        ensures(ret || old(self.active.clone()) == self.active),
        ensures(ret == self.nodes.contains_key(id)),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    #[allow(clippy::missing_panics_doc, reason = "Should never panic")]
    /// Sets the active status of a node with the specified identifier, using identical activation behavior to [`DependentWeave`].
    pub fn set_node_active_status_dependent_semantics(&mut self, id: &K, value: bool) -> bool {
        if value {
            if let Some(node) = self.nodes.get_mut(id) {
                if node.active && !node.to.iter().any(|id| self.active.contains(id)) {
                    return true;
                }

                node.active = value;
                if value {
                    self.active.insert(node.id);
                } else {
                    self.active.remove(id);
                }
            } else {
                return false;
            }

            for root in &self.roots {
                topological_sort(
                    &self.nodes,
                    root,
                    &mut self.scratchpad_stack,
                    &mut self.scratchpad_list, // topological order
                    &mut self.scratchpad_set,
                    &mut self.scratchpad_map,
                );
            }

            self.scratchpad_set.clear();
            self.scratchpad_map.clear();

            for id in self.scratchpad_list.drain(..) {
                let node = &self.nodes[&id];

                let best_parent = node
                    .from
                    .iter()
                    .map(|id| (id, self.scratchpad_map_3[id])) // score: (connectors, active)
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
                    self.scratchpad_map_2.insert(id, *parent); // predecessors
                }

                self.scratchpad_map_3.insert(id, score);
            }

            let mut current = Some(id);

            while let Some(id) = current {
                self.scratchpad_set.insert(*id);
                current = self.scratchpad_map_2.get(id);
            }

            self.scratchpad_map_2.clear();
            self.scratchpad_map_3.clear();

            self.scratchpad_list
                .extend(self.active.difference(&self.scratchpad_set).copied());

            for id in self.scratchpad_list.drain(..) {
                self.nodes.get_mut(&id).unwrap().active = false;
                self.active.remove(&id);
            }

            self.scratchpad_list
                .extend(self.scratchpad_set.difference(&self.active).copied());

            self.scratchpad_set.clear();

            for id in self.scratchpad_list.drain(..) {
                self.nodes.get_mut(&id).unwrap().active = true;
                self.active.insert(id);
            }
        } else {
            if let Some(node) = self.nodes.get(id) {
                if !node.active || node.to.iter().any(|id| self.active.contains(id)) {
                    return true;
                }
            } else {
                return false;
            }

            self.active.iter().for_each(|active| {
                self.nodes.get_mut(active).unwrap().active = false;
            });
            self.active.clear();
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
            scratchpad_list: Vec::with_capacity(value.nodes.capacity()),
            scratchpad_list_2: Vec::with_capacity(value.nodes.capacity()),
            scratchpad_set: HashSet::with_capacity_and_hasher(value.nodes.capacity(), S::default()),
            scratchpad_set_2: HashSet::with_capacity_and_hasher(
                value.nodes.capacity(),
                S::default(),
            ),
            scratchpad_map: HashMap::with_capacity_and_hasher(value.nodes.capacity(), S::default()),
            scratchpad_map_2: HashMap::with_capacity_and_hasher(
                value.nodes.capacity(),
                S::default(),
            ),
            scratchpad_map_3: HashMap::with_capacity_and_hasher(
                value.nodes.capacity(),
                S::default(),
            ),
            scratchpad_stack: Vec::with_capacity(value.nodes.capacity()),
            scratchpad_queue: VecDeque::with_capacity(value.nodes.capacity()),
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
            metadata: value.metadata,
        };

        if let Some(active) = value.active {
            output.set_node_active_status(&active, true);
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
                scratchpad: value.scratchpad_stack,
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
    fn get_node(&self, id: &K) -> Option<&IndependentNode<K, T, S>> {
        self.nodes.get(id)
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
    fn get_ordered_node_identifiers(&mut self, output: &mut Vec<K>) {
        output.clear();

        for root in &self.roots {
            topological_sort(
                &self.nodes,
                root,
                &mut self.scratchpad_stack,
                output,
                &mut self.scratchpad_set,
                &mut self.scratchpad_map,
            );
        }

        self.scratchpad_set.clear();
        self.scratchpad_map.clear();
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
    fn get_ordered_node_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();

        if self.nodes.contains_key(id) {
            descendant_subgraph(
                &self.nodes,
                *id,
                &mut self.scratchpad_stack,
                &mut self.scratchpad_set,
            );

            topological_sort_subgraph(
                &self.nodes,
                &|id| self.scratchpad_set.contains(id),
                id,
                &mut self.scratchpad_stack,
                output,
                &mut self.scratchpad_set_2,
                &mut self.scratchpad_map,
            );

            self.scratchpad_set.clear();
            self.scratchpad_set_2.clear();
            self.scratchpad_map.clear();
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

        for root in &self.roots {
            topological_sort_subgraph(
                &self.nodes,
                &|id| self.active.contains(id),
                root,
                &mut self.scratchpad_stack,
                &mut self.scratchpad_list,
                &mut self.scratchpad_set,
                &mut self.scratchpad_map,
            );
        }

        self.scratchpad_set.clear();
        self.scratchpad_map.clear();

        longest_candidate_path_to_root(
            &self.nodes,
            &self.scratchpad_list,
            &|id| self.active.contains(id),
            &mut self.scratchpad_map,
            output,
        );

        self.scratchpad_list.clear();
        self.scratchpad_map.clear();
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

        ancestor_subgraph(
            &self.nodes,
            *id,
            &mut self.scratchpad_stack,
            &mut self.scratchpad_set,
        );

        for root in &self.roots {
            topological_sort_subgraph(
                &self.nodes,
                &|id| self.active.contains(id),
                root,
                &mut self.scratchpad_stack,
                &mut self.scratchpad_list,
                &mut self.scratchpad_set_2,
                &mut self.scratchpad_map,
            );
        }

        self.scratchpad_map.clear();

        longest_candidate_path_to_root(
            &self.nodes,
            &self.scratchpad_list,
            &|id| self.active.contains(id) && self.scratchpad_set.contains(id),
            &mut self.scratchpad_map,
            &mut self.scratchpad_list_2,
        );

        self.scratchpad_list.clear();
        self.scratchpad_set.clear();
        self.scratchpad_set_2.clear();
        self.scratchpad_map.clear();
        self.scratchpad_map_2.clear();

        if let Some(target) = self.scratchpad_list_2.first().copied() {
            shortest_path_to_ancestor(
                &self.nodes,
                id,
                &|node| node.id == target,
                &mut self.scratchpad_queue,
                &mut self.scratchpad_map_2,
                &mut self.scratchpad_set_2,
                output,
            );

            output.reverse();
            output.pop();
            output.append(&mut self.scratchpad_list_2);
        } else {
            shortest_path_to_ancestor(
                &self.nodes,
                id,
                &|node| node.from.is_empty(),
                &mut self.scratchpad_queue,
                &mut self.scratchpad_map_2,
                &mut self.scratchpad_set_2,
                output,
            );

            output.reverse();
        }

        self.scratchpad_set_2.clear();
        self.scratchpad_map_2.clear();
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
    fn add_node(&mut self, mut node: IndependentNode<K, T, S>) -> bool {
        if self.nodes.contains_key(&node.id)
            || !node.validate()
            || !node.from.iter().all(|id| self.nodes.contains_key(id))
            || !node.to.iter().all(|id| self.nodes.contains_key(id))
        {
            return false;
        }

        if !node.to.is_empty() && !node.from.is_empty() {
            for parent in node.from.iter().copied() {
                ancestor_subgraph(
                    &self.nodes,
                    parent,
                    &mut self.scratchpad_stack,
                    &mut self.scratchpad_set,
                );
            }

            if node
                .to
                .iter()
                .any(|child| self.scratchpad_set.contains(child))
            {
                self.scratchpad_set.clear();
                return false;
            }

            self.scratchpad_set.clear();
        }

        let root_index = node
            .to
            .iter()
            .filter_map(|child| self.roots.get_index_of(child))
            .min();

        for child in &node.to {
            let child = &self.nodes[child];
            if child.from.is_empty() {
                if child.active {
                    node.active = true;
                }
                self.roots.shift_remove(&child.id);
            }
        }

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

        for child in &node.to {
            let child = self.nodes.get_mut(child).unwrap();
            child.from.insert(node.id);
        }

        if node.bookmarked {
            self.bookmarked.insert(node.id);
        }

        let id = node.id;
        let active = node.active;
        node.active = false;

        self.nodes.insert(node.id, node);

        if active {
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
    fn set_node_active_status(&mut self, id: &K, value: bool) -> bool {
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
    fn remove_node(&mut self, id: &K) -> Option<IndependentNode<K, T, S>> {
        let mut removed_node = None;

        self.scratchpad_stack.push(*id);

        while let Some(id) = self.scratchpad_stack.pop() {
            if let Some(node) = self.nodes.remove(&id) {
                if node.from.is_empty() {
                    self.roots.shift_remove(&id);
                }
                if node.bookmarked {
                    self.bookmarked.shift_remove(&id);
                }
                if node.active {
                    self.active.remove(&id);
                }

                for parent in &node.from {
                    if let Some(parent) = self.nodes.get_mut(parent) {
                        parent.to.shift_remove(&node.id);
                    }
                }
                for child in node.to.iter().rev() {
                    if let Some(child) = self.nodes.get_mut(child) {
                        child.from.shift_remove(&node.id);

                        if child.from.is_empty() {
                            self.scratchpad_stack.push(child.id);
                        }
                    }
                }

                if removed_node.is_none() {
                    removed_node = Some(node);
                }
            }
        }

        if removed_node.is_some() {
            self.fix_orphaned_activations();
            removed_node
        } else {
            None
        }
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
    fn remove_node_tracked(
        &mut self,
        id: &K,
        mut on_removal: impl FnMut(IndependentNode<K, T, S>),
    ) -> bool {
        let had_node = self.nodes.contains_key(id);

        self.scratchpad_stack.push(*id);

        while let Some(id) = self.scratchpad_stack.pop() {
            if let Some(node) = self.nodes.remove(&id) {
                if node.from.is_empty() {
                    self.roots.shift_remove(&id);
                }
                if node.bookmarked {
                    self.bookmarked.shift_remove(&id);
                }
                if node.active {
                    self.active.remove(&id);
                }

                for parent in &node.from {
                    if let Some(parent) = self.nodes.get_mut(parent) {
                        parent.to.shift_remove(&node.id);
                    }
                }
                for child in node.to.iter().rev() {
                    if let Some(child) = self.nodes.get_mut(child) {
                        child.from.shift_remove(&node.id);

                        if child.from.is_empty() {
                            self.scratchpad_stack.push(child.id);
                        }
                    }
                }

                on_removal(node);
            }
        }

        if had_node {
            self.fix_orphaned_activations();
            true
        } else {
            false
        }
    }
    #[cfg_attr(debug_assertions, contract(
        ensures(self.nodes.is_empty()),
        ensures(self.validate())
    ))]
    fn remove_all_nodes(&mut self) {
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
        let mut scratchpad = Vec::with_capacity(self.nodes.len());
        let mut scratchpad_map = HashMap::with_capacity_and_hasher(self.nodes.len(), S::default());

        self.validate_scratchpads()
            && self
                .roots
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
            && !detect_cycles(
                &self.nodes,
                self.roots.iter().copied(),
                &mut scratchpad,
                &mut scratchpad_map,
            )
            && active_path_is_valid(&self.nodes, self.roots.iter(), &self.active)
    }
    fn validate_scratchpads(&self) -> bool {
        self.scratchpad_list.is_empty()
            && self.scratchpad_list_2.is_empty()
            && self.scratchpad_set.is_empty()
            && self.scratchpad_set_2.is_empty()
            && self.scratchpad_map.is_empty()
            && self.scratchpad_map_2.is_empty()
            && self.scratchpad_map_3.is_empty()
            && self.scratchpad_stack.is_empty()
            && self.scratchpad_queue.is_empty()
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
    fn set_node_bookmarked_status(&mut self, id: &K, value: bool) -> bool {
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
        ensures(output.len() == self.nodes.len()),
        ensures(valid_topological_sort(&self.nodes, output)),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.roots.clone()) == self.roots),
        ensures(old(self.active.clone()) == self.active),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        invariant(self.validate())
    ))]
    fn get_ordered_node_identifiers_mirrored(&mut self, output: &mut Vec<K>) {
        output.clear();

        for root in &self.roots {
            topological_sort_mirrored(
                &self.nodes,
                root,
                &mut self.scratchpad_stack,
                output,
                &mut self.scratchpad_set,
                &mut self.scratchpad_map,
            );
        }

        self.scratchpad_set.clear();
        self.scratchpad_map.clear();
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
    fn get_ordered_node_identifiers_mirrored_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();

        if self.nodes.contains_key(id) {
            descendant_subgraph(
                &self.nodes,
                *id,
                &mut self.scratchpad_stack,
                &mut self.scratchpad_set,
            );

            topological_sort_subgraph_mirrored(
                &self.nodes,
                &|id| self.scratchpad_set.contains(id),
                id,
                &mut self.scratchpad_stack,
                output,
                &mut self.scratchpad_set_2,
                &mut self.scratchpad_map,
            );

            self.scratchpad_set.clear();
            self.scratchpad_set_2.clear();
            self.scratchpad_map.clear();
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
    fn sort_node_children_by(
        &mut self,
        id: &K,
        mut cmp: impl FnMut(&IndependentNode<K, T, S>, &IndependentNode<K, T, S>) -> Ordering,
    ) -> bool {
        if let Some(mut node) = self.nodes.remove(id) {
            node.to.sort_by(|a, b| cmp(&self.nodes[a], &self.nodes[b]));
            self.nodes.insert(node.id, node);

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
    fn sort_node_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool {
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
        self.roots
            .sort_by(|a, b| cmp(&self.nodes[a], &self.nodes[b]));
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
        self.bookmarked
            .sort_by(|a, b| cmp(&self.nodes[a], &self.nodes[b]));
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
    fn split_node(&mut self, id: &K, at: usize, new_id: K) -> bool {
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

                    for child in &node.to {
                        let child = self.nodes.get_mut(child).unwrap();

                        if let Some(index) = child.from.get_index_of(&left_node.id) {
                            if child.from.replace_index(index, node.id).is_err() {
                                child.from.shift_remove_index(index);
                            }
                        } else {
                            child.from.insert(node.id);
                        }
                        if child.active && left_node.active {
                            node.active = true;
                            self.active.insert(node.id);
                        }
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

                            if let Some(index) = child.from.get_index_of(&node.id) {
                                if child.from.replace_index(index, parent.id).is_err() {
                                    child.from.shift_remove_index(index);
                                }
                            } else {
                                child.from.insert(parent.id);
                            }
                        }

                        let parent_id = parent.id;

                        self.nodes.insert(parent.id, parent);

                        self.bookmarked.shift_remove(&node.id);
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

impl<K, T, M, S> crate::SemiIndependentWeave<K, IndependentNode<K, T, S>, T>
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

impl<K, T, M, S> DeduplicatableWeave<K, IndependentNode<K, T, S>, T>
    for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents + DeduplicatableContents,
    S: BuildHasher + Default + Clone,
{
    fn find_duplicates(&self, id: &K) -> impl Iterator<Item = K> {
        self.nodes.get(id).into_iter().flat_map(|node| {
            self.sibling_ids_from_all_parents_including_roots(node)
                .filter_map(|id| self.nodes.get(&id))
                .filter_map(|sibling| {
                    if node.contents.is_duplicate_of(&sibling.contents) {
                        Some(sibling.id)
                    } else {
                        None
                    }
                })
        })
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
        ensures(ret || old(self.nodes().get(id).map(|node| node.from.clone())).as_ref() == self.nodes().get(id).map(|node| &node.from)),
        ensures(ret || old(self.roots.clone()) == self.roots),
        ensures(ret || old(self.active.clone()) == self.active),
        ensures(old(self.nodes().get(id).map(|node| node.to.clone())).as_ref() == self.nodes().get(id).map(|node| &node.to)),
        ensures(old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()),
        ensures(old(self.bookmarked.clone()) == self.bookmarked),
        ensures(old(self.active.contains(id)) == self.active.contains(id)),
        invariant(self.validate())
    ))]
    fn move_node(&mut self, id: &K, new_parents: &[K]) -> bool {
        if new_parents
            .iter()
            .any(|new_parent| !self.nodes.contains_key(new_parent))
        {
            return false;
        }

        if let Some(node) = self.nodes.get(id)
            && !node.to.is_empty()
            && !new_parents.is_empty()
        {
            for child in node.to.iter().copied() {
                descendant_subgraph(
                    &self.nodes,
                    child,
                    &mut self.scratchpad_stack,
                    &mut self.scratchpad_set,
                );
            }

            if new_parents
                .iter()
                .any(|new_parent| self.scratchpad_set.contains(new_parent))
            {
                self.scratchpad_set.clear();
                return false;
            }

            self.scratchpad_set.clear();
        }

        let new_parents: IndexSet<K, S> = new_parents.iter().copied().collect();

        if new_parents.contains(id) {
            return false;
        }

        if let Some(node) = self.nodes.get_mut(id) {
            for child in &node.to {
                if new_parents.contains(child) {
                    return false;
                }
            }

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
        } else {
            return false;
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
        let mut scratchpad = Vec::with_capacity(self.nodes.len());
        let mut scratchpad_map = HashMap::with_capacity(self.nodes.len());

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
            && !archived_detect_cycles(
                &self.nodes,
                self.roots.iter().copied(),
                &mut scratchpad,
                &mut scratchpad_map,
            )
            && archived_active_path_is_valid(&self.nodes, self.roots.iter(), &self.active)
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
    fn get_node(&self, id: &K::Archived) -> Option<&ArchivedIndependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    fn get_ordered_node_identifiers(&self, output: &mut Vec<K::Archived>) {
        output.clear();
        let mut scratchpad = Vec::with_capacity(self.len());
        let mut scratchpad_2 = Vec::with_capacity(self.len());
        let mut identifier_set = HashSet::with_capacity(self.len());
        let mut scratchpad_map = HashMap::with_capacity(self.len());

        for root in self.roots.iter() {
            archived_topological_sort(
                &self.nodes,
                root,
                &mut scratchpad,
                &mut scratchpad_2,
                output,
                &mut identifier_set,
                &mut scratchpad_map,
            );
        }
    }
    fn get_ordered_node_identifiers_from(&self, id: &K::Archived, output: &mut Vec<K::Archived>) {
        output.clear();

        if self.nodes.contains_key(id) {
            let mut scratchpad = Vec::with_capacity(self.len());
            let mut scratchpad_2 = Vec::with_capacity(self.len());
            let mut scratchpad_set = HashSet::with_capacity(self.len());
            let mut scratchpad_set_2 = HashSet::with_capacity(self.len());
            let mut scratchpad_map = HashMap::with_capacity(self.len());

            archived_descendant_subgraph(&self.nodes, *id, &mut scratchpad, &mut scratchpad_set);

            archived_topological_sort_subgraph(
                &self.nodes,
                &|id| scratchpad_set.contains(id),
                id,
                &mut scratchpad,
                &mut scratchpad_2,
                output,
                &mut scratchpad_set_2,
                &mut scratchpad_map,
            );
        }
    }
    fn get_active_path(&self, output: &mut Vec<K::Archived>) {
        output.clear();
        let mut scratchpad_list = Vec::with_capacity(self.len());
        let mut scratchpad_list_2 = Vec::with_capacity(self.len());
        let mut scratchpad_list_3 = Vec::with_capacity(self.len());
        let mut scratchpad_set = HashSet::with_capacity(self.len());
        let mut scratchpad_map = HashMap::with_capacity(self.len());

        for root in self.roots.iter() {
            archived_topological_sort_subgraph(
                &self.nodes,
                &|id| self.active.contains(id),
                root,
                &mut scratchpad_list,
                &mut scratchpad_list_2,
                &mut scratchpad_list_3,
                &mut scratchpad_set,
                &mut scratchpad_map,
            );
        }

        scratchpad_map.clear();

        archived_longest_candidate_path_to_root(
            &self.nodes,
            &scratchpad_list_3,
            &|id| self.active.contains(id),
            &mut scratchpad_map,
            output,
        );
    }
    fn get_path_from(&self, id: &K::Archived, output: &mut Vec<K::Archived>) {
        output.clear();
        if !self.nodes.contains_key(id) {
            return;
        }

        let mut scratchpad_list = Vec::with_capacity(self.len());
        let mut scratchpad_list_2 = Vec::with_capacity(self.len());
        let mut scratchpad_stack = Vec::with_capacity(self.len());
        let mut scratchpad_queue = VecDeque::with_capacity(self.len());
        let mut scratchpad_set = HashSet::with_capacity(self.len());
        let mut scratchpad_set_2 = HashSet::with_capacity(self.len());
        let mut scratchpad_map = HashMap::with_capacity(self.len());
        let mut scratchpad_map_2 = HashMap::with_capacity(self.len());

        archived_ancestor_subgraph(&self.nodes, *id, &mut scratchpad_stack, &mut scratchpad_set);

        for root in self.roots.iter() {
            archived_topological_sort_subgraph(
                &self.nodes,
                &|id| self.active.contains(id),
                root,
                &mut scratchpad_stack,
                &mut scratchpad_list_2,
                &mut scratchpad_list,
                &mut scratchpad_set_2,
                &mut scratchpad_map,
            );
        }

        scratchpad_map.clear();

        archived_longest_candidate_path_to_root(
            &self.nodes,
            &scratchpad_list,
            &|id| self.active.contains(id) && scratchpad_set.contains(id),
            &mut scratchpad_map,
            &mut scratchpad_list_2,
        );

        scratchpad_set_2.clear();

        if let Some(target) = scratchpad_list_2.first().copied() {
            archived_shortest_path_to_ancestor(
                &self.nodes,
                id,
                &|node| node.id == target,
                &mut scratchpad_queue,
                &mut scratchpad_map_2,
                &mut scratchpad_set_2,
                output,
            );

            output.reverse();
            output.pop();
            output.append(&mut scratchpad_list_2);
        } else {
            archived_shortest_path_to_ancestor(
                &self.nodes,
                id,
                &|node| node.from.is_empty(),
                &mut scratchpad_queue,
                &mut scratchpad_map_2,
                &mut scratchpad_set_2,
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
impl<K, T, M, S> ImmutableSortableWeave<K::Archived, ArchivedIndependentNode<K, T, S>, T::Archived>
    for ArchivedIndependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive + IndependentContents,
    M: Archive,
    S: BuildHasher + Default + Clone,
{
    fn get_ordered_node_identifiers_mirrored(&self, output: &mut Vec<K::Archived>) {
        output.clear();
        let mut scratchpad = Vec::with_capacity(self.len());
        let mut identifier_set = HashSet::with_capacity(self.len());
        let mut scratchpad_map = HashMap::with_capacity(self.len());

        for root in self.roots.iter() {
            archived_topological_sort_mirrored(
                &self.nodes,
                root,
                &mut scratchpad,
                output,
                &mut identifier_set,
                &mut scratchpad_map,
            );
        }
    }
    fn get_ordered_node_identifiers_mirrored_from(
        &self,
        id: &K::Archived,
        output: &mut Vec<K::Archived>,
    ) {
        output.clear();

        if self.nodes.contains_key(id) {
            let mut scratchpad = Vec::with_capacity(self.len());
            let mut scratchpad_set = HashSet::with_capacity(self.len());
            let mut scratchpad_set_2 = HashSet::with_capacity(self.len());
            let mut scratchpad_map = HashMap::with_capacity(self.len());

            archived_descendant_subgraph(&self.nodes, *id, &mut scratchpad, &mut scratchpad_set);

            archived_topological_sort_subgraph_mirrored(
                &self.nodes,
                &|id| scratchpad_set.contains(id),
                id,
                &mut scratchpad,
                output,
                &mut scratchpad_set_2,
                &mut scratchpad_map,
            );
        }
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

#[cfg(feature = "rkyv")]
fn archived_topological_sort<'a, K, N, T, S>(
    nodes: &'a ArchivedHashMap<K, N>,
    id: &'a K,
    scratchpad: &mut Vec<K>,
    scratchpad_2: &mut Vec<K>,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K, S>,
    identifier_map: &mut HashMap<K, usize, S>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push(*id);

    while let Some(id) = scratchpad.pop() {
        let node = &nodes[&id];

        if identifier_set.contains(&id)
            || identifier_map
                .get(&id)
                .copied()
                .unwrap_or_else(|| node.from().len())
                != 0
        {
            continue;
        }

        identifiers.push(id);
        identifier_set.insert(id);

        for child in node.to().iter().copied() {
            let remaining = identifier_map
                .entry(child)
                .or_insert_with(|| nodes[&child].from().len());
            *remaining = remaining.strict_sub(1);

            scratchpad_2.push(child);
        }

        scratchpad_2.reverse();
        scratchpad.append(scratchpad_2);
    }
}

#[cfg(feature = "rkyv")]
#[allow(clippy::too_many_arguments, reason = "Rkyv limitation")]
fn archived_topological_sort_subgraph<'a, K, N, T, S>(
    nodes: &'a ArchivedHashMap<K, N>,
    filter: &impl Fn(&K) -> bool,
    id: &'a K,
    scratchpad: &mut Vec<K>,
    scratchpad_2: &mut Vec<K>,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K, S>,
    identifier_map: &mut HashMap<K, usize, S>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push(*id);

    while let Some(id) = scratchpad.pop() {
        let node = &nodes[&id];

        if !filter(&id)
            || identifier_set.contains(&id)
            || identifier_map
                .get(&id)
                .copied()
                .unwrap_or_else(|| node.from().iter().filter(|&parent| filter(parent)).count())
                != 0
        {
            continue;
        }

        identifiers.push(id);
        identifier_set.insert(id);

        for child in node.to().iter().copied() {
            let remaining = identifier_map.entry(child).or_insert_with(|| {
                nodes[&child]
                    .from()
                    .iter()
                    .filter(|&parent| filter(parent))
                    .count()
            });
            *remaining = remaining.strict_sub(1);

            scratchpad_2.push(child);
        }

        scratchpad_2.reverse();
        scratchpad.append(scratchpad_2);
    }
}

#[cfg(feature = "rkyv")]
fn archived_topological_sort_subgraph_mirrored<'a, K, N, T, S>(
    nodes: &'a ArchivedHashMap<K, N>,
    filter: &impl Fn(&K) -> bool,
    id: &'a K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K, S>,
    identifier_map: &mut HashMap<K, usize, S>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push(*id);

    while let Some(id) = scratchpad.pop() {
        let node = &nodes[&id];

        if !filter(&id)
            || identifier_set.contains(&id)
            || identifier_map
                .get(&id)
                .copied()
                .unwrap_or_else(|| node.from().iter().filter(|&parent| filter(parent)).count())
                != 0
        {
            continue;
        }

        identifiers.push(id);
        identifier_set.insert(id);

        for child in node.to().iter().copied() {
            let remaining = identifier_map.entry(child).or_insert_with(|| {
                nodes[&child]
                    .from()
                    .iter()
                    .filter(|&parent| filter(parent))
                    .count()
            });
            *remaining = remaining.strict_sub(1);

            scratchpad.push(child);
        }
    }
}

#[cfg(feature = "rkyv")]
fn archived_topological_sort_mirrored<'a, K, N, T, S>(
    nodes: &'a ArchivedHashMap<K, N>,
    id: &'a K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K, S>,
    identifier_map: &mut HashMap<K, usize, S>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push(*id);

    while let Some(id) = scratchpad.pop() {
        let node = &nodes[&id];

        if identifier_set.contains(&id)
            || identifier_map
                .get(&id)
                .copied()
                .unwrap_or_else(|| node.from().len())
                != 0
        {
            continue;
        }

        identifiers.push(id);
        identifier_set.insert(id);

        for child in node.to().iter().copied() {
            let remaining = identifier_map
                .entry(child)
                .or_insert_with(|| nodes[&child].from().len());
            *remaining = remaining.strict_sub(1);

            scratchpad.push(child);
        }
    }
}

#[cfg(feature = "rkyv")]
fn archived_detect_cycles<'a, K, N, T, S>(
    nodes: &'a ArchivedHashMap<K, N>,
    roots: impl Iterator<Item = K>,
    scratchpad: &mut Vec<Step<K, K>>,
    scratchpad_map: &mut HashMap<K, bool, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
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

                            scratchpad.extend(nodes[&id].to().iter().copied().map(Step::Enter));
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

#[cfg(feature = "rkyv")]
#[allow(clippy::too_many_arguments, reason = "Rkyv limitation")]
fn archived_shortest_path_to_ancestor<'a, K, N, T, S>(
    nodes: &'a ArchivedHashMap<K, N>,
    id: &'a K,
    target: &impl Fn(&'a N) -> bool,
    scratchpad: &mut VecDeque<K>,
    scratchpad_map: &mut HashMap<K, K, S>,
    scratchpad_set: &mut HashSet<K, S>,
    path: &mut Vec<K>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
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

        for parent in node.from().iter().copied() {
            if scratchpad_set.insert(parent) {
                scratchpad.push_front(parent);
                scratchpad_map.insert(parent, id);
            }
        }
    }
}

#[cfg(feature = "rkyv")]
fn archived_longest_candidate_path_to_root<'a, K, N, T, S>(
    nodes: &'a ArchivedHashMap<K, N>,
    topological_order: &'a [K],
    is_candidate: &impl Fn(&K) -> bool,
    scratchpad_map: &mut HashMap<K, usize, S>,
    reversed_path: &mut Vec<K>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
    S: BuildHasher + Default + Clone,
{
    let mut longest_distance = None;

    for id in topological_order {
        if !is_candidate(id) {
            continue;
        }

        let node = &nodes[id];
        let distance = if node.from().is_empty() {
            Some(0)
        } else {
            node.from()
                .iter()
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
            .iter()
            .filter(|id| scratchpad_map.contains_key(*id))
            .min_by_key(|id| Reverse(scratchpad_map[*id]));
    }
}

#[cfg(feature = "rkyv")]
fn archived_ancestor_subgraph<'a, K, N, T, S>(
    nodes: &'a ArchivedHashMap<K, N>,
    id: K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push(id);

    while let Some(id) = scratchpad.pop() {
        if identifiers.insert(id) {
            scratchpad.extend(nodes[&id].from().iter().copied());
        }
    }
}

#[cfg(feature = "rkyv")]
fn archived_descendant_subgraph<'a, K, N, T, S>(
    nodes: &'a ArchivedHashMap<K, N>,
    id: K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push(id);

    while let Some(id) = scratchpad.pop() {
        if identifiers.insert(id) {
            scratchpad.extend(nodes[&id].to().iter().copied());
        }
    }
}

#[cfg(feature = "rkyv")]
fn archived_active_path_is_valid<'a, K, N, T>(
    nodes: &'a ArchivedHashMap<K, N>,
    roots: impl Iterator<Item = &'a K>,
    active: &'a ArchivedHashSet<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T, From = ArchivedIndexSet<K>, To = ArchivedIndexSet<K>> + 'a,
{
    let mut scratchpad = Vec::with_capacity(nodes.len());
    let mut scratchpad_list = Vec::with_capacity(nodes.len());
    let mut scratchpad_list_2 = Vec::with_capacity(nodes.len());
    let mut scratchpad_set = HashSet::with_capacity(nodes.len());
    let mut scratchpad_map = HashMap::with_capacity(nodes.len());

    for root in roots {
        archived_topological_sort(
            nodes,
            root,
            &mut scratchpad,
            &mut scratchpad_list_2,
            &mut scratchpad_list,
            &mut scratchpad_set,
            &mut scratchpad_map,
        );
    }

    scratchpad_list_2.clear();
    scratchpad_set.clear();
    scratchpad_map.clear();

    archived_longest_candidate_path_to_root(
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
