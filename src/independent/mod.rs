//! [`IndependentWeave`] is a DAG-based [`Weave`] where each [`Node`] does *not* depend on the contents of the previous Node.

use alloc::vec::Vec;
use core::{
    cmp::Ordering,
    hash::{BuildHasher, Hash},
    mem,
};

use creusot_std::macros::*;
use hashbrown::{HashMap, HashSet, hash_map::Entry};
use indexmap::IndexSet;
use scratchpads::{Scratchpad, ScratchpadMap};

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
    ActivePathWeave, BookmarkableWeave, BuildableNode, DiscreteContentResult, DiscreteContents,
    DiscreteWeave, IndependentContents, IndependentWeave as IndependentWeaveTrait, MetadataWeave,
    Node, SemiIndependentWeave, SortableBookmarkableWeave, SortableWeave, Verifiable,
    VerifiableIterator, Weave, default_hasher, min_len, same_id,
};

use crate::dependent::{DependentNode, DependentWeave, retain_absent};

use graph::{
    ancestor_subgraph, ancestor_subgraph_reaches, descendant_subgraph, descendant_subgraph_reaches,
    longest_candidate_path_to_root, shortest_path_to_ancestor, topological_sort,
    topological_sort_subgraph,
};
use support::{
    activate_exactly, all_present_in_slice, all_single_parent, any_active, any_active_survivor,
    best_scored, dependent_nodes, independent_nodes,
    contains_all, contains_other, deactivate_all, extend_difference, extend_from_hash_set,
    extend_from_scratch, extend_keys, extend_surviving_keys, extends_active_end, first_root_index,
    index_set_from_slice, retain_unlisted, same_ids, sort_by_nodes, validate,
};

mod graph;
mod support;

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

#[cfg(creusot)]
use creusot_std::{
    invariant::inv,
    logic::{FSet, such_that},
    prelude::*,
};

#[cfg(creusot)]
use crate::verification::{
    Id, Model, bookmark_set, calls, compares_by, dedup, fed, first, id_of, identity, ids, iterates,
    lemma_calls_snoc, lemma_dedup, lemma_dedup_no_dup, lemma_empty_snoc, lemma_full_prefix,
    lemma_ids_empty, lemma_ids_prefix, lemma_ids_reverse, lemma_ids_singleton, lemma_ids_snoc,
    lemma_no_dup_snoc, lemma_prefix_contains, lemma_replaced_in_place, lemma_snoc_contains,
    lemma_sorted_agree, member_of, no_dup, order_by, same_elements, set_bound, sorted_stably,
    total_preorder, value_of,
};
#[cfg(creusot)]
use creusot_std::logic::Mapping;

#[cfg(creusot)]
use verification::*;

#[cfg(creusot)]
pub mod verification;

#[cfg_attr(not(creusot), derive(Default, Debug, Clone))]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
/// A [`Node`] in an [`IndependentWeave`] document.
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

#[cfg(not(creusot))]
#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, S> PartialEq for IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents + PartialEq,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.from.len() == other.from.len()
            && self.to.len() == other.to.len()
            && self.active == other.active
            && self.bookmarked == other.bookmarked
            && self.from.iter().zip(other.from.iter()).all(|(a, b)| a == b)
            && self.to.iter().zip(other.to.iter()).all(|(a, b)| a == b)
            && self.contents == other.contents
    }
}

#[cfg(not(creusot))]
#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, S> Eq for IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents + Eq,
    S: BuildHasher + Default + Clone,
{
}

impl<K, T, S> IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[check(terminates)]
    #[ensures(result == self@.valid())]
    fn validate(&self) -> bool {
        self.from.is_disjoint(&self.to)
            && !self.from.contains(&self.id)
            && !self.to.contains(&self.id)
    }
}

impl<K, T, S> Node<K, T> for IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type From = IndexSet<K, S>;
    type To = IndexSet<K, S>;

    #[inline]
    #[check(terminates)]
    #[ensures(result == self.id)]
    fn id(&self) -> K {
        self.id
    }
    #[inline]
    #[check(terminates)]
    #[ensures(*result == self.from)]
    fn from(&self) -> &Self::From {
        &self.from
    }
    #[inline]
    #[check(terminates)]
    #[ensures(*result == self.to)]
    fn to(&self) -> &Self::To {
        &self.to
    }
    #[inline]
    #[check(terminates)]
    #[ensures(result == self.active)]
    fn is_active(&self) -> bool {
        self.active
    }
    #[inline]
    #[check(terminates)]
    #[ensures(*result == self.contents)]
    fn contents(&self) -> &T {
        &self.contents
    }
}

impl<K, T, S> BuildableNode<K, T> for IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    #[check(terminates)]
    #[ensures(result.id == id
        && result.from == from
        && result.to == to
        && result.active == is_active
        && !result.bookmarked
        && result.contents == contents)]
    fn new(id: K, from: Self::From, to: Self::To, is_active: bool, contents: T) -> Self {
        Self {
            id,
            from,
            to,
            active: is_active,
            bookmarked: false,
            contents,
        }
    }
}

impl<K, T, S> From<DependentNode<K, T, S>> for IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    #[check(terminates)]
    #[ensures(converted_node(result, value, value.active))]
    #[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
    fn from(value: DependentNode<K, T, S>) -> Self {
        let from = match value.from {
            Some(parent) => {
                let mut from = IndexSet::with_capacity_and_hasher(1, default_hasher());
                from.insert(parent);
                proof_assert! {
                    lemma_empty_snoc(parent.deep_model());
                    from.model() == Seq::singleton(parent.deep_model())
                };
                from
            }
            None => IndexSet::with_hasher(default_hasher()),
        };
        proof_assert!(from.model() == parent_seq(value.from.deep_model()));

        Self {
            id: value.id,
            from,
            to: value.to,
            active: value.active,
            bookmarked: value.bookmarked,
            contents: value.contents,
        }
    }
}

impl<K, T, S> TryFrom<IndependentNode<K, T, S>> for DependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type Error = IndependentNode<K, T, S>;

    #[inline]
    #[check(terminates)]
    #[ensures(match result {
        Ok(node) => value@.from.len() < 2
            && converted_node(value, node, value.active)
            && node.active == value.active,
        Err(node) => 2 <= value@.from.len() && node == value,
    })]
    #[allow(clippy::manual_map, reason = "Closures can't be proven to end")]
    #[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
    fn try_from(value: IndependentNode<K, T, S>) -> Result<Self, Self::Error> {
        if value.from.len() < 2 {
            let from = match value.from.first() {
                Some(parent) => Some(*parent),
                None => None,
            };
            proof_assert!(value@.from.len() == 1 ==> value@.from.ext_eq(Seq::singleton(value@.from[0])));
            proof_assert!(value@.from.len() == 0 ==> value@.from.ext_eq(Seq::empty()));
            proof_assert!(value@.from == parent_seq(from.deep_model()));

            Ok(Self {
                id: value.id,
                from,
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
/// However, this additional flexibility results in worse performance and memory usage characteristics:
/// - Memory overhead is approximately doubled.
/// - Updating node activation has a worst-case time complexity of O(V + E) rather than [`DependentWeave`]'s O(1), so bulk operations must be done carefully to prevent accidentally quadratic behavior.
///
/// For best performance, it is recommended that you use random node identifiers and the [`Hasher`](core::hash::Hasher) implementation from the [nohash-hasher](https://crates.io/crates/nohash-hasher) crate. If your node identifiers end with random data (such as ULIDs in their raw representation), use the [hash_hasher](https://crates.io/crates/hash_hasher) crate instead.
#[cfg_attr(not(creusot), derive(Debug, Clone))]
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
    pub(super) nodes: HashMap<K, IndependentNode<K, T, S>, S>,
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "IndexSet<K, S>: SerdeSerialize",
            deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    pub(super) roots: IndexSet<K, S>,
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "HashSet<K, S>: SerdeSerialize",
            deserialize = "HashSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    pub(super) active: HashSet<K, S>,
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "IndexSet<K, S>: SerdeSerialize",
            deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    pub(super) bookmarked: IndexSet<K, S>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    pub(super) scratchpad: Scratchpad,

    /// The metadata associated with the weave.
    pub metadata: M,
}

impl<K, T, M, S> Default for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
    M: Default,
{
    #[inline]
    fn default() -> Self {
        Self::new(M::default())
    }
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
    K: Hash + Copy + Eq + Ord + Verifiable + SerdeDeserialize<'de>,
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

#[cfg(not(creusot))]
#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, M, S> PartialEq for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents + PartialEq,
    M: PartialEq,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.roots.len() == other.roots.len()
            && self.bookmarked.len() == other.bookmarked.len()
            && self.nodes.len() == other.nodes.len()
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

#[cfg(not(creusot))]
#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, M, S> Eq for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents + Eq,
    M: Eq,
    S: BuildHasher + Default + Clone,
{
}

impl<K, T, M, S> IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    /// Creates a new, empty [`IndependentWeave`].
    #[inline]
    #[check(terminates)]
    #[ensures(result@.nodes.len() == 0)]
    #[ensures(result.metadata == metadata)]
    pub fn new(metadata: M) -> Self {
        snapshot! { lemma_empty::<K, T, S> };

        Self {
            nodes: HashMap::with_hasher(default_hasher()),
            roots: IndexSet::with_hasher(default_hasher()),
            active: HashSet::with_hasher(default_hasher()),
            bookmarked: IndexSet::with_hasher(default_hasher()),
            scratchpad: Scratchpad::new(),
            metadata,
        }
    }
    /// Creates a new, empty [`IndependentWeave`] with at least the specified capacity.
    ///
    /// This function over-allocates for worst-case memory usage rather than average-case.
    #[inline]
    #[check(terminates)]
    #[ensures(result@.nodes.len() == 0)]
    #[ensures(result.metadata == metadata)]
    pub fn with_capacity(capacity: usize, metadata: M) -> Self {
        snapshot! { lemma_empty::<K, T, S> };

        let nodes = HashMap::with_capacity_and_hasher(capacity, default_hasher());
        let capacity = nodes.capacity();

        Self {
            nodes,
            roots: IndexSet::with_capacity_and_hasher(capacity, default_hasher()),
            active: HashSet::with_capacity_and_hasher(capacity, default_hasher()),
            bookmarked: IndexSet::with_capacity_and_hasher(capacity, default_hasher()),
            scratchpad: Scratchpad::new(),
            metadata,
        }
    }
    /// Returns the worst-case number of nodes that the weave can hold without reallocating.
    ///
    /// May be lower than `self.len()`.
    #[inline]
    #[check(terminates)]
    pub fn capacity(&self) -> usize {
        min_len(
            min_len(self.nodes.capacity(), self.roots.capacity()),
            self.bookmarked.capacity(),
        )
    }
    /// Reserves capacity for at least `additional` more nodes.
    #[inline]
    #[check(terminates)]
    #[ensures((^self)@ == self@)]
    #[ensures((^self).metadata == self.metadata)]
    pub fn reserve(&mut self, additional: usize) {
        self.nodes.reserve(additional);
        self.roots
            .reserve(self.nodes.capacity().saturating_sub(self.roots.len()));
        self.active
            .reserve(self.nodes.capacity().saturating_sub(self.active.len()));
        self.bookmarked
            .reserve(self.nodes.capacity().saturating_sub(self.bookmarked.len()));
    }
    /// Shrinks the capacity of the weave as much as possible.
    #[inline]
    #[check(terminates)]
    #[ensures((^self)@ == self@)]
    #[ensures((^self).metadata == self.metadata)]
    pub fn shrink_to_fit(&mut self) {
        self.nodes.shrink_to_fit();
        self.roots.shrink_to_fit();
        self.active.shrink_to_fit();
        self.bookmarked.shrink_to_fit();
        self.scratchpad = Scratchpad::new();
    }
    #[requires(structure(nodes.model(), roots.model(), active.model()))]
    #[requires(ready_to_activate(nodes.model(), active.model(), id.deep_model(), value))]
    #[ensures(result == nodes.model().contains(&id.deep_model()))]
    #[ensures(result ==> value == (^active).model().contains(&id.deep_model()))]
    #[ensures(!result ==> (^nodes).model() == nodes.model() && (^active).model() == active.model())]
    #[ensures(same_graph((^nodes).model(), nodes.model()))]
    #[ensures(structure((^nodes).model(), roots.model(), (^active).model()))]
    #[ensures(forms_path((^nodes).model(), (^active).model()))]
    #[ensures(result && nodes.model()[id.deep_model()]@.active == value
        ==> (^active).model() == active.model())]
    #[ensures(result && value && !nodes.model()[id.deep_model()]@.active
        ==> activated(nodes.model(), active.model(), id.deep_model(), (^active).model()))]
    #[ensures(result && !value && nodes.model()[id.deep_model()]@.active
        ==> longest_path_within(
            nodes.model(),
            active.model().remove(id.deep_model()),
            (^active).model(),
        ))]
    #[allow(clippy::too_many_lines, reason = "Mirrors the original control flow")]
    #[check(terminates)]
    fn update_node_activity_in_place(
        nodes: &mut HashMap<K, IndependentNode<K, T, S>, S>,
        roots: &IndexSet<K, S>,
        active: &mut HashSet<K, S>,
        scratchpad: &mut Scratchpad,
        id: &K,
        value: bool,
    ) -> bool {
        snapshot! { lemma_snoc_contains::<Id<K>> };
        proof! {
            let n0 = snapshot!(nodes.model());
            let a0 = snapshot!(active.model());
            let id0 = snapshot!(id.deep_model());
            let rank = snapshot!(rank_of(*n0));
        }

        let at_end = if let Some(node) = nodes.get(id) {
            if node.active == value {
                return true;
            }

            proof_assert!(linked(*n0, *id0));
            if value {
                (node.from.is_empty() && active.is_empty())
                    || extends_active_end(nodes, active, &node.from)
            } else {
                !any_active(active, &node.to)
            }
        } else {
            return false;
        };
        proof_assert!(n0[*id0]@.active == a0.contains(&*id0) && n0[*id0]@.active != value);
        proof_assert!(value ==> at_end == extends_path(*n0, *a0, *id0));
        proof_assert!(!value ==> at_end == !has_child_in(*n0, *a0, *id0));

        let node = nodes.get_mut(id).unwrap();
        node.active = value;
        proof! {
            let n1 = snapshot!(n0.add(*id0, *node));
        }
        proof_assert!(node.id.deep_model() == *id0);
        if value {
            active.insert(node.id);
        } else {
            active.remove(id);
        }
        proof! {
            let a1 = snapshot!(active.model());
        }
        proof_assert!(same_graph(*n1, *n0));
        proof_assert!(forall<k> n1.contains(&k) ==> n1[k]@.active == a1.contains(&k));
        proof_assert! {
            lemma_same_graph_structure(*n1, *n0, roots.model(), *a0, *a1);
            structure(*n1, roots.model(), *a1)
        };
        proof_assert!(ranked(*n1, *rank));

        proof_assert!(value ==> *a1 == a0.add(*id0));
        proof_assert!(!value ==> *a1 == a0.remove(*id0));

        if at_end {
            proof_assert!(forms_path(*n0, *a1));
            proof_assert! {
                lemma_same_graph_forms_path(*n1, *n0, *a1);
                forms_path(*n1, *a1)
            };
            proof_assert!(value ==> activated(*n0, *a0, *id0, *a1));
            proof_assert!(!value ==> longest_whole_ok(*n0, *a1));
            return true;
        }

        if value {
            let has_descendants = !node.to.is_empty();
            proof_assert!(nodes.model() == *n1);

            let guard = scratchpad.guard();

            let mut stack = guard.vec();
            let mut closure = guard.set(default_hasher());
            let mut closure_roots = guard.vec();
            proof_assert! { lemma_ids_empty(closure_roots.model()); ids(closure_roots.model()) == Seq::empty() };

            ancestor_subgraph(nodes, *id, &mut stack, &mut closure, &mut closure_roots);

            let mut topological = guard.vec_with_capacity(closure.len());
            let mut scratchpad_map = guard.map_with_capacity(closure.len(), default_hasher());
            let mut scratchpad_map_2: ScratchpadMap<'_, K, ((usize, usize), Option<K>), S> =
                guard.map_with_capacity(closure.len(), default_hasher());
            let mut scratchpad_set = guard.set(default_hasher());
            let admitted = snapshot!(member_of(closure.model()));
            proof_assert! {
                lemma_ids_empty(topological.model());
                lemma_kahn_empty(*n1, *admitted);
                kahn_state(*n1, *admitted, ids(topological.model()), scratchpad_map.model())
            };

            let mut i = 0;
            #[invariant(i@ <= closure_roots.model().len())]
            #[invariant(nodes.model() == *n1)]
            #[invariant(stack.model().len() == 0)]
            #[invariant(kahn_state(*n1, *admitted, ids(topological.model()), scratchpad_map.model()))]
            #[invariant(forall<k> ids(topological.model()).contains(k) && n1[k]@.from.len() == 0
                ==> exists<j> 0 <= j && j < i@ && ids(closure_roots.model())[j] == k)]
            #[invariant(forall<j> 0 <= j && j < i@
                ==> ids(topological.model()).contains(ids(closure_roots.model())[j]))]
            #[variant(closure_roots.model().len() - i@)]
            while i < closure_roots.len() {
                proof_assert!(ids(closure_roots.model())[i@] == closure_roots.model()[i@].deep_model());
                proof_assert!(n1[ids(closure_roots.model())[i@]]@.from.len() == 0);
                proof_assert! {
                    lemma_root_unsorted(*n1, ids(closure_roots.model()), i@, ids(topological.model()));
                    !ids(topological.model()).contains(ids(closure_roots.model())[i@])
                };
                proof! {
                    let before = snapshot!(ids(topological.model()));
                    let before_len = snapshot!(topological.model().len());
                }
                proof_assert!(before.len() == *before_len);
                topological_sort_subgraph(
                    nodes,
                    &closure,
                    None,
                    admitted,
                    closure_roots[i],
                    &mut stack,
                    &mut topological,
                    &mut scratchpad_map,
                );
                proof_assert!(forall<m> 0 <= m && m < before.len() ==> ids(topological.model())[m] == before[m]);
                proof_assert!(ids(topological.model())[before.len()] == ids(closure_roots.model())[i@]);
                proof_assert!(ids(topological.model()).len() == topological.model().len());
                proof_assert! {
                    lemma_admitted_have_parents(nodes.model(), *admitted, topological.model(), *before_len);
                    forall<m> *before_len < m && m < topological.model().len()
                        ==> nodes.model()[topological.model()[m].deep_model()]@.from.len() > 0
                };
                proof_assert!(forall<m> before.len() < m && m < ids(topological.model()).len()
                    ==> ids(topological.model())[m] == topological.model()[m].deep_model());
                proof_assert!(forall<m> before.len() < m && m < ids(topological.model()).len()
                    ==> n1[ids(topological.model())[m]]@.from.len() > 0);
                proof_assert!(forall<k> ids(topological.model()).contains(k) && n1[k]@.from.len() == 0
                    ==> before.contains(k) || k == ids(closure_roots.model())[i@]);
                proof_assert!(forall<k> before.contains(k) ==> ids(topological.model()).contains(k));
                proof_assert!(ids(topological.model()).contains(ids(closure_roots.model())[i@]));
                #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                {
                    i += 1;
                }
            }
            proof! {
                let topo = snapshot!(ids(topological.model()));
            }
            proof_assert! {
                lemma_ancestors_sorted(*n1, closure.model(), ids(closure_roots.model()), *admitted, *topo, scratchpad_map.model());
                forall<k> closure.model().contains(&k) ==> topo.contains(k)
            };

            proof_assert!(kahn_order(*n1, *admitted, *topo, Seq::empty()) && no_dup(*topo));

            let mut i = 0;
            #[invariant(i@ <= topological.model().len())]
            #[invariant(nodes.model() == *n1)]
            #[invariant(forall<j> 0 <= j && j < i@
                ==> scratchpad_map_2.model().contains(&topo[j]))]
            #[invariant(forall<k> scratchpad_map_2.model().contains(&k)
                ==> parent_scored(*n1, scratchpad_map_2.model(), k, i@))]
            #[invariant(forall<k> scratchpad_map_2.model().contains(&k)
                ==> exists<j> 0 <= j && j < i@ && topo[j] == k)]
            #[invariant(forall<k> scratchpad_map_2.model().contains(&k)
                ==> up_scored(*n1, *a1, scratchpad_map_2.model(), k))]
            #[variant(topological.model().len() - i@)]
            while i < topological.len() {
                let id = topological[i];
                proof_assert!(id.deep_model() == topo[i@] && admitted[topo[i@]]);
                let node = &nodes[&id];
                proof_assert!(forall<p> node@.from.contains(p) ==> {
                    lemma_parent_before(*n1, *admitted, *topo, i@, p);
                    true
                } && scratchpad_map_2.model().contains(&p));

                let best_parent = best_scored(&node.from, &scratchpad_map_2);

                let (parent, score) = if let Some((parent, mut score)) = best_parent {
                    proof_assert!(parent_scored(*n1, scratchpad_map_2.model(), parent.deep_model(), i@));
                    proof! {
                        let best = snapshot!(score);
                    }
                    proof_assert!(*best == scratchpad_map_2.model()[parent.deep_model()].0
                        && forall<c> node@.from.contains(c)
                            ==> score_le(scratchpad_map_2.model()[c].0, *best));
                    #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                    if node.active {
                        score.1 += 1;
                    } else {
                        score.0 += 1;
                    }
                    proof_assert!(score.0@ == best.0@ + if node.active { 0 } else { 1 });
                    proof_assert!(score.1@ == best.1@ + if node.active { 1 } else { 0 });

                    (Some(parent), score)
                } else {
                    proof_assert!(node@.from.len() == 0);
                    (None, if node.active { (0, 1) } else { (1, 0) })
                };
                proof_assert!(node.active == a1.contains(&id.deep_model()));
                proof_assert!(score.0@ + score.1@ <= i@ + 1);
                proof_assert!(match parent {
                    None => n1[id.deep_model()]@.from.len() == 0,
                    Some(p) => n1[id.deep_model()]@.from.contains(p.deep_model())
                        && scratchpad_map_2.model().contains(&p.deep_model()),
                });
                proof_assert!(parent_insert_ok(*n1, scratchpad_map_2.model(), id.deep_model(), (score, parent), i@));
                proof_assert!(!scratchpad_map_2.model().contains(&topo[i@]));
                proof_assert!(n1[topo[i@]]@.active == a1.contains(&topo[i@]));
                proof_assert!(forall<j> scratchpad_map_2.model().contains(&j)
                    ==> parent_linked(*n1, scratchpad_map_2.model(), j)
                        && up_scored(*n1, *a1, scratchpad_map_2.model(), j));
                proof_assert!(forall<p> n1[id.deep_model()]@.from.contains(p)
                    ==> scratchpad_map_2.model().contains(&p));
                proof_assert!(up_entry(*n1, *a1, scratchpad_map_2.model(), id.deep_model(), (score, parent)));
                proof_assert!(up_insert_ok(*n1, *a1, scratchpad_map_2.model(), id.deep_model(), (score, parent)));

                scratchpad_map_2.insert(id, (score, parent));
                #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                {
                    i += 1;
                }
            }
            proof_assert!(forall<k> closure.model().contains(&k) ==> scratchpad_map_2.model().contains(&k));
            proof! {
                let scores = snapshot!(scratchpad_map_2.model());
                let mut chain = snapshot!(Seq::<Id<K>>::empty());
            }
            proof_assert!(forall<k> scores.contains(&k) ==> parent_linked(*n1, *scores, k));
            proof_assert!(forall<k> scores.contains(&k) ==> up_scored(*n1, *a1, *scores, k));

            let mut current = Some(id);

            #[invariant(ancestor_walk(*n1, *rank, *scores, *chain, current.deep_model(), *id0))]
            #[invariant(forall<k> scratchpad_set.model().contains(&k) == chain.contains(k))]
            #[invariant(up_walk(*a1, *scores, *chain, current.deep_model(), *id0))]
            #[variant(match current.deep_model() { Some(x) => rank[x] + 1, None => 0 })]
            while let Some(id) = current {
                proof_assert!(current.deep_model() == Some(id.deep_model()));
                proof_assert!(ancestor_step_ok(
                    *n1,
                    *rank,
                    *scores,
                    *chain,
                    id.deep_model(),
                    *id0
                ));
                proof_assert!(up_walk_step_ok(*n1, *a1, *scores, *chain, id.deep_model(), *id0));
                proof_assert!(up_rank_ok(*n1, *rank, *scores, id.deep_model()));
                scratchpad_set.insert(*id);
                current = match scratchpad_map_2.get(id) {
                    Some((_, parent)) => parent.as_ref(),
                    None => None,
                };
                proof_assert!(current.deep_model() == scores[id.deep_model()].1.deep_model());
                proof! {
                    chain = snapshot!(chain.snoc(id.deep_model()));
                }
            }
            proof_assert! {
                lemma_ancestor_done(*n1, *rank, *scores, *chain, *id0, scratchpad_set.model());
                path_of(*n1, scratchpad_set.model(), *chain) && chain[0] == *id0
            };
            proof_assert!(path_up(*n1, *id0, *chain));
            proof_assert!(up_walk_done_ok(*n1, *a1, *scores, *chain, *id0));
            proof! {
                let mut witness = snapshot!(*chain);
                let mut down = snapshot!(Seq::singleton(*id0));
            }

            closure.clear();
            topological.clear();
            scratchpad_map.clear();
            scratchpad_map_2.clear();

            proof_assert!(!a0.contains(&*id0) && a1.contains(&*id0));
            proof_assert!(has_descendants == (n1[*id0]@.to.len() != 0));
            proof_assert!(active.model().len() == 1 ==> a0.len() == 0 && forall<k> !a0.contains(&k));
            proof_assert!(!(active.model().len() != 1 && has_descendants)
                ==> nothing_below_ok(*n1, *a0, *id0));

            if active.len() != 1 && has_descendants {
                descendant_subgraph(nodes, *id, &mut stack, &mut closure);

                let has_active_descendant = contains_other(&closure, active, id);
                proof_assert!(!has_active_descendant
                    ==> nothing_below_closed_ok(*n1, *a0, *id0, closure.model()));

                if has_active_descendant {
                    let admitted = snapshot!(member_of(closure.model()));
                    proof_assert! {
                        lemma_ids_empty(topological.model());
                        lemma_kahn_empty(*n1, *admitted);
                        ids(topological.model()) == Seq::empty()
                            && kahn_state(*n1, *admitted, ids(topological.model()), scratchpad_map.model())
                    };
                    proof_assert!(descendants_start_ok(*n1, closure.model(), *admitted, *id0));
                    proof_assert!(forall<p> !admitted_parent(*n1, *admitted, *id0, p));

                    topological_sort_subgraph(
                        nodes,
                        &closure,
                        None,
                        admitted,
                        *id,
                        &mut stack,
                        &mut topological,
                        &mut scratchpad_map,
                    );
                    proof! {
                        let topo = snapshot!(ids(topological.model()));
                    }
                    proof_assert!(topo[0] == *id0);
                    proof_assert!(all_linked(*n1) && acyclic(*n1));
                    proof_assert!(forall<k> closure.model().contains(&k) ==> n1.contains(&k));
                    proof_assert!(forall<k> closure.model().contains(&k) && k != *id0
                        ==> has_parent_in(*n1, closure.model(), k));
                    proof_assert!(forall<k> admitted[k] == closure.model().contains(&k));
                    proof_assert!(kahn_state(*n1, *admitted, *topo, scratchpad_map.model()));
                    proof_assert!(0 < topo.len());
                    proof_assert!(topo.contains(*id0));
                    proof_assert!(forall<k> closure.model().contains(&k) && k != *id0 ==> {
                        let p = such_that(|p| n1[k]@.from.contains(p) && closure.model().contains(&p));
                        admitted_parent(*n1, *admitted, k, p)
                    });
                    proof_assert!(forall<k> closure.model().contains(&k) && k != *id0
                        ==> exists<p> admitted_parent(*n1, *admitted, k, p));
                    proof_assert!(forall<k> n1.contains(&k) && admitted[k] && !topo.contains(k)
                        ==> exists<p> admitted_parent(*n1, *admitted, k, p));
                    proof_assert! {
                        lemma_kahn_complete(*n1, *admitted, *topo, scratchpad_map.model());
                        forall<k> closure.model().contains(&k) ==> topo.contains(k)
                    };
                    proof_assert!(kahn_order(*n1, *admitted, *topo, Seq::empty()));

                    let mut i = topological.len();
                    #[invariant(nodes.model() == *n1)]
                    #[invariant(ids(topological.model()) == *topo)]
                    #[invariant(i@ <= topo.len())]
                    #[invariant(forall<j> i@ <= j && j < topo.len()
                        ==> scratchpad_map_2.model().contains(&topo[j]))]
                    #[invariant(forall<k> scratchpad_map_2.model().contains(&k)
                        ==> child_scored(*n1, scratchpad_map_2.model(), k, topo.len() - i@))]
                    #[invariant(forall<k> scratchpad_map_2.model().contains(&k)
                        ==> exists<j> i@ <= j && j < topo.len() && topo[j] == k)]
                    #[invariant(forall<k> scratchpad_map_2.model().contains(&k)
                        ==> down_scored(*n1, *a1, scratchpad_map_2.model(), k))]
                    #[variant(i@)]
                    while i > 0 {
                        #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                        {
                            i -= 1;
                        }
                        let id = topological[i];
                        proof_assert!(id.deep_model() == topo[i@] && admitted[topo[i@]]);
                        let node = &nodes[&id];
                        proof_assert!(node@ == n1[topo[i@]]@ && closure.model().contains(&topo[i@]));
                        proof_assert!(forall<c> node@.to.contains(c)
                            ==> closure.model().contains(&c) && topo.contains(c));
                        proof_assert!(forall<c> node@.to.contains(c) ==> {
                            lemma_child_after(*n1, *admitted, *topo, i@, c);
                            true
                        } && scratchpad_map_2.model().contains(&c));

                        let best_child = best_scored(&node.to, &scratchpad_map_2);

                        let (child, score) = if let Some((child, mut score)) = best_child {
                            proof_assert!(child_scored(*n1, scratchpad_map_2.model(), child.deep_model(), topo.len() - i@ - 1));
                            proof! {
                                let best = snapshot!(score);
                            }
                            proof_assert!(*best == scratchpad_map_2.model()[child.deep_model()].0
                                && forall<d> node@.to.contains(d)
                                    ==> score_le(scratchpad_map_2.model()[d].0, *best));
                            #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                            if node.active {
                                score.1 += 1;
                            } else {
                                score.0 += 1;
                            }
                            proof_assert!(score.0@ == best.0@ + if node.active { 0 } else { 1 });
                            proof_assert!(score.1@ == best.1@ + if node.active { 1 } else { 0 });

                            (Some(child), score)
                        } else {
                            proof_assert!(node@.to.len() == 0);
                            (None, if node.active { (0, 1) } else { (1, 0) })
                        };
                        proof_assert!(node.active == a1.contains(&id.deep_model()));
                        proof_assert!(score.0@ + score.1@ <= topo.len() - i@);
                        proof_assert!(match child {
                            None => true,
                            Some(c) => n1[id.deep_model()]@.to.contains(c.deep_model())
                                && scratchpad_map_2.model().contains(&c.deep_model()),
                        });
                        proof_assert!(child_insert_ok(*n1, scratchpad_map_2.model(), id.deep_model(), (score, child), topo.len() - i@ - 1));
                        proof_assert!(no_dup(*topo) && !scratchpad_map_2.model().contains(&topo[i@]));
                        proof_assert!(n1[topo[i@]]@.active == a1.contains(&topo[i@]));
                        proof_assert!(forall<j> scratchpad_map_2.model().contains(&j)
                            ==> child_linked(*n1, scratchpad_map_2.model(), j)
                                && down_scored(*n1, *a1, scratchpad_map_2.model(), j));
                        proof_assert!(forall<c> n1[id.deep_model()]@.to.contains(c)
                            ==> scratchpad_map_2.model().contains(&c));
                        proof_assert!(down_entry(*n1, *a1, scratchpad_map_2.model(), id.deep_model(), (score, child)));
                        proof_assert!(down_insert_ok(*n1, *rank, *a1, scratchpad_map_2.model(), id.deep_model(), (score, child)));

                        scratchpad_map_2.insert(id, (score, child));
                    }
                    topological.clear();
                    proof! {
                        let scores = snapshot!(scratchpad_map_2.model());
                    }
                    proof_assert!(scores.contains(&*id0));
                    proof_assert!(forall<k> scores.contains(&k) ==> child_linked(*n1, *scores, k));
                    proof_assert!(forall<k> scores.contains(&k) ==> down_scored(*n1, *a1, *scores, k));
                    proof_assert!(down_walk_start_ok(*n1, *a1, *scores, *id0));
                    proof! {
                        down = snapshot!(Seq::empty());
                    }
                    proof_assert! {
                        lemma_path_ranks_all(*n1, *rank, *witness);
                        descendant_walk(*n1, *rank, *scores, scratchpad_set.model(), *witness, Some(*id0))
                    };

                    let mut current = Some(id);

                    #[invariant(nodes.model() == *n1)]
                    #[invariant(descendant_walk(*n1, *rank, *scores, scratchpad_set.model(), *witness, current.deep_model()))]
                    #[invariant(scratchpad_set.model().contains(&*id0))]
                    #[invariant(down_walk(*n1, *a1, *scores, *down, current.deep_model(), *id0))]
                    #[invariant(forall<k> scratchpad_set.model().contains(&k)
                        == (chain.contains(k) || down.contains(k)))]
                    #[variant(match current.deep_model() { Some(x) => set_bound(n1.keys(), *rank) - rank[x], None => 0 })]
                    while let Some(id) = current {
                        proof_assert!(current.deep_model() == Some(id.deep_model()));
                        proof_assert!(descendant_step_ok(
                            *n1,
                            *rank,
                            *scores,
                            scratchpad_set.model(),
                            *witness,
                            id.deep_model()
                        ));
                        proof_assert!(down_walk_step_ok(*n1, *a1, *scores, *down, id.deep_model(), *id0));
                        proof_assert!(down_rank_ok(*n1, *rank, *scores, id.deep_model()));
                        proof_assert!(n1[id.deep_model()]@.active == a1.contains(&id.deep_model()));
                        proof! {
                            witness = snapshot!(walk_prepend(*witness, id.deep_model()));
                            down = snapshot!(down.snoc(id.deep_model()));
                        }
                        scratchpad_set.insert(*id);

                        let (score, successor) = &scratchpad_map_2[id];
                        current = if score.1 > usize::from(nodes[id].active) {
                            successor.as_ref()
                        } else {
                            None
                        };
                        proof_assert!(current.deep_model() == None
                            || current.deep_model() == scores[id.deep_model()].1.deep_model());
                    }
                    proof_assert!(0 < down.len() && scores.contains(&down[down.len() - 1]));
                    proof_assert!(down_walk_done_ok(*n1, *rank, *a0, *a1, *scores, *down, *id0));
                }
            }
            proof_assert!(path_of(*n1, scratchpad_set.model(), *witness));
            proof_assert!(scratchpad_set.model().contains(&*id0));
            proof_assert!(topological.model().len() == 0);
            proof_assert!(below_found(*n1, *a0, *id0, *down));
            proof_assert!(chain.contains(*id0));
            proof_assert!(forall<k> scratchpad_set.model().contains(&k)
                == (chain.contains(k) || down.contains(k)));
            proof_assert!(rerouted_ok(*n1, *n0, *a0, *id0, scratchpad_set.model(), *chain, *down));

            activate_exactly(nodes, active, &scratchpad_set, &mut topological);
            proof_assert!(!extends_path(*n0, *a0, *id0) && activated(*n0, *a0, *id0, active.model()));
            proof_assert!(same_graph(nodes.model(), *n1));
            proof_assert! {
                lemma_same_graph_trans(nodes.model(), *n1, *n0);
                same_graph(nodes.model(), *n0)
            };
            proof_assert!(forall<k> active.model().contains(&k) ==> nodes.model().contains(&k));
            proof_assert!(forall<k> nodes.model().contains(&k)
                ==> nodes.model()[k]@.active == active.model().contains(&k));
            proof_assert! {
                lemma_same_graph_structure(nodes.model(), *n1, roots.model(), *a1, active.model());
                structure(nodes.model(), roots.model(), active.model())
            };
            proof_assert! {
                lemma_same_graph_path_of(nodes.model(), *n1, active.model(), *witness);
                path_of(nodes.model(), active.model(), *witness)
            };
        } else {
            proof_assert!(nodes.model() == *n1);
            Self::fix_orphaned_activations(nodes, roots, active, scratchpad);
            proof_assert! {
                lemma_same_graph_trans(nodes.model(), *n1, *n0);
                same_graph(nodes.model(), *n0)
            };
            proof_assert!(same_graph_activation_ok(*n1, *n0));
            proof_assert!(longest_path_within(*n0, a0.remove(*id0), active.model()));
        }

        true
    }
    #[requires(structure(nodes.model(), roots.model(), active.model()))]
    #[ensures(same_graph((^nodes).model(), nodes.model()))]
    #[ensures(forall<k> (^active).model().contains(&k) ==> active.model().contains(&k))]
    #[ensures(structure((^nodes).model(), roots.model(), (^active).model()))]
    #[ensures(forms_path((^nodes).model(), (^active).model()))]
    #[ensures(longest_path_within(nodes.model(), active.model(), (^active).model()))]
    #[check(terminates)]
    fn fix_orphaned_activations(
        nodes: &mut HashMap<K, IndependentNode<K, T, S>, S>,
        roots: &IndexSet<K, S>,
        active: &mut HashSet<K, S>,
        scratchpad: &mut Scratchpad,
    ) {
        proof! {
            let n0 = snapshot!(nodes.model());
            let a0 = snapshot!(active.model());
        }

        if active.is_empty() {
            proof_assert!(forall<k> !a0.contains(&k));
            proof_assert!(path_of(*n0, *a0, Seq::empty()));
            proof_assert!(forall<q: Seq<Id<K>>> (forall<i> 0 <= i && i < q.len() ==> a0.contains(&q[i]))
                ==> q.len() == 0);
            return;
        }

        let guard = scratchpad.guard();

        let mut stack = guard.vec();
        let mut closure = guard.set_with_capacity(active.len(), default_hasher());
        let mut closure_roots = guard.vec();
        let mut active_ids = guard.vec_with_capacity(active.len());

        extend_from_hash_set(&mut active_ids, active);
        proof_assert! { lemma_ids_empty(closure_roots.model()); ids(closure_roots.model()) == Seq::empty() };

        let mut i = 0;
        #[invariant(i@ <= active_ids.model().len())]
        #[invariant(nodes.model() == *n0)]
        #[invariant(stack.model().len() == 0)]
        #[invariant(forall<k> closure.model().contains(&k) ==> n0.contains(&k))]
        #[invariant(parent_closed(*n0, closure.model()))]
        #[invariant(roots_of(*n0, closure.model(), ids(closure_roots.model())))]
        #[invariant(forall<j> 0 <= j && j < i@
            ==> closure.model().contains(&ids(active_ids.model())[j]))]
        #[variant(active_ids.model().len() - i@)]
        while i < active_ids.len() {
            proof_assert!(ids(active_ids.model())[i@] == active_ids.model()[i@].deep_model());
            proof_assert!(ids(active_ids.model()).contains(active_ids.model()[i@].deep_model()));
            ancestor_subgraph(
                nodes,
                active_ids[i],
                &mut stack,
                &mut closure,
                &mut closure_roots,
            );
            #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
            {
                i += 1;
            }
        }

        proof_assert!(forall<k> a0.contains(&k) ==> closure.model().contains(&k));
        let mut topological = guard.vec_with_capacity(closure.len());
        let mut scratchpad_map = guard.map_with_capacity(closure.len(), default_hasher());
        let admitted = snapshot!(|k| closure.model().contains(&k));
        proof_assert! {
            lemma_ids_empty(topological.model());
            lemma_kahn_empty(*n0, *admitted);
            kahn_state(*n0, *admitted, ids(topological.model()), scratchpad_map.model())
        };

        let mut i = 0;
        #[invariant(i@ <= roots.model().len())]
        #[invariant(nodes.model() == *n0)]
        #[invariant(stack.model().len() == 0)]
        #[invariant(kahn_state(*n0, *admitted, ids(topological.model()), scratchpad_map.model()))]
        #[invariant(forall<k> ids(topological.model()).contains(k) && n0[k]@.from.len() == 0
            ==> exists<j> 0 <= j && j < i@ && roots.model()[j] == k)]
        #[invariant(forall<j> 0 <= j && j < i@ && closure.model().contains(&roots.model()[j])
            ==> ids(topological.model()).contains(roots.model()[j]))]
        #[variant(roots.model().len() - i@)]
        while i < roots.len() {
            let root = roots[i];
            if closure.contains(&root) {
                proof_assert!(roots.model().contains(root.deep_model()));
                proof_assert!(n0.contains(&root.deep_model()) && n0[root.deep_model()]@.from.len() == 0);
                proof_assert!(!ids(topological.model()).contains(root.deep_model()));
                proof! {
                    let before = snapshot!(ids(topological.model()));
                }
                topological_sort_subgraph(
                    nodes,
                    &closure,
                    None,
                    admitted,
                    root,
                    &mut stack,
                    &mut topological,
                    &mut scratchpad_map,
                );
                proof_assert!(forall<m> 0 <= m && m < before.len() ==> ids(topological.model())[m] == before[m]);
                proof_assert!(ids(topological.model())[before.len()] == roots.model()[i@]);
                proof_assert!(forall<m> before.len() < m && m < ids(topological.model()).len()
                    ==> n0[ids(topological.model())[m]]@.from.len() > 0);
                proof_assert!(forall<k> ids(topological.model()).contains(k) && n0[k]@.from.len() == 0
                    ==> before.contains(k) || k == roots.model()[i@]);
                proof_assert!(forall<k> before.contains(k) ==> ids(topological.model()).contains(k));
            }
            #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
            {
                i += 1;
            }
        }
        proof! {
            let topo = snapshot!(ids(topological.model()));
        }
        proof_assert!(forall<j> 0 <= j && j < ids(closure_roots.model()).len()
            ==> roots.model().contains(ids(closure_roots.model())[j]) && topo.contains(ids(closure_roots.model())[j]));
        proof_assert! {
            lemma_ancestors_sorted(*n0, closure.model(), ids(closure_roots.model()), *admitted, *topo, scratchpad_map.model());
            forall<k> a0.contains(&k) ==> topo.contains(k)
        };

        let mut candidate_path = guard.vec_with_capacity(active.len());
        let candidate = snapshot!(member_of(active.model()));
        proof_assert! {
            lemma_kahn_candidates_ordered(*n0, *admitted, *topo, *candidate);
            no_dup(*topo) && candidates_ordered(*n0, *topo, *candidate)
        };

        longest_candidate_path_to_root(
            nodes,
            &topological,
            active,
            None,
            candidate,
            &mut guard.map_with_capacity(active.len(), default_hasher()),
            &mut candidate_path,
        );
        proof! {
            let path = snapshot!(ids(candidate_path.model()));
        }
        proof_assert!(valid_path(*n0, *path) && no_dup(*path));
        proof_assert!(forall<k> path.contains(k) ==> a0.contains(&k));
        proof_assert!(forall<q> valid_path(*n0, q) && no_dup(q)
            && (forall<i> 0 <= i && i < q.len() ==> a0.contains(&q[i]))
            ==> candidate_root_path(*n0, *topo, *candidate, q) && q.len() <= path.len());

        topological.clear();
        let mut disjoint = topological;

        closure.clear();
        let mut candidate_path_set = closure;

        let mut i = 0;
        #[invariant(i@ <= candidate_path.model().len())]
        #[invariant(forall<k> candidate_path_set.model().contains(&k)
            == exists<j> 0 <= j && j < i@ && path[j] == k)]
        #[variant(candidate_path.model().len() - i@)]
        while i < candidate_path.len() {
            candidate_path_set.insert(candidate_path[i]);
            #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
            {
                i += 1;
            }
        }
        proof_assert!(forall<k> candidate_path_set.model().contains(&k) == path.contains(k));
        candidate_path.clear();

        extend_difference(&mut disjoint, active, &candidate_path_set);
        proof! {
            let orphans = snapshot!(ids(disjoint.model()));
        }

        let mut i = 0;
        #[invariant(i@ <= disjoint.model().len())]
        #[invariant(forall<k> active.model().contains(&k)
            == (a0.contains(&k) && !exists<j> 0 <= j && j < i@ && orphans[j] == k))]
        #[invariant(same_graph(nodes.model(), *n0))]
        #[invariant(forall<k> n0.contains(&k) ==> nodes.model()[k]@.active
            == (n0[k]@.active && !exists<j> 0 <= j && j < i@ && orphans[j] == k))]
        #[variant(disjoint.model().len() - i@)]
        while i < disjoint.len() {
            let orphan = disjoint[i];
            proof_assert!(orphan.deep_model() == orphans[i@]);
            active.remove(&orphan);
            if let Some(node) = nodes.get_mut(&orphan) {
                node.active = false;
            }
            #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
            {
                i += 1;
            }
        }
        disjoint.clear();

        proof_assert!(forall<k> active.model().contains(&k) == path.contains(k));
        proof_assert!(path_of(*n0, active.model(), *path));
        proof_assert!(longest_path_within(*n0, *a0, active.model()));
        proof_assert!(forall<k> n0.contains(&k) ==> nodes.model()[k]@.active == active.model().contains(&k));
        proof_assert! {
            lemma_same_graph_structure(nodes.model(), *n0, roots.model(), *a0, active.model());
            structure(nodes.model(), roots.model(), active.model())
        };
        proof_assert! {
            lemma_same_graph_path_of(nodes.model(), *n0, active.model(), *path);
            path_of(nodes.model(), active.model(), *path)
        };
    }
    #[allow(clippy::missing_panics_doc, reason = "Should never panic")]
    /// Sets the active status of a node with the specified identifier, using identical activation behavior to [`DependentWeave`].
    #[ensures(result ==> value || !(^self)@.active.contains(&id.deep_model())
        || ((^self)@.active == self@.active
            && has_child_in((^self)@.nodes, (^self)@.active, id.deep_model())))]
    #[ensures(result ==> !value || (^self)@.active.contains(&id.deep_model())
        && !has_child_in((^self)@.nodes, (^self)@.active, id.deep_model()))]
    #[ensures(!result ==> (^self)@.active == self@.active)]
    #[ensures(result == self@.nodes.contains(&id.deep_model()))]
    #[ensures((^self)@.same_keys(self@))]
    #[ensures(same_elements((^self)@.roots, self@.roots))]
    #[ensures(same_elements((^self)@.bookmarked, self@.bookmarked))]
    // Beyond the contract: which nodes become active, and that only which nodes are active changes.
    #[ensures(result && value
        ==> activated_at_end(self@.nodes, self@.active, id.deep_model(), (^self)@.active))]
    #[ensures(result && !value ==> (^self)@.active
        == if self@.active.contains(&id.deep_model())
            && !has_child_in(self@.nodes, self@.active, id.deep_model())
        {
            self@.active.remove(id.deep_model())
        } else {
            self@.active
        })]
    #[ensures(same_graph((^self)@.nodes, self@.nodes))]
    #[ensures((^self)@.roots == self@.roots)]
    #[ensures((^self)@.bookmarked == self@.bookmarked)]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    #[allow(clippy::too_many_lines, reason = "Most of its length is proof code")]
    #[check(terminates)]
    pub fn set_active_dependent_semantics(&mut self, id: &K, value: bool) -> bool {
        snapshot! { lemma_reactivated((*self)@) };
        proof! {
            let old = snapshot!((*self)@);
            let id0 = snapshot!(id.deep_model());
            let rank = snapshot!(rank_of(old.nodes));
        }
        proof_assert! {
            lemma_ready((*self)@, *id0, value);
            ready_to_activate(old.nodes, old.active, *id0, value)
        };
        proof_assert!(ranked(old.nodes, *rank));
        proof_assert!(self.nodes.model() == old.nodes && self.active.model() == old.active);
        proof_assert!(forall<k> old.nodes.contains(&k) ==> old.flagged(k) && linked(old.nodes, k));

        if value {
            if let Some(node) = self.nodes.get(id) {
                if node.active && !any_active(&self.active, &node.to) {
                    return true;
                }

                if !node.active
                    && ((node.from.is_empty() && self.active.is_empty())
                        || extends_active_end(&self.nodes, &self.active, &node.from))
                {
                    proof_assert!(extends_path(old.nodes, old.active, *id0));
                    proof_assert! {
                        lemma_extends_childless(old.nodes, old.active, *id0);
                        !has_child_in(old.nodes, old.active.add(*id0), *id0)
                    };
                    self.nodes.get_mut(id).unwrap().active = true;
                    proof_assert!(forall<k> k != *id0 ==> self.nodes.model().lookup(k) == old.nodes.lookup(k));
                    proof_assert!(self.nodes.model().contains(&*id0));
                    proof_assert!(self.nodes.model()[*id0]@.id == old.nodes[*id0]@.id
                        && self.nodes.model()[*id0]@.from == old.nodes[*id0]@.from
                        && self.nodes.model()[*id0]@.to == old.nodes[*id0]@.to
                        && self.nodes.model()[*id0]@.bookmarked == old.nodes[*id0]@.bookmarked
                        && self.nodes.model()[*id0].contents == old.nodes[*id0].contents
                        && self.nodes.model()[*id0]@.active);
                    proof! {
                        let view = snapshot!(IndependentWeaveView {
                            nodes: self.nodes.model(),
                            roots: self.roots.model(),
                            active: old.active.add(*id0),
                            bookmarked: self.bookmarked.model(),
                        });
                    }
                    proof_assert!(same_graph(view.nodes, old.nodes));
                    proof_assert!(forall<k> view.nodes.contains(&k)
                        ==> view.nodes[k]@.active == view.active.contains(&k));
                    proof_assert! {
                        lemma_same_graph_structure(view.nodes, old.nodes, old.roots, old.active, view.active);
                        structure(view.nodes, view.roots, view.active)
                    };
                    proof_assert! {
                        lemma_same_graph_forms_path(view.nodes, old.nodes, view.active);
                        forms_path(view.nodes, view.active)
                    };
                    proof_assert!(reactivated(*old, *view));
                    self.active.insert(*id);
                    return true;
                }
            } else {
                return false;
            }

            let Self {
                nodes,
                active,
                scratchpad,
                ..
            } = self;

            proof_assert!(nodes.model() == old.nodes && active.model() == old.active);

            let guard = scratchpad.guard();

            let mut stack = guard.vec();
            let mut closure = guard.set(default_hasher());
            let mut closure_roots = guard.vec();
            proof_assert! { lemma_ids_empty(closure_roots.model()); ids(closure_roots.model()) == Seq::empty() };

            ancestor_subgraph(nodes, *id, &mut stack, &mut closure, &mut closure_roots);

            let mut topological = guard.vec_with_capacity(closure.len());
            let mut scratchpad_map = guard.map_with_capacity(closure.len(), default_hasher());
            let mut scratchpad_map_2: ScratchpadMap<'_, K, ((usize, usize), Option<K>), S> =
                guard.map_with_capacity(closure.len(), default_hasher());
            proof! {
                let n1 = snapshot!(old.nodes);
            }
            let admitted = snapshot!(member_of(closure.model()));
            proof_assert!(parent_closed(*n1, closure.model()));
            proof_assert!(forall<k> admitted[k] == closure.model().contains(&k));
            proof_assert!(forall<k, p> admitted[k] && n1[k]@.from.contains(p) ==> admitted[p]);
            proof_assert! {
                lemma_ids_empty(topological.model());
                lemma_kahn_empty(*n1, *admitted);
                kahn_state(*n1, *admitted, ids(topological.model()), scratchpad_map.model())
            };

            let mut i = 0;
            #[invariant(i@ <= closure_roots.model().len())]
            #[invariant(nodes.model() == *n1)]
            #[invariant(stack.model().len() == 0)]
            #[invariant(kahn_state(*n1, *admitted, ids(topological.model()), scratchpad_map.model()))]
            #[invariant(forall<k> ids(topological.model()).contains(k) && n1[k]@.from.len() == 0
                ==> exists<j> 0 <= j && j < i@ && ids(closure_roots.model())[j] == k)]
            #[invariant(forall<j> 0 <= j && j < i@
                ==> ids(topological.model()).contains(ids(closure_roots.model())[j]))]
            #[variant(closure_roots.model().len() - i@)]
            while i < closure_roots.len() {
                proof_assert!(ids(closure_roots.model())[i@] == closure_roots.model()[i@].deep_model());
                proof_assert!(n1[ids(closure_roots.model())[i@]]@.from.len() == 0);
                proof_assert! {
                    lemma_root_unsorted(*n1, ids(closure_roots.model()), i@, ids(topological.model()));
                    !ids(topological.model()).contains(ids(closure_roots.model())[i@])
                };
                proof! {
                    let before = snapshot!(ids(topological.model()));
                    let before_len = snapshot!(topological.model().len());
                }
                proof_assert!(before.len() == *before_len);
                topological_sort_subgraph(
                    nodes,
                    &closure,
                    None,
                    admitted,
                    closure_roots[i],
                    &mut stack,
                    &mut topological, // topological order
                    &mut scratchpad_map,
                );
                proof_assert!(forall<m> 0 <= m && m < before.len() ==> ids(topological.model())[m] == before[m]);
                proof_assert!(ids(topological.model())[before.len()] == ids(closure_roots.model())[i@]);
                proof_assert!(ids(topological.model()).len() == topological.model().len());
                proof_assert! {
                    lemma_admitted_have_parents(nodes.model(), *admitted, topological.model(), *before_len);
                    forall<m> *before_len < m && m < topological.model().len()
                        ==> nodes.model()[topological.model()[m].deep_model()]@.from.len() > 0
                };
                proof_assert!(forall<m> before.len() < m && m < ids(topological.model()).len()
                    ==> ids(topological.model())[m] == topological.model()[m].deep_model());
                proof_assert!(forall<m> before.len() < m && m < ids(topological.model()).len()
                    ==> n1[ids(topological.model())[m]]@.from.len() > 0);
                proof_assert!(forall<k> ids(topological.model()).contains(k) && n1[k]@.from.len() == 0
                    ==> before.contains(k) || k == ids(closure_roots.model())[i@]);
                proof_assert!(forall<k> before.contains(k) ==> ids(topological.model()).contains(k));
                proof_assert!(ids(topological.model()).contains(ids(closure_roots.model())[i@]));
                #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                {
                    i += 1;
                }
            }
            proof! {
                let topo = snapshot!(ids(topological.model()));
            }
            proof_assert! {
                lemma_ancestors_sorted(*n1, closure.model(), ids(closure_roots.model()), *admitted, *topo, scratchpad_map.model());
                forall<k> closure.model().contains(&k) ==> topo.contains(k)
            };

            proof_assert!(kahn_order(*n1, *admitted, *topo, Seq::empty()) && no_dup(*topo));
            proof_assert!(forall<k> n1.contains(&k) ==> n1[k]@.active == old.active.contains(&k));

            let mut i = 0;
            #[invariant(i@ <= topological.model().len())]
            #[invariant(nodes.model() == *n1)]
            #[invariant(forall<j> 0 <= j && j < i@
                ==> scratchpad_map_2.model().contains(&topo[j]))]
            #[invariant(forall<k> scratchpad_map_2.model().contains(&k)
                ==> parent_scored(*n1, scratchpad_map_2.model(), k, i@))]
            #[invariant(forall<k> scratchpad_map_2.model().contains(&k)
                ==> exists<j> 0 <= j && j < i@ && topo[j] == k)]
            #[invariant(forall<k> scratchpad_map_2.model().contains(&k)
                ==> up_scored(*n1, old.active, scratchpad_map_2.model(), k))]
            #[variant(topological.model().len() - i@)]
            while i < topological.len() {
                let id = topological[i];
                proof_assert!(id.deep_model() == topo[i@] && admitted[topo[i@]]);
                let node = &nodes[&id];
                proof_assert!(node@ == n1[topo[i@]]@);
                proof_assert!(forall<p> node@.from.contains(p) ==> admitted_parent(*n1, *admitted, topo[i@], p));
                proof_assert!(kahn_order(*n1, *admitted, *topo, Seq::empty()));
                proof_assert!(forall<p> node@.from.contains(p) ==> {
                    lemma_parent_before(*n1, *admitted, *topo, i@, p);
                    true
                } && scratchpad_map_2.model().contains(&p));

                let best_parent = best_scored(&node.from, &scratchpad_map_2);

                let (parent, score) = if let Some((parent, mut score)) = best_parent {
                    proof_assert!(parent_scored(*n1, scratchpad_map_2.model(), parent.deep_model(), i@));
                    proof! {
                        let best = snapshot!(score);
                    }
                    proof_assert!(*best == scratchpad_map_2.model()[parent.deep_model()].0
                        && forall<c> node@.from.contains(c)
                            ==> score_le(scratchpad_map_2.model()[c].0, *best));
                    #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                    if node.active {
                        score.1 += 1;
                    } else {
                        score.0 += 1;
                    }
                    proof_assert!(score.0@ == best.0@ + if node.active { 0 } else { 1 });
                    proof_assert!(score.1@ == best.1@ + if node.active { 1 } else { 0 });

                    (Some(parent), score)
                } else {
                    proof_assert!(node@.from.len() == 0);
                    (None, if node.active { (0, 1) } else { (1, 0) })
                };
                proof_assert!(node.active == old.active.contains(&id.deep_model()));
                proof_assert!(score.0@ + score.1@ <= i@ + 1);
                proof_assert!(match parent {
                    None => n1[id.deep_model()]@.from.len() == 0,
                    Some(p) => n1[id.deep_model()]@.from.contains(p.deep_model())
                        && scratchpad_map_2.model().contains(&p.deep_model()),
                });
                proof_assert!(parent_insert_ok(*n1, scratchpad_map_2.model(), id.deep_model(), (score, parent), i@));
                proof_assert!(!scratchpad_map_2.model().contains(&topo[i@]));
                proof_assert!(forall<j> scratchpad_map_2.model().contains(&j)
                    ==> parent_linked(*n1, scratchpad_map_2.model(), j)
                        && up_scored(*n1, old.active, scratchpad_map_2.model(), j));
                proof_assert!(forall<p> n1[id.deep_model()]@.from.contains(p)
                    ==> scratchpad_map_2.model().contains(&p));
                proof_assert!(up_entry(*n1, old.active, scratchpad_map_2.model(), id.deep_model(), (score, parent)));
                proof_assert!(up_insert_ok(*n1, old.active, scratchpad_map_2.model(), id.deep_model(), (score, parent)));

                scratchpad_map_2.insert(id, (score, parent));
                #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                {
                    i += 1;
                }
            }
            topological.clear();

            closure.clear();
            let mut scratchpad_set = closure;

            let mut disjoint = topological;

            proof_assert!(forall<k> admitted[k] ==> scratchpad_map_2.model().contains(&k));
            proof_assert!(admitted[*id0]);
            proof! {
                let scores = snapshot!(scratchpad_map_2.model());
                let mut chain = snapshot!(Seq::<Id<K>>::empty());
            }
            proof_assert!(forall<k> scores.contains(&k) ==> parent_linked(*n1, *scores, k));
            proof_assert!(forall<k> scores.contains(&k) ==> up_scored(*n1, old.active, *scores, k));
            proof_assert!(forall<k> !scratchpad_set.model().contains(&k));

            let mut current = Some(id);

            #[invariant(ancestor_walk(*n1, *rank, *scores, *chain, current.deep_model(), *id0))]
            #[invariant(forall<k> scratchpad_set.model().contains(&k) == chain.contains(k))]
            #[invariant(up_walk(old.active, *scores, *chain, current.deep_model(), *id0))]
            #[variant(match current.deep_model() { Some(x) => rank[x] + 1, None => 0 })]
            while let Some(id) = current {
                proof_assert!(current.deep_model() == Some(id.deep_model()));
                proof_assert!(ancestor_step_ok(
                    *n1,
                    *rank,
                    *scores,
                    *chain,
                    id.deep_model(),
                    *id0
                ));
                proof_assert!(up_walk_step_ok(*n1, old.active, *scores, *chain, id.deep_model(), *id0));
                scratchpad_set.insert(*id);
                current = match scratchpad_map_2.get(id) {
                    Some((_, parent)) => parent.as_ref(),
                    None => None,
                };
                proof_assert!(current.deep_model() == scores[id.deep_model()].1.deep_model());
                proof! {
                    chain = snapshot!(chain.snoc(id.deep_model()));
                }
            }
            proof_assert! {
                lemma_ancestor_done(*n1, *rank, *scores, *chain, *id0, scratchpad_set.model());
                path_of(*n1, scratchpad_set.model(), *chain) && chain[0] == *id0
            };
            proof_assert!(scratchpad_set.model().contains(&*id0));
            proof_assert! {
                lemma_path_start_childless(*n1, scratchpad_set.model(), *chain, *id0);
                !has_child_in(*n1, scratchpad_set.model(), *id0)
            };
            proof_assert!(disjoint.model().len() == 0);
            proof_assert!(path_up(*n1, *id0, *chain));
            proof_assert!(up_walk_done_ok(*n1, old.active, *scores, *chain, *id0));

            activate_exactly(nodes, active, &scratchpad_set, &mut disjoint);
            proof_assert!(!(old.active.contains(&*id0) && !has_child_in(old.nodes, old.active, *id0)));
            proof_assert!(!(!old.active.contains(&*id0) && extends_path(old.nodes, old.active, *id0)));
            proof_assert!(forall<k> active.model().contains(&k) == chain.contains(k));
            proof_assert!(activated_at_end(old.nodes, old.active, *id0, active.model()));
            proof_assert!(same_graph(nodes.model(), *n1));
            proof_assert!(forall<k> active.model().contains(&k) ==> nodes.model().contains(&k));
            proof_assert!(forall<k> nodes.model().contains(&k)
                ==> nodes.model()[k]@.active == active.model().contains(&k));
            proof_assert! {
                lemma_same_graph_structure(nodes.model(), *n1, old.roots, old.active, active.model());
                structure(nodes.model(), old.roots, active.model())
            };
            proof_assert! {
                lemma_same_graph_path_of(nodes.model(), *n1, active.model(), *chain);
                path_of(nodes.model(), active.model(), *chain)
            };
            proof_assert!(forms_path(nodes.model(), active.model()));
            proof_assert!(!has_child_in(nodes.model(), active.model(), *id0));
        } else if let Some(node) = self.nodes.get_mut(id) {
            proof_assert!(old.nodes.add(*id0, *node).ext_eq(old.nodes));
            proof! {
                let unchanged = snapshot!(IndependentWeaveView {
                    nodes: old.nodes.add(*id0, *node),
                    roots: old.roots,
                    active: old.active,
                    bookmarked: old.bookmarked,
                });
            }
            proof_assert!(*unchanged == *old && unchanged.valid());
            if !node.active || any_active(&self.active, &node.to) {
                return true;
            }

            proof_assert!(!has_child_in(old.nodes, old.active, *id0));
            node.active = false;
            proof! {
                let view = snapshot!(IndependentWeaveView {
                    nodes: old.nodes.add(*id0, *node),
                    roots: old.roots,
                    active: old.active.remove(*id0),
                    bookmarked: old.bookmarked,
                });
            }
            proof_assert!(same_graph(view.nodes, old.nodes));
            proof_assert!(forall<k> view.nodes.contains(&k)
                ==> view.nodes[k]@.active == view.active.contains(&k));
            proof_assert! {
                lemma_same_graph_structure(view.nodes, old.nodes, old.roots, old.active, view.active);
                structure(view.nodes, view.roots, view.active)
            };
            proof_assert! {
                lemma_same_graph_forms_path(view.nodes, old.nodes, view.active);
                forms_path(view.nodes, view.active)
            };
            proof_assert!(reactivated(*old, *view));
            self.active.remove(&node.id);
        } else {
            return false;
        }

        true
    }
}

impl<K, T, M, S> From<DependentWeave<K, T, M, S>> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    // The nodes, roots, bookmarks and metadata are kept, and the active path leads from the active
    // node up to its root.
    #[ensures(forall<k> result@.nodes.contains(&k) == value@.nodes.contains(&k))]
    #[ensures(forall<k> value@.nodes.contains(&k)
        ==> converted_node(result@.nodes[k], value@.nodes[k], result@.active.contains(&k)))]
    #[ensures(result@.roots == value@.roots)]
    #[ensures(result@.bookmarked == value@.bookmarked)]
    #[ensures(result.metadata == value.metadata)]
    #[ensures(match value@.active {
        Some(a) => activated(result@.nodes, FSet::empty(), a, result@.active),
        None => result@.active == FSet::empty(),
    })]
    #[check(terminates)]
    fn from(value: DependentWeave<K, T, M, S>) -> Self {
        proof! {
            let old = snapshot!(value@);
        }
        let active = HashSet::with_capacity_and_hasher(value.nodes.capacity(), default_hasher());
        let nodes = independent_nodes(value.nodes);
        proof! {
            let mid = snapshot!(nodes.model());
        }
        proof_assert! {
            lemma_from_dependent_valid(*old, *mid);
            IndependentWeaveView {
                nodes: *mid,
                roots: old.roots,
                active: active.model(),
                bookmarked: old.bookmarked,
            }.valid()
        };

        let mut output = Self {
            active,
            nodes,
            roots: value.roots,
            bookmarked: value.bookmarked,
            scratchpad: value.scratchpad,
            metadata: value.metadata,
        };

        if let Some(active) = value.active {
            output.set_active(&active, true);
        }
        proof_assert!(same_graph_activation_ok(output@.nodes, *mid));

        debug_assert!(output.validate(), "Converted weave is malformed");

        output
    }
}

#[allow(clippy::panic_in_result_fn, reason = "Should never panic")]
impl<K, T, M, S> TryFrom<IndependentWeave<K, T, M, S>> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type Error = IndependentWeave<K, T, M, S>;

    // Succeeds exactly when every node has at most one parent. The nodes, roots, bookmarks and
    // metadata are kept, and the active node is the end of the active path.
    #[ensures(match result {
        Ok(weave) => (forall<k> value@.nodes.contains(&k) ==> value@.nodes[k]@.from.len() < 2)
            && converted_to(weave@.nodes, value@.nodes, value@.active)
            && weave@.roots == value@.roots
            && weave@.bookmarked == value@.bookmarked
            && weave.metadata == value.metadata
            && match weave@.active {
                Some(a) => value@.active.contains(&a) && !has_child_in(value@.nodes, value@.active, a),
                None => value@.active.len() == 0,
            },
        Err(weave) => (exists<k> value@.nodes.contains(&k) && 2 <= value@.nodes[k]@.from.len())
            && weave@ == value@
            && weave.metadata == value.metadata,
    })]
    #[check(terminates)]
    fn try_from(value: IndependentWeave<K, T, M, S>) -> Result<Self, Self::Error> {
        if all_single_parent(&value.nodes) {
            proof! {
                let old = snapshot!(value@);
            }
            let (nodes, active) = dependent_nodes(value.nodes, &value.active);
            proof_assert! {
                lemma_to_dependent_valid(*old, nodes.model(), active.deep_model());
                crate::dependent::verification::DependentWeaveView {
                    nodes: nodes.model(),
                    roots: old.roots,
                    active: active.deep_model(),
                    bookmarked: old.bookmarked,
                }.valid()
            };

            let output = Self {
                nodes,
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
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type Nodes = HashMap<K, IndependentNode<K, T, S>, S>;
    type Roots = IndexSet<K, S>;

    #[inline]
    #[check(terminates)]
    #[ensures(result@ == self@.nodes.len())]
    fn len(&self) -> usize {
        self.nodes.len()
    }
    #[inline]
    #[check(terminates)]
    #[ensures(result == (self@.nodes.len() == 0))]
    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    #[inline]
    #[check(terminates)]
    #[ensures(result.model() == self@.nodes)]
    fn nodes(&self) -> &Self::Nodes {
        &self.nodes
    }
    #[inline]
    #[check(terminates)]
    #[ensures(result.model() == self@.roots)]
    fn roots(&self) -> &Self::Roots {
        &self.roots
    }
    #[inline]
    #[check(terminates)]
    #[ensures(result == self@.nodes.contains(&id.deep_model()))]
    fn contains(&self, id: &K) -> bool {
        self.nodes.contains_key(id)
    }
    #[inline]
    #[check(terminates)]
    #[ensures(result == self@.active.contains(&id.deep_model()))]
    fn contains_active(&self, id: &K) -> bool {
        self.active.contains(id)
    }
    #[inline]
    #[check(terminates)]
    #[ensures(match result {
        Some(node) => self@.nodes.lookup(id.deep_model()) == Some(*node),
        None => !self@.nodes.contains(&id.deep_model()),
    })]
    fn get(&self, id: &K) -> Option<&IndependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    #[inline]
    #[check(terminates)]
    #[ensures(match result {
        Some(from) => self@.nodes.contains(&id.deep_model())
            && *from == self@.nodes[id.deep_model()].from,
        None => !self@.nodes.contains(&id.deep_model()),
    })]
    #[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
    fn get_parents(&self, id: &K) -> Option<&IndexSet<K, S>> {
        match self.nodes.get(id) {
            Some(node) => Some(&node.from),
            None => None,
        }
    }
    #[inline]
    #[check(terminates)]
    #[ensures(match result {
        Some(to) => self@.nodes.contains(&id.deep_model())
            && *to == self@.nodes[id.deep_model()].to,
        None => !self@.nodes.contains(&id.deep_model()),
    })]
    #[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
    fn get_children(&self, id: &K) -> Option<&IndexSet<K, S>> {
        match self.nodes.get(id) {
            Some(node) => Some(&node.to),
            None => None,
        }
    }
    #[inline]
    #[check(terminates)]
    #[ensures(match result {
        Some(contents) => self@.nodes.contains(&id.deep_model())
            && *contents == self@.nodes[id.deep_model()].contents,
        None => !self@.nodes.contains(&id.deep_model()),
    })]
    #[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
    fn get_contents(&self, id: &K) -> Option<&T> {
        match self.nodes.get(id) {
            Some(node) => Some(&node.contents),
            None => None,
        }
    }
    #[ensures((^output)@.len() == (^self)@.nodes.len())]
    #[ensures(topological((^self)@.nodes, ids((^output)@)))]
    #[ensures((^self)@.same_keys(self@))]
    #[ensures(same_elements((^self)@.roots, self@.roots))]
    #[ensures((^self)@.active == self@.active)]
    #[ensures(same_elements((^self)@.bookmarked, self@.bookmarked))]
    // Beyond the contract: the weave and its metadata are unchanged.
    #[ensures((^self)@ == self@)]
    #[ensures((^self).metadata == self.metadata)]
    // Beyond the contract: the order is that of Kahn's algorithm, starting from the roots in order.
    #[ensures(ids((^output)@) == kahn_sorted(self@.nodes, self@.roots))]
    #[check(terminates)]
    fn get_ordered_identifiers(&mut self, output: &mut Vec<K>) {
        output.clear();
        output.reserve(self.nodes.len());

        let guard = self.scratchpad.guard();

        topological_sort(
            &self.nodes,
            &self.roots,
            &mut guard.vec_with_capacity(self.roots.len()),
            output,
            &mut guard.map_with_capacity(self.nodes.len(), default_hasher()),
        );
    }
    #[ensures(no_dup(ids((^output)@)))]
    #[ensures((^self)@.nodes.contains(&id.deep_model())
        ==> first(ids((^output)@)) == Some(id.deep_model()))]
    #[ensures(!(^self)@.nodes.contains(&id.deep_model()) ==> (^output)@.len() == 0)]
    #[ensures((^self)@.same_keys(self@))]
    #[ensures(same_elements((^self)@.roots, self@.roots))]
    #[ensures((^self)@.active == self@.active)]
    #[ensures(same_elements((^self)@.bookmarked, self@.bookmarked))]
    // Beyond the contract: the output lists exactly the node and its descendants, and the weave and
    // its metadata are unchanged.
    #[ensures(forall<d> ids((^output)@).contains(d)
        == ((^self)@.nodes.contains(&d) && (^self)@.nodes.contains(&id.deep_model())
            && descends((^self)@.nodes, d, id.deep_model())))]
    #[ensures((^self)@ == self@)]
    #[ensures((^self).metadata == self.metadata)]
    // Beyond the contract: each node comes after those of its parents which are listed.
    #[ensures(forall<i, p> 0 <= i && i < (^output)@.len()
        && self@.nodes[ids((^output)@)[i]]@.from.contains(p) && ids((^output)@).contains(p)
        ==> ids((^output)@).subsequence(0, i).contains(p))]
    // Beyond the contract: the order is that of Kahn's algorithm over the node's descendants.
    #[ensures(self@.nodes.contains(&id.deep_model()) ==> ids((^output)@) == kahn_run(
        self@.nodes,
        descendant_of(self@.nodes, id.deep_model()),
        self@.nodes.len() + 1,
        Seq::empty(),
        Seq::singleton(id.deep_model()),
    ))]
    #[check(terminates)]
    fn get_ordered_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();
        proof_assert! { lemma_ids_empty(output@); ids(output@) == Seq::empty() };
        proof! {
            let old = snapshot!((*self)@);
            let nodes = snapshot!(self.nodes.model());
            let rank = snapshot!(rank_of(*nodes));
        }

        if self.nodes.contains_key(id) {
            let guard = self.scratchpad.guard();

            let mut stack = guard.vec();
            let mut descendants = guard.set(default_hasher());

            descendant_subgraph(&self.nodes, *id, &mut stack, &mut descendants);

            output.reserve(descendants.len());

            let mut ordered = guard.vec_with_capacity(descendants.len());
            let mut identifier_map = guard.map_with_capacity(descendants.len(), default_hasher());
            let admitted = snapshot!(|k| descendants.model().contains(&k));
            proof_assert! {
                lemma_ids_empty(ordered.model());
                lemma_kahn_empty(*nodes, *admitted);
                ids(ordered.model()) == Seq::empty()
                    && kahn_state(*nodes, *admitted, ids(ordered.model()), identifier_map.model())
            };
            proof_assert!(forall<p> admitted_parent(*nodes, *admitted, id.deep_model(), p) ==> {
                lemma_closure_rank(*nodes, *rank, descendants.model(), id.deep_model(), p);
                false
            });

            topological_sort_subgraph(
                &self.nodes,
                &descendants,
                None,
                admitted,
                *id,
                &mut stack,
                &mut ordered,
                &mut identifier_map,
            );
            proof_assert! {
                lemma_kahn_complete(*nodes, *admitted, ids(ordered.model()), identifier_map.model());
                forall<k> nodes.contains(&k) && descendants.model().contains(&k)
                    ==> ids(ordered.model()).contains(k)
            };
            proof_assert!(forall<d> ids(ordered.model()).contains(d) ==> descendants.model().contains(&d));
            proof_assert!(forall<i, p> 0 <= i && i < ordered.model().len()
                && nodes[ids(ordered.model())[i]]@.from.contains(p) && ids(ordered.model()).contains(p)
                ==> admitted_parent(*nodes, *admitted, ids(ordered.model())[i], p)
                    && ids(ordered.model()).subsequence(0, i).contains(p));
            proof_assert!(forall<d> descendants.model().contains(&d)
            ==> descends(*nodes, d, id.deep_model()) && {
                let _ = lemma_closure_descends::<K, T, S>;
                true
            });
            proof_assert!(forall<d> nodes.contains(&d) && descends(*nodes, d, id.deep_model())
                ==> descendants.model().contains(&d));
            proof_assert!(admitted.ext_eq(descendant_of(*nodes, id.deep_model())));

            extend_from_scratch(output, &ordered);
        }
        proof_assert!(self@ == *old);
    }
    #[ensures((^output)@.len() == (^self)@.active.len())]
    #[ensures(forall<i> 0 <= i && i < (^output)@.len() ==> (^self)@.active.contains(&ids((^output)@)[i]))]
    #[ensures(no_dup(ids((^output)@)))]
    #[ensures(valid_path((^self)@.nodes, ids((^output)@)))]
    #[ensures((^self)@.same_keys(self@))]
    #[ensures(same_elements((^self)@.roots, self@.roots))]
    #[ensures((^self)@.active == self@.active)]
    #[ensures(same_elements((^self)@.bookmarked, self@.bookmarked))]
    // Beyond the contract: the weave and its metadata are unchanged.
    #[ensures((^self)@ == self@)]
    #[ensures((^self).metadata == self.metadata)]
    #[check(terminates)]
    fn get_active_path(&mut self, output: &mut Vec<K>) {
        output.clear();
        proof_assert! { lemma_ids_empty(output@); ids(output@) == Seq::empty() };

        if self.active.is_empty() {
            return;
        }

        output.reserve(self.active.len());

        proof! {
            let nodes = snapshot!(self.nodes.model());
            let roots = snapshot!(self.roots.model());
            let active_set = snapshot!(self.active.model());
            let path = snapshot!(such_that(|path| path_of(*nodes, *active_set, path)));
            let root = snapshot!(path[path.len() - 1]);
        }
        proof_assert! {
            lemma_lists_set_len(*path, *active_set);
            0 < path.len() && active_set.contains(&*root) && nodes[*root]@.from.len() == 0
                && roots.contains(*root)
        };
        proof_assert!(forall<k> active_set.contains(&k) && nodes[k]@.from.len() == 0 ==> k == *root);
        let active = &self.active;
        let guard = self.scratchpad.guard();
        let admitted = snapshot!(|k| active.model().contains(&k));

        let mut stack = guard.vec();
        let mut topological_subgraph = guard.vec_with_capacity(active.len());
        let mut scratchpad_map = guard.map_with_capacity(active.len(), default_hasher());
        proof_assert! {
            lemma_ids_empty(topological_subgraph.model());
            lemma_kahn_empty(*nodes, *admitted);
            kahn_state(*nodes, *admitted, ids(topological_subgraph.model()), scratchpad_map.model())
        };

        let mut i = 0;
        #[invariant(i@ <= roots.len())]
        #[invariant(stack.model().len() == 0)]
        #[invariant(kahn_state(*nodes, *admitted, ids(topological_subgraph.model()), scratchpad_map.model()))]
        #[invariant(forall<k> ids(topological_subgraph.model()).contains(k) && nodes[k]@.from.len() == 0
            ==> exists<j> 0 <= j && j < i@ && roots[j] == k)]
        #[invariant(forall<j> 0 <= j && j < i@ && roots[j] == *root
            ==> ids(topological_subgraph.model()).contains(*root))]
        #[variant(roots.len() - i@)]
        while i < self.roots.len() {
            let root = self.roots[i];
            if active.contains(&root) {
                proof_assert!(!ids(topological_subgraph.model()).contains(root.deep_model()));
                topological_sort_subgraph(
                    &self.nodes,
                    active,
                    None,
                    admitted,
                    root,
                    &mut stack,
                    &mut topological_subgraph,
                    &mut scratchpad_map,
                );
            }
            #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
            {
                i += 1;
            }
        }
        proof_assert!(forall<k> admitted[k] == active_set.contains(&k));
        proof_assert!(exists<j> 0 <= j && j < roots.len() && roots[j] == *root);
        proof_assert!(ids(topological_subgraph.model()).contains(*root));
        proof_assert!(forall<k> active_set.contains(&k) && k != *root
            ==> exists<i> 0 <= i && i + 1 < path.len() && path[i] == k);
        proof_assert!(forall<i> 0 <= i && i + 1 < path.len() ==> path.contains(path[i + 1])
            && active_set.contains(&path[i + 1])
            && admitted_parent(*nodes, *admitted, path[i], path[i + 1]));
        proof_assert!(forall<k> active_set.contains(&k) && k != *root
            ==> exists<p> admitted_parent(*nodes, *admitted, k, p));
        proof_assert!(forall<k> nodes.contains(&k) && admitted[k]
            && !ids(topological_subgraph.model()).contains(k)
            ==> exists<p> admitted_parent(*nodes, *admitted, k, p));
        proof_assert! {
            lemma_kahn_complete(*nodes, *admitted, ids(topological_subgraph.model()), scratchpad_map.model());
            forall<k> active_set.contains(&k) ==> ids(topological_subgraph.model()).contains(k)
        };
        proof_assert! {
            lemma_path_chain(*nodes, *active_set, *admitted, *path, ids(topological_subgraph.model()));
            candidate_chain(*nodes, ids(topological_subgraph.model()), *admitted, *path)
        };

        proof_assert! {
            lemma_chain_ordered(*nodes, ids(topological_subgraph.model()), *admitted, *path);
            no_dup(ids(topological_subgraph.model()))
                && candidates_ordered(*nodes, ids(topological_subgraph.model()), *admitted)
        };

        let mut path = guard.vec_with_capacity(active.len());

        longest_candidate_path_to_root(
            &self.nodes,
            &topological_subgraph,
            active,
            None,
            admitted,
            &mut guard.map_with_capacity(active.len(), default_hasher()),
            &mut path,
        );

        extend_from_scratch(output, &path);
    }
    #[ensures((^self)@.nodes.contains(&id.deep_model())
        ==> first(ids((^output)@)) == Some(id.deep_model()))]
    #[ensures(!(^self)@.nodes.contains(&id.deep_model()) ==> (^output)@.len() == 0)]
    #[ensures(no_dup(ids((^output)@)))]
    #[ensures(valid_path((^self)@.nodes, ids((^output)@)))]
    #[ensures((^self)@.same_keys(self@))]
    #[ensures(same_elements((^self)@.roots, self@.roots))]
    #[ensures((^self)@.active == self@.active)]
    #[ensures(same_elements((^self)@.bookmarked, self@.bookmarked))]
    // Beyond the contract: the weave and its metadata are unchanged.
    #[ensures((^self)@ == self@)]
    #[ensures((^self).metadata == self.metadata)]
    // Beyond the contract: the path routes through the active path. It holds every active node
    // which `id` descends from, after all of its inactive nodes.
    #[ensures(forall<k> self@.active.contains(&k) && descends(self@.nodes, id.deep_model(), k)
        ==> ids((^output)@).contains(k))]
    #[ensures(forall<i, j> 0 <= i && i < j && j < (^output)@.len()
        && self@.active.contains(&ids((^output)@)[i])
        ==> self@.active.contains(&ids((^output)@)[j]))]
    // Beyond the contract: its inactive nodes lead up from `id` through as few nodes as possible: to
    // its first active node, or to a root if none of them is active.
    #[ensures(forall<i, q> 0 <= i && i < (^output)@.len()
        && self@.active.contains(&ids((^output)@)[i])
        && (forall<j> 0 <= j && j < i ==> !self@.active.contains(&ids((^output)@)[j]))
        && descending_to(self@.nodes, q, id.deep_model()) && q[0] == ids((^output)@)[i]
        ==> i + 1 <= q.len())]
    #[ensures((forall<j> 0 <= j && j < (^output)@.len() ==> !self@.active.contains(&ids((^output)@)[j]))
        ==> forall<q> descending_to(self@.nodes, q, id.deep_model())
            && self@.nodes[q[0]]@.from.len() == 0
            ==> (^output)@.len() <= q.len())]
    #[check(terminates)]
    fn get_path_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();
        proof_assert! { lemma_ids_empty(output@); ids(output@) == Seq::empty() };
        if !self.nodes.contains_key(id) {
            proof_assert!(forall<k> self@.active.contains(&k) && descends(self@.nodes, id.deep_model(), k)
            ==> {
                lemma_descends_rank(self@.nodes, rank_of(self@.nodes), id.deep_model(), k);
                false
            });
            return;
        }

        proof! {
            let old = snapshot!(self@);
            let nodes = snapshot!(self.nodes.model());
            let id0 = snapshot!(id.deep_model());
            let rank = snapshot!(rank_of(*nodes));
        }
        proof_assert!(ranked(*nodes, *rank));
        proof_assert!(forall<k> nodes.contains(&k) ==> linked(*nodes, k));

        {
            let active = &self.active;
            let guard = self.scratchpad.guard();
            let mut stack = guard.vec();
            let mut ancestors = guard.set(default_hasher());
            let mut root_ancestors = guard.vec();
            proof_assert! { lemma_ids_empty(root_ancestors.model()); ids(root_ancestors.model()) == Seq::empty() };

            ancestor_subgraph(
                &self.nodes,
                *id,
                &mut stack,
                &mut ancestors,
                &mut root_ancestors,
            );
            proof_assert!(forall<k> ancestors.model().contains(&k) && k != *id0
                ==> has_child_in(*nodes, ancestors.model(), k));

            let mut active_topological_subgraph =
                guard.vec_with_capacity(min_len(active.len(), ancestors.len()));
            let mut scratchpad_map =
                guard.map_with_capacity(min_len(active.len(), ancestors.len()), default_hasher());
            let admitted = snapshot!(member_of(active.model().intersection(ancestors.model())));
            proof_assert!(forall<k> admitted[k] == (active.model().contains(&k) && ancestors.model().contains(&k)));
            proof_assert! {
                lemma_ids_empty(active_topological_subgraph.model());
                lemma_kahn_empty(*nodes, *admitted);
                kahn_state(*nodes, *admitted, ids(active_topological_subgraph.model()), scratchpad_map.model())
            };

            let mut i = 0;
            #[invariant(i@ <= root_ancestors.model().len())]
            #[invariant(stack.model().len() == 0)]
            #[invariant(kahn_state(*nodes, *admitted, ids(active_topological_subgraph.model()), scratchpad_map.model()))]
            #[invariant(forall<k> ids(active_topological_subgraph.model()).contains(k) && nodes[k]@.from.len() == 0
                ==> exists<j> 0 <= j && j < i@ && ids(root_ancestors.model())[j] == k)]
            #[invariant(forall<j> 0 <= j && j < i@
                && active.model().contains(&ids(root_ancestors.model())[j])
                ==> ids(active_topological_subgraph.model()).contains(ids(root_ancestors.model())[j]))]
            #[variant(root_ancestors.model().len() - i@)]
            while i < root_ancestors.len() {
                let root = root_ancestors[i];
                proof_assert!(ids(root_ancestors.model())[i@] == root.deep_model());
                if active.contains(&root) {
                    proof_assert!(nodes[root.deep_model()]@.from.len() == 0);
                    proof_assert! {
                        lemma_root_unsorted(*nodes, ids(root_ancestors.model()), i@, ids(active_topological_subgraph.model()));
                        !ids(active_topological_subgraph.model()).contains(root.deep_model())
                    };
                    proof! {
                        let before = snapshot!(ids(active_topological_subgraph.model()));
                        let before_len = snapshot!(active_topological_subgraph.model().len());
                    }
                    proof_assert!(before.len() == *before_len);
                    topological_sort_subgraph(
                        &self.nodes,
                        active,
                        Some(&ancestors),
                        admitted,
                        root,
                        &mut stack,
                        &mut active_topological_subgraph,
                        &mut scratchpad_map,
                    );
                    proof_assert!(
                        ids(active_topological_subgraph.model())[before.len()] == root.deep_model()
                    );
                    proof_assert!(
                        ids(active_topological_subgraph.model()).len()
                            == active_topological_subgraph.model().len()
                    );
                    proof_assert! {
                        lemma_admitted_have_parents(*nodes, *admitted, active_topological_subgraph.model(), *before_len);
                        forall<m> *before_len < m && m < active_topological_subgraph.model().len()
                            ==> nodes[active_topological_subgraph.model()[m].deep_model()]@.from.len() > 0
                    };
                    proof_assert!(forall<m> before.len() < m && m < ids(active_topological_subgraph.model()).len()
                        ==> ids(active_topological_subgraph.model())[m]
                            == active_topological_subgraph.model()[m].deep_model());
                    proof_assert!(forall<m> 0 <= m && m < before.len()
                        ==> ids(active_topological_subgraph.model())[m] == before[m]);
                    proof_assert!(forall<k> ids(active_topological_subgraph.model()).contains(k) && nodes[k]@.from.len() == 0
                        ==> before.contains(k) || k == root.deep_model());
                }
                #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                {
                    i += 1;
                }
            }

            proof_assert!(forall<i> 0 <= i && i < active_topological_subgraph.model().len()
                ==> ids(active_topological_subgraph.model())[i] == active_topological_subgraph.model()[i].deep_model()
                    && nodes.contains(&active_topological_subgraph.model()[i].deep_model()));

            let mut reversed_path = guard.vec();
            proof_assert! {
                lemma_kahn_candidates_ordered(*nodes, *admitted, ids(active_topological_subgraph.model()), *admitted);
                no_dup(ids(active_topological_subgraph.model()))
                    && candidates_ordered(*nodes, ids(active_topological_subgraph.model()), *admitted)
            };

            proof! {
                let anc = snapshot!(ancestors.model());
                let order = snapshot!(ids(active_topological_subgraph.model()));
                let routed = snapshot!(active.model().intersection(ancestors.model()));
            }
            proof_assert!(forall<k> admitted[k] && nodes[k]@.from.len() == 0 ==> order.contains(k));
            proof_assert!(forall<x> member_of(*anc)[x] == anc.contains(&x));
            proof_assert!(forall<x> routed.contains(&x) == admitted[x]);
            proof_assert!(forall<k> active.model().contains(&k) ==> nodes.contains(&k));
            proof_assert!(forall<k> nodes.contains(&k) && descends(*nodes, *id0, k) ==> {
                lemma_descends_closed(*nodes, member_of(*anc), *id0, k);
                anc.contains(&k)
            });
            proof_assert!(forall<k> active.model().contains(&k) && descends(*nodes, *id0, k) ==> admitted[k]);
            proof_assert!((exists<k> admitted[k]) ==> {
                lemma_active_ancestors(
                    *nodes,
                    active.model(),
                    *anc,
                    *admitted,
                    *order,
                    scratchpad_map.model(),
                    such_that(|path| path_of(*nodes, active.model(), path)),
                );
                exists<q> path_of(*nodes, *routed, q) && candidate_chain(*nodes, *order, *admitted, q)
            });

            longest_candidate_path_to_root(
                &self.nodes,
                &active_topological_subgraph,
                active,
                Some(&ancestors),
                admitted,
                &mut guard.map_with_capacity(min_len(active.len(), ancestors.len()), default_hasher()),
                &mut reversed_path,
            );
            proof_assert!(if exists<k> admitted[k] {
                path_of(*nodes, *routed, ids(reversed_path.model()))
            } else {
                reversed_path.model().len() == 0
            });

            let mut scratchpad_map_2 = guard.map_with_capacity(ancestors.len(), default_hasher());

            if let Some(target) = reversed_path.first().copied() {
                proof! {
                    let up = snapshot!(ids(reversed_path.model()));
                }
                proof_assert!(
                    0 < reversed_path.model().len() && reversed_path.model()[0] == target
                );
                proof_assert!(0 < up.len() && up[0] == target.deep_model());
                proof_assert!(admitted[up[0]] && ancestors.model().contains(&target.deep_model()));
                shortest_path_to_ancestor(
                    &self.nodes,
                    id,
                    Some(&target),
                    snapshot!(|node: IndependentNode<K, T, S>| node.id.deep_model()
                        == target.deep_model()),
                    &mut stack,
                    &mut scratchpad_map_2,
                    output,
                );
                proof_assert!(output@.len() == 0 ==> ids(stack.model()).contains(*id0)
                    && (forall<x, p> ids(stack.model()).contains(x) && nodes[x]@.from.contains(p)
                        ==> ids(stack.model()).contains(p))
                    && forall<k> ids(stack.model()).contains(k)
                        ==> nodes[k].id.deep_model() != target.deep_model());
                proof_assert!(forall<x> ancestors.model().contains(&x)
                    ==> rank[x] < set_bound(ancestors.model(), *rank));
                proof_assert!(all_linked(*nodes));
                proof_assert!(forall<x> ancestors.model().contains(&x) ==> nodes.contains(&x));
                proof_assert!(forall<x> ancestors.model().contains(&x) && x != *id0
                    ==> has_child_in(*nodes, ancestors.model(), x));
                proof_assert!(ancestors.model().contains(&target.deep_model()));
                proof_assert!(output@.len() == 0 ==> closure_ancestor_ok(
                    *nodes,
                    *rank,
                    ancestors.model(),
                    *id0,
                    target.deep_model(),
                    ids(stack.model()),
                    set_bound(ancestors.model(), *rank),
                ));
                proof_assert!(output@.len() == 0 ==> ids(stack.model()).contains(target.deep_model()));
                proof_assert!(nodes[target.deep_model()].id.deep_model() == target.deep_model());
                proof_assert!(output@.len() != 0);
                proof! {
                    let down = snapshot!(ids(output@));
                    let n = snapshot!(output@.len());
                }
                proof_assert!(nodes[down[0]]@.id == target.deep_model() && down[0] == up[0]);
                proof_assert!(descending_to(*nodes, *down, *id0));

                output.reverse();
                proof_assert! { lemma_ids_reverse(output@); ids(output@) == down.reverse() };
                output.pop();
                proof_assert! {
                    lemma_ids_prefix(output@, *n - 1);
                    ids(output@) == down.reverse().subsequence(0, *n - 1)
                };
                proof! {
                    let pre = snapshot!(ids(output@));
                }
                extend_from_scratch(output, &reversed_path);
                proof_assert! {
                    lemma_join_descent(*nodes, *rank, *down, *up, *id0);
                    valid_path(*nodes, ids(output@)) && no_dup(ids(output@)) && first(ids(output@)) == Some(*id0)
                };
                proof_assert!(ids(output@) == pre.concat(*up));
                proof_assert!(path_of(*nodes, *routed, *up));
                proof_assert!(forall<i> 0 <= i && i < output@.len() ==> {
                    lemma_closed_path_at(*nodes, member_of(*anc), ids(output@), i);
                    anc.contains(&ids(output@)[i])
                });
                proof_assert!(output@.len() == pre.len() + up.len());
                proof_assert!(forall<i> 0 <= i && i < pre.len() ==> ids(output@)[i] == pre[i]);
                proof_assert!(forall<i> 0 <= i && i < up.len() ==> ids(output@)[pre.len() + i] == up[i]);
                proof_assert!(forall<i, m> 0 <= i && i < pre.len() && 0 <= m && m < up.len()
                    ==> ids(output@)[i] != ids(output@)[pre.len() + m]);
                proof_assert!(forall<i> 0 <= i && i < pre.len() ==> anc.contains(&pre[i]) && !up.contains(pre[i]));
                proof_assert!(forall<k> admitted[k] ==> up.contains(k) && {
                    crate::verification::lemma_concat_contains(*pre, *up, k);
                    ids(output@).contains(k)
                });
                proof_assert!(forall<i> 0 <= i && i < pre.len() ==> !routed.contains(&pre[i]));
                proof_assert!(forall<i> 0 <= i && i < pre.len() ==> !active.model().contains(&pre[i]));
                proof_assert!(forall<i> pre.len() <= i && i < output@.len()
                    ==> ids(output@)[i] == up[i - pre.len()] && active.model().contains(&ids(output@)[i]));
                proof_assert!(forall<q> descending_to(*nodes, q, *id0) && q[0] == target.deep_model()
                    ==> nodes[q[0]].id.deep_model() == target.deep_model() && *n <= q.len());
                proof_assert!(pre.len() + 1 == *n && ids(output@)[pre.len()] == target.deep_model());
            } else {
                shortest_path_to_ancestor(
                    &self.nodes,
                    id,
                    None,
                    snapshot!(|node: IndependentNode<K, T, S>| node.from.model().len() == 0),
                    &mut stack,
                    &mut scratchpad_map_2,
                    output,
                );
                proof_assert!(output@.len() == 0 ==> {
                    lemma_has_root(*nodes, *rank, ids(stack.model()), *id0);
                    false
                });
                proof! {
                    let down = snapshot!(ids(output@));
                }
                proof_assert!(descending_to(*nodes, *down, *id0));

                output.reverse();
                proof_assert! {
                    lemma_ids_reverse(output@);
                    lemma_reverse_descent(*nodes, *down, *id0);
                    valid_path(*nodes, ids(output@)) && no_dup(ids(output@)) && first(ids(output@)) == Some(*id0)
                };
                proof_assert!(
                    reversed_path.model().len() == 0 && ids(reversed_path.model()).len() == 0
                );
                proof_assert!(forall<k> !admitted[k]);
                proof_assert!(ids(output@)[0] == *id0 && member_of(*anc)[*id0]);
                proof_assert!(forall<i> 0 <= i && i < output@.len() ==> {
                    lemma_closed_path_at(*nodes, member_of(*anc), ids(output@), i);
                    anc.contains(&ids(output@)[i])
                });
                proof_assert!(forall<i> 0 <= i && i < output@.len() ==> !active.model().contains(&ids(output@)[i]));
                proof_assert!(output@.len() == down.len());
                proof_assert!(forall<q> descending_to(*nodes, q, *id0) && nodes[q[0]]@.from.len() == 0
                    ==> output@.len() <= q.len());
            }
        }
        proof_assert!(self@ == *old);
    }
    #[ensures(result ==> (^self)@.nodes.len() == self@.nodes.len() + 1)]
    #[ensures(result ==> !self@.nodes.contains(&node@.id))]
    #[ensures(result ==> (^self)@.nodes.contains(&node@.id))]
    #[ensures(result ==> node.active == (^self)@.active.contains(&node@.id)
        || (!node.active && (^self)@.active.contains(&node@.id)
            && exists<c> node@.to.contains(c) && self@.active.contains(&c)))]
    #[ensures(result ==> node.bookmarked == (^self)@.bookmarked.contains(node@.id))]
    #[ensures(result ==> node@.from.len() != 0 || (^self)@.roots.contains(node@.id))]
    #[ensures(!result ==> (^self)@.same_keys(self@))]
    #[ensures(!result ==> same_elements((^self)@.roots, self@.roots))]
    #[ensures(!result ==> (^self)@.active == self@.active)]
    #[ensures(!result ==> same_elements((^self)@.bookmarked, self@.bookmarked))]
    // Beyond the contract: a failed insertion changes nothing, and a successful one adds only the
    // node's identifier.
    #[ensures(!result ==> (^self)@ == self@)]
    #[ensures(result ==> forall<k> (^self)@.nodes.contains(&k)
        == (self@.nodes.contains(&k) || k == node@.id))]
    // Beyond the contract: which nodes become active.
    #[ensures(result && !inserted_active(self@, node@) ==> (^self)@.active == self@.active)]
    #[ensures(result && inserted_active(self@, node@)
        ==> activated((^self)@.nodes, self@.active, node@.id, (^self)@.active))]
    // Beyond the contract: when insertion succeeds, and how it links the node, which keeps its
    // contents, while the other nodes keep theirs.
    #[ensures(result == insertable(self@.nodes, node@))]
    #[ensures(result ==> inserted(self@, (^self)@, node@, node.contents))]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    #[allow(clippy::too_many_lines, reason = "Most of its length is proof code")]
    #[check(terminates)]
    fn insert(&mut self, mut node: IndependentNode<K, T, S>) -> bool {
        snapshot! { lemma_prefix_contains::<Id<K>> };
        snapshot! { lemma_full_prefix::<Id<K>> };
        snapshot! { lemma_snoc_contains::<Id<K>> };
        snapshot! { crate::verification::lemma_in_order_refl::<Id<K>> };
        proof! {
            let old = snapshot!((*self)@);
            let n = snapshot!(node@);
            let contents = snapshot!(node.contents);
            let mut separator = snapshot!(|_k: Id<K>| node.to.model().len() == 0);
        }

        if self.nodes.contains_key(&node.id)
            || !node.validate()
            || !contains_all(&node.from, &self.nodes)
            || !contains_all(&node.to, &self.nodes)
        {
            proof_assert!(!insertable(old.nodes, *n));
            return false;
        }

        if !node.to.is_empty() && !node.from.is_empty() {
            let guard = self.scratchpad.guard();

            let mut stack = guard.vec_with_capacity(node.from.len());
            let mut ancestors = guard.set_with_capacity(node.from.len(), default_hasher());

            if ancestor_subgraph_reaches(
                &self.nodes,
                &node.from,
                &node.to,
                snapshot!(|k| node.to.model().contains(k)),
                &mut stack,
                &mut ancestors,
            ) {
                proof_assert! {
                    lemma_not_insertable(old.nodes, *n);
                    !insertable(old.nodes, *n)
                };
                return false;
            }

            proof! {
                separator = snapshot!(|k| ancestors.model().contains(&k));
            }
        }

        proof_assert!(separates(old.nodes, *n, *separator));
        proof_assert!(insertable(old.nodes, *n));
        proof_assert!(forall<k> old.nodes.contains(&k) ==> linked(old.nodes, k) && old.flagged(k));
        proof_assert!(forall<k> old.nodes.contains(&k) ==> !old.nodes[k]@.from.contains(n.id)
            && !old.nodes[k]@.to.contains(n.id));

        let root_index = if node.from.is_empty() {
            first_root_index(&self.roots, &node.to)
        } else {
            None
        };

        let mut detached_root = false;

        let mut i = 0;
        #[invariant(i@ <= node.to.model().len())]
        #[invariant(forall<k> self.nodes.model().contains(&k) == old.nodes.contains(&k))]
        #[invariant(forall<k> old.nodes.contains(&k)
            ==> self.nodes.model()[k].contents == old.nodes[k].contents)]
        #[invariant(node.contents == *contents)]
        #[invariant(forall<k> old.nodes.contains(&k)
            ==> self.nodes.model()[k]@.id == old.nodes[k]@.id
                && self.nodes.model()[k]@.to == old.nodes[k]@.to
                && self.nodes.model()[k]@.active == old.nodes[k]@.active
                && self.nodes.model()[k]@.bookmarked == old.nodes[k]@.bookmarked
                && self.nodes.model()[k]@.from
                    == if n.to.subsequence(0, i@).contains(k) {
                        old.nodes[k]@.from.snoc(n.id)
                    } else {
                        old.nodes[k]@.from
                    })]
        #[invariant(node@.id == n.id && node@.from == n.from && node@.to == n.to
            && node@.bookmarked == n.bookmarked)]
        #[invariant(node.active == (n.active || exists<j> 0 <= j && j < i@
            && old.nodes[n.to[j]]@.from.len() == 0 && old.active.contains(&n.to[j])))]
        #[invariant(detached_root == exists<j> 0 <= j && j < i@
            && old.nodes[n.to[j]]@.from.len() == 0)]
        #[variant(node.to.model().len() - i@)]
        while i < node.to.len() {
            proof_assert!(n.to.contains(n.to[i@]) && !n.to.subsequence(0, i@).contains(n.to[i@]));
            let child = self.nodes.get_mut(&node.to[i]).unwrap();

            if child.from.is_empty() {
                node.active |= child.active;
                detached_root = true;
            }

            child.from.insert(node.id);
            #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
            {
                i += 1;
            }
        }

        proof_assert!(forall<k> old.nodes.contains(&k) ==> self.nodes.model()[k]@.from
            == if n.to.contains(k) { old.nodes[k]@.from.snoc(n.id) } else { old.nodes[k]@.from });
        proof_assert!(node.active == (n.active || exists<c> n.to.contains(c)
            && old.nodes[c]@.from.len() == 0 && old.active.contains(&c)));
        proof_assert!(detached_root == exists<c> n.to.contains(c) && old.nodes[c]@.from.len() == 0);
        proof_assert!(!detached_root ==> forall<k> old.roots.contains(k) ==> !n.to.contains(k));

        if detached_root {
            retain_unlisted(&mut self.roots, &node.to);
        }

        proof_assert!(forall<k> self.roots.model().contains(k)
            == (old.roots.contains(k) && !n.to.contains(k)));
        proof_assert!(crate::verification::in_order(self.roots.model(), old.roots));
        proof_assert!(!detached_root ==> self.roots.model() == old.roots);

        let extends_active = node.active
            && node.to.is_empty()
            && extends_active_end(&self.nodes, &self.active, &node.from);

        proof_assert!(extends_active == (node.active && n.to.len() == 0
            && exists<p> n.from.contains(p) && old.active.contains(&p)
                && !has_child_in(old.nodes, old.active, p)));

        if node.from.is_empty() {
            if let Some(index) = root_index {
                proof_assert!(no_dup(old.roots.subsequence(0, index@)));
                proof_assert!(forall<k> old.roots.subsequence(0, index@).contains(k)
                    ==> self.roots.model().contains(k));
                proof_assert! {
                    crate::verification::lemma_subset_len(
                        old.roots.subsequence(0, index@),
                        self.roots.model(),
                    );
                    index@ <= self.roots.model().len()
                };
                proof_assert! {
                    lemma_insert_in_place(self.roots.model(), old.roots, index@, n.id);
                    crate::verification::in_order(
                        self.roots.model().add_at(index@, n.id),
                        old.roots.set(index@, n.id),
                    )
                };
                proof_assert!(forall<i> 0 <= i && i < old.roots.len() && n.to.contains(old.roots[i])
                    && (forall<j> 0 <= j && j < i ==> !n.to.contains(old.roots[j]))
                    ==> i == index@);
                proof! {
                    let before = snapshot!(self.roots.model());
                }
                proof_assert!(!before.contains(n.id));
                self.roots.shift_insert(index, node.id);
                proof_assert!(self.roots.model() == before.add_at(index@, n.id));
                proof_assert!(forall<i> 0 <= i && i < old.roots.len() && n.to.contains(old.roots[i])
                    && (forall<j> 0 <= j && j < i ==> !n.to.contains(old.roots[j]))
                    ==> crate::verification::in_order(self.roots.model(), old.roots.set(i, n.id)));
            } else {
                self.roots.insert(node.id);
            }
        } else {
            let mut i = 0;
            #[invariant(i@ <= node.from.model().len())]
            #[invariant(forall<k> self.nodes.model().contains(&k) == old.nodes.contains(&k))]
            #[invariant(forall<k> old.nodes.contains(&k)
                ==> self.nodes.model()[k].contents == old.nodes[k].contents)]
            #[invariant(forall<k> old.nodes.contains(&k)
                ==> self.nodes.model()[k]@.id == old.nodes[k]@.id
                    && self.nodes.model()[k]@.active == old.nodes[k]@.active
                    && self.nodes.model()[k]@.bookmarked == old.nodes[k]@.bookmarked
                    && self.nodes.model()[k]@.from
                        == (if n.to.contains(k) { old.nodes[k]@.from.snoc(n.id) } else { old.nodes[k]@.from })
                    && self.nodes.model()[k]@.to
                        == if n.from.subsequence(0, i@).contains(k) {
                            old.nodes[k]@.to.snoc(n.id)
                        } else {
                            old.nodes[k]@.to
                        })]
            #[variant(node.from.model().len() - i@)]
            while i < node.from.len() {
                proof_assert!(n.from.contains(n.from[i@]) && !n.from.subsequence(0, i@).contains(n.from[i@]));
                proof_assert!(old.nodes.contains(&n.from[i@])
                    && self.nodes.model()[n.from[i@]]@.to == old.nodes[n.from[i@]]@.to
                    && !old.nodes[n.from[i@]]@.to.contains(n.id));
                let parent = self.nodes.get_mut(&node.from[i]).unwrap();
                parent.to.insert(node.id);
                #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                {
                    i += 1;
                }
            }
        }

        proof_assert!(forall<k> old.nodes.contains(&k) ==> self.nodes.model()[k]@.to
            == if n.from.contains(k) { old.nodes[k]@.to.snoc(n.id) } else { old.nodes[k]@.to });
        proof_assert!(n.from.len() == 0 ==> forall<i> 0 <= i && i < old.roots.len()
            && n.to.contains(old.roots[i])
            && (forall<j> 0 <= j && j < i ==> !n.to.contains(old.roots[j]))
            ==> crate::verification::in_order(self.roots.model(), old.roots.set(i, n.id)));

        if node.bookmarked {
            self.bookmarked.insert(node.id);
        }

        let id = node.id;
        let active = node.active;

        if !extends_active {
            node.active = false;
        }

        proof! {
            let view = snapshot!(IndependentWeaveView {
                nodes: self.nodes.model().add(n.id, node),
                roots: self.roots.model(),
                active: if extends_active { self.active.model().add(n.id) } else { self.active.model() },
                bookmarked: self.bookmarked.model(),
            });
        }
        proof_assert!(forall<k> view.nodes.contains(&k) == (old.nodes.contains(&k) || k == n.id));
        proof_assert!(view.nodes[n.id]@ == NodeView {
            id: n.id,
            from: n.from,
            to: n.to,
            active: view.active.contains(&n.id),
            bookmarked: n.bookmarked,
        });
        proof_assert!(forall<k> old.nodes.contains(&k) ==> view.nodes[k]@.id == old.nodes[k]@.id
            && view.nodes[k]@.active == old.nodes[k]@.active
            && view.nodes[k]@.bookmarked == old.nodes[k]@.bookmarked
            && view.nodes[k]@.from == (if n.to.contains(k) { old.nodes[k]@.from.snoc(n.id) } else { old.nodes[k]@.from })
            && view.nodes[k]@.to == (if n.from.contains(k) { old.nodes[k]@.to.snoc(n.id) } else { old.nodes[k]@.to }));
        proof_assert!(forall<k> view.roots.contains(k)
            == ((k == n.id && n.from.len() == 0) || (old.roots.contains(k) && !n.to.contains(k))));
        proof_assert!(forall<k> view.bookmarked.contains(k)
            == (old.bookmarked.contains(k) || (k == n.id && n.bookmarked)));
        proof_assert!(forall<k> k != n.id ==> view.active.contains(&k) == old.active.contains(&k));
        proof_assert!(no_dup(view.roots) && no_dup(view.bookmarked));
        proof_assert!(insert_shape(*old, *view, *n));
        proof_assert!(!old.bookmarked.contains(n.id));
        proof_assert!(
            view.bookmarked
                == if n.bookmarked {
                    old.bookmarked.snoc(n.id)
                } else {
                    old.bookmarked
                }
        );
        proof_assert!(n.from.len() == 0 && (forall<c> n.to.contains(c) ==> !old.roots.contains(c))
            ==> view.roots == old.roots.snoc(n.id));
        proof_assert!(n.from.len() != 0 ==> view.roots == self.roots.model());
        proof_assert!(inserted(*old, *view, *n, *contents));
        proof_assert! { lemma_insert_structure(*old, *view, *n); view.valid_structure() };
        proof_assert!(extends_active ==> {
            lemma_insert_extends(*old, *view, *n);
            view.valid()
        });
        proof_assert!(!extends_active && !active ==> {
            lemma_insert_inactive(*old, *view, *n);
            view.valid()
        });
        proof_assert!(!extends_active ==> {
            lemma_insert_ready(*old, *view, *n);
            ready_to_activate(view.nodes, view.active, n.id, true)
        });
        snapshot! { lemma_reactivated(*view) };
        proof_assert!(active == inserted_active(*old, *n));
        proof_assert!(!old.active.contains(&n.id));
        proof_assert!(forall<p> n.from.contains(p)
            ==> has_child_in(view.nodes, old.active, p) == has_child_in(old.nodes, old.active, p));
        proof_assert!(extends_active ==> extends_path(view.nodes, old.active, n.id));

        self.nodes.insert(node.id, node);
        proof_assert!(
            self.nodes.model() == view.nodes
                && self.roots.model() == view.roots
                && self.bookmarked.model() == view.bookmarked
        );
        proof_assert!(inserted(*old, self@, *n, *contents));

        if extends_active {
            self.active.insert(id);
            proof_assert!(activated(view.nodes, old.active, n.id, self.active.model()));
        } else if active {
            proof_assert!(!view.nodes[n.id]@.active && view.active == old.active);
            Self::update_node_activity_in_place(
                &mut self.nodes,
                &self.roots,
                &mut self.active,
                &mut self.scratchpad,
                &id,
                true,
            );
            proof_assert! {
                crate::verification::lemma_same_keys_len(self.nodes.model(), view.nodes);
                self.nodes.model().len() == view.nodes.len()
            };
            proof_assert!(same_graph_activation_ok(self.nodes.model(), view.nodes));
            proof_assert!(same_graph(self.nodes.model(), view.nodes));
            proof_assert!(
                self.roots.model() == view.roots && self.bookmarked.model() == view.bookmarked
            );
        }
        proof_assert!(inserted(*old, self@, *n, *contents));

        true
    }
    #[inline]
    #[ensures(result ==> value == (^self)@.active.contains(&id.deep_model()))]
    #[ensures(!result ==> (^self)@.active == self@.active)]
    #[ensures(result == self@.nodes.contains(&id.deep_model()))]
    #[ensures((^self)@.same_keys(self@))]
    #[ensures(same_elements((^self)@.roots, self@.roots))]
    #[ensures(same_elements((^self)@.bookmarked, self@.bookmarked))]
    // Beyond the contract: which nodes become active, and that only which nodes are active changes.
    #[ensures(self@.active.contains(&id.deep_model()) == value ==> (^self)@.active == self@.active)]
    #[ensures(result && value && !self@.active.contains(&id.deep_model())
        ==> activated(self@.nodes, self@.active, id.deep_model(), (^self)@.active))]
    #[ensures(result && !value && self@.active.contains(&id.deep_model())
        ==> longest_path_within(
            self@.nodes,
            self@.active.remove(id.deep_model()),
            (^self)@.active,
        ))]
    #[ensures(same_graph((^self)@.nodes, self@.nodes))]
    #[ensures((^self)@.roots == self@.roots)]
    #[ensures((^self)@.bookmarked == self@.bookmarked)]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    #[check(terminates)]
    fn set_active(&mut self, id: &K, value: bool) -> bool {
        snapshot! { lemma_reactivated((*self)@) };
        proof_assert! {
            lemma_ready((*self)@, id.deep_model(), value);
            ready_to_activate(self@.nodes, self@.active, id.deep_model(), value)
        };
        proof_assert!(self@.nodes.contains(&id.deep_model())
            ==> self@.nodes[id.deep_model()]@.active == self@.active.contains(&id.deep_model()));
        proof_assert!(!self@.nodes.contains(&id.deep_model()) ==> !self@.active.contains(&id.deep_model()));

        Self::update_node_activity_in_place(
            &mut self.nodes,
            &self.roots,
            &mut self.active,
            &mut self.scratchpad,
            id,
            value,
        )
    }
    #[ensures(!(^self)@.nodes.contains(&id.deep_model()))]
    #[ensures((result != None) == self@.nodes.contains(&id.deep_model()))]
    #[ensures(match result { Some(node) => node@.id == id.deep_model(), None => true })]
    #[ensures(result != None ==> (^self)@.nodes.len() < self@.nodes.len())]
    #[ensures(result != None ==> (^self)@.active.len() <= self@.active.len())]
    #[ensures(result != None ==> (^self)@.bookmarked.len() <= self@.bookmarked.len())]
    #[ensures(result == None ==> (^self)@.same_keys(self@))]
    #[ensures(result == None ==> same_elements((^self)@.roots, self@.roots))]
    #[ensures(result == None ==> (^self)@.active == self@.active)]
    #[ensures(result == None ==> same_elements((^self)@.bookmarked, self@.bookmarked))]
    // Beyond the contract: the removed node is returned, no node is added, and the active path is a
    // longest path to a root through the nodes which were active.
    #[ensures(result != None ==> result == self@.nodes.lookup(id.deep_model()))]
    #[ensures(forall<k> (^self)@.nodes.contains(&k) ==> self@.nodes.contains(&k))]
    #[ensures(result != None
        ==> longest_path_within((^self)@.nodes, self@.active, (^self)@.active))]
    // Beyond the contract: exactly the node and the nodes left without parents are removed, the
    // rest of the weave keeps its contents and order, and a failed removal changes nothing.
    #[ensures(result != None ==> exists<done: Seq<Id<K>>> removal(self@.nodes, id.deep_model(), done)
        && removal_shape(self@, (^self)@, done))]
    #[ensures(result == None ==> (^self)@ == self@)]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    #[allow(clippy::too_many_lines, reason = "Most of its length is proof code")]
    #[check(terminates)]
    fn remove(&mut self, id: &K) -> Option<IndependentNode<K, T, S>> {
        snapshot! { lemma_ids_snoc::<K> };
        snapshot! { lemma_snoc_contains::<Id<K>> };
        proof! {
            let old = snapshot!((*self)@);
            let t = snapshot!(id.deep_model());
            let rank = snapshot!(rank_of(old.nodes));
        }
        proof_assert!(!old.nodes.contains(&*t) ==> old.nodes.remove(*t).ext_eq(old.nodes));
        proof_assert!(in_order_refl_ok(old.roots) && in_order_refl_ok(old.bookmarked));
        proof_assert!(!old.nodes.contains(&*t) ==> IndependentWeaveView {
            nodes: old.nodes.remove(*t),
            roots: old.roots,
            active: old.active,
            bookmarked: old.bookmarked,
        } == *old);

        let node = self.nodes.remove(id)?;

        proof! {
            let n = snapshot!(old.nodes[*t]@);
        }
        proof_assert!(old.nodes.contains(&*t) && old.nodes.lookup(*t) == Some(node));
        proof_assert!(cascade_init_ok(*old, *t, *rank));
        proof_assert!(n.id == *t
            && (n.from.len() == 0) == old.roots.contains(*t)
            && no_dup(n.from)
            && no_dup(n.to)
            && !n.to.contains(*t)
            && (forall<k> n.to.contains(k) ==> old.nodes.contains(&k))
            && forall<k> old.nodes.contains(&k) ==> old.nodes[k]@.active == old.active.contains(&k)
                && old.nodes[k]@.bookmarked == old.bookmarked.contains(k));
        proof_assert!(node@ == *n);

        if node.from.is_empty() {
            self.roots.shift_remove(id);
            proof_assert!(shift_removed_ok(self.roots.model(), old.roots, *t));
            proof_assert!(kept_sub_ok(self.nodes.model(), old.nodes));
            proof_assert!(detaching_start_ok(old.nodes, *t, n.from));
            proof_assert!(detaching(old.nodes, self.nodes.model(), *t, n.from));
        } else {
            proof_assert!(!old.roots.contains(*t));
            proof_assert!(detaching_start_ok(old.nodes, *t, n.from.subsequence(0, 0)));
            proof_assert!(detaching(
                old.nodes,
                self.nodes.model(),
                *t,
                n.from.subsequence(0, 0)
            ));
            proof_assert!(kept_sub_ok(self.nodes.model(), old.nodes));
            let mut i = 0;
            #[invariant(i@ <= node.from.model().len())]
            #[invariant(detaching(old.nodes, self.nodes.model(), *t, n.from.subsequence(0, i@)))]
            #[invariant(kept_in_order(self.nodes.model(), old.nodes))]
            #[invariant(self.roots.model() == old.roots)]
            #[invariant(self.active.model() == old.active)]
            #[invariant(self.bookmarked.model() == old.bookmarked)]
            #[variant(node.from.model().len() - i@)]
            while i < node.from.len() {
                proof! {
                    let before = snapshot!(self.nodes.model());
                    let key = snapshot!(n.from[i@]);
                }
                proof_assert!(prefix_step_ok(n.from, i@));
                proof_assert!((forall<x> n.from.subsequence(0, i@ + 1).contains(x)
                    == (n.from.subsequence(0, i@).contains(x) || x == *key))
                    && !n.from.subsequence(0, i@).contains(*key));
                if let Some(parent) = self.nodes.get_mut(&node.from[i]) {
                    proof! {
                        let pto = snapshot!(parent@.to);
                    }
                    parent.to.shift_remove(id);
                    proof_assert!(shift_removed_ok(parent@.to, *pto, *t));
                }
                proof_assert!(forall<k> k != *key ==> self.nodes.model().lookup(k) == before.lookup(k));
                proof_assert!(self.nodes.model().contains(&*key) == before.contains(&*key));
                proof_assert!(before.contains(&*key) ==> self.nodes.model()[*key]@.id == before[*key]@.id
                    && self.nodes.model()[*key]@.from == before[*key]@.from
                    && self.nodes.model()[*key]@.active == before[*key]@.active
                    && self.nodes.model()[*key]@.bookmarked == before[*key]@.bookmarked
                    && forall<c> self.nodes.model()[*key]@.to.contains(c)
                        == (before[*key]@.to.contains(c) && c != *t));
                proof_assert!(detaching_step_ok(old.nodes, *before, self.nodes.model(), *t,
                    n.from.subsequence(0, i@), n.from.subsequence(0, i@ + 1), *key));
                proof_assert!(kept_children_step_ok(
                    old.nodes,
                    *before,
                    self.nodes.model(),
                    *key
                ));
                proof_assert!(detaching(old.nodes, self.nodes.model(), *t, n.from.subsequence(0, i@ + 1)));
                #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                {
                    i += 1;
                }
            }
            proof_assert! { lemma_full_prefix(n.from); n.from.subsequence(0, n.from.len()) == n.from };
        }
        proof_assert!(detaching_done_ok(old.nodes, self.nodes.model(), *t, n.from));
        proof_assert!(detached(old.nodes, self.nodes.model(), *t));

        proof! {
            let n1 = snapshot!(self.nodes.model());
        }
        proof_assert!(forall<k> self.roots.model().contains(k) == (old.roots.contains(k) && k != *t));
        proof_assert!(kept_in_order(*n1, old.nodes));
        proof_assert!(filtered(self.roots.model(), old.roots, Seq::singleton(*t)));

        let mut removed_active = node.active;
        let mut removed_bookmark = node.bookmarked;

        if removed_active {
            self.active.remove(id);
        }

        proof_assert!(forall<k> self.active.model().contains(&k) == (old.active.contains(&k) && k != *t));
        proof_assert!(removed_bookmark == old.bookmarked.contains(*t));
        proof_assert!(keys_subset_len_ok(self.nodes.model(), old.nodes.remove(*t)));
        proof_assert!(self.nodes.model().len() < old.nodes.len());

        if node.to.is_empty() {
            proof_assert!(self.bookmarked.model() == old.bookmarked);
            proof_assert!(!removed_bookmark ==> forall<k> self.bookmarked.model().contains(k)
                == (old.bookmarked.contains(k) && k != *t));
            proof_assert!(!removed_bookmark ==> cascade_leaf_valid_ok(
                *old,
                self.nodes.model(),
                self.roots.model(),
                self.active.model(),
                self.bookmarked.model(),
                *rank,
                *t,
            ));
            if removed_bookmark {
                proof_assert!(old.bookmarked.contains(*t));
                proof! {
                    let bi = snapshot!(such_that(|i: Int| 0 <= i && i < old.bookmarked.len()
                        && old.bookmarked[i] == *t));
                }
                proof_assert!(0 <= *bi && *bi < old.bookmarked.len() && old.bookmarked[*bi] == *t);
                proof_assert!(removed_ok(old.bookmarked, *bi));
                proof_assert!(forall<k> old.bookmarked.removed(*bi).contains(k)
                    == (old.bookmarked.contains(k) && k != *t));
                proof_assert!(cascade_leaf_valid_ok(
                    *old,
                    self.nodes.model(),
                    self.roots.model(),
                    self.active.model(),
                    old.bookmarked.removed(*bi),
                    *rank,
                    *t,
                ));
                self.bookmarked.shift_remove(id);
                proof_assert!(shift_removed_ok(
                    self.bookmarked.model(),
                    old.bookmarked,
                    *t
                ));
            }
            proof_assert!(filtered(
                self.bookmarked.model(),
                old.bookmarked,
                Seq::singleton(*t)
            ));
            proof_assert!(removal_leaf_ok(*old, (*self)@, *t));
            proof_assert!(fset_subset_len_ok(self.active.model(), old.active));
            proof_assert!(self.active.model().len() <= old.active.len());
            proof_assert!(seq_subset_len_ok(self.bookmarked.model(), old.bookmarked));
            proof_assert!(self.bookmarked.model().len() <= old.bookmarked.len());
            proof_assert!((*self)@.valid());
            proof_assert!(longest_whole_ok(self.nodes.model(), self.active.model()));
            proof_assert!(longest_widen_ok(self.nodes.model(), self.active.model(), old.active, self.active.model()));
            return Some(node);
        }

        proof! {
            let r1 = snapshot!(self.roots.model());
            let mut done = snapshot!(Seq::<Id<K>>::empty());
            let mut mid = snapshot!(*old);
        }

        let Self {
            nodes,
            roots,
            active,
            bookmarked,
            scratchpad,
            ..
        } = self;

        {
            let guard = scratchpad.guard();
            let mut stack = guard.vec();
            proof_assert!(no_dup_len_ok(n.to.snoc(*t), old.nodes));
            proof_assert! {
                lemma_no_dup_snoc(n.to, *t);
                n.to.len() + 1 <= old.nodes.len()
            };
            let mut removed = guard.vec_with_capacity(node.to.len().strict_add(1));
            let mut remaining_parents = guard.map_with_capacity(node.to.len(), default_hasher());

            proof! {
                let r0 = snapshot!(removed.model());
            }
            removed.push(*id);
            proof_assert! {
                lemma_ids_singleton(*r0, *id);
                lemma_empty_snoc(*t);
                ids(removed.model()) == Seq::<Id<K>>::empty().snoc(*t)
            };
            proof_assert!(cascade_start_ok(old.nodes, *rank, *t));
            proof_assert! {
                lemma_ids_empty(stack.model());
                cascade_inner(old.nodes, *rank, *t, Seq::empty(), *t, n.to, ids(stack.model()),
                    remaining_parents.model())
            };

            let mut i = node.to.len();
            proof_assert! { lemma_full_prefix(n.to); n.to.subsequence(0, i@) == n.to };
            #[invariant(i@ <= n.to.len())]
            #[invariant(cascade_inner(old.nodes, *rank, *t, Seq::empty(), *t, n.to.subsequence(0, i@),
                ids(stack.model()), remaining_parents.model()))]
            #[invariant(nodes.model() == *n1)]
            #[invariant(roots.model() == *r1)]
            #[invariant(forall<k> active.model().contains(&k) == (old.active.contains(&k) && k != *t))]
            #[invariant(bookmarked.model() == old.bookmarked)]
            #[invariant(ids(removed.model()) == Seq::<Id<K>>::empty().snoc(*t))]
            #[variant(i@)]
            while i > 0 {
                #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                {
                    i -= 1;
                }
                let child = node.to[i];
                proof! {
                    let counts = snapshot!(remaining_parents.model());
                    let st = snapshot!(ids(stack.model()));
                }
                proof_assert!(cascade_count_ok(old.nodes, *rank, *t, Seq::empty(), *t, i@, *st, *counts));
                proof_assert!(old.nodes.contains(&child.deep_model())
                    && child.deep_model() != *t
                    && 0 <= left_parents(old.nodes, Seq::empty().snoc(*t), child.deep_model())
                    && (counts.contains(&child.deep_model()) ==> counts[child.deep_model()]@
                        == left_parents(old.nodes, Seq::empty().snoc(*t), child.deep_model()) + 1)
                    && (!counts.contains(&child.deep_model()) ==> old.nodes[child.deep_model()]@.from.len()
                        == left_parents(old.nodes, Seq::empty().snoc(*t), child.deep_model()) + 1));
                proof_assert!(nodes.model().contains(&child.deep_model())
                    && nodes.model()[child.deep_model()]@.from == old.nodes[child.deep_model()]@.from);
                proof_assert!(
                    nodes.model().lookup(child.deep_model())
                        == Some(nodes.model()[child.deep_model()])
                );
                let remaining = match remaining_parents.entry(child) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => entry.insert(nodes[&child].from.len()),
                };
                proof_assert!(counts.contains(&child.deep_model())
                    ==> (*remaining)@ == counts[child.deep_model()]@);
                proof_assert!(!counts.contains(&child.deep_model())
                    ==> (*remaining)@ == nodes.model()[child.deep_model()]@.from.len());
                proof_assert!((*remaining)@
                    == left_parents(old.nodes, Seq::empty().snoc(*t), child.deep_model()) + 1);
                #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                {
                    *remaining -= 1;
                }
                proof! {
                    let nv = snapshot!(*remaining);
                }
                proof_assert!(nv@ == left_parents(old.nodes, Seq::empty().snoc(*t), child.deep_model()));

                if *remaining == 0 {
                    stack.push(child);
                }
                proof_assert!(remaining_parents.model() == counts.add(child.deep_model(), *nv));
                proof_assert!(cascade_visit_ok(old.nodes, *rank, *t, Seq::empty(), *t, i@, *st, *counts, *nv));
                proof_assert!(cascade_inner(old.nodes, *rank, *t, Seq::empty(), *t, n.to.subsequence(0, i@),
                    ids(stack.model()), remaining_parents.model()));
            }
            proof_assert!(cascade_next_ok(
                old.nodes,
                *rank,
                *t,
                Seq::empty(),
                *t,
                ids(stack.model()),
                remaining_parents.model()
            ));
            proof_assert!(cascade_outer(
                old.nodes,
                *rank,
                *t,
                ids(removed.model()),
                ids(stack.model()),
                remaining_parents.model()
            ));
            proof_assert!(forall<k> ids(removed.model()).contains(k) == (k == *t));

            #[invariant(cascade_outer(old.nodes, *rank, *t, ids(removed.model()), ids(stack.model()),
                remaining_parents.model()))]
            #[invariant(forall<k> nodes.model().contains(&k)
                == (n1.contains(&k) && !ids(removed.model()).contains(k)))]
            #[invariant(forall<k> nodes.model().contains(&k) ==> nodes.model()[k] == n1[k])]
            #[invariant(forall<k> active.model().contains(&k)
                == (old.active.contains(&k) && !ids(removed.model()).contains(k)))]
            #[invariant(!removed_active
                ==> forall<k> ids(removed.model()).contains(k) ==> !old.active.contains(&k))]
            #[invariant(!removed_bookmark
                ==> forall<k> ids(removed.model()).contains(k) ==> !old.bookmarked.contains(k))]
            #[invariant(roots.model() == *r1)]
            #[invariant(bookmarked.model() == old.bookmarked)]
            #[variant(nodes.model().len())]
            while let Some(id) = stack.pop() {
                proof! {
                    let prev = snapshot!(ids(removed.model()));
                    let rest = snapshot!(ids(stack.model()));
                }
                proof_assert!(cascade_pop_ok(
                    old.nodes,
                    *rank,
                    *t,
                    *prev,
                    *rest,
                    id.deep_model(),
                    remaining_parents.model()
                ));
                proof_assert!(cascade_inner(old.nodes, *rank, *t, *prev, id.deep_model(),
                    old.nodes[id.deep_model()]@.to, *rest, remaining_parents.model())
                    && id.deep_model() != *t
                    && old.nodes.contains(&id.deep_model())
                    && !prev.contains(id.deep_model())
                    && prev.contains(*t)
                    && !n.from.contains(id.deep_model()));
                proof_assert!(
                    n1.contains(&id.deep_model()) && nodes.model().contains(&id.deep_model())
                );
                let node = nodes.remove(&id).unwrap();
                removed.push(id);
                proof_assert!(ids(removed.model()) == prev.snoc(id.deep_model()));
                proof_assert!(node == n1[id.deep_model()]);
                proof_assert!(n1[id.deep_model()]@.to == old.nodes[id.deep_model()]@.to);
                proof_assert!(node@.to == old.nodes[id.deep_model()]@.to);
                proof_assert!(
                    node.active == old.active.contains(&id.deep_model())
                        && node.bookmarked == old.bookmarked.contains(id.deep_model())
                );

                removed_bookmark |= node.bookmarked;
                if node.active {
                    active.remove(&id);
                    removed_active = true;
                }

                proof! {
                    let ns = snapshot!(nodes.model());
                    let cur = snapshot!(id.deep_model());
                }
                proof_assert!(forall<k> prev.snoc(*cur).contains(k) == (prev.contains(k) || k == *cur));
                proof_assert!(forall<k> ns.contains(&k) == (n1.contains(&k) && !prev.snoc(*cur).contains(k)));
                proof_assert!(forall<k> ns.contains(&k) ==> ns[k] == n1[k]);
                proof_assert!(forall<k> n1.contains(&k) ==> n1[k]@.from == old.nodes[k]@.from);

                let mut i = node.to.len();
                proof_assert! {
                    lemma_full_prefix(node.to.model());
                    old.nodes[*cur]@.to.subsequence(0, i@) == old.nodes[*cur]@.to
                };
                #[invariant(i@ <= node.to.model().len())]
                #[invariant(node.to.model() == old.nodes[*cur]@.to)]
                #[invariant(cascade_inner(old.nodes, *rank, *t, *prev, *cur,
                    old.nodes[*cur]@.to.subsequence(0, i@), ids(stack.model()),
                    remaining_parents.model()))]
                #[invariant(nodes.model() == *ns)]
                #[invariant(forall<k> active.model().contains(&k)
                    == (old.active.contains(&k) && !ids(removed.model()).contains(k)))]
                #[invariant(roots.model() == *r1)]
                #[invariant(bookmarked.model() == old.bookmarked)]
                #[invariant(ids(removed.model()) == prev.snoc(*cur))]
                #[variant(i@)]
                while i > 0 {
                    #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                    {
                        i -= 1;
                    }
                    let child = node.to[i];
                    proof! {
                        let counts = snapshot!(remaining_parents.model());
                        let st = snapshot!(ids(stack.model()));
                    }
                    proof_assert!(cascade_count_ok(old.nodes, *rank, *t, *prev, *cur, i@, *st, *counts));
                    proof_assert!(old.nodes.contains(&child.deep_model())
                        && child.deep_model() != *t
                        && !prev.snoc(*cur).contains(child.deep_model())
                        && 0 <= left_parents(old.nodes, prev.snoc(*cur), child.deep_model())
                        && (counts.contains(&child.deep_model()) ==> counts[child.deep_model()]@
                            == left_parents(old.nodes, prev.snoc(*cur), child.deep_model()) + 1)
                        && (!counts.contains(&child.deep_model()) ==> old.nodes[child.deep_model()]@.from.len()
                            == left_parents(old.nodes, prev.snoc(*cur), child.deep_model()) + 1));
                    proof_assert!(nodes.model().contains(&child.deep_model())
                        && nodes.model()[child.deep_model()]@.from == old.nodes[child.deep_model()]@.from);
                    proof_assert!(
                        nodes.model().lookup(child.deep_model())
                            == Some(nodes.model()[child.deep_model()])
                    );
                    let remaining = match remaining_parents.entry(child) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => entry.insert(nodes[&child].from.len()),
                    };
                    proof_assert!(counts.contains(&child.deep_model())
                        ==> (*remaining)@ == counts[child.deep_model()]@);
                    proof_assert!(!counts.contains(&child.deep_model())
                        ==> (*remaining)@ == nodes.model()[child.deep_model()]@.from.len());
                    proof_assert!((*remaining)@
                        == left_parents(old.nodes, prev.snoc(*cur), child.deep_model()) + 1);
                    #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                    {
                        *remaining -= 1;
                    }
                    proof! {
                        let nv = snapshot!(*remaining);
                    }
                    proof_assert!(nv@ == left_parents(old.nodes, prev.snoc(*cur), child.deep_model()));

                    if *remaining == 0 {
                        stack.push(child);
                    }
                    proof_assert!(remaining_parents.model() == counts.add(child.deep_model(), *nv));
                    proof_assert!(cascade_visit_ok(old.nodes, *rank, *t, *prev, *cur, i@, *st, *counts, *nv));
                    proof_assert!(cascade_inner(old.nodes, *rank, *t, *prev, *cur,
                        old.nodes[*cur]@.to.subsequence(0, i@), ids(stack.model()),
                        remaining_parents.model()));
                }
                proof_assert!(cascade_next_ok(
                    old.nodes,
                    *rank,
                    *t,
                    *prev,
                    *cur,
                    ids(stack.model()),
                    remaining_parents.model()
                ));
                proof_assert!(cascade_outer(
                    old.nodes,
                    *rank,
                    *t,
                    ids(removed.model()),
                    ids(stack.model()),
                    remaining_parents.model()
                ));
            }

            proof! {
                done = snapshot!(ids(removed.model()));
            }
            proof_assert! { lemma_ids_empty(stack.model()); ids(stack.model()) == Seq::empty() };
            proof_assert!(cascade_end_ok(
                old.nodes,
                *rank,
                *t,
                *done,
                remaining_parents.model()
            ));
            proof_assert!(cascade_done(old.nodes, *rank, *t, *done)
                && done.contains(*t)
                && (done.len() == 1 ==> forall<k> done.contains(k) == (k == *t))
                && (forall<k> remaining_parents.model().contains(&k)
                    && remaining_parents.model()[k]@ == 0 ==> done.contains(k))
                && forall<k, p> old.nodes.contains(&k) && old.nodes[k]@.from.contains(p) && done.contains(p)
                    ==> remaining_parents.model().contains(&k));

            if removed_active && !any_active_survivor(&remaining_parents, active) {
                removed_active = false;
            }
            proof_assert!(!removed_active ==> (forall<k> done.contains(k) ==> !old.active.contains(&k))
                || !exists<k> remaining_parents.model().contains(&k) && remaining_parents.model()[k]@ > 0
                    && old.active.contains(&k) && !done.contains(k));
            proof_assert!(forall<k> old.active.contains(&k) ==> old.nodes.contains(&k));
            proof_assert!(!removed_active
                ==> active_kept_ok(old.nodes, old.active, *done, remaining_parents.model()));

            let mut children = guard.vec();

            proof! {
                let n2 = snapshot!(nodes.model());
            }
            proof_assert!(forall<k> n2.contains(&k) == (n1.contains(&k) && !done.contains(k)));
            proof_assert!(forall<k> n2.contains(&k) ==> n2[k] == n1[k] && old.nodes.contains(&k));

            proof_assert!(forall<k> n2.contains(&k) ==> n2[k]@.from == old.nodes[k]@.from);
            proof_assert!(kept_in_order(*n2, old.nodes));

            if removed.len() == 1 {
                proof_assert!(forall<k> done.contains(k) == (k == *t));
                extend_keys(&mut children, &remaining_parents);
                proof_assert!(unparenting_start_ok(
                    *n2,
                    *done,
                    ids(children.model()).subsequence(0, 0)
                ));
                proof_assert!(unparenting(
                    *n2,
                    nodes.model(),
                    *done,
                    ids(children.model()).subsequence(0, 0)
                ));
                let mut i = 0;
                #[invariant(i@ <= children.model().len())]
                #[invariant(unparenting(*n2, nodes.model(), *done,
                    ids(children.model()).subsequence(0, i@)))]
                #[invariant(kept_in_order(nodes.model(), old.nodes))]
                #[invariant(forall<k> active.model().contains(&k)
                    == (old.active.contains(&k) && !done.contains(k)))]
                #[invariant(roots.model() == *r1)]
                #[invariant(bookmarked.model() == old.bookmarked)]
                #[variant(children.model().len() - i@)]
                while i < children.len() {
                    proof! {
                        let before = snapshot!(nodes.model());
                        let key = snapshot!(children.model()[i@].deep_model());
                    }
                    proof_assert!(prefix_step_ok(ids(children.model()), i@));
                    proof_assert!(ids(children.model())[i@] == *key
                        && forall<x> ids(children.model()).subsequence(0, i@ + 1).contains(x)
                            == (ids(children.model()).subsequence(0, i@).contains(x) || x == *key));
                    proof! {
                        let mut cv = snapshot!(before[*key]);
                    }
                    if let Some(child) = nodes.get_mut(&children[i]) {
                        proof_assert!(before.contains(&*key) && *child == before[*key]);
                        proof! {
                            let cfrom = snapshot!(child@.from);
                        }
                        child.from.shift_remove(id);
                        proof_assert!(shift_removed_ok(child@.from, *cfrom, *t));
                        proof_assert!(child@.id == before[*key]@.id
                            && child@.to == before[*key]@.to
                            && child@.active == before[*key]@.active
                            && child@.bookmarked == before[*key]@.bookmarked);
                        proof_assert!(forall<p> child@.from.contains(p)
                            == (before[*key]@.from.contains(p) && !done.contains(p)));
                        proof! {
                            cv = snapshot!(*child);
                        }
                    }
                    proof_assert!(before.contains(&*key) ==> nodes.model() == before.add(*key, *cv));
                    proof_assert!(!before.contains(&*key) ==> nodes.model() == *before);
                    proof_assert!(before.contains(&*key) ==> cv@.id == before[*key]@.id
                        && cv@.to == before[*key]@.to
                        && cv@.active == before[*key]@.active
                        && cv@.bookmarked == before[*key]@.bookmarked
                        && forall<p> cv@.from.contains(p)
                            == (before[*key]@.from.contains(p) && !done.contains(p)));
                    proof_assert!(unparenting_set_ok(*n2, *before, nodes.model(), *done,
                        ids(children.model()).subsequence(0, i@),
                        ids(children.model()).subsequence(0, i@ + 1), *key, *cv));
                    proof_assert!(kept_parents_set_ok(
                        old.nodes,
                        *before,
                        nodes.model(),
                        *key,
                        *cv
                    ));
                    proof_assert!(unparenting(*n2, nodes.model(), *done, ids(children.model()).subsequence(0, i@ + 1)));
                    #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                    {
                        i += 1;
                    }
                }
                proof_assert! {
                    lemma_full_prefix(ids(children.model()));
                    unparenting(*n2, nodes.model(), *done, ids(children.model()))
                };
                proof_assert!(forall<k, p> n2.contains(&k) && n2[k]@.from.contains(p) && done.contains(p)
                    ==> remaining_parents.model().contains(&k) && ids(children.model()).contains(k));
                if removed_bookmark {
                    bookmarked.shift_remove(id);
                    proof_assert!(shift_removed_ok(bookmarked.model(), old.bookmarked, *t));
                }
            } else {
                let mut removed_set = guard.set_with_capacity(removed.len(), default_hasher());
                let mut i = 0;
                #[invariant(i@ <= removed.model().len())]
                #[invariant(forall<k> removed_set.model().contains(&k)
                    == ids(removed.model()).subsequence(0, i@).contains(k))]
                #[invariant(nodes.model() == *n2)]
                #[invariant(forall<k> active.model().contains(&k)
                    == (old.active.contains(&k) && !done.contains(k)))]
                #[invariant(roots.model() == *r1)]
                #[invariant(bookmarked.model() == old.bookmarked)]
                #[variant(removed.model().len() - i@)]
                while i < removed.len() {
                    proof_assert!(prefix_step_ok(ids(removed.model()), i@));
                    proof_assert!(forall<x> ids(removed.model()).subsequence(0, i@ + 1).contains(x)
                        == (ids(removed.model()).subsequence(0, i@).contains(x)
                            || x == ids(removed.model())[i@]));
                    proof_assert!(ids(removed.model())[i@] == removed.model()[i@].deep_model());
                    removed_set.insert(removed[i]);
                    #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                    {
                        i += 1;
                    }
                }
                proof_assert! {
                    lemma_full_prefix(ids(removed.model()));
                    forall<k> removed_set.model().contains(&k) == done.contains(k)
                };

                extend_surviving_keys(&mut children, &remaining_parents);
                proof_assert!(unparenting_start_ok(
                    *n2,
                    *done,
                    ids(children.model()).subsequence(0, 0)
                ));
                proof_assert!(unparenting(
                    *n2,
                    nodes.model(),
                    *done,
                    ids(children.model()).subsequence(0, 0)
                ));
                let mut i = 0;
                #[invariant(i@ <= children.model().len())]
                #[invariant(unparenting(*n2, nodes.model(), *done,
                    ids(children.model()).subsequence(0, i@)))]
                #[invariant(kept_in_order(nodes.model(), old.nodes))]
                #[invariant(forall<k> removed_set.model().contains(&k) == done.contains(k))]
                #[invariant(forall<k> active.model().contains(&k)
                    == (old.active.contains(&k) && !done.contains(k)))]
                #[invariant(roots.model() == *r1)]
                #[invariant(bookmarked.model() == old.bookmarked)]
                #[variant(children.model().len() - i@)]
                while i < children.len() {
                    proof! {
                        let before = snapshot!(nodes.model());
                        let key = snapshot!(children.model()[i@].deep_model());
                    }
                    proof_assert!(prefix_step_ok(ids(children.model()), i@));
                    proof_assert!(ids(children.model())[i@] == *key
                        && forall<x> ids(children.model()).subsequence(0, i@ + 1).contains(x)
                            == (ids(children.model()).subsequence(0, i@).contains(x) || x == *key));
                    proof! {
                        let mut cv = snapshot!(before[*key]);
                    }
                    if let Some(child) = nodes.get_mut(&children[i]) {
                        proof_assert!(before.contains(&*key) && *child == before[*key]);
                        retain_absent(&mut child.from, &removed_set);
                        proof_assert!(child@.id == before[*key]@.id
                            && child@.to == before[*key]@.to
                            && child@.active == before[*key]@.active
                            && child@.bookmarked == before[*key]@.bookmarked);
                        proof_assert!(forall<p> child@.from.contains(p)
                            == (before[*key]@.from.contains(p) && !removed_set.model().contains(&p)));
                        proof_assert!(forall<p> child@.from.contains(p)
                            == (before[*key]@.from.contains(p) && !done.contains(p)));
                        proof! {
                            cv = snapshot!(*child);
                        }
                    }
                    proof_assert!(before.contains(&*key) ==> nodes.model() == before.add(*key, *cv));
                    proof_assert!(!before.contains(&*key) ==> nodes.model() == *before);
                    proof_assert!(before.contains(&*key) ==> cv@.id == before[*key]@.id
                        && cv@.to == before[*key]@.to
                        && cv@.active == before[*key]@.active
                        && cv@.bookmarked == before[*key]@.bookmarked
                        && forall<p> cv@.from.contains(p)
                            == (before[*key]@.from.contains(p) && !done.contains(p)));
                    proof_assert!(unparenting_set_ok(*n2, *before, nodes.model(), *done,
                        ids(children.model()).subsequence(0, i@),
                        ids(children.model()).subsequence(0, i@ + 1), *key, *cv));
                    proof_assert!(kept_parents_set_ok(
                        old.nodes,
                        *before,
                        nodes.model(),
                        *key,
                        *cv
                    ));
                    proof_assert!(unparenting(*n2, nodes.model(), *done, ids(children.model()).subsequence(0, i@ + 1)));
                    #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                    {
                        i += 1;
                    }
                }
                proof_assert! {
                    lemma_full_prefix(ids(children.model()));
                    unparenting(*n2, nodes.model(), *done, ids(children.model()))
                };
                proof_assert!(forall<k, p> n2.contains(&k) && n2[k]@.from.contains(p) && done.contains(p)
                    ==> remaining_parents.model().contains(&k) && remaining_parents.model()[k]@ > 0
                        && ids(children.model()).contains(k));

                if removed_bookmark {
                    retain_absent(bookmarked, &removed_set);
                }
            }

            proof_assert!(unparenting_done_ok(
                *n1,
                *n2,
                nodes.model(),
                *done,
                ids(children.model())
            ));
            proof_assert!(pruned(*n1, nodes.model(), *done));
            proof_assert!(forall<k> bookmarked.model().contains(k)
                == (old.bookmarked.contains(k) && !done.contains(k)));
            proof_assert!(filtered(bookmarked.model(), old.bookmarked, *done));
            proof_assert!(detached(old.nodes, *n1, *t));
            proof_assert!(forall<k> roots.model().contains(k) == (old.roots.contains(k) && k != *t));
            proof_assert!(forall<k> active.model().contains(&k) == (old.active.contains(&k) && !done.contains(k)));
            proof_assert!(cascade_shape_ok(
                *old,
                *n1,
                nodes.model(),
                roots.model(),
                active.model(),
                *rank,
                *t,
                *done
            ));
            proof_assert!(cascade_shape(
                *old,
                nodes.model(),
                roots.model(),
                active.model(),
                *done
            ));
            proof_assert!(cascade_structure_ok(
                *old,
                nodes.model(),
                roots.model(),
                active.model(),
                *rank,
                *t,
                *done
            ));
            proof_assert!(structure(nodes.model(), roots.model(), active.model())
                && (forall<k> nodes.model().contains(&k)
                    ==> nodes.model()[k]@.bookmarked == old.bookmarked.contains(k))
                && forall<k> old.bookmarked.contains(k) && !done.contains(k) ==> nodes.model().contains(&k));
            proof! {
                mid = snapshot!(IndependentWeaveView {
                    nodes: nodes.model(),
                    roots: roots.model(),
                    active: active.model(),
                    bookmarked: bookmarked.model(),
                });
            }
            proof_assert!(mid.valid_structure());
            proof_assert!(removal_ok(old.nodes, *rank, *t, *done));
            proof_assert!(removal_cascade_shape_ok(*old, *mid, *done));
            proof_assert!(!removed_active ==> cascade_path_ok(*old, mid.nodes, mid.roots, mid.active, *done));
            proof_assert!(!removed_active ==> forms_path(mid.nodes, mid.active));
            proof_assert!(reactivated_ok(*mid));
            proof_assert!(forall<k> mid.nodes.contains(&k) ==> old.nodes.contains(&k) && k != *t);
            proof_assert!(keys_subset_len_ok(mid.nodes, old.nodes.remove(*t)));
            proof_assert!(mid.nodes.len() < old.nodes.len());
            proof_assert!(seq_subset_len_ok(mid.bookmarked, old.bookmarked));
            proof_assert!(mid.bookmarked.len() <= old.bookmarked.len());
            proof_assert!(fset_subset_len_ok(mid.active, old.active));
            proof_assert!(mid.active.len() <= old.active.len());
        }

        proof_assert!(forall<k> mid.nodes.contains(&k) && old.active.contains(&k)
            ==> mid.active.contains(&k));
        proof_assert!(forall<k> mid.active.contains(&k) ==> old.active.contains(&k));
        proof_assert!(!removed_active ==> longest_whole_ok(mid.nodes, mid.active));
        if removed_active {
            Self::fix_orphaned_activations(nodes, roots, active, scratchpad);
            proof_assert!(same_keys_len_ok(nodes.model(), mid.nodes));
            proof_assert!(nodes.model().len() == mid.nodes.len());
            proof_assert!(fset_subset_len_ok(active.model(), old.active));
            proof_assert!(active.model().len() <= old.active.len());
            proof_assert!(same_graph_activation_ok(nodes.model(), mid.nodes));
            proof_assert!(removal_reactivated_ok(
                *old,
                *mid,
                IndependentWeaveView {
                    nodes: nodes.model(),
                    roots: roots.model(),
                    active: active.model(),
                    bookmarked: bookmarked.model(),
                },
                *done
            ));
        }
        proof_assert!(longest_path_within(
            nodes.model(),
            mid.active,
            active.model()
        ));
        proof_assert!(longest_widen_ok(
            nodes.model(),
            mid.active,
            old.active,
            active.model()
        ));
        proof_assert!(
            removal(old.nodes, *t, *done)
                && removal_shape(
                    *old,
                    IndependentWeaveView {
                        nodes: nodes.model(),
                        roots: roots.model(),
                        active: active.model(),
                        bookmarked: bookmarked.model(),
                    },
                    *done
                )
        );

        Some(node)
    }
    #[requires(forall<f> on_removal.hist_inv(f)
        ==> forall<n: IndependentNode<K, T, S>> inv(n) ==> f.precondition((n,)))]
    #[ensures(!(^self)@.nodes.contains(&id.deep_model()))]
    #[ensures(result == self@.nodes.contains(&id.deep_model()))]
    #[ensures(result ==> (^self)@.nodes.len() < self@.nodes.len())]
    #[ensures(result ==> (^self)@.active.len() <= self@.active.len())]
    #[ensures(result ==> (^self)@.bookmarked.len() <= self@.bookmarked.len())]
    #[ensures(!result ==> (^self)@.same_keys(self@))]
    #[ensures(!result ==> same_elements((^self)@.roots, self@.roots))]
    #[ensures(!result ==> (^self)@.active == self@.active)]
    #[ensures(!result ==> same_elements((^self)@.bookmarked, self@.bookmarked))]
    // Beyond the contract: no node is added, and the active path is a longest path to a root
    // through the nodes which were active.
    #[ensures(forall<k> (^self)@.nodes.contains(&k) ==> self@.nodes.contains(&k))]
    #[ensures(result ==> longest_path_within((^self)@.nodes, self@.active, (^self)@.active))]
    // Beyond the contract: exactly the node and the nodes left without parents are removed, the
    // rest of the weave keeps its contents and order, and a failed removal changes nothing.
    #[ensures(result ==> exists<done: Seq<Id<K>>> removal(self@.nodes, id.deep_model(), done)
        && removal_shape(self@, (^self)@, done))]
    #[ensures(!result ==> (^self)@ == self@)]
    // Beyond the contract: `on_removal` is called with each removed node, as it was, once each:
    // first `id`, and then each node after all of its parents.
    #[ensures(exists<done: Seq<Id<K>>, passed: Seq<IndependentNode<K, T, S>>>
        (result ==> removal(self@.nodes, id.deep_model(), done) && no_dup(done)
            && removal_order(self@.nodes, id.deep_model(), done))
        && (!result ==> done.len() == 0)
        && as_removed(self@.nodes, done, passed)
        && fed(on_removal, passed))]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    #[allow(clippy::too_many_lines, reason = "Most of its length is proof code")]
    fn remove_tracked(
        &mut self,
        id: &K,
        mut on_removal: impl FnMut(IndependentNode<K, T, S>),
    ) -> bool {
        snapshot! { lemma_ids_snoc::<K> };
        snapshot! { lemma_snoc_contains::<Id<K>> };
        proof! {
            let old = snapshot!((*self)@);
            let t = snapshot!(id.deep_model());
            let rank = snapshot!(rank_of(old.nodes));
            let callback = snapshot!(on_removal);
            let mut states = snapshot!(Seq::singleton(on_removal));
            let mut passed = snapshot!(Seq::<IndependentNode<K, T, S>>::empty());
        }
        proof_assert!(!old.nodes.contains(&*t) ==> old.nodes.remove(*t).ext_eq(old.nodes));
        proof_assert!(in_order_refl_ok(old.roots) && in_order_refl_ok(old.bookmarked));
        proof_assert!(!old.nodes.contains(&*t) ==> IndependentWeaveView {
            nodes: old.nodes.remove(*t),
            roots: old.roots,
            active: old.active,
            bookmarked: old.bookmarked,
        } == *old);

        let Some(node) = self.nodes.remove(id) else {
            proof_assert!(as_removed(old.nodes, Seq::empty(), *passed));
            proof_assert!(calls(*states, *passed) && states[0] == on_removal);
            return false;
        };

        proof! {
            let n = snapshot!(old.nodes[*t]@);
        }
        proof_assert!(old.nodes.contains(&*t) && old.nodes.lookup(*t) == Some(node));
        proof_assert!(cascade_init_ok(*old, *t, *rank));
        proof_assert!(n.id == *t
            && (n.from.len() == 0) == old.roots.contains(*t)
            && no_dup(n.from)
            && no_dup(n.to)
            && !n.to.contains(*t)
            && (forall<k> n.to.contains(k) ==> old.nodes.contains(&k))
            && forall<k> old.nodes.contains(&k) ==> old.nodes[k]@.active == old.active.contains(&k)
                && old.nodes[k]@.bookmarked == old.bookmarked.contains(k));
        proof_assert!(node@ == *n);

        if node.from.is_empty() {
            self.roots.shift_remove(id);
            proof_assert!(shift_removed_ok(self.roots.model(), old.roots, *t));
            proof_assert!(kept_sub_ok(self.nodes.model(), old.nodes));
            proof_assert!(detaching_start_ok(old.nodes, *t, n.from));
            proof_assert!(detaching(old.nodes, self.nodes.model(), *t, n.from));
        } else {
            proof_assert!(!old.roots.contains(*t));
            proof_assert!(detaching_start_ok(old.nodes, *t, n.from.subsequence(0, 0)));
            proof_assert!(detaching(
                old.nodes,
                self.nodes.model(),
                *t,
                n.from.subsequence(0, 0)
            ));
            proof_assert!(kept_sub_ok(self.nodes.model(), old.nodes));
            let mut i = 0;
            #[invariant(i@ <= node.from.model().len())]
            #[invariant(detaching(old.nodes, self.nodes.model(), *t, n.from.subsequence(0, i@)))]
            #[invariant(kept_in_order(self.nodes.model(), old.nodes))]
            #[invariant(self.roots.model() == old.roots)]
            #[invariant(self.active.model() == old.active)]
            #[invariant(self.bookmarked.model() == old.bookmarked)]
            #[variant(node.from.model().len() - i@)]
            while i < node.from.len() {
                proof! {
                    let before = snapshot!(self.nodes.model());
                    let key = snapshot!(n.from[i@]);
                }
                proof_assert!(prefix_step_ok(n.from, i@));
                proof_assert!((forall<x> n.from.subsequence(0, i@ + 1).contains(x)
                    == (n.from.subsequence(0, i@).contains(x) || x == *key))
                    && !n.from.subsequence(0, i@).contains(*key));
                if let Some(parent) = self.nodes.get_mut(&node.from[i]) {
                    proof! {
                        let pto = snapshot!(parent@.to);
                    }
                    parent.to.shift_remove(id);
                    proof_assert!(shift_removed_ok(parent@.to, *pto, *t));
                }
                proof_assert!(forall<k> k != *key ==> self.nodes.model().lookup(k) == before.lookup(k));
                proof_assert!(self.nodes.model().contains(&*key) == before.contains(&*key));
                proof_assert!(before.contains(&*key) ==> self.nodes.model()[*key]@.id == before[*key]@.id
                    && self.nodes.model()[*key]@.from == before[*key]@.from
                    && self.nodes.model()[*key]@.active == before[*key]@.active
                    && self.nodes.model()[*key]@.bookmarked == before[*key]@.bookmarked
                    && forall<c> self.nodes.model()[*key]@.to.contains(c)
                        == (before[*key]@.to.contains(c) && c != *t));
                proof_assert!(detaching_step_ok(old.nodes, *before, self.nodes.model(), *t,
                    n.from.subsequence(0, i@), n.from.subsequence(0, i@ + 1), *key));
                proof_assert!(kept_children_step_ok(
                    old.nodes,
                    *before,
                    self.nodes.model(),
                    *key
                ));
                proof_assert!(detaching(old.nodes, self.nodes.model(), *t, n.from.subsequence(0, i@ + 1)));
                #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                {
                    i += 1;
                }
            }
            proof_assert! { lemma_full_prefix(n.from); n.from.subsequence(0, n.from.len()) == n.from };
        }
        proof_assert!(detaching_done_ok(old.nodes, self.nodes.model(), *t, n.from));
        proof_assert!(detached(old.nodes, self.nodes.model(), *t));

        proof! {
            let n1 = snapshot!(self.nodes.model());
        }
        proof_assert!(forall<k> self.roots.model().contains(k) == (old.roots.contains(k) && k != *t));
        proof_assert!(kept_in_order(*n1, old.nodes));
        proof_assert!(filtered(self.roots.model(), old.roots, Seq::singleton(*t)));

        let mut removed_active = node.active;
        let mut removed_bookmark = node.bookmarked;

        if removed_active {
            self.active.remove(id);
        }

        proof_assert!(forall<k> self.active.model().contains(&k) == (old.active.contains(&k) && k != *t));
        proof_assert!(removed_bookmark == old.bookmarked.contains(*t));
        proof_assert!(keys_subset_len_ok(self.nodes.model(), old.nodes.remove(*t)));
        proof_assert!(self.nodes.model().len() < old.nodes.len());

        if node.to.is_empty() {
            proof_assert!(self.bookmarked.model() == old.bookmarked);
            proof_assert!(!removed_bookmark ==> forall<k> self.bookmarked.model().contains(k)
                == (old.bookmarked.contains(k) && k != *t));
            proof_assert!(!removed_bookmark ==> cascade_leaf_valid_ok(
                *old,
                self.nodes.model(),
                self.roots.model(),
                self.active.model(),
                self.bookmarked.model(),
                *rank,
                *t,
            ));
            if removed_bookmark {
                proof_assert!(old.bookmarked.contains(*t));
                proof! {
                    let bi = snapshot!(such_that(|i: Int| 0 <= i && i < old.bookmarked.len()
                        && old.bookmarked[i] == *t));
                }
                proof_assert!(0 <= *bi && *bi < old.bookmarked.len() && old.bookmarked[*bi] == *t);
                proof_assert!(removed_ok(old.bookmarked, *bi));
                proof_assert!(forall<k> old.bookmarked.removed(*bi).contains(k)
                    == (old.bookmarked.contains(k) && k != *t));
                proof_assert!(cascade_leaf_valid_ok(
                    *old,
                    self.nodes.model(),
                    self.roots.model(),
                    self.active.model(),
                    old.bookmarked.removed(*bi),
                    *rank,
                    *t,
                ));
                self.bookmarked.shift_remove(id);
                proof_assert!(shift_removed_ok(
                    self.bookmarked.model(),
                    old.bookmarked,
                    *t
                ));
            }
            proof_assert!(filtered(
                self.bookmarked.model(),
                old.bookmarked,
                Seq::singleton(*t)
            ));
            proof_assert!(removal_leaf_ok(*old, (*self)@, *t));
            proof_assert!(fset_subset_len_ok(self.active.model(), old.active));
            proof_assert!(self.active.model().len() <= old.active.len());
            proof_assert!(seq_subset_len_ok(self.bookmarked.model(), old.bookmarked));
            proof_assert!(self.bookmarked.model().len() <= old.bookmarked.len());
            proof_assert!((*self)@.valid());
            proof_assert!(longest_whole_ok(self.nodes.model(), self.active.model()));
            proof_assert!(longest_widen_ok(self.nodes.model(), self.active.model(), old.active, self.active.model()));
            proof_assert!(callback.hist_inv(on_removal));
            proof! {
                let x = snapshot!(node);
                passed = snapshot!(passed.snoc(node));
            }
            on_removal(node);
            proof_assert! {
                lemma_calls_snoc(*states, Seq::empty(), *x, on_removal);
                lemma_empty_snoc(*x);
                calls(states.snoc(on_removal), *passed)
            };
            proof! {
                states = snapshot!(states.snoc(on_removal));
            }
            proof_assert!(states[0] == *callback && states[1] == on_removal);
            proof_assert!(as_removed(old.nodes, Seq::singleton(*t), *passed));
            proof_assert! {
                lemma_parents_first_single(old.nodes, *t);
                parents_first(old.nodes, Seq::singleton(*t))
            };
            proof_assert! {
                lemma_removal_order(old.nodes, *t, Seq::singleton(*t));
                removal_order(old.nodes, *t, Seq::singleton(*t))
            };
            return true;
        }

        proof! {
            let r1 = snapshot!(self.roots.model());
            let mut done = snapshot!(Seq::<Id<K>>::empty());
            let mut mid = snapshot!(*old);
        }

        let Self {
            nodes,
            roots,
            active,
            bookmarked,
            scratchpad,
            ..
        } = self;

        {
            let guard = scratchpad.guard();
            let mut stack = guard.vec();
            proof_assert!(no_dup_len_ok(n.to.snoc(*t), old.nodes));
            proof_assert! {
                lemma_no_dup_snoc(n.to, *t);
                n.to.len() + 1 <= old.nodes.len()
            };
            let mut removed = guard.vec_with_capacity(node.to.len().strict_add(1));
            let mut remaining_parents = guard.map_with_capacity(node.to.len(), default_hasher());

            proof! {
                let r0 = snapshot!(removed.model());
            }
            removed.push(*id);
            proof_assert! {
                lemma_ids_singleton(*r0, *id);
                lemma_empty_snoc(*t);
                ids(removed.model()) == Seq::<Id<K>>::empty().snoc(*t)
            };
            proof_assert!(cascade_start_ok(old.nodes, *rank, *t));
            proof_assert! {
                lemma_ids_empty(stack.model());
                cascade_inner(old.nodes, *rank, *t, Seq::empty(), *t, n.to, ids(stack.model()),
                    remaining_parents.model())
            };

            let mut i = node.to.len();
            proof_assert! { lemma_full_prefix(n.to); n.to.subsequence(0, i@) == n.to };
            #[invariant(i@ <= n.to.len())]
            #[invariant(cascade_inner(old.nodes, *rank, *t, Seq::empty(), *t, n.to.subsequence(0, i@),
                ids(stack.model()), remaining_parents.model()))]
            #[invariant(nodes.model() == *n1)]
            #[invariant(roots.model() == *r1)]
            #[invariant(forall<k> active.model().contains(&k) == (old.active.contains(&k) && k != *t))]
            #[invariant(bookmarked.model() == old.bookmarked)]
            #[invariant(ids(removed.model()) == Seq::<Id<K>>::empty().snoc(*t))]
            while i > 0 {
                #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                {
                    i -= 1;
                }
                let child = node.to[i];
                proof! {
                    let counts = snapshot!(remaining_parents.model());
                    let st = snapshot!(ids(stack.model()));
                }
                proof_assert!(cascade_count_ok(old.nodes, *rank, *t, Seq::empty(), *t, i@, *st, *counts));
                proof_assert!(old.nodes.contains(&child.deep_model())
                    && child.deep_model() != *t
                    && 0 <= left_parents(old.nodes, Seq::empty().snoc(*t), child.deep_model())
                    && (counts.contains(&child.deep_model()) ==> counts[child.deep_model()]@
                        == left_parents(old.nodes, Seq::empty().snoc(*t), child.deep_model()) + 1)
                    && (!counts.contains(&child.deep_model()) ==> old.nodes[child.deep_model()]@.from.len()
                        == left_parents(old.nodes, Seq::empty().snoc(*t), child.deep_model()) + 1));
                proof_assert!(nodes.model().contains(&child.deep_model())
                    && nodes.model()[child.deep_model()]@.from == old.nodes[child.deep_model()]@.from);
                proof_assert!(
                    nodes.model().lookup(child.deep_model())
                        == Some(nodes.model()[child.deep_model()])
                );
                let remaining = match remaining_parents.entry(child) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => entry.insert(nodes[&child].from.len()),
                };
                proof_assert!(counts.contains(&child.deep_model())
                    ==> (*remaining)@ == counts[child.deep_model()]@);
                proof_assert!(!counts.contains(&child.deep_model())
                    ==> (*remaining)@ == nodes.model()[child.deep_model()]@.from.len());
                proof_assert!((*remaining)@
                    == left_parents(old.nodes, Seq::empty().snoc(*t), child.deep_model()) + 1);
                #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                {
                    *remaining -= 1;
                }
                proof! {
                    let nv = snapshot!(*remaining);
                }
                proof_assert!(nv@ == left_parents(old.nodes, Seq::empty().snoc(*t), child.deep_model()));

                if *remaining == 0 {
                    stack.push(child);
                }
                proof_assert!(remaining_parents.model() == counts.add(child.deep_model(), *nv));
                proof_assert!(cascade_visit_ok(old.nodes, *rank, *t, Seq::empty(), *t, i@, *st, *counts, *nv));
                proof_assert!(cascade_inner(old.nodes, *rank, *t, Seq::empty(), *t, n.to.subsequence(0, i@),
                    ids(stack.model()), remaining_parents.model()));
            }
            proof_assert!(cascade_next_ok(
                old.nodes,
                *rank,
                *t,
                Seq::empty(),
                *t,
                ids(stack.model()),
                remaining_parents.model()
            ));
            proof_assert!(cascade_outer(
                old.nodes,
                *rank,
                *t,
                ids(removed.model()),
                ids(stack.model()),
                remaining_parents.model()
            ));
            proof_assert!(forall<k> ids(removed.model()).contains(k) == (k == *t));

            proof_assert!(callback.hist_inv(on_removal));
            proof! {
                let x = snapshot!(node);
                passed = snapshot!(passed.snoc(node));
            }
            on_removal(node);
            proof_assert! {
                lemma_calls_snoc(*states, Seq::empty(), *x, on_removal);
                lemma_empty_snoc(*x);
                calls(states.snoc(on_removal), *passed)
            };
            proof! {
                states = snapshot!(states.snoc(on_removal));
            }
            proof_assert!(states[0] == *callback && states[1] == on_removal);
            proof_assert!(as_removed(old.nodes, ids(removed.model()), *passed));
            proof_assert! {
                lemma_parents_first_single(old.nodes, *t);
                parents_first(old.nodes, ids(removed.model()))
            };

            #[invariant(cascade_outer(old.nodes, *rank, *t, ids(removed.model()), ids(stack.model()),
                remaining_parents.model()))]
            #[invariant(forall<k> nodes.model().contains(&k)
                == (n1.contains(&k) && !ids(removed.model()).contains(k)))]
            #[invariant(forall<k> nodes.model().contains(&k) ==> nodes.model()[k] == n1[k])]
            #[invariant(forall<k> active.model().contains(&k)
                == (old.active.contains(&k) && !ids(removed.model()).contains(k)))]
            #[invariant(!removed_active
                ==> forall<k> ids(removed.model()).contains(k) ==> !old.active.contains(&k))]
            #[invariant(!removed_bookmark
                ==> forall<k> ids(removed.model()).contains(k) ==> !old.bookmarked.contains(k))]
            #[invariant(roots.model() == *r1)]
            #[invariant(bookmarked.model() == old.bookmarked)]
            #[invariant(callback.hist_inv(on_removal))]
            #[invariant(calls(*states, *passed)
                && states[0] == *callback
                && states[passed.len()] == on_removal)]
            #[invariant(as_removed(old.nodes, ids(removed.model()), *passed))]
            #[invariant(parents_first(old.nodes, ids(removed.model())))]
            while let Some(id) = stack.pop() {
                proof! {
                    let prev = snapshot!(ids(removed.model()));
                    let rest = snapshot!(ids(stack.model()));
                }
                proof_assert!(cascade_pop_ok(
                    old.nodes,
                    *rank,
                    *t,
                    *prev,
                    *rest,
                    id.deep_model(),
                    remaining_parents.model()
                ));
                proof_assert! {
                    lemma_cascade_parents_first(
                        old.nodes,
                        *rank,
                        *t,
                        *prev,
                        *rest,
                        id.deep_model(),
                        remaining_parents.model(),
                    );
                    parents_first(old.nodes, prev.snoc(id.deep_model()))
                };
                proof_assert!(cascade_inner(old.nodes, *rank, *t, *prev, id.deep_model(),
                    old.nodes[id.deep_model()]@.to, *rest, remaining_parents.model())
                    && id.deep_model() != *t
                    && old.nodes.contains(&id.deep_model())
                    && !prev.contains(id.deep_model())
                    && prev.contains(*t)
                    && !n.from.contains(id.deep_model()));
                proof_assert!(
                    n1.contains(&id.deep_model()) && nodes.model().contains(&id.deep_model())
                );
                let node = nodes.remove(&id).unwrap();
                removed.push(id);
                proof_assert!(ids(removed.model()) == prev.snoc(id.deep_model()));
                proof_assert!(parents_first(old.nodes, ids(removed.model())));
                proof_assert!(node == n1[id.deep_model()]);
                proof_assert!(n1[id.deep_model()]@.to == old.nodes[id.deep_model()]@.to);
                proof_assert!(node@.to == old.nodes[id.deep_model()]@.to);
                proof_assert!(
                    node.active == old.active.contains(&id.deep_model())
                        && node.bookmarked == old.bookmarked.contains(id.deep_model())
                );
                proof_assert!(detached(old.nodes, *n1, *t) && kept_in_order(*n1, old.nodes));
                proof_assert!(node@ == old.nodes[id.deep_model()]@
                    && node.contents == old.nodes[id.deep_model()].contents);

                removed_bookmark |= node.bookmarked;
                if node.active {
                    active.remove(&id);
                    removed_active = true;
                }

                proof! {
                    let ns = snapshot!(nodes.model());
                    let cur = snapshot!(id.deep_model());
                }
                proof_assert!(forall<k> prev.snoc(*cur).contains(k) == (prev.contains(k) || k == *cur));
                proof_assert!(forall<k> ns.contains(&k) == (n1.contains(&k) && !prev.snoc(*cur).contains(k)));
                proof_assert!(forall<k> ns.contains(&k) ==> ns[k] == n1[k]);
                proof_assert!(forall<k> n1.contains(&k) ==> n1[k]@.from == old.nodes[k]@.from);

                let mut i = node.to.len();
                proof_assert! {
                    lemma_full_prefix(node.to.model());
                    old.nodes[*cur]@.to.subsequence(0, i@) == old.nodes[*cur]@.to
                };
                #[invariant(i@ <= node.to.model().len())]
                #[invariant(node.to.model() == old.nodes[*cur]@.to)]
                #[invariant(cascade_inner(old.nodes, *rank, *t, *prev, *cur,
                    old.nodes[*cur]@.to.subsequence(0, i@), ids(stack.model()),
                    remaining_parents.model()))]
                #[invariant(nodes.model() == *ns)]
                #[invariant(forall<k> active.model().contains(&k)
                    == (old.active.contains(&k) && !ids(removed.model()).contains(k)))]
                #[invariant(roots.model() == *r1)]
                #[invariant(bookmarked.model() == old.bookmarked)]
                #[invariant(ids(removed.model()) == prev.snoc(*cur))]
                while i > 0 {
                    #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                    {
                        i -= 1;
                    }
                    let child = node.to[i];
                    proof! {
                        let counts = snapshot!(remaining_parents.model());
                        let st = snapshot!(ids(stack.model()));
                    }
                    proof_assert!(cascade_count_ok(old.nodes, *rank, *t, *prev, *cur, i@, *st, *counts));
                    proof_assert!(old.nodes.contains(&child.deep_model())
                        && child.deep_model() != *t
                        && !prev.snoc(*cur).contains(child.deep_model())
                        && 0 <= left_parents(old.nodes, prev.snoc(*cur), child.deep_model())
                        && (counts.contains(&child.deep_model()) ==> counts[child.deep_model()]@
                            == left_parents(old.nodes, prev.snoc(*cur), child.deep_model()) + 1)
                        && (!counts.contains(&child.deep_model()) ==> old.nodes[child.deep_model()]@.from.len()
                            == left_parents(old.nodes, prev.snoc(*cur), child.deep_model()) + 1));
                    proof_assert!(nodes.model().contains(&child.deep_model())
                        && nodes.model()[child.deep_model()]@.from == old.nodes[child.deep_model()]@.from);
                    proof_assert!(
                        nodes.model().lookup(child.deep_model())
                            == Some(nodes.model()[child.deep_model()])
                    );
                    let remaining = match remaining_parents.entry(child) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => entry.insert(nodes[&child].from.len()),
                    };
                    proof_assert!(counts.contains(&child.deep_model())
                        ==> (*remaining)@ == counts[child.deep_model()]@);
                    proof_assert!(!counts.contains(&child.deep_model())
                        ==> (*remaining)@ == nodes.model()[child.deep_model()]@.from.len());
                    proof_assert!((*remaining)@
                        == left_parents(old.nodes, prev.snoc(*cur), child.deep_model()) + 1);
                    #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                    {
                        *remaining -= 1;
                    }
                    proof! {
                        let nv = snapshot!(*remaining);
                    }
                    proof_assert!(nv@ == left_parents(old.nodes, prev.snoc(*cur), child.deep_model()));

                    if *remaining == 0 {
                        stack.push(child);
                    }
                    proof_assert!(remaining_parents.model() == counts.add(child.deep_model(), *nv));
                    proof_assert!(cascade_visit_ok(old.nodes, *rank, *t, *prev, *cur, i@, *st, *counts, *nv));
                    proof_assert!(cascade_inner(old.nodes, *rank, *t, *prev, *cur,
                        old.nodes[*cur]@.to.subsequence(0, i@), ids(stack.model()),
                        remaining_parents.model()));
                }
                proof_assert!(cascade_next_ok(
                    old.nodes,
                    *rank,
                    *t,
                    *prev,
                    *cur,
                    ids(stack.model()),
                    remaining_parents.model()
                ));
                proof_assert!(cascade_outer(
                    old.nodes,
                    *rank,
                    *t,
                    ids(removed.model()),
                    ids(stack.model()),
                    remaining_parents.model()
                ));

                proof! {
                    let x = snapshot!(node);
                    let args = snapshot!(*passed);
                    passed = snapshot!(passed.snoc(node));
                }
                on_removal(node);
                proof_assert! {
                    lemma_calls_snoc(*states, *args, *x, on_removal);
                    calls(states.snoc(on_removal), *passed)
                };
                proof_assert! {
                    lemma_as_removed_snoc(old.nodes, *prev, *args, *cur, *x);
                    as_removed(old.nodes, ids(removed.model()), *passed)
                };
                proof! {
                    states = snapshot!(states.snoc(on_removal));
                }
            }

            proof! {
                done = snapshot!(ids(removed.model()));
            }
            proof_assert!(as_removed(old.nodes, *done, *passed));
            proof_assert! {
                lemma_cascade_removal_order(
                    old.nodes,
                    *rank,
                    *t,
                    *done,
                    ids(stack.model()),
                    remaining_parents.model(),
                );
                removal_order(old.nodes, *t, *done)
            };
            proof_assert! { lemma_ids_empty(stack.model()); ids(stack.model()) == Seq::empty() };
            proof_assert!(cascade_end_ok(
                old.nodes,
                *rank,
                *t,
                *done,
                remaining_parents.model()
            ));
            proof_assert!(cascade_done(old.nodes, *rank, *t, *done)
                && done.contains(*t)
                && (done.len() == 1 ==> forall<k> done.contains(k) == (k == *t))
                && (forall<k> remaining_parents.model().contains(&k)
                    && remaining_parents.model()[k]@ == 0 ==> done.contains(k))
                && forall<k, p> old.nodes.contains(&k) && old.nodes[k]@.from.contains(p) && done.contains(p)
                    ==> remaining_parents.model().contains(&k));

            if removed_active && !any_active_survivor(&remaining_parents, active) {
                removed_active = false;
            }
            proof_assert!(!removed_active ==> (forall<k> done.contains(k) ==> !old.active.contains(&k))
                || !exists<k> remaining_parents.model().contains(&k) && remaining_parents.model()[k]@ > 0
                    && old.active.contains(&k) && !done.contains(k));
            proof_assert!(forall<k> old.active.contains(&k) ==> old.nodes.contains(&k));
            proof_assert!(!removed_active
                ==> active_kept_ok(old.nodes, old.active, *done, remaining_parents.model()));

            let mut children = guard.vec();

            proof! {
                let n2 = snapshot!(nodes.model());
            }
            proof_assert!(forall<k> n2.contains(&k) == (n1.contains(&k) && !done.contains(k)));
            proof_assert!(forall<k> n2.contains(&k) ==> n2[k] == n1[k] && old.nodes.contains(&k));

            proof_assert!(forall<k> n2.contains(&k) ==> n2[k]@.from == old.nodes[k]@.from);
            proof_assert!(kept_in_order(*n2, old.nodes));

            if removed.len() == 1 {
                proof_assert!(forall<k> done.contains(k) == (k == *t));
                extend_keys(&mut children, &remaining_parents);
                proof_assert!(unparenting_start_ok(
                    *n2,
                    *done,
                    ids(children.model()).subsequence(0, 0)
                ));
                proof_assert!(unparenting(
                    *n2,
                    nodes.model(),
                    *done,
                    ids(children.model()).subsequence(0, 0)
                ));
                let mut i = 0;
                #[invariant(i@ <= children.model().len())]
                #[invariant(unparenting(*n2, nodes.model(), *done,
                    ids(children.model()).subsequence(0, i@)))]
                #[invariant(kept_in_order(nodes.model(), old.nodes))]
                #[invariant(forall<k> active.model().contains(&k)
                    == (old.active.contains(&k) && !done.contains(k)))]
                #[invariant(roots.model() == *r1)]
                #[invariant(bookmarked.model() == old.bookmarked)]
                #[variant(children.model().len() - i@)]
                while i < children.len() {
                    proof! {
                        let before = snapshot!(nodes.model());
                        let key = snapshot!(children.model()[i@].deep_model());
                    }
                    proof_assert!(prefix_step_ok(ids(children.model()), i@));
                    proof_assert!(ids(children.model())[i@] == *key
                        && forall<x> ids(children.model()).subsequence(0, i@ + 1).contains(x)
                            == (ids(children.model()).subsequence(0, i@).contains(x) || x == *key));
                    proof! {
                        let mut cv = snapshot!(before[*key]);
                    }
                    if let Some(child) = nodes.get_mut(&children[i]) {
                        proof_assert!(before.contains(&*key) && *child == before[*key]);
                        proof! {
                            let cfrom = snapshot!(child@.from);
                        }
                        child.from.shift_remove(id);
                        proof_assert!(shift_removed_ok(child@.from, *cfrom, *t));
                        proof_assert!(child@.id == before[*key]@.id
                            && child@.to == before[*key]@.to
                            && child@.active == before[*key]@.active
                            && child@.bookmarked == before[*key]@.bookmarked);
                        proof_assert!(forall<p> child@.from.contains(p)
                            == (before[*key]@.from.contains(p) && !done.contains(p)));
                        proof! {
                            cv = snapshot!(*child);
                        }
                    }
                    proof_assert!(before.contains(&*key) ==> nodes.model() == before.add(*key, *cv));
                    proof_assert!(!before.contains(&*key) ==> nodes.model() == *before);
                    proof_assert!(before.contains(&*key) ==> cv@.id == before[*key]@.id
                        && cv@.to == before[*key]@.to
                        && cv@.active == before[*key]@.active
                        && cv@.bookmarked == before[*key]@.bookmarked
                        && forall<p> cv@.from.contains(p)
                            == (before[*key]@.from.contains(p) && !done.contains(p)));
                    proof_assert!(unparenting_set_ok(*n2, *before, nodes.model(), *done,
                        ids(children.model()).subsequence(0, i@),
                        ids(children.model()).subsequence(0, i@ + 1), *key, *cv));
                    proof_assert!(kept_parents_set_ok(
                        old.nodes,
                        *before,
                        nodes.model(),
                        *key,
                        *cv
                    ));
                    proof_assert!(unparenting(*n2, nodes.model(), *done, ids(children.model()).subsequence(0, i@ + 1)));
                    #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                    {
                        i += 1;
                    }
                }
                proof_assert! {
                    lemma_full_prefix(ids(children.model()));
                    unparenting(*n2, nodes.model(), *done, ids(children.model()))
                };
                proof_assert!(forall<k, p> n2.contains(&k) && n2[k]@.from.contains(p) && done.contains(p)
                    ==> remaining_parents.model().contains(&k) && ids(children.model()).contains(k));
                if removed_bookmark {
                    bookmarked.shift_remove(id);
                    proof_assert!(shift_removed_ok(bookmarked.model(), old.bookmarked, *t));
                }
            } else {
                let mut removed_set = guard.set_with_capacity(removed.len(), default_hasher());
                let mut i = 0;
                #[invariant(i@ <= removed.model().len())]
                #[invariant(forall<k> removed_set.model().contains(&k)
                    == ids(removed.model()).subsequence(0, i@).contains(k))]
                #[invariant(nodes.model() == *n2)]
                #[invariant(forall<k> active.model().contains(&k)
                    == (old.active.contains(&k) && !done.contains(k)))]
                #[invariant(roots.model() == *r1)]
                #[invariant(bookmarked.model() == old.bookmarked)]
                #[variant(removed.model().len() - i@)]
                while i < removed.len() {
                    proof_assert!(prefix_step_ok(ids(removed.model()), i@));
                    proof_assert!(forall<x> ids(removed.model()).subsequence(0, i@ + 1).contains(x)
                        == (ids(removed.model()).subsequence(0, i@).contains(x)
                            || x == ids(removed.model())[i@]));
                    proof_assert!(ids(removed.model())[i@] == removed.model()[i@].deep_model());
                    removed_set.insert(removed[i]);
                    #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                    {
                        i += 1;
                    }
                }
                proof_assert! {
                    lemma_full_prefix(ids(removed.model()));
                    forall<k> removed_set.model().contains(&k) == done.contains(k)
                };

                extend_surviving_keys(&mut children, &remaining_parents);
                proof_assert!(unparenting_start_ok(
                    *n2,
                    *done,
                    ids(children.model()).subsequence(0, 0)
                ));
                proof_assert!(unparenting(
                    *n2,
                    nodes.model(),
                    *done,
                    ids(children.model()).subsequence(0, 0)
                ));
                let mut i = 0;
                #[invariant(i@ <= children.model().len())]
                #[invariant(unparenting(*n2, nodes.model(), *done,
                    ids(children.model()).subsequence(0, i@)))]
                #[invariant(kept_in_order(nodes.model(), old.nodes))]
                #[invariant(forall<k> removed_set.model().contains(&k) == done.contains(k))]
                #[invariant(forall<k> active.model().contains(&k)
                    == (old.active.contains(&k) && !done.contains(k)))]
                #[invariant(roots.model() == *r1)]
                #[invariant(bookmarked.model() == old.bookmarked)]
                #[variant(children.model().len() - i@)]
                while i < children.len() {
                    proof! {
                        let before = snapshot!(nodes.model());
                        let key = snapshot!(children.model()[i@].deep_model());
                    }
                    proof_assert!(prefix_step_ok(ids(children.model()), i@));
                    proof_assert!(ids(children.model())[i@] == *key
                        && forall<x> ids(children.model()).subsequence(0, i@ + 1).contains(x)
                            == (ids(children.model()).subsequence(0, i@).contains(x) || x == *key));
                    proof! {
                        let mut cv = snapshot!(before[*key]);
                    }
                    if let Some(child) = nodes.get_mut(&children[i]) {
                        proof_assert!(before.contains(&*key) && *child == before[*key]);
                        retain_absent(&mut child.from, &removed_set);
                        proof_assert!(child@.id == before[*key]@.id
                            && child@.to == before[*key]@.to
                            && child@.active == before[*key]@.active
                            && child@.bookmarked == before[*key]@.bookmarked);
                        proof_assert!(forall<p> child@.from.contains(p)
                            == (before[*key]@.from.contains(p) && !removed_set.model().contains(&p)));
                        proof_assert!(forall<p> child@.from.contains(p)
                            == (before[*key]@.from.contains(p) && !done.contains(p)));
                        proof! {
                            cv = snapshot!(*child);
                        }
                    }
                    proof_assert!(before.contains(&*key) ==> nodes.model() == before.add(*key, *cv));
                    proof_assert!(!before.contains(&*key) ==> nodes.model() == *before);
                    proof_assert!(before.contains(&*key) ==> cv@.id == before[*key]@.id
                        && cv@.to == before[*key]@.to
                        && cv@.active == before[*key]@.active
                        && cv@.bookmarked == before[*key]@.bookmarked
                        && forall<p> cv@.from.contains(p)
                            == (before[*key]@.from.contains(p) && !done.contains(p)));
                    proof_assert!(unparenting_set_ok(*n2, *before, nodes.model(), *done,
                        ids(children.model()).subsequence(0, i@),
                        ids(children.model()).subsequence(0, i@ + 1), *key, *cv));
                    proof_assert!(kept_parents_set_ok(
                        old.nodes,
                        *before,
                        nodes.model(),
                        *key,
                        *cv
                    ));
                    proof_assert!(unparenting(*n2, nodes.model(), *done, ids(children.model()).subsequence(0, i@ + 1)));
                    #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                    {
                        i += 1;
                    }
                }
                proof_assert! {
                    lemma_full_prefix(ids(children.model()));
                    unparenting(*n2, nodes.model(), *done, ids(children.model()))
                };
                proof_assert!(forall<k, p> n2.contains(&k) && n2[k]@.from.contains(p) && done.contains(p)
                    ==> remaining_parents.model().contains(&k) && remaining_parents.model()[k]@ > 0
                        && ids(children.model()).contains(k));

                if removed_bookmark {
                    retain_absent(bookmarked, &removed_set);
                }
            }

            proof_assert!(unparenting_done_ok(
                *n1,
                *n2,
                nodes.model(),
                *done,
                ids(children.model())
            ));
            proof_assert!(pruned(*n1, nodes.model(), *done));
            proof_assert!(forall<k> bookmarked.model().contains(k)
                == (old.bookmarked.contains(k) && !done.contains(k)));
            proof_assert!(filtered(bookmarked.model(), old.bookmarked, *done));
            proof_assert!(detached(old.nodes, *n1, *t));
            proof_assert!(forall<k> roots.model().contains(k) == (old.roots.contains(k) && k != *t));
            proof_assert!(forall<k> active.model().contains(&k) == (old.active.contains(&k) && !done.contains(k)));
            proof_assert!(cascade_shape_ok(
                *old,
                *n1,
                nodes.model(),
                roots.model(),
                active.model(),
                *rank,
                *t,
                *done
            ));
            proof_assert!(cascade_shape(
                *old,
                nodes.model(),
                roots.model(),
                active.model(),
                *done
            ));
            proof_assert!(cascade_structure_ok(
                *old,
                nodes.model(),
                roots.model(),
                active.model(),
                *rank,
                *t,
                *done
            ));
            proof_assert!(structure(nodes.model(), roots.model(), active.model())
                && (forall<k> nodes.model().contains(&k)
                    ==> nodes.model()[k]@.bookmarked == old.bookmarked.contains(k))
                && forall<k> old.bookmarked.contains(k) && !done.contains(k) ==> nodes.model().contains(&k));
            proof_assert!(no_dup(bookmarked.model())
                && (forall<k> bookmarked.model().contains(k) ==> nodes.model().contains(&k))
                && forall<k> nodes.model().contains(&k)
                    ==> nodes.model()[k]@.bookmarked == bookmarked.model().contains(k));
            proof! {
                mid = snapshot!(IndependentWeaveView {
                    nodes: nodes.model(),
                    roots: roots.model(),
                    active: active.model(),
                    bookmarked: bookmarked.model(),
                });
            }
            proof_assert!(mid.valid_structure());
            proof_assert!(removal_ok(old.nodes, *rank, *t, *done));
            proof_assert!(removal_cascade_shape_ok(*old, *mid, *done));
            proof_assert!(!removed_active ==> cascade_path_ok(*old, mid.nodes, mid.roots, mid.active, *done));
            proof_assert!(!removed_active ==> forms_path(mid.nodes, mid.active));
            proof_assert!(reactivated_ok(*mid));
            proof_assert!(forall<k> mid.nodes.contains(&k) ==> old.nodes.contains(&k) && k != *t);
            proof_assert!(keys_subset_len_ok(mid.nodes, old.nodes.remove(*t)));
            proof_assert!(mid.nodes.len() < old.nodes.len());
            proof_assert!(seq_subset_len_ok(mid.bookmarked, old.bookmarked));
            proof_assert!(mid.bookmarked.len() <= old.bookmarked.len());
            proof_assert!(fset_subset_len_ok(mid.active, old.active));
            proof_assert!(mid.active.len() <= old.active.len());
        }

        proof_assert!(forall<k> mid.nodes.contains(&k) && old.active.contains(&k)
            ==> mid.active.contains(&k));
        proof_assert!(forall<k> mid.active.contains(&k) ==> old.active.contains(&k));
        proof_assert!(!removed_active ==> longest_whole_ok(mid.nodes, mid.active));
        if removed_active {
            Self::fix_orphaned_activations(nodes, roots, active, scratchpad);
            proof_assert!(same_keys_len_ok(nodes.model(), mid.nodes));
            proof_assert!(nodes.model().len() == mid.nodes.len());
            proof_assert!(fset_subset_len_ok(active.model(), old.active));
            proof_assert!(active.model().len() <= old.active.len());
            proof_assert!(same_graph_activation_ok(nodes.model(), mid.nodes));
            proof_assert!(removal_reactivated_ok(
                *old,
                *mid,
                IndependentWeaveView {
                    nodes: nodes.model(),
                    roots: roots.model(),
                    active: active.model(),
                    bookmarked: bookmarked.model(),
                },
                *done
            ));
        }
        proof_assert!(longest_path_within(
            nodes.model(),
            mid.active,
            active.model()
        ));
        proof_assert!(longest_widen_ok(
            nodes.model(),
            mid.active,
            old.active,
            active.model()
        ));
        proof_assert!(
            removal(old.nodes, *t, *done)
                && removal_shape(
                    *old,
                    IndependentWeaveView {
                        nodes: nodes.model(),
                        roots: roots.model(),
                        active: active.model(),
                        bookmarked: bookmarked.model(),
                    },
                    *done
                )
        );
        proof_assert!(no_dup(*done) && as_removed(old.nodes, *done, *passed));
        proof_assert!(removal_order(old.nodes, *t, *done));
        proof_assert!(
            calls(*states, *passed) && states[0] == *callback && states[passed.len()] == on_removal
        );

        true
    }
    #[check(terminates)]
    #[ensures((^self)@.nodes.len() == 0)]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn clear(&mut self) {
        snapshot! { lemma_empty::<K, T, S> };

        self.nodes.clear();
        self.roots.clear();
        self.active.clear();
        self.bookmarked.clear();
    }
}

impl<K, T, M, S> IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    /// Validates that the weave is internally consistent.
    #[check(terminates)]
    #[ensures(result == self@.valid())]
    pub fn validate(&self) -> bool {
        validate(&self.nodes, &self.roots, &self.active, &self.bookmarked)
    }
}

impl<K, T, M, S> MetadataWeave<K, IndependentNode<K, T, S>, T, M> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    #[check(terminates)]
    #[ensures(*result == self.metadata)]
    fn metadata(&self) -> &M {
        &self.metadata
    }
    #[inline]
    #[requires(forall<m: &mut M> inv(m) ==> callback.precondition((m,)))]
    #[ensures((^self)@ == self@)]
    // Beyond the contract: the callback is called on the metadata, and leaves it as the weave's new
    // metadata.
    #[ensures(exists<m: &mut M> *m == self.metadata && ^m == (^self).metadata
        && callback.postcondition_once((m,), result))]
    fn metadata_mut<O>(&mut self, callback: impl FnOnce(&mut M) -> O) -> O {
        callback(&mut self.metadata)
    }
}

impl<K, T, M, S> BookmarkableWeave<K, IndependentNode<K, T, S>, T> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type Bookmarks = IndexSet<K, S>;

    #[inline]
    #[check(terminates)]
    #[ensures(result.model() == self@.bookmarked)]
    fn bookmarks(&self) -> &Self::Bookmarks {
        &self.bookmarked
    }
    #[inline]
    #[check(terminates)]
    #[ensures(result == self@.bookmarked.contains(id.deep_model()))]
    fn contains_bookmark(&self, id: &K) -> bool {
        self.bookmarked.contains(id)
    }
    #[check(terminates)]
    #[ensures(result ==> value == (^self)@.bookmarked.contains(id.deep_model()))]
    #[ensures(!result ==> (^self)@.bookmarked == self@.bookmarked)]
    #[ensures(result == self@.nodes.contains(&id.deep_model()))]
    #[ensures((^self)@.same_keys(self@))]
    #[ensures((^self)@.roots == self@.roots)]
    #[ensures((^self)@.active == self@.active)]
    // Beyond the contract: nothing else changes.
    #[ensures(forall<k> k != id.deep_model() ==> (^self)@.nodes.lookup(k) == self@.nodes.lookup(k))]
    #[ensures(result ==> (^self)@.nodes[id.deep_model()].contents == self@.nodes[id.deep_model()].contents
        && (^self)@.nodes[id.deep_model()]@.from == self@.nodes[id.deep_model()]@.from
        && (^self)@.nodes[id.deep_model()]@.to == self@.nodes[id.deep_model()]@.to
        && (^self)@.nodes[id.deep_model()]@.active == self@.nodes[id.deep_model()]@.active)]
    // Beyond the contract: a new bookmark comes last, and the other bookmarks keep their order.
    #[ensures(result
        ==> bookmark_set((^self)@.bookmarked, self@.bookmarked, id.deep_model(), value))]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn set_bookmarked(&mut self, id: &K, value: bool) -> bool {
        snapshot! { lemma_rebookmarked((*self)@, id.deep_model(), value) };

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
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[requires(forall<f> cmp.hist_inv(f)
        ==> forall<a: &IndependentNode<K, T, S>, b: &IndependentNode<K, T, S>>
            inv(a) && inv(b) ==> f.precondition((a, b)))]
    #[ensures(result == self@.nodes.contains(&id.deep_model()))]
    #[ensures(result ==> same_elements(
        (^self)@.nodes[id.deep_model()]@.to,
        self@.nodes[id.deep_model()]@.to,
    ))]
    #[ensures((^self)@.same_keys(self@))]
    #[ensures((^self)@.roots == self@.roots)]
    #[ensures((^self)@.active == self@.active)]
    #[ensures((^self)@.bookmarked == self@.bookmarked)]
    // Beyond the contract: the children are sorted stably, and nothing else changes.
    #[ensures(result ==> forall<le: Mapping<IndependentNode<K, T, S>, Mapping<IndependentNode<K, T, S>, bool>>>
        total_preorder(le) && compares_by(cmp, identity(), le)
        ==> sorted_stably((^self)@.nodes[id.deep_model()]@.to, self@.nodes[id.deep_model()]@.to, order_by(le, value_of(self@.nodes))))]
    #[ensures(forall<k> k != id.deep_model() ==> (^self)@.nodes.lookup(k) == self@.nodes.lookup(k))]
    #[ensures(result ==> (^self)@.nodes[id.deep_model()]@.id == self@.nodes[id.deep_model()]@.id
        && (^self)@.nodes[id.deep_model()]@.from == self@.nodes[id.deep_model()]@.from
        && (^self)@.nodes[id.deep_model()]@.active == self@.nodes[id.deep_model()]@.active
        && (^self)@.nodes[id.deep_model()]@.bookmarked == self@.nodes[id.deep_model()]@.bookmarked
        && (^self)@.nodes[id.deep_model()].contents == self@.nodes[id.deep_model()].contents)]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn sort_children_by(
        &mut self,
        id: &K,
        mut cmp: impl FnMut(&IndependentNode<K, T, S>, &IndependentNode<K, T, S>) -> Ordering,
    ) -> bool {
        snapshot! { lemma_reflagged((*self)@) };
        proof! {
            let old = snapshot!((*self)@);
        }

        if let Some(node) = self.nodes.get_mut(id) {
            let mut set = mem::take(&mut node.to);
            proof! {
                let unsorted = snapshot!(set.model());
                let taken = snapshot!(self.nodes.model());
            }

            sort_by_nodes(&mut set, &self.nodes, &mut self.scratchpad, &mut cmp);
            proof_assert!(forall<c> unsorted.contains(c) ==> c != id.deep_model()
                && taken[c] == old.nodes[c]);
            proof_assert!(forall<le: Mapping<IndependentNode<K, T, S>, Mapping<IndependentNode<K, T, S>, bool>>>
            sorted_stably(set.model(), *unsorted, order_by(le, value_of(*taken)))
            ==> {
                lemma_sorted_agree(
                    set.model(),
                    *unsorted,
                    order_by(le, value_of(*taken)),
                    order_by(le, value_of(old.nodes)),
                );
                sorted_stably(set.model(), *unsorted, order_by(le, value_of(old.nodes)))
            });

            self.nodes.get_mut(id).unwrap().to = set;

            true
        } else {
            false
        }
    }
    #[requires(forall<f> cmp.hist_inv(f) ==> forall<a: &K, b: &K> inv(a) && inv(b) ==> f.precondition((a, b)))]
    #[ensures(result == self@.nodes.contains(&id.deep_model()))]
    #[ensures(result ==> same_elements(
        (^self)@.nodes[id.deep_model()]@.to,
        self@.nodes[id.deep_model()]@.to,
    ))]
    #[ensures((^self)@.same_keys(self@))]
    #[ensures((^self)@.roots == self@.roots)]
    #[ensures((^self)@.active == self@.active)]
    #[ensures((^self)@.bookmarked == self@.bookmarked)]
    // Beyond the contract: the children are sorted stably, and nothing else changes.
    #[ensures(result ==> forall<le: Mapping<Id<K>, Mapping<Id<K>, bool>>>
        total_preorder(le) && compares_by(cmp, id_of::<K>(), le)
        ==> sorted_stably((^self)@.nodes[id.deep_model()]@.to, self@.nodes[id.deep_model()]@.to, le))]
    #[ensures(forall<k> k != id.deep_model() ==> (^self)@.nodes.lookup(k) == self@.nodes.lookup(k))]
    #[ensures(result ==> (^self)@.nodes[id.deep_model()]@.id == self@.nodes[id.deep_model()]@.id
        && (^self)@.nodes[id.deep_model()]@.from == self@.nodes[id.deep_model()]@.from
        && (^self)@.nodes[id.deep_model()]@.active == self@.nodes[id.deep_model()]@.active
        && (^self)@.nodes[id.deep_model()]@.bookmarked == self@.nodes[id.deep_model()]@.bookmarked
        && (^self)@.nodes[id.deep_model()].contents == self@.nodes[id.deep_model()].contents)]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn sort_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool {
        snapshot! { lemma_reflagged((*self)@) };

        if let Some(node) = self.nodes.get_mut(id) {
            node.to.sort_by(cmp);

            true
        } else {
            false
        }
    }
    #[requires(forall<f> cmp.hist_inv(f)
        ==> forall<a: &IndependentNode<K, T, S>, b: &IndependentNode<K, T, S>>
            inv(a) && inv(b) ==> f.precondition((a, b)))]
    #[ensures(same_elements((^self)@.roots, self@.roots))]
    #[ensures((^self)@.nodes == self@.nodes)]
    #[ensures((^self)@.active == self@.active)]
    #[ensures((^self)@.bookmarked == self@.bookmarked)]
    // Beyond the contract: the roots are sorted stably.
    #[ensures(forall<le: Mapping<IndependentNode<K, T, S>, Mapping<IndependentNode<K, T, S>, bool>>>
        total_preorder(le) && compares_by(cmp, identity(), le)
        ==> sorted_stably((^self)@.roots, self@.roots, order_by(le, value_of(self@.nodes))))]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn sort_roots_by(
        &mut self,
        mut cmp: impl FnMut(&IndependentNode<K, T, S>, &IndependentNode<K, T, S>) -> Ordering,
    ) {
        snapshot! { lemma_reflagged((*self)@) };

        sort_by_nodes(&mut self.roots, &self.nodes, &mut self.scratchpad, &mut cmp);
    }
    #[requires(forall<f> cmp.hist_inv(f) ==> forall<a: &K, b: &K> inv(a) && inv(b) ==> f.precondition((a, b)))]
    #[ensures(same_elements((^self)@.roots, self@.roots))]
    #[ensures((^self)@.nodes == self@.nodes)]
    #[ensures((^self)@.active == self@.active)]
    #[ensures((^self)@.bookmarked == self@.bookmarked)]
    // Beyond the contract: the roots are sorted stably.
    #[ensures(forall<le: Mapping<Id<K>, Mapping<Id<K>, bool>>>
        total_preorder(le) && compares_by(cmp, id_of::<K>(), le)
        ==> sorted_stably((^self)@.roots, self@.roots, le))]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.roots.sort_by(cmp);
    }
}

impl<K, T, M, S> SortableBookmarkableWeave<K, IndependentNode<K, T, S>, T>
    for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[requires(forall<f> cmp.hist_inv(f)
        ==> forall<a: &IndependentNode<K, T, S>, b: &IndependentNode<K, T, S>>
            inv(a) && inv(b) ==> f.precondition((a, b)))]
    #[ensures(same_elements((^self)@.bookmarked, self@.bookmarked))]
    #[ensures((^self)@.nodes == self@.nodes)]
    #[ensures((^self)@.roots == self@.roots)]
    #[ensures((^self)@.active == self@.active)]
    // Beyond the contract: the bookmarks are sorted stably.
    #[ensures(forall<le: Mapping<IndependentNode<K, T, S>, Mapping<IndependentNode<K, T, S>, bool>>>
        total_preorder(le) && compares_by(cmp, identity(), le)
        ==> sorted_stably((^self)@.bookmarked, self@.bookmarked, order_by(le, value_of(self@.nodes))))]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn sort_bookmarks_by(
        &mut self,
        mut cmp: impl FnMut(&IndependentNode<K, T, S>, &IndependentNode<K, T, S>) -> Ordering,
    ) {
        snapshot! { lemma_reflagged((*self)@) };

        sort_by_nodes(
            &mut self.bookmarked,
            &self.nodes,
            &mut self.scratchpad,
            &mut cmp,
        );
    }
    #[requires(forall<f> cmp.hist_inv(f) ==> forall<a: &K, b: &K> inv(a) && inv(b) ==> f.precondition((a, b)))]
    #[ensures(same_elements((^self)@.bookmarked, self@.bookmarked))]
    #[ensures((^self)@.nodes == self@.nodes)]
    #[ensures((^self)@.roots == self@.roots)]
    #[ensures((^self)@.active == self@.active)]
    // Beyond the contract: the bookmarks are sorted stably.
    #[ensures(forall<le: Mapping<Id<K>, Mapping<Id<K>, bool>>>
        total_preorder(le) && compares_by(cmp, id_of::<K>(), le)
        ==> sorted_stably((^self)@.bookmarked, self@.bookmarked, le))]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn sort_bookmarks_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.bookmarked.sort_by(cmp);
    }
}

impl<K, T, M, S> ActivePathWeave<K, IndependentNode<K, T, S>, T> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type Active = HashSet<K, S>;

    #[inline]
    #[check(terminates)]
    #[ensures(result.model() == self@.active)]
    fn active(&self) -> &Self::Active {
        &self.active
    }
    #[ensures((^self)@.same_keys(self@))]
    #[ensures(same_elements((^self)@.roots, self@.roots))]
    #[ensures(same_elements((^self)@.bookmarked, self@.bookmarked))]
    // Beyond the contract: only which nodes are active changes.
    #[ensures(same_graph((^self)@.nodes, self@.nodes))]
    #[ensures((^self)@.roots == self@.roots)]
    #[ensures((^self)@.bookmarked == self@.bookmarked)]
    // Beyond the contract: the active path becomes one of the longest paths to a root through the
    // nodes which `active` lists.
    #[ensures(exists<listed: Seq<K>, requested: FSet<Id<K>>> iterates(active, listed)
        && (forall<k> requested.contains(&k) == (ids(listed).contains(k) && self@.nodes.contains(&k)))
        && longest_path_within(self@.nodes, requested, (^self)@.active))]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn set_active_path(&mut self, mut active: impl VerifiableIterator<Item = K>) {
        snapshot! { lemma_reactivated((*self)@) };
        snapshot! { lemma_ids_snoc::<K> };
        snapshot! { lemma_snoc_contains::<Id<K>> };
        proof! {
            let old = snapshot!((*self)@);
            let start = snapshot!(active);
            let mut listed = snapshot!(Seq::<K>::empty());
        }

        deactivate_all(&mut self.nodes, &mut self.active);
        proof_assert! { lemma_ids_empty(*listed); ids(*listed) == Seq::empty() };

        #[invariant(start.produces(*listed, active))]
        #[invariant(same_graph(self.nodes.model(), old.nodes))]
        #[invariant(forall<k> self.active.model().contains(&k)
            == (ids(*listed).contains(k) && old.nodes.contains(&k)))]
        #[invariant(forall<k> old.nodes.contains(&k)
            ==> self.nodes.model()[k]@.active == self.active.model().contains(&k))]
        loop {
            let iter = &mut active;
            proof! {
                let done = snapshot!(iter);
            }
            let Some(id) = iter.next() else {
                proof_assert!(start.produces(*listed, **done) && (*done).completed());
                proof_assert!(iterates(*start, *listed));
                break;
            };

            if let Some(node) = self.nodes.get_mut(&id) {
                node.active = true;
                self.active.insert(id);
            }
            proof_assert!(listed.concat(Seq::singleton(id)).ext_eq(listed.snoc(id)));
            proof! {
                listed = snapshot!(listed.snoc(id));
            }
        }
        proof! {
            let requested = snapshot!(self.active.model());
            let listed_nodes = snapshot!(self.nodes.model());
        }
        proof_assert!(same_graph(self.nodes.model(), old.nodes));
        proof_assert! {
            lemma_same_graph_structure(self.nodes.model(), old.nodes, old.roots, old.active, self.active.model());
            structure(self.nodes.model(), self.roots.model(), self.active.model())
        };
        Self::fix_orphaned_activations(
            &mut self.nodes,
            &self.roots,
            &mut self.active,
            &mut self.scratchpad,
        );
        proof_assert!(same_graph_activation_ok(*listed_nodes, old.nodes));
        proof_assert!(longest_path_within(
            old.nodes,
            *requested,
            self.active.model()
        ));
    }
}

impl<K, T, M, S> DiscreteWeave<K, IndependentNode<K, T, S>, T> for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    #[check(terminates)]
    #[ensures(result ==> (^self)@.nodes.len() == self@.nodes.len() + 1)]
    #[ensures(result ==> (^self)@.nodes.contains(&id.deep_model()))]
    #[ensures(result ==> (^self)@.nodes.contains(&new_id.deep_model()))]
    #[ensures(result ==> !self@.nodes.contains(&new_id.deep_model()))]
    #[ensures(result ==> (^self)@.nodes[id.deep_model()]@.to.contains(new_id.deep_model())
        && (^self)@.nodes[id.deep_model()]@.to.len() == 1)]
    #[ensures(result ==> (^self)@.nodes[new_id.deep_model()]@.from.contains(id.deep_model())
        && (^self)@.nodes[new_id.deep_model()]@.from.len() == 1)]
    #[ensures(result ==> same_elements(
        self@.nodes[id.deep_model()]@.to,
        (^self)@.nodes[new_id.deep_model()]@.to,
    ))]
    #[ensures(!result ==> (^self)@.same_keys(self@))]
    #[ensures(!result ==> (^self)@.active == self@.active)]
    #[ensures(same_elements((^self)@.roots, self@.roots))]
    #[ensures(same_elements((^self)@.bookmarked, self@.bookmarked))]
    // Beyond the contract: the right side is active exactly when the split node is active and has
    // an active child.
    #[ensures(result ==> (^self)@.active
        == if self@.active.contains(&id.deep_model())
            && has_child_in(self@.nodes, self@.active, id.deep_model())
        {
            self@.active.add(new_id.deep_model())
        } else {
            self@.active
        })]
    // Beyond the contract: how the node is split, that no other contents change, and that a failed
    // split changes nothing.
    #[ensures(result ==> split_shape(self@, (^self)@, id.deep_model(), new_id.deep_model()))]
    #[ensures(result ==> forall<k> self@.nodes.contains(&k) && k != id.deep_model()
        ==> (^self)@.nodes[k].contents == self@.nodes[k].contents)]
    #[ensures(!result ==> (^self)@ == self@)]
    #[ensures((^self)@.roots == self@.roots)]
    #[ensures((^self)@.bookmarked == self@.bookmarked)]
    // Beyond the contract: what the two nodes contain after a split, and that the contents refused
    // to split if splitting fails although the nodes allow it.
    #[ensures(result ==> self@.nodes[id.deep_model()].contents.splits(
        at,
        DiscreteContentResult::Two(
            (^self)@.nodes[id.deep_model()].contents,
            (^self)@.nodes[new_id.deep_model()].contents,
        ),
    ))]
    #[ensures(!result && self@.nodes.contains(&id.deep_model())
        && !self@.nodes.contains(&new_id.deep_model()) && id.deep_model() != new_id.deep_model()
        ==> self@.nodes[id.deep_model()].contents.splits(
            at,
            DiscreteContentResult::One(self@.nodes[id.deep_model()].contents),
        ))]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn split(&mut self, id: &K, at: usize, new_id: K) -> bool {
        snapshot! { lemma_reflagged((*self)@) };
        snapshot! { lemma_prefix_contains::<Id<K>> };
        snapshot! { lemma_full_prefix::<Id<K>> };
        snapshot! { lemma_empty_snoc::<Id<K>> };
        proof! {
            let old = snapshot!((*self)@);
        }

        if self.nodes.contains_key(&new_id) || same_id(id, &new_id) {
            return false;
        }

        proof_assert!(!old.nodes.contains(&id.deep_model())
            ==> old.nodes.remove(id.deep_model()).ext_eq(old.nodes));
        if let Some(mut node) = self.nodes.remove(id) {
            match node.contents.split(at) {
                DiscreteContentResult::Two(left, right) => {
                    let left_node = IndependentNode {
                        id: node.id,
                        from: node.from,
                        to: {
                            let mut to = IndexSet::with_capacity_and_hasher(1, default_hasher());
                            to.insert(new_id);
                            to
                        },
                        active: node.active,
                        bookmarked: node.bookmarked,
                        contents: left,
                    };

                    node.from = {
                        let mut from = IndexSet::with_capacity_and_hasher(1, default_hasher());
                        from.insert(node.id);
                        from
                    };
                    node.id = new_id;
                    node.contents = right;
                    node.active = false;
                    node.bookmarked = false;

                    let mut child_active = false;

                    proof_assert!(linked(old.nodes, id.deep_model()));
                    proof_assert!(forall<k> old.nodes.contains(&k) ==> linked(old.nodes, k));
                    let mut i = 0;
                    #[invariant(i@ <= node.to.model().len())]
                    #[invariant(self.nodes.model().len() == old.nodes.len() - 1)]
                    #[invariant(forall<k> self.nodes.model().contains(&k)
                        == (old.nodes.contains(&k) && k != id.deep_model()))]
                    #[invariant(forall<k> self.nodes.model().contains(&k)
                        ==> self.nodes.model()[k].contents == old.nodes[k].contents)]
                    #[invariant(forall<k> self.nodes.model().contains(&k)
                        ==> self.nodes.model()[k]@.id == old.nodes[k]@.id
                            && self.nodes.model()[k]@.to == old.nodes[k]@.to
                            && self.nodes.model()[k]@.active == old.nodes[k]@.active
                            && self.nodes.model()[k]@.bookmarked == old.nodes[k]@.bookmarked
                            && if node.to.model().subsequence(0, i@).contains(k) {
                                replaced(
                                    self.nodes.model()[k]@.from,
                                    old.nodes[k]@.from,
                                    id.deep_model(),
                                    new_id.deep_model(),
                                )
                            } else {
                                self.nodes.model()[k]@.from == old.nodes[k]@.from
                            })]
                    #[invariant(child_active == exists<j> 0 <= j && j < i@
                        && old.active.contains(&node.to.model()[j]))]
                    #[variant(node.to.model().len() - i@)]
                    while i < node.to.len() {
                        proof_assert!(old.nodes[id.deep_model()]@.to.contains(node.to.model()[i@]));
                        let child = self.nodes.get_mut(&node.to[i]).unwrap();
                        let index = child.from.get_index_of(&left_node.id).unwrap();
                        proof! {
                            let before = snapshot!(child.from.model());
                        }
                        proof_assert!(!node.to.model().subsequence(0, i@).contains(node.to.model()[i@]));
                        proof_assert!(*before == old.nodes[node.to.model()[i@]]@.from);

                        assert!(
                            child.from.replace_index(index, node.id).is_ok(),
                            "Should be unreachable"
                        );
                        proof_assert! {
                            lemma_replaced_in_place(
                                child.from.model(),
                                *before,
                                index@,
                                id.deep_model(),
                                new_id.deep_model(),
                            );
                            true
                        };

                        child_active |= child.active;
                        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                        {
                            i += 1;
                        }
                    }

                    proof_assert!(
                        child_active == has_child_in(old.nodes, old.active, id.deep_model())
                    );

                    if left_node.active && child_active {
                        node.active = true;
                        self.active.insert(node.id);
                    }

                    self.nodes.insert(left_node.id, left_node);
                    proof! {
                        let view = snapshot!(IndependentWeaveView {
                            nodes: self.nodes.model().add(new_id.deep_model(), node),
                            roots: self.roots.model(),
                            active: self.active.model(),
                            bookmarked: self.bookmarked.model(),
                        });
                    }
                    proof_assert!(split_shape(
                        *old,
                        *view,
                        id.deep_model(),
                        new_id.deep_model()
                    ));
                    proof_assert! {
                        lemma_split_valid(*old, *view, id.deep_model(), new_id.deep_model());
                        view.valid()
                    };
                    self.nodes.insert(node.id, node);

                    true
                }
                DiscreteContentResult::One(content) => {
                    node.contents = content;
                    proof_assert!(linked(old.nodes, id.deep_model()));
                    proof_assert!(node@ == old.nodes[id.deep_model()]@
                        && node.id.deep_model() == id.deep_model());
                    proof_assert!(forall<k> self.nodes.model().add(node.id.deep_model(), node).contains(&k)
                        ==> self.nodes.model().add(node.id.deep_model(), node)[k]@ == old.nodes[k]@);
                    proof_assert!(
                        self.nodes
                            .model()
                            .add(node.id.deep_model(), node)
                            .ext_eq(old.nodes)
                    );
                    self.nodes.insert(node.id, node);
                    false
                }
            }
        } else {
            false
        }
    }
    #[check(terminates)]
    #[ensures(result != None ==> (^self)@.nodes.len() == self@.nodes.len() - 1)]
    #[ensures(result != None ==> !(^self)@.nodes.contains(&id.deep_model()))]
    #[ensures(result != None ==> self@.nodes.contains(&id.deep_model()))]
    #[ensures(match result {
        Some(r) => self@.active.contains(&id.deep_model())
            ==> (^self)@.active.contains(&r.deep_model()),
        None => true,
    })]
    #[ensures(match result {
        Some(r) => (^self)@.nodes[r.deep_model()]@.active == self@.nodes[r.deep_model()]@.active,
        None => true,
    })]
    #[ensures(match result {
        Some(r) => same_elements(
            (^self)@.nodes[r.deep_model()]@.from,
            self@.nodes[r.deep_model()]@.from,
        ),
        None => true,
    })]
    #[ensures(match result {
        Some(r) => same_elements(
            (^self)@.nodes[r.deep_model()]@.to,
            self@.nodes[id.deep_model()]@.to,
        ),
        None => true,
    })]
    #[ensures(match result {
        Some(r) => self@.nodes[id.deep_model()]@.from == Seq::singleton(r.deep_model()),
        None => true,
    })]
    #[ensures(result == None ==> (^self)@.same_keys(self@))]
    #[ensures(result == None ==> (^self)@.active == self@.active)]
    #[ensures(result == None ==> same_elements((^self)@.bookmarked, self@.bookmarked))]
    #[ensures(same_elements((^self)@.roots, self@.roots))]
    // Beyond the contract: only `id` stops being active.
    #[ensures(match result {
        Some(_) => (^self)@.active == self@.active.remove(id.deep_model()),
        None => true,
    })]
    // Beyond the contract: how the node is merged, that no other contents change, and that a failed
    // merge changes nothing.
    #[ensures(match result {
        Some(r) => merge_shape(self@, (^self)@, id.deep_model(), r.deep_model())
            && forall<k> (^self)@.nodes.contains(&k) && k != r.deep_model()
                ==> (^self)@.nodes[k].contents == self@.nodes[k].contents,
        None => (^self)@ == self@,
    })]
    #[ensures((^self)@.roots == self@.roots)]
    // Beyond the contract: what the merged node contains, and that the contents refused to merge if
    // merging fails although the nodes allow it.
    #[ensures(match result {
        Some(r) => self@.nodes[r.deep_model()].contents.merges(
            self@.nodes[id.deep_model()].contents,
            DiscreteContentResult::One((^self)@.nodes[r.deep_model()].contents),
        ),
        None => forall<p> self@.nodes.contains(&id.deep_model())
            && self@.nodes[id.deep_model()]@.from == Seq::singleton(p)
            && self@.nodes[p]@.to.len() <= 1
            ==> self@.nodes[p].contents.merges(
                self@.nodes[id.deep_model()].contents,
                DiscreteContentResult::Two(
                    self@.nodes[p].contents,
                    self@.nodes[id.deep_model()].contents,
                ),
            ),
    })]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn merge_with_parent(&mut self, id: &K) -> Option<K> {
        snapshot! { lemma_reflagged((*self)@) };
        snapshot! { lemma_prefix_contains::<Id<K>> };
        snapshot! { lemma_full_prefix::<Id<K>> };
        proof! {
            let old = snapshot!((*self)@);
        }

        proof_assert!(!old.nodes.contains(&id.deep_model())
            ==> old.nodes.remove(id.deep_model()).ext_eq(old.nodes));
        if let Some(mut node) = self.nodes.remove(id) {
            proof_assert!(linked(old.nodes, id.deep_model()));
            if node.from.len() != 1 {
                proof_assert!(
                    self.nodes
                        .model()
                        .add(id.deep_model(), node)
                        .ext_eq(old.nodes)
                );
                self.nodes.insert(node.id, node);
                return None;
            }

            proof! {
                let p = snapshot!(node.from.model()[0]);
            }
            proof_assert!(old.nodes.contains(&*p) && linked(old.nodes, *p));
            let parent = match node.from.first() {
                Some(parent_id) => self.nodes.remove(parent_id),
                None => None,
            };
            if let Some(mut parent) = parent {
                if parent.to.len() > 1 {
                    self.nodes.insert(parent.id, parent);
                    proof_assert!(node.id.deep_model() == id.deep_model());
                    proof_assert!(forall<k> self.nodes.model().add(id.deep_model(), node).contains(&k)
                        == old.nodes.contains(&k));
                    proof_assert!(forall<k> self.nodes.model().add(id.deep_model(), node).contains(&k)
                        ==> self.nodes.model().add(id.deep_model(), node)[k]@ == old.nodes[k]@);
                    proof_assert!(
                        self.nodes
                            .model()
                            .add(id.deep_model(), node)
                            .ext_eq(old.nodes)
                    );
                    self.nodes.insert(node.id, node);
                    return None;
                }

                match parent.contents.merge(node.contents) {
                    DiscreteContentResult::Two(left, right) => {
                        parent.contents = left;
                        node.contents = right;
                        self.nodes.insert(parent.id, parent);
                        proof_assert!(node.id.deep_model() == id.deep_model());
                        proof_assert!(forall<k> self.nodes.model().add(id.deep_model(), node).contains(&k)
                            == old.nodes.contains(&k));
                        proof_assert!(forall<k> self.nodes.model().add(id.deep_model(), node).contains(&k)
                            ==> self.nodes.model().add(id.deep_model(), node)[k]@ == old.nodes[k]@);
                        proof_assert!(
                            self.nodes
                                .model()
                                .add(id.deep_model(), node)
                                .ext_eq(old.nodes)
                        );
                        self.nodes.insert(node.id, node);
                        None
                    }
                    DiscreteContentResult::One(content) => {
                        parent.contents = content;
                        parent.to = node.to;

                        proof_assert!(old.nodes[*p]@.to.len() == 1
                            && old.nodes[*p]@.to[0] == id.deep_model());
                        proof_assert!(forall<k> old.nodes[*p]@.to.contains(k) == (k == id.deep_model()));
                        proof_assert!(forall<c> old.nodes[id.deep_model()]@.to.contains(c)
                            ==> c != *p && c != id.deep_model() && linked(old.nodes, c)
                                && !old.nodes[c]@.from.contains(*p));
                        let mut i = 0;
                        #[invariant(i@ <= parent.to.model().len())]
                        #[invariant(self.nodes.model().len() == old.nodes.len() - 2)]
                        #[invariant(forall<k> self.nodes.model().contains(&k)
                            == (old.nodes.contains(&k) && k != id.deep_model() && k != *p))]
                        #[invariant(forall<k> self.nodes.model().contains(&k)
                            ==> self.nodes.model()[k].contents == old.nodes[k].contents)]
                        #[invariant(forall<k> self.nodes.model().contains(&k)
                            ==> self.nodes.model()[k]@.id == old.nodes[k]@.id
                                && self.nodes.model()[k]@.to == old.nodes[k]@.to
                                && self.nodes.model()[k]@.active == old.nodes[k]@.active
                                && self.nodes.model()[k]@.bookmarked == old.nodes[k]@.bookmarked
                                && if parent.to.model().subsequence(0, i@).contains(k) {
                                    replaced(
                                        self.nodes.model()[k]@.from,
                                        old.nodes[k]@.from,
                                        id.deep_model(),
                                        *p,
                                    )
                                } else {
                                    self.nodes.model()[k]@.from == old.nodes[k]@.from
                                })]
                        #[variant(parent.to.model().len() - i@)]
                        while i < parent.to.len() {
                            proof_assert!(old.nodes[id.deep_model()]@.to.contains(parent.to.model()[i@]));
                            let child = self.nodes.get_mut(&parent.to[i]).unwrap();
                            let index = child.from.get_index_of(&node.id).unwrap();
                            proof! {
                                let before = snapshot!(child.from.model());
                            }
                            proof_assert!(
                                !parent.to.model().subsequence(0, i@).contains(parent.to.model()[i@])
                            );
                            proof_assert!(*before == old.nodes[parent.to.model()[i@]]@.from);

                            assert!(
                                child.from.replace_index(index, parent.id).is_ok(),
                                "Should be unreachable"
                            );
                            proof_assert! {
                                lemma_replaced_in_place(
                                    child.from.model(),
                                    *before,
                                    index@,
                                    id.deep_model(),
                                    *p,
                                );
                                true
                            };
                            #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                            {
                                i += 1;
                            }
                        }

                        let parent_id = parent.id;

                        if node.bookmarked && !parent.bookmarked {
                            parent.bookmarked = true;
                            let index = self.bookmarked.get_index_of(&node.id).unwrap();
                            proof! {
                                let before = snapshot!(self.bookmarked.model());
                            }
                            assert!(
                                self.bookmarked.replace_index(index, parent.id).is_ok(),
                                "Should be unreachable"
                            );
                            proof_assert! {
                                lemma_replaced_in_place(
                                    self.bookmarked.model(),
                                    *before,
                                    index@,
                                    id.deep_model(),
                                    *p,
                                );
                                true
                            };
                        } else if node.bookmarked {
                            self.bookmarked.shift_remove(&node.id);
                        }

                        self.nodes.insert(parent.id, parent);
                        proof! {
                            let view = snapshot!(IndependentWeaveView {
                                nodes: self.nodes.model(),
                                roots: self.roots.model(),
                                active: self.active.model().remove(id.deep_model()),
                                bookmarked: self.bookmarked.model(),
                            });
                        }
                        proof_assert!(merge_shape(*old, *view, id.deep_model(), *p));
                        proof_assert! {
                            lemma_merge_valid(*old, *view, id.deep_model(), *p);
                            view.valid()
                                && (old.active.contains(&id.deep_model()) ==> old.active.contains(&*p))
                        };
                        proof_assert!(old.nodes[id.deep_model()]@.from.ext_eq(Seq::singleton(*p)));
                        self.active.remove(&node.id);

                        Some(parent_id)
                    }
                }
            } else {
                proof_assert!(
                    self.nodes
                        .model()
                        .add(id.deep_model(), node)
                        .ext_eq(old.nodes)
                );
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
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    #[requires(forall<c: &mut T> inv(c) ==> callback.precondition((c,)))]
    #[ensures((result != None) == self@.nodes.contains(&id.deep_model()))]
    #[ensures((^self)@.same_keys(self@))]
    #[ensures((^self)@.roots == self@.roots)]
    #[ensures((^self)@.active == self@.active)]
    #[ensures((^self)@.bookmarked == self@.bookmarked)]
    // Beyond the contract: only the node's contents change.
    #[ensures(forall<k> k != id.deep_model() ==> (^self)@.nodes.lookup(k) == self@.nodes.lookup(k))]
    #[ensures(self@.nodes.contains(&id.deep_model())
        ==> (^self)@.nodes[id.deep_model()]@ == self@.nodes[id.deep_model()]@)]
    // Beyond the contract: the callback is called on the node's contents, and leaves them as the
    // node's new contents.
    #[ensures(match result {
        Some(o) => exists<c: &mut T> *c == self@.nodes[id.deep_model()].contents
            && ^c == (^self)@.nodes[id.deep_model()].contents
            && callback.postcondition_once((c,), o),
        None => resolve(callback),
    })]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn get_contents_mut<O>(&mut self, id: &K, callback: impl FnOnce(&mut T) -> O) -> Option<O> {
        snapshot! { lemma_reflagged((*self)@) };

        match self.nodes.get_mut(id) {
            Some(node) => Some(callback(&mut node.contents)),
            None => None,
        }
    }
}

impl<K, T, M, S> IndependentWeaveTrait<K, IndependentNode<K, T, S>, T>
    for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[ensures(result ==> forall<k> (^self)@.nodes[id.deep_model()]@.from.contains(k)
        == ids(new_parents@).contains(k))]
    #[ensures(!result ==> (^self)@.nodes.lookup(id.deep_model()) == None
        || (^self)@.nodes[id.deep_model()]@.from == self@.nodes[id.deep_model()]@.from)]
    #[ensures(!result ==> same_elements((^self)@.roots, self@.roots))]
    #[ensures(!result ==> (^self)@.active == self@.active)]
    #[ensures(self@.nodes.contains(&id.deep_model())
        ==> (^self)@.nodes[id.deep_model()]@.to == self@.nodes[id.deep_model()]@.to)]
    #[ensures((^self)@.same_keys(self@))]
    #[ensures(same_elements((^self)@.bookmarked, self@.bookmarked))]
    #[ensures(self@.active.contains(&id.deep_model()) == (^self)@.active.contains(&id.deep_model()))]
    // Beyond the contract: moving an active node recomputes the active path through it.
    #[ensures(result && (!self@.active.contains(&id.deep_model())
        || self@.nodes[id.deep_model()]@.from == ids(new_parents@))
        ==> (^self)@.active == self@.active)]
    #[ensures(result && self@.active.contains(&id.deep_model())
        && self@.nodes[id.deep_model()]@.from != ids(new_parents@)
        ==> activated(
            (^self)@.nodes,
            self@.active.remove(id.deep_model()),
            id.deep_model(),
            (^self)@.active,
        ))]
    // Beyond the contract: when moving succeeds, that a failed move changes nothing, and how a
    // successful one relinks the node.
    #[ensures(result == ((forall<i> 0 <= i && i < new_parents@.len()
            ==> self@.nodes.contains(&new_parents@[i].deep_model()))
        && self@.nodes.contains(&id.deep_model())
        && not_below(self@.nodes, id.deep_model(), ids(new_parents@))))]
    #[ensures(!result ==> (^self)@ == self@)]
    #[ensures(result ==> moved(self@, (^self)@, id.deep_model(), (^self)@.nodes[id.deep_model()]@.from))]
    // Beyond the contract: the node's new parents are `new_parents` in order, without repetitions.
    #[ensures(result ==> (^self)@.nodes[id.deep_model()]@.from == dedup(ids(new_parents@)))]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    #[check(terminates)]
    fn move_to(&mut self, id: &K, new_parents: &[K]) -> bool {
        snapshot! { lemma_reactivated((*self)@) };
        snapshot! { lemma_prefix_contains::<Id<K>> };
        snapshot! { lemma_full_prefix::<Id<K>> };
        snapshot! { crate::verification::lemma_in_order_refl::<Id<K>> };
        snapshot! { crate::verification::lemma_in_order_removed::<Id<K>> };
        snapshot! { crate::verification::lemma_in_order_trans::<Id<K>> };
        proof! {
            let old = snapshot!((*self)@);
            let id0 = snapshot!(id.deep_model());
            let slice = snapshot!(ids(new_parents@));
        }
        proof_assert!(forall<k> old.nodes.contains(&k) ==> linked(old.nodes, k) && old.flagged(k));

        if !all_present_in_slice(new_parents, &self.nodes) {
            return false;
        }

        let Some(node) = self.nodes.get(id) else {
            return false;
        };

        if same_ids(&node.from, new_parents) {
            proof_assert! {
                lemma_parents_not_below(old.nodes, *id0);
                not_below(old.nodes, *id0, *slice)
            };
            proof_assert!(moved(*old, *old, *id0, old.nodes[*id0]@.from));
            proof_assert! { lemma_dedup_no_dup(*slice); dedup(*slice) == *slice };
            return true;
        }

        let new_parents: IndexSet<K, S> = index_set_from_slice(new_parents);
        proof! {
            let parents = snapshot!(new_parents.model());
        }
        proof_assert!(forall<q> parents.contains(q) ==> old.nodes.contains(&q));

        proof_assert! {
            lemma_dedup(*slice);
            forall<k> parents.contains(k) == slice.contains(k)
        };
        if new_parents.contains(id) {
            proof_assert! {
                lemma_below(old.nodes, *id0, *slice);
                !not_below(old.nodes, *id0, *slice)
            };
            return false;
        }

        proof! {
            let mut below = snapshot!(FSet::<Id<K>>::empty());
        }
        if !node.to.is_empty() && !new_parents.is_empty() {
            let guard = self.scratchpad.guard();

            let mut stack = guard.vec_with_capacity(node.to.len());
            let mut descendants = guard.set_with_capacity(node.to.len(), default_hasher());

            if descendant_subgraph_reaches(
                &self.nodes,
                &node.to,
                &new_parents,
                snapshot!(|k| new_parents.model().contains(k)),
                &mut stack,
                &mut descendants,
            ) {
                proof_assert! {
                    lemma_below(old.nodes, *id0, *slice);
                    !not_below(old.nodes, *id0, *slice)
                };
                return false;
            }

            proof! {
                below = snapshot!(descendants.model());
            }
        }
        proof_assert!(linked(old.nodes, *id0));
        proof_assert!(forall<c> old.nodes[*id0]@.to.contains(c) ==> below.contains(&c)
            || parents.len() == 0);
        proof_assert!(child_closed(old.nodes, below.add(*id0)) || parents.len() == 0);
        proof_assert!(forall<q> parents.contains(q) ==> !below.add(*id0).contains(&q)
            || parents.len() == 0);
        proof_assert!(not_below(old.nodes, *id0, *parents));
        proof_assert!(not_below(old.nodes, *id0, *slice));
        proof_assert!(forall<k> old.nodes.contains(&k) ==> linked(old.nodes, k));

        if let Some(node) = self.nodes.get_mut(id) {
            let old_parents = mem::replace(&mut node.from, IndexSet::with_hasher(default_hasher()));
            proof! {
                let olds = snapshot!(old_parents.model());
            }
            proof_assert!(*olds == old.nodes[*id0]@.from);
            proof_assert!(forall<k> old.nodes.contains(&k) && k != *id0
                ==> old.nodes[k]@.to.contains(*id0) == olds.contains(k));

            let mut i = 0;
            #[invariant(forall<k> self.nodes.model().contains(&k) == old.nodes.contains(&k))]
            #[invariant(self.nodes.model()[*id0]@ == NodeView {
                id: *id0,
                from: Seq::empty(),
                to: old.nodes[*id0]@.to,
                active: old.nodes[*id0]@.active,
                bookmarked: old.nodes[*id0]@.bookmarked,
            })]
            #[invariant(i@ <= old_parents.model().len())]
            #[invariant(forall<k> old.nodes.contains(&k) && k != *id0
                ==> self.nodes.model()[k]@.id == old.nodes[k]@.id
                    && self.nodes.model()[k]@.from == old.nodes[k]@.from
                    && self.nodes.model()[k]@.active == old.nodes[k]@.active
                    && self.nodes.model()[k]@.bookmarked == old.nodes[k]@.bookmarked
                    && forall<x> self.nodes.model()[k]@.to.contains(x) == if x == *id0 {
                        olds.contains(k)
                            && !(olds.subsequence(0, i@).contains(k) && !parents.contains(k))
                    } else {
                        old.nodes[k]@.to.contains(x)
                    })]
            #[invariant(forall<k> old.nodes.contains(&k)
                ==> self.nodes.model()[k].contents == old.nodes[k].contents)]
            #[invariant(forall<k> old.nodes.contains(&k) && k != *id0
                ==> crate::verification::in_order(self.nodes.model()[k]@.to, old.nodes[k]@.to)
                    && (!(olds.contains(k) && !parents.contains(k))
                        ==> self.nodes.model()[k]@.to == old.nodes[k]@.to))]
            #[variant(old_parents.model().len() - i@)]
            while i < old_parents.len() {
                let old_parent = old_parents[i];
                proof_assert!(old_parent.deep_model() == olds[i@] && olds[i@] != *id0);
                if !new_parents.contains(&old_parent)
                    && let Some(old_parent) = self.nodes.get_mut(&old_parent)
                {
                    old_parent.to.shift_remove(id);
                }
                #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                {
                    i += 1;
                }
            }
            proof! {
                let nodes_a = snapshot!(self.nodes.model());
            }

            let mut i = 0;
            #[invariant(forall<k> self.nodes.model().contains(&k) == old.nodes.contains(&k))]
            #[invariant(self.nodes.model()[*id0]@ == NodeView {
                id: *id0,
                from: Seq::empty(),
                to: old.nodes[*id0]@.to,
                active: old.nodes[*id0]@.active,
                bookmarked: old.nodes[*id0]@.bookmarked,
            })]
            #[invariant(i@ <= new_parents.model().len())]
            #[invariant(forall<k> old.nodes.contains(&k) && k != *id0
                ==> self.nodes.model()[k]@.id == old.nodes[k]@.id
                    && self.nodes.model()[k]@.from == old.nodes[k]@.from
                    && self.nodes.model()[k]@.active == old.nodes[k]@.active
                    && self.nodes.model()[k]@.bookmarked == old.nodes[k]@.bookmarked
                    && forall<x> self.nodes.model()[k]@.to.contains(x) == if x == *id0 {
                        (olds.contains(k) && parents.contains(k))
                            || (parents.subsequence(0, i@).contains(k) && !olds.contains(k))
                    } else {
                        old.nodes[k]@.to.contains(x)
                    })]
            #[invariant(forall<k> old.nodes.contains(&k)
                ==> self.nodes.model()[k].contents == old.nodes[k].contents)]
            #[invariant(forall<k> old.nodes.contains(&k) && k != *id0
                ==> self.nodes.model()[k]@.to
                    == if parents.subsequence(0, i@).contains(k) && !olds.contains(k) {
                        nodes_a[k]@.to.snoc(*id0)
                    } else {
                        nodes_a[k]@.to
                    })]
            #[variant(new_parents.model().len() - i@)]
            while i < new_parents.len() {
                let new_parent = new_parents[i];
                proof_assert!(new_parent.deep_model() == parents[i@] && parents[i@] != *id0);
                if !old_parents.contains(&new_parent)
                    && let Some(new_parent) = self.nodes.get_mut(&new_parent)
                {
                    new_parent.to.insert(*id);
                }
                #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                {
                    i += 1;
                }
            }
        }

        proof! {
            let nodes1 = snapshot!(self.nodes.model());
        }
        proof_assert!(forall<k> old.nodes.contains(&k) && k != *id0
        ==> forall<x> nodes1[k]@.to.contains(x) == if x == *id0 {
            parents.contains(k)
        } else {
            old.nodes[k]@.to.contains(x)
        });

        let Self {
            nodes,
            roots,
            active,
            scratchpad,
            ..
        } = self;

        let node = nodes.get_mut(id).unwrap();
        node.from = new_parents;

        if node.from.is_empty() {
            roots.insert(node.id);
        } else {
            roots.shift_remove(&node.id);
        }
        proof! {
            let view = snapshot!(IndependentWeaveView {
                nodes: nodes1.add(*id0, *node),
                roots: roots.model(),
                active: old.active,
                bookmarked: old.bookmarked,
            });
        }
        proof_assert!(forall<k> view.nodes.contains(&k) == old.nodes.contains(&k));
        proof_assert!(forall<k> view.roots.contains(k)
            == if k == *id0 { parents.len() == 0 } else { old.roots.contains(k) });
        proof_assert!(move_shape(*old, *view, *id0, *parents));
        proof_assert!(forall<k> old.nodes.contains(&k) && k != *id0 && parents.contains(k) && !old.nodes[*id0]@.from.contains(k)
            ==> view.nodes[k]@.to == old.nodes[k]@.to.snoc(*id0));
        proof_assert!(forall<k> old.nodes.contains(&k) && k != *id0 && !(parents.contains(k) && !old.nodes[*id0]@.from.contains(k))
            ==> crate::verification::in_order(view.nodes[k]@.to, old.nodes[k]@.to));
        proof_assert!(if parents.len() == 0 && !old.roots.contains(*id0) {
            view.roots == old.roots.snoc(*id0)
        } else {
            crate::verification::in_order(view.roots, old.roots)
        });
        proof_assert!(moved(*old, *view, *id0, *parents));
        proof_assert!(!node.active ==> {
            lemma_move_structure(*old, *view, *id0, *parents);
            lemma_move_inactive(*old, *view, *id0, *parents);
            view.valid()
        });

        if node.active {
            node.active = false;
            proof! {
                let mid = snapshot!(IndependentWeaveView {
                    nodes: nodes1.add(*id0, *node),
                    roots: view.roots,
                    active: old.active.remove(*id0),
                    bookmarked: old.bookmarked,
                });
            }
            proof_assert!(move_shape(*old, *mid, *id0, *parents));
            proof_assert! {
                lemma_move_structure(*old, *mid, *id0, *parents);
                mid.valid_structure()
            };
            proof_assert! {
                lemma_move_ready(*old, *mid, *id0, *parents);
                ready_to_activate(mid.nodes, mid.active, *id0, true)
            };
            snapshot! { lemma_reactivated(*mid) };
            active.remove(id);
            proof_assert!(
                nodes.model() == mid.nodes
                    && roots.model() == mid.roots
                    && active.model() == mid.active
            );
            proof_assert!(!mid.nodes[*id0]@.active);
            Self::update_node_activity_in_place(nodes, roots, active, scratchpad, id, true);
            proof_assert!(same_graph_activation_ok(nodes.model(), mid.nodes));
            proof_assert!(same_graph(nodes.model(), mid.nodes));
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
#[allow(unsafe_code, reason = "rkyv limitation")]
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
                |id| descendants.contains(id),
                *id,
                &mut stack,
                |id| output.push(id),
                &mut guard.map_with_capacity(descendants.len(), S::default()),
            );
        }
    }
    fn get_active_path(&self, output: &mut Vec<K::Archived>) {
        output.clear();

        if self.active.is_empty() {
            return;
        }

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
                |id| self.active.contains(id),
                root,
                &mut stack,
                |id| topological_subgraph.push(id),
                &mut scratchpad_map,
            );
        }

        archived_longest_candidate_path_to_root(
            &self.nodes,
            &topological_subgraph,
            |id| self.active.contains(id),
            &mut guard.map_with_capacity(self.active.len(), S::default()),
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
        let mut root_ancestors = guard.vec();

        archived_ancestor_subgraph(&self.nodes, *id, &mut stack, &mut ancestors, |id| {
            root_ancestors.push(id);
        });

        let mut active_topological_subgraph =
            guard.vec_with_capacity(self.active.len().min(ancestors.len()));
        let mut scratchpad_map =
            guard.map_with_capacity(self.active.len().min(ancestors.len()), S::default());

        for root in root_ancestors
            .drain(..)
            .filter(|id| self.active.contains(id))
        {
            archived_topological_sort_subgraph(
                &self.nodes,
                |id| self.active.contains(id) && ancestors.contains(id),
                root,
                &mut stack,
                |id| active_topological_subgraph.push(id),
                &mut scratchpad_map,
            );
        }

        let mut reversed_path = guard.vec();

        archived_longest_candidate_path_to_root(
            &self.nodes,
            &active_topological_subgraph,
            |id| self.active.contains(id) && ancestors.contains(id),
            &mut guard.map_with_capacity(self.active.len().min(ancestors.len()), S::default()),
            |id| reversed_path.push(id),
        );

        let mut scratchpad_map_2 = guard.map_with_capacity(ancestors.len(), S::default());

        if let Some(target) = reversed_path.first().copied() {
            archived_shortest_path_to_ancestor(
                &self.nodes,
                id,
                |node| node.id == target,
                &mut stack,
                &mut scratchpad_map_2,
                output,
            );

            output.reverse();
            output.pop();
            output.extend_from_slice(&reversed_path);
        } else {
            archived_shortest_path_to_ancestor(
                &self.nodes,
                id,
                |node| node.from.is_empty(),
                &mut stack,
                &mut scratchpad_map_2,
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
