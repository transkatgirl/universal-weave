//! [`DependentWeave`] is a tree-based [`Weave`] where each [`Node`] depends on the contents of the previous Node.

use alloc::vec::Vec;
use core::{
    cmp::Ordering,
    hash::{BuildHasher, Hash},
    mem,
};

use creusot_std::macros::*;
use hashbrown::{HashMap, hash_map::Entry};
use indexmap::IndexSet;
use scratchpads::{Scratchpad, ScratchpadVec};

#[cfg(creusot)]
use creusot_std::prelude::*;

#[cfg(feature = "rkyv")]
use hashbrown::HashSet;

#[cfg(feature = "rkyv")]
use rkyv::{
    Archive, Deserialize, Serialize,
    bytecheck::Verify,
    collections::swiss_table::{ArchivedHashMap, ArchivedIndexSet},
    option::ArchivedOption,
    rancor::{Fallible, Source, fail},
    with::Skip,
};

#[cfg(feature = "serde")]
use serde::{
    Deserialize as SerdeDeserialize, Deserializer as SerdeDeserializer,
    Serialize as SerdeSerialize, de::Error as _,
};

use crate::{
    ActiveSingularWeave, BookmarkableWeave, BuildableNode, DiscreteContentResult, DiscreteContents,
    DiscreteWeave, IndependentContents, MetadataWeave, Node, SemiIndependentWeave,
    SortableBookmarkableWeave, SortableWeave, Verifiable, Weave, same_id,
};

#[cfg(feature = "rkyv")]
use crate::{
    ImmutableActiveSingularWeave, ImmutableBookmarkableWeave, ImmutableMetadataWeave,
    ImmutableWeave, archived_set_reverse_order,
};

#[cfg(any(feature = "serde", feature = "rkyv"))]
use crate::contract::ValidationError;

#[cfg(creusot)]
use crate::verification::{
    Id, Model, bookmark_set, calls, compares_by, fed, first, id_of, identity, ids, in_order,
    lemma_calls_snoc, lemma_concat_contains, lemma_concat_snoc, lemma_empty_snoc,
    lemma_full_prefix, lemma_ids_empty, lemma_ids_singleton, lemma_ids_snoc, lemma_in_order_refl,
    lemma_in_order_shift_remove, lemma_prefix_contains, lemma_replaced_in_place, lemma_reverse,
    lemma_snoc_contains, lemma_sorted_agree, lemma_subset_len, no_dup, order_by, same_elements,
    sorted_stably, total_preorder, value_of,
};
#[cfg(creusot)]
use creusot_std::logic::Mapping;

#[cfg(creusot)]
use verification::*;

#[cfg(feature = "loro")]
pub mod loro;

#[cfg(all(feature = "legacy", not(creusot)))]
#[deprecated]
pub mod legacy_dependent;

#[cfg(creusot)]
pub mod verification;

#[cfg_attr(not(creusot), derive(Default, Debug, Clone))]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
/// A [`Node`] in a [`DependentWeave`] document.
#[must_use]
pub struct DependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// The node's unique identifier.
    pub id: K,
    /// The identifier corresponding to the node's parent.
    pub from: Option<K>,
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
    /// [`DependentWeave`] only considers the node at the start of an active path to be active.
    pub active: bool,
    /// If the node is bookmarked.
    pub bookmarked: bool,
    /// The node's contents.
    pub contents: T,
}

#[cfg(not(creusot))]
#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, S> PartialEq for DependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: PartialEq,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.from == other.from
            && self.active == other.active
            && self.bookmarked == other.bookmarked
            && self.to.len() == other.to.len()
            && self.to.iter().zip(other.to.iter()).all(|(a, b)| a == b)
            && self.contents == other.contents
    }
}

#[cfg(not(creusot))]
#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, S> Eq for DependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: Eq,
    S: BuildHasher + Default + Clone,
{
}

impl<K, T, S> DependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    #[check(terminates)]
    #[ensures(result == self@.valid())]
    #[inline]
    fn validate(&self) -> bool {
        if let Some(from) = self.from
            && (self.to.contains(&from) || same_id(&from, &self.id))
        {
            return false;
        }

        !self.to.contains(&self.id)
    }
}

impl<K, T, S> Node<K, T> for DependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    type From = Option<K>;
    type To = IndexSet<K, S>;

    #[check(terminates)]
    #[ensures(result == self.id)]
    #[inline]
    fn id(&self) -> K {
        self.id
    }
    #[check(terminates)]
    #[ensures(*result == self.from)]
    #[inline]
    fn from(&self) -> &Self::From {
        &self.from
    }
    #[check(terminates)]
    #[ensures(*result == self.to)]
    #[inline]
    fn to(&self) -> &Self::To {
        &self.to
    }
    #[check(terminates)]
    #[ensures(result == self.active)]
    #[inline]
    fn is_active(&self) -> bool {
        self.active
    }
    #[check(terminates)]
    #[ensures(*result == self.contents)]
    #[inline]
    fn contents(&self) -> &T {
        &self.contents
    }
}

impl<K, T, S> BuildableNode<K, T> for DependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    #[check(terminates)]
    #[ensures(result.id == id
        && result.from == from
        && result.to == to
        && result.active == is_active
        && !result.bookmarked
        && result.contents == contents)]
    #[inline]
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

/// A tree-based [`Weave`] where each [`Node`] depends on the contents of the previous Node.
///
/// For best performance, it is recommended that you use random node identifiers and the [`Hasher`](core::hash::Hasher) implementation from the [nohash-hasher](https://crates.io/crates/nohash-hasher) crate. If your node identifiers end with random data (such as ULIDs in their raw representation), use the [hash_hasher](https://crates.io/crates/hash_hasher) crate instead.
#[cfg_attr(not(creusot), derive(Debug, Clone))]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize))]
#[cfg_attr(feature = "rkyv", rkyv(bytecheck(verify)))]
#[must_use]
pub struct DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "HashMap<K, DependentNode<K, T, S>, S>: SerdeSerialize",
            deserialize = "HashMap<K, DependentNode<K, T, S>, S>: SerdeDeserialize<'de>"
        ))
    )]
    pub(super) nodes: HashMap<K, DependentNode<K, T, S>, S>,
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "IndexSet<K, S>: SerdeSerialize",
            deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    pub(super) roots: IndexSet<K, S>,
    pub(super) active: Option<K>,
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

impl<K, T, M, S> Default for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
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
#[serde(rename = "DependentWeave")]
struct ProxyDependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    #[serde(bound(
        serialize = "HashMap<K, DependentNode<K, T, S>, S>: SerdeSerialize",
        deserialize = "HashMap<K, DependentNode<K, T, S>, S>: SerdeDeserialize<'de>"
    ))]
    nodes: HashMap<K, DependentNode<K, T, S>, S>,
    #[serde(bound(
        serialize = "IndexSet<K, S>: SerdeSerialize",
        deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
    ))]
    roots: IndexSet<K, S>,
    active: Option<K>,
    #[serde(bound(
        serialize = "IndexSet<K, S>: SerdeSerialize",
        deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
    ))]
    bookmarked: IndexSet<K, S>,
    metadata: M,
}

#[cfg(feature = "serde")]
#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<'de, K, T, M, S> SerdeDeserialize<'de> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable + SerdeDeserialize<'de>,
    T: SerdeDeserialize<'de>,
    M: SerdeDeserialize<'de>,
    S: BuildHasher + Default + Clone,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: SerdeDeserializer<'de>,
    {
        let proxy = ProxyDependentWeave::deserialize(deserializer)?;

        if validate(
            &proxy.nodes,
            &proxy.roots,
            proxy.active.as_ref(),
            &proxy.bookmarked,
        ) {
            Ok(Self {
                nodes: proxy.nodes,
                roots: proxy.roots,
                active: proxy.active,
                bookmarked: proxy.bookmarked,
                scratchpad: Scratchpad::new(),
                metadata: proxy.metadata,
            })
        } else {
            Err(D::Error::custom(ValidationError))
        }
    }
}

#[cfg(not(creusot))]
#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<K, T, M, S> PartialEq for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: PartialEq,
    M: PartialEq,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.roots.len() == other.roots.len()
            && self.bookmarked.len() == other.bookmarked.len()
            && self.active == other.active
            && self.nodes.len() == other.nodes.len()
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
impl<K, T, M, S> Eq for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: Eq,
    M: Eq,
    S: BuildHasher + Default + Clone,
{
}

impl<K, T, M, S> DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    /// Creates a new, empty [`DependentWeave`].
    #[ensures(result@.nodes.len() == 0)]
    #[ensures(result.metadata == metadata)]
    #[inline]
    pub fn new(metadata: M) -> Self {
        snapshot! { lemma_empty_acyclic::<K, T, S> };

        Self {
            nodes: HashMap::with_hasher(S::default()),
            roots: IndexSet::with_hasher(S::default()),
            active: None,
            bookmarked: IndexSet::with_hasher(S::default()),
            scratchpad: Scratchpad::new(),
            metadata,
        }
    }
    /// Creates a new, empty [`DependentWeave`] with at least the specified capacity.
    ///
    /// This function over-allocates for worst-case memory usage rather than average-case.
    #[ensures(result@.nodes.len() == 0)]
    #[ensures(result.metadata == metadata)]
    #[inline]
    pub fn with_capacity(capacity: usize, metadata: M) -> Self {
        snapshot! { lemma_empty_acyclic::<K, T, S> };

        let nodes = HashMap::with_capacity_and_hasher(capacity, S::default());
        let capacity = nodes.capacity();

        Self {
            nodes,
            roots: IndexSet::with_capacity_and_hasher(capacity, S::default()),
            active: None,
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
            .min(self.bookmarked.capacity())
    }
    /// Reserves capacity for at least `additional` more nodes.
    #[check(terminates)]
    #[ensures((^self)@ == self@)]
    #[ensures((^self).metadata == self.metadata)]
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.nodes.reserve(additional);
        self.roots
            .reserve(self.nodes.capacity().saturating_sub(self.roots.len()));
        self.bookmarked
            .reserve(self.nodes.capacity().saturating_sub(self.bookmarked.len()));
    }
    /// Shrinks the capacity of the weave as much as possible.
    #[check(terminates)]
    #[ensures((^self)@ == self@)]
    #[ensures((^self).metadata == self.metadata)]
    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.nodes.shrink_to_fit();
        self.roots.shrink_to_fit();
        self.bookmarked.shrink_to_fit();
        self.scratchpad = Scratchpad::new();
    }
}

impl<K, T, M, S> Weave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    type Nodes = HashMap<K, DependentNode<K, T, S>, S>;
    type Roots = IndexSet<K, S>;

    #[check(terminates)]
    #[ensures(result@ == self@.nodes.len())]
    #[inline]
    fn len(&self) -> usize {
        self.nodes.len()
    }
    #[check(terminates)]
    #[ensures(result == (self@.nodes.len() == 0))]
    #[inline]
    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    #[check(terminates)]
    #[ensures(result.model() == self@.nodes)]
    #[inline]
    fn nodes(&self) -> &Self::Nodes {
        &self.nodes
    }
    #[check(terminates)]
    #[ensures(result.model() == self@.roots)]
    #[inline]
    fn roots(&self) -> &Self::Roots {
        &self.roots
    }
    #[check(terminates)]
    #[ensures(result == self@.nodes.contains(&id.deep_model()))]
    #[inline]
    fn contains(&self, id: &K) -> bool {
        self.nodes.contains_key(id)
    }
    #[check(terminates)]
    #[ensures(result == (self@.active == Some(id.deep_model())))]
    #[inline]
    #[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
    fn contains_active(&self, id: &K) -> bool {
        match self.active {
            Some(active) => same_id(&active, id),
            None => false,
        }
    }
    #[check(terminates)]
    #[ensures(match result {
        Some(node) => self@.nodes.lookup(id.deep_model()) == Some(*node),
        None => !self@.nodes.contains(&id.deep_model()),
    })]
    #[inline]
    fn get(&self, id: &K) -> Option<&DependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    #[check(terminates)]
    #[ensures(match result {
        Some(from) => self@.nodes.contains(&id.deep_model())
            && *from == self@.nodes[id.deep_model()].from,
        None => !self@.nodes.contains(&id.deep_model()),
    })]
    #[inline]
    #[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
    fn get_parents(&self, id: &K) -> Option<&Option<K>> {
        match self.nodes.get(id) {
            Some(node) => Some(&node.from),
            None => None,
        }
    }
    #[check(terminates)]
    #[ensures(match result {
        Some(to) => self@.nodes.contains(&id.deep_model())
            && *to == self@.nodes[id.deep_model()].to,
        None => !self@.nodes.contains(&id.deep_model()),
    })]
    #[inline]
    #[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
    fn get_children(&self, id: &K) -> Option<&IndexSet<K, S>> {
        match self.nodes.get(id) {
            Some(node) => Some(&node.to),
            None => None,
        }
    }
    #[check(terminates)]
    #[ensures(match result {
        Some(contents) => self@.nodes.contains(&id.deep_model())
            && *contents == self@.nodes[id.deep_model()].contents,
        None => !self@.nodes.contains(&id.deep_model()),
    })]
    #[inline]
    #[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
    fn get_contents(&self, id: &K) -> Option<&T> {
        match self.nodes.get(id) {
            Some(node) => Some(&node.contents),
            None => None,
        }
    }
    #[check(terminates)]
    #[ensures((^output)@.len() == (^self)@.nodes.len())]
    #[ensures(topological((^self)@.nodes, ids((^output)@)))]
    #[ensures((^self)@.same_keys(self@))]
    #[ensures(same_elements((^self)@.roots, self@.roots))]
    #[ensures((^self)@.active == self@.active)]
    #[ensures(same_elements((^self)@.bookmarked, self@.bookmarked))]
    // Beyond the contract: the weave and its metadata are unchanged.
    #[ensures((^self)@ == self@)]
    #[ensures((^self).metadata == self.metadata)]
    // Beyond the contract: the order is the pre-order traversal of the roots' subtrees, in order.
    #[ensures(ids((^output)@) == preorder(self@.nodes, self@.roots))]
    fn get_ordered_identifiers(&mut self, output: &mut Vec<K>) {
        output.clear();
        output.reserve(self.nodes.len());
        proof_assert! { lemma_ids_empty(output@); ids(output@) == Seq::empty() };

        proof! {
            let view = snapshot!((*self)@);
        }
        let guard = self.scratchpad.guard();
        let mut stack = guard.vec();

        let mut i = 0;

        #[invariant(i@ <= view.roots.len())]
        #[invariant(stack.model().len() == 0)]
        #[invariant(ordered_prefix(*view, i@, ids(output@)))]
        #[invariant(ids(output@) == preorder(view.nodes, view.roots.subsequence(0, i@)))]
        #[variant(view.roots.len() - i@)]
        while i < self.roots.len() {
            topological_sort(&self.nodes, self.roots[i], &mut stack, output);
            proof_assert! {
                lemma_ordered_step(*view, i@, ids(output@), ids(output@));
                true
            };
            proof_assert!(view.roots.subsequence(0, i@ + 1).ext_eq(
                view.roots.subsequence(0, i@).concat(Seq::singleton(view.roots[i@]))
            ));
            proof_assert! {
                lemma_preorder_concat(
                    view.nodes,
                    view.roots.subsequence(0, i@),
                    Seq::singleton(view.roots[i@]),
                );
                ids(output@) == preorder(view.nodes, view.roots.subsequence(0, i@ + 1))
            };
            #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
            {
                i += 1;
            }
        }
        proof_assert! { lemma_ordered_done(*view, ids(output@)); true };
        proof_assert! { lemma_full_prefix(view.roots); true };
    }
    #[check(terminates)]
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
        == ((^self)@.nodes.contains(&d) && descends((^self)@.nodes, d, id.deep_model())))]
    #[ensures((^self)@ == self@)]
    #[ensures((^self).metadata == self.metadata)]
    #[ensures(self@.nodes.contains(&id.deep_model())
        ==> traversal(self@.nodes, id.deep_model(), ids((^output)@)))]
    // Beyond the contract: the order is the pre-order traversal of the node's subtree.
    #[ensures(self@.nodes.contains(&id.deep_model())
        ==> ids((^output)@) == preorder(self@.nodes, Seq::singleton(id.deep_model())))]
    fn get_ordered_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        snapshot! { lemma_concat_contains::<Id<K>> };
        snapshot! { lemma_ancestor_node::<K, T, S> };
        output.clear();
        proof_assert! { lemma_ids_empty(output@); ids(output@) == Seq::empty() };

        if self.nodes.contains_key(id) {
            let guard = self.scratchpad.guard();

            topological_sort(&self.nodes, *id, &mut guard.vec(), output);
        }
    }
    #[check(terminates)]
    #[ensures((^self)@.active == first(ids((^output)@)))]
    #[ensures(no_dup(ids((^output)@)))]
    #[ensures(valid_path((^self)@.nodes, ids((^output)@)))]
    #[ensures((^self)@.same_keys(self@))]
    #[ensures(same_elements((^self)@.roots, self@.roots))]
    #[ensures((^self)@.active == self@.active)]
    #[ensures(same_elements((^self)@.bookmarked, self@.bookmarked))]
    // Beyond the contract: the weave and its metadata are unchanged.
    #[ensures((^self)@ == self@)]
    #[ensures((^self).metadata == self.metadata)]
    fn get_active_path(&mut self, output: &mut Vec<K>) {
        output.clear();

        if let Some(active) = self.active {
            path_to_root(&self.nodes, active, output);
        }
    }
    #[check(terminates)]
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
    fn get_path_from(&mut self, id: &K, output: &mut Vec<K>) {
        output.clear();

        if self.nodes.contains_key(id) {
            path_to_root(&self.nodes, *id, output);
        }
    }
    #[check(terminates)]
    #[ensures(result ==> (^self)@.nodes.len() == self@.nodes.len() + 1)]
    #[ensures(result ==> !self@.nodes.contains(&node@.id))]
    #[ensures(result ==> (^self)@.nodes.contains(&node@.id))]
    #[ensures(result ==> node.active == ((^self)@.active == Some(node@.id)))]
    #[ensures(result ==> node.bookmarked == (^self)@.bookmarked.contains(node@.id))]
    #[ensures(result ==> node@.from != None || (^self)@.roots.contains(node@.id))]
    #[ensures(result ==> node@.from == None || same_elements((^self)@.roots, self@.roots))]
    #[ensures(!result ==> (^self)@ == self@)]
    // Beyond the contract: when insertion succeeds, and what it adds.
    #[ensures(result == (node@.from != Some(node@.id)
        && node@.to.len() == 0
        && !self@.nodes.contains(&node@.id)
        && match node@.from { Some(p) => self@.nodes.contains(&p), None => true }))]
    #[ensures(result ==> (^self)@.nodes.lookup(node@.id) == Some(node))]
    #[ensures(result ==> forall<k> (^self)@.nodes.contains(&k)
        == (self@.nodes.contains(&k) || k == node@.id))]
    #[ensures(result ==> (^self)@.active == if node.active { Some(node@.id) } else { self@.active })]
    // Beyond the contract: the parent gains the node as its last child, a root node becomes the last
    // root, a bookmarked node the last bookmark, and nothing else changes.
    #[ensures(result ==> forall<k> self@.nodes.contains(&k) ==> (^self)@.nodes[k]@.id == self@.nodes[k]@.id
        && (^self)@.nodes[k]@.from == self@.nodes[k]@.from
        && (^self)@.nodes[k]@.to
            == if node@.from == Some(k) { self@.nodes[k]@.to.snoc(node@.id) } else { self@.nodes[k]@.to }
        && (^self)@.nodes[k]@.bookmarked == self@.nodes[k]@.bookmarked
        && (^self)@.nodes[k].contents == self@.nodes[k].contents)]
    #[ensures(result ==> (^self)@.roots
        == if node@.from == None { self@.roots.snoc(node@.id) } else { self@.roots })]
    #[ensures(result ==> (^self)@.bookmarked
        == if node.bookmarked { self@.bookmarked.snoc(node@.id) } else { self@.bookmarked })]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn insert(&mut self, node: DependentNode<K, T, S>) -> bool {
        let own_parent = match node.from {
            Some(from) => same_id(&from, &node.id),
            None => false,
        };
        if own_parent || !node.to.is_empty() {
            return false;
        }

        match self.nodes.entry(node.id) {
            Entry::Occupied(_) => return false,
            Entry::Vacant(entry) => {
                let (id, from, active, bookmarked) =
                    (node.id, node.from, node.active, node.bookmarked);

                entry.insert(node);

                if let Some(from) = from {
                    if let Some(parent) = self.nodes.get_mut(&from) {
                        parent.to.insert(id);
                    } else {
                        self.nodes.remove(&id);
                        return false;
                    }
                } else {
                    self.roots.insert(id);
                }

                if active {
                    if let Some(active) = self.active
                        && let Some(active) = self.nodes.get_mut(&active)
                    {
                        active.active = false;
                    }

                    self.active = Some(id);
                }

                if bookmarked {
                    self.bookmarked.insert(id);
                }
            }
        }

        true
    }
    #[check(terminates)]
    #[ensures(result ==> value == ((^self)@.active == Some(id.deep_model())))]
    #[ensures(!result ==> (^self)@.active == self@.active)]
    #[ensures(result == self@.nodes.contains(&id.deep_model()))]
    #[ensures((^self)@.same_keys(self@))]
    #[ensures((^self)@.roots == self@.roots)]
    #[ensures((^self)@.bookmarked == self@.bookmarked)]
    // Beyond the contract: which node becomes active, and that nothing else changes.
    #[ensures(result ==> (^self)@.active == self@.active_after_set(id.deep_model(), value))]
    #[ensures(same_graph((^self)@.nodes, self@.nodes))]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn set_active(&mut self, id: &K, value: bool) -> bool {
        snapshot! { lemma_reflagged((*self)@) };

        match self.nodes.get_mut(id) {
            Some(node) => {
                node.active = value;

                if value {
                    if let Some(active) = self.active
                        && !same_id(&active, &node.id)
                        && let Some(active) = self.nodes.get_mut(&active)
                    {
                        active.active = false;
                    }

                    self.active = Some(*id);
                } else if let Some(active) = self.active
                    && same_id(&active, &node.id)
                {
                    self.active = node.from;

                    if let Some(active) = self.active
                        && let Some(active) = self.nodes.get_mut(&active)
                    {
                        active.active = true;
                    }
                }

                true
            }
            None => false,
        }
    }
    #[ensures(!(^self)@.nodes.contains(&id.deep_model()))]
    #[ensures((result != None) == self@.nodes.contains(&id.deep_model()))]
    #[ensures(match result { Some(node) => node@.id == id.deep_model(), None => true })]
    #[ensures(result != None ==> (^self)@.nodes.len() < self@.nodes.len())]
    #[ensures(result != None ==> (^self)@.bookmarked.len() <= self@.bookmarked.len())]
    #[ensures(result == None ==> (^self)@.same_keys(self@))]
    #[ensures(result == None ==> same_elements((^self)@.roots, self@.roots))]
    #[ensures(result == None ==> (^self)@.active == self@.active)]
    #[ensures(result == None ==> same_elements((^self)@.bookmarked, self@.bookmarked))]
    // Beyond the contract: exactly the subtree rooted at `id` is removed.
    #[ensures(result != None ==> result == self@.nodes.lookup(id.deep_model()))]
    #[ensures(forall<k> (^self)@.nodes.contains(&k)
        == (self@.nodes.contains(&k) && !in_subtree(self@.nodes, id.deep_model(), k)))]
    #[ensures(forall<k> (^self)@.nodes.contains(&k)
        ==> survives_removal((^self)@.nodes[k], self@.nodes[k], id.deep_model()))]
    #[ensures((^self)@.active == self@.active_after_removal(id.deep_model()))]
    // Beyond the contract: the roots, the bookmarks and each node's children keep their order, and a
    // failed removal changes nothing.
    #[ensures(in_order((^self)@.roots, self@.roots))]
    #[ensures(in_order((^self)@.bookmarked, self@.bookmarked))]
    #[ensures(forall<k> (^self)@.nodes.contains(&k)
        ==> in_order((^self)@.nodes[k]@.to, self@.nodes[k]@.to))]
    #[ensures(result == None ==> (^self)@ == self@)]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn remove(&mut self, id: &K) -> Option<DependentNode<K, T, S>> {
        snapshot! { lemma_ids_snoc::<K> };
        snapshot! { lemma_ids_singleton::<K> };
        snapshot! { lemma_snoc_contains::<Id<K>> };
        proof! {
            let old = snapshot!((*self)@);
            let target = snapshot!(id.deep_model());
            let sources = snapshot!(Seq::singleton(*target));
            let mut added = snapshot!(Seq::<Id<K>>::empty());
        }
        let Self {
            nodes,
            roots,
            active,
            bookmarked,
            scratchpad,
            ..
        } = self;

        let mut removed_node = None;
        let mut removed_active = false;

        let guard = scratchpad.guard();
        let mut stack = guard.vec();
        let mut removed_bookmarks = guard.set(S::default());

        stack.push(*id);
        proof_assert! {
            old.nodes.contains(&*target) ==> {
                lemma_single_source(old.nodes, *target);
                lemma_dfs_init(old.nodes, *sources);
                dfs(old.nodes, *sources, *added, ids(stack.model()))
            }
        };

        #[invariant(old.nodes.contains(&*target)
            ==> dfs(old.nodes, *sources, *added, ids(stack.model())))]
        #[invariant(!old.nodes.contains(&*target) ==> added.len() == 0
            && forall<i> 0 <= i && i < stack.model().len() ==> ids(stack.model())[i] == *target)]
        #[invariant(forall<k> nodes.model().lookup(k)
            == if added.contains(k) { None } else { old.nodes.lookup(k) })]
        #[invariant(nodes.model().len() == old.nodes.len() - added.len())]
        #[invariant(forall<k> removed_bookmarks.model().contains(&k)
            == (added.contains(k) && old.nodes[k]@.bookmarked))]
        #[invariant(removed_active == match old.active { Some(a) => added.contains(a), None => false })]
        #[invariant(active.deep_model() == if removed_active { None } else { old.active })]
        #[invariant(match removed_node {
            None => added.len() == 0,
            Some(n) => 0 < added.len() && old.nodes.lookup(*target) == Some(n),
        })]
        #[invariant(forall<k> roots.model().contains(k)
            == (old.roots.contains(k) && (k != *target || added.len() == 0)))]
        #[invariant(in_order(roots.model(), old.roots))]
        #[invariant(added.len() == 0 ==> roots.model() == old.roots)]
        #[invariant(bookmarked.model() == old.bookmarked)]
        #[variant((nodes.model().len(), stack.model().len()))]
        while let Some(id) = stack.pop() {
            proof! {
                let rest = snapshot!(ids(stack.model()));
            }
            if let Some(node) = nodes.remove(&id) {
                if node.bookmarked {
                    removed_bookmarks.insert(id);
                }
                if node.active {
                    *active = None;
                    removed_active = true;
                }

                extend_from_set(&mut stack, &node.to);
                proof_assert! {
                    lemma_dfs_step(old.nodes, *sources, *added, *rest, id.deep_model(), node.to.model());
                    dfs(old.nodes, *sources, added.snoc(id.deep_model()), ids(stack.model()))
                };
                proof_assert!(added.len() == 0 ==> id.deep_model() == *target);
                proof! {
                    added = snapshot!(added.snoc(id.deep_model()));
                }

                if removed_node.is_none() {
                    if node.from.is_none() {
                        roots.shift_remove(&id);
                        proof_assert! {
                            lemma_in_order_shift_remove(roots.model(), old.roots, id.deep_model());
                            in_order(roots.model(), old.roots)
                        };
                    }
                    removed_node = Some(node);
                }
            }
        }

        if let Some(removed) = removed_node {
            proof_assert! {
                lemma_dfs_done(old.nodes, *sources, *added);
                forall<d> added.contains(d) == in_subtree(old.nodes, *target, d)
            };
            proof_assert! {
                lemma_parent_outside(old.nodes, *target);
                match old.nodes[*target]@.from {
                    Some(p) => nodes.model().contains(&p) && old.nodes[p]@.to.contains(*target),
                    None => old.roots.contains(*target),
                }
            };
            proof_assert!(forall<k> old.bookmarked.contains(k) && in_subtree(old.nodes, *target, k)
                ==> removed_bookmarks.model().contains(&k));
            proof_assert!(
                removed_active
                    == match old.active {
                        Some(a) => in_subtree(old.nodes, *target, a),
                        None => false,
                    }
            );
            if let Some(parent_node) = removed.from.as_ref().and_then(|id| nodes.get_mut(id)) {
                proof! {
                    let children = snapshot!(parent_node.to.model());
                }
                parent_node.to.shift_remove(id);
                proof_assert! {
                    lemma_in_order_shift_remove(parent_node.to.model(), *children, id.deep_model());
                    in_order(parent_node.to.model(), *children)
                };
            }
            if !removed_bookmarks.is_empty() {
                bookmarked.retain(|id| !removed_bookmarks.contains(id));
            }
            if removed_active {
                *active = removed.from;

                if let Some(node) = active.and_then(|id| nodes.get_mut(&id)) {
                    node.active = true;
                }
            }
            proof! {
                let new = snapshot!(DependentWeaveView {
                    nodes: nodes.model(),
                    roots: roots.model(),
                    active: active.deep_model(),
                    bookmarked: bookmarked.model(),
                });
            }
            proof_assert!(forall<k> new.nodes.contains(&k)
                == (old.nodes.contains(&k) && !in_subtree(old.nodes, *target, k)));
            proof_assert!(forall<k> new.roots.contains(k) == (old.roots.contains(k) && k != *target));
            proof_assert!(new.active == old.active_after_removal(*target));
            proof_assert!(forall<k> new.bookmarked.contains(k)
                == (old.bookmarked.contains(k) && !in_subtree(old.nodes, *target, k)));
            proof_assert!(forall<k> new.nodes.contains(&k) ==> new.nodes[k]@.id == old.nodes[k]@.id
                && new.nodes[k]@.from == old.nodes[k]@.from
                && new.nodes[k]@.bookmarked == old.nodes[k]@.bookmarked
                && new.nodes[k].contents == old.nodes[k].contents
                && new.nodes[k]@.active == (new.active == Some(k))
                && forall<c> new.nodes[k]@.to.contains(c) == (old.nodes[k]@.to.contains(c) && c != *target));
            proof_assert!(removal_shape(*old, *new, *target));
            proof_assert! {
                let _ = lemma_in_order_refl::<Id<K>>;
                in_order(new.roots, old.roots)
                    && in_order(new.bookmarked, old.bookmarked)
                    && forall<k> new.nodes.contains(&k) ==> in_order(new.nodes[k]@.to, old.nodes[k]@.to)
            };
            proof_assert! {
                lemma_subset_len(new.bookmarked, old.bookmarked);
                new.bookmarked.len() <= old.bookmarked.len()
            };
            proof_assert! { lemma_remove_valid(*old, *new, *target); new.valid() };
            Some(removed)
        } else {
            proof_assert! {
                let _ = lemma_ancestor_node::<K, T, S>;
                !old.nodes.contains(&*target)
                    && (forall<k> !in_subtree(old.nodes, *target, k))
                    && nodes.model().ext_eq(old.nodes)
                    && roots.model() == old.roots
            };
            None
        }
    }
    #[requires(forall<f> on_removal.hist_inv(f)
        ==> forall<n: DependentNode<K, T, S>> inv(n) ==> f.precondition((n,)))]
    #[ensures(!(^self)@.nodes.contains(&id.deep_model()))]
    #[ensures(result == self@.nodes.contains(&id.deep_model()))]
    #[ensures(result ==> (^self)@.nodes.len() < self@.nodes.len())]
    #[ensures(result ==> (^self)@.bookmarked.len() <= self@.bookmarked.len())]
    #[ensures(!result ==> (^self)@.same_keys(self@))]
    #[ensures(!result ==> same_elements((^self)@.roots, self@.roots))]
    #[ensures(!result ==> (^self)@.active == self@.active)]
    #[ensures(!result ==> same_elements((^self)@.bookmarked, self@.bookmarked))]
    // Beyond the contract: exactly the subtree rooted at `id` is removed.
    #[ensures(forall<k> (^self)@.nodes.contains(&k)
        == (self@.nodes.contains(&k) && !in_subtree(self@.nodes, id.deep_model(), k)))]
    #[ensures(forall<k> (^self)@.nodes.contains(&k)
        ==> survives_removal((^self)@.nodes[k], self@.nodes[k], id.deep_model()))]
    #[ensures((^self)@.active == self@.active_after_removal(id.deep_model()))]
    // Beyond the contract: the roots, the bookmarks and each node's children keep their order, and a
    // failed removal changes nothing.
    #[ensures(in_order((^self)@.roots, self@.roots))]
    #[ensures(in_order((^self)@.bookmarked, self@.bookmarked))]
    #[ensures(forall<k> (^self)@.nodes.contains(&k)
        ==> in_order((^self)@.nodes[k]@.to, self@.nodes[k]@.to))]
    #[ensures(!result ==> (^self)@ == self@)]
    // Beyond the contract: `on_removal` is called with each removed node, as it was, in the order of
    // a traversal of the subtree.
    #[ensures(exists<order: Seq<Id<K>>> (result ==> traversal(self@.nodes, id.deep_model(), order))
        && (!result ==> order.len() == 0)
        && fed(on_removal, order.map(value_of(self@.nodes))))]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    #[allow(clippy::option_if_let_else, reason = "The branches hold proofs")]
    fn remove_tracked(
        &mut self,
        id: &K,
        mut on_removal: impl FnMut(DependentNode<K, T, S>),
    ) -> bool {
        snapshot! { lemma_ids_snoc::<K> };
        snapshot! { lemma_ids_singleton::<K> };
        snapshot! { lemma_snoc_contains::<Id<K>> };
        proof! {
            let old = snapshot!((*self)@);
            let target = snapshot!(id.deep_model());
            let sources = snapshot!(Seq::singleton(*target));
            let mut added = snapshot!(Seq::<Id<K>>::empty());
            let callback = snapshot!(on_removal);
            let mut states = snapshot!(Seq::singleton(on_removal));
        }
        let Self {
            nodes,
            roots,
            active,
            bookmarked,
            scratchpad,
            ..
        } = self;

        let mut removed_node_parent: Option<Option<K>> = None;
        let mut removed_active = false;

        let guard = scratchpad.guard();
        let mut stack = guard.vec();
        let mut removed_bookmarks = guard.set(S::default());

        stack.push(*id);
        proof_assert! {
            old.nodes.contains(&*target) ==> {
                lemma_single_source(old.nodes, *target);
                lemma_dfs_init(old.nodes, *sources);
                dfs(old.nodes, *sources, *added, ids(stack.model()))
            }
        };

        #[invariant(old.nodes.contains(&*target)
            ==> dfs(old.nodes, *sources, *added, ids(stack.model())))]
        #[invariant(!old.nodes.contains(&*target) ==> added.len() == 0
            && forall<i> 0 <= i && i < stack.model().len() ==> ids(stack.model())[i] == *target)]
        #[invariant(forall<k> nodes.model().lookup(k)
            == if added.contains(k) { None } else { old.nodes.lookup(k) })]
        #[invariant(nodes.model().len() == old.nodes.len() - added.len())]
        #[invariant(forall<k> removed_bookmarks.model().contains(&k)
            == (added.contains(k) && old.nodes[k]@.bookmarked))]
        #[invariant(removed_active == match old.active { Some(a) => added.contains(a), None => false })]
        #[invariant(active.deep_model() == if removed_active { None } else { old.active })]
        #[invariant(match removed_node_parent {
            None => added.len() == 0,
            Some(from) => 0 < added.len() && from.deep_model() == old.nodes[*target]@.from,
        })]
        #[invariant(forall<k> roots.model().contains(k)
            == (old.roots.contains(k) && (k != *target || added.len() == 0)))]
        #[invariant(in_order(roots.model(), old.roots))]
        #[invariant(added.len() == 0 ==> roots.model() == old.roots)]
        #[invariant(bookmarked.model() == old.bookmarked)]
        #[invariant(callback.hist_inv(on_removal))]
        #[invariant(calls(*states, added.map(value_of(old.nodes))))]
        #[invariant(states[0] == *callback && states[added.len()] == on_removal)]
        #[variant((nodes.model().len(), stack.model().len()))]
        while let Some(id) = stack.pop() {
            proof! {
                let rest = snapshot!(ids(stack.model()));
            }
            if let Some(node) = nodes.remove(&id) {
                proof! {
                    let prev = snapshot!(*added);
                }
                if node.bookmarked {
                    removed_bookmarks.insert(id);
                }
                if node.active {
                    *active = None;
                    removed_active = true;
                }

                extend_from_set_rev(&mut stack, &node.to);
                proof_assert! {
                    lemma_reverse(node.to.model());
                    lemma_dfs_step(
                        old.nodes,
                        *sources,
                        *added,
                        *rest,
                        id.deep_model(),
                        node.to.model().reverse(),
                    );
                    dfs(old.nodes, *sources, added.snoc(id.deep_model()), ids(stack.model()))
                };
                proof_assert!(added.len() == 0 ==> id.deep_model() == *target);
                proof! {
                    added = snapshot!(added.snoc(id.deep_model()));
                }

                if removed_node_parent.is_none() {
                    if node.from.is_none() {
                        roots.shift_remove(&id);
                        proof_assert! {
                            lemma_in_order_shift_remove(roots.model(), old.roots, id.deep_model());
                            in_order(roots.model(), old.roots)
                        };
                    }
                    removed_node_parent = Some(node.from);
                }

                proof_assert!(node == old.nodes[id.deep_model()]);
                on_removal(node);
                proof_assert! {
                    lemma_calls_snoc(
                        *states,
                        prev.map(value_of(old.nodes)),
                        old.nodes[id.deep_model()],
                        on_removal,
                    );
                    prev.snoc_map(id.deep_model(), value_of(old.nodes));
                    calls(states.snoc(on_removal), added.map(value_of(old.nodes)))
                };
                proof! {
                    states = snapshot!(states.snoc(on_removal));
                }
            }
        }

        if let Some(parent) = removed_node_parent {
            proof_assert! {
                lemma_dfs_done(old.nodes, *sources, *added);
                forall<d> added.contains(d) == in_subtree(old.nodes, *target, d)
            };
            proof_assert! {
                lemma_ids_empty(stack.model());
                lemma_traversal(old.nodes, *target, *added);
                traversal(old.nodes, *target, *added)
            };
            proof_assert! {
                lemma_parent_outside(old.nodes, *target);
                match old.nodes[*target]@.from {
                    Some(p) => nodes.model().contains(&p) && old.nodes[p]@.to.contains(*target),
                    None => old.roots.contains(*target),
                }
            };
            proof_assert!(forall<k> old.bookmarked.contains(k) && in_subtree(old.nodes, *target, k)
                ==> removed_bookmarks.model().contains(&k));
            proof_assert!(
                removed_active
                    == match old.active {
                        Some(a) => in_subtree(old.nodes, *target, a),
                        None => false,
                    }
            );
            if let Some(parent_node) = parent.as_ref().and_then(|id| nodes.get_mut(id)) {
                proof! {
                    let children = snapshot!(parent_node.to.model());
                }
                parent_node.to.shift_remove(id);
                proof_assert! {
                    lemma_in_order_shift_remove(parent_node.to.model(), *children, id.deep_model());
                    in_order(parent_node.to.model(), *children)
                };
            }
            if !removed_bookmarks.is_empty() {
                bookmarked.retain(|id| !removed_bookmarks.contains(id));
            }
            if removed_active {
                *active = parent;

                if let Some(node) = active.and_then(|id| nodes.get_mut(&id)) {
                    node.active = true;
                }
            }
            proof! {
                let new = snapshot!(DependentWeaveView {
                    nodes: nodes.model(),
                    roots: roots.model(),
                    active: active.deep_model(),
                    bookmarked: bookmarked.model(),
                });
            }
            proof_assert!(forall<k> new.nodes.contains(&k)
                == (old.nodes.contains(&k) && !in_subtree(old.nodes, *target, k)));
            proof_assert!(forall<k> new.roots.contains(k) == (old.roots.contains(k) && k != *target));
            proof_assert!(new.active == old.active_after_removal(*target));
            proof_assert!(forall<k> new.bookmarked.contains(k)
                == (old.bookmarked.contains(k) && !in_subtree(old.nodes, *target, k)));
            proof_assert!(forall<k> new.nodes.contains(&k) ==> new.nodes[k]@.id == old.nodes[k]@.id
                && new.nodes[k]@.from == old.nodes[k]@.from
                && new.nodes[k]@.bookmarked == old.nodes[k]@.bookmarked
                && new.nodes[k].contents == old.nodes[k].contents
                && new.nodes[k]@.active == (new.active == Some(k))
                && forall<c> new.nodes[k]@.to.contains(c) == (old.nodes[k]@.to.contains(c) && c != *target));
            proof_assert!(removal_shape(*old, *new, *target));
            proof_assert! {
                let _ = lemma_in_order_refl::<Id<K>>;
                in_order(new.roots, old.roots)
                    && in_order(new.bookmarked, old.bookmarked)
                    && forall<k> new.nodes.contains(&k) ==> in_order(new.nodes[k]@.to, old.nodes[k]@.to)
            };
            proof_assert! {
                lemma_subset_len(new.bookmarked, old.bookmarked);
                new.bookmarked.len() <= old.bookmarked.len()
            };
            proof_assert! { lemma_remove_valid(*old, *new, *target); new.valid() };
            true
        } else {
            proof_assert! {
                let _ = lemma_ancestor_node::<K, T, S>;
                !old.nodes.contains(&*target)
                    && (forall<k> !in_subtree(old.nodes, *target, k))
                    && nodes.model().ext_eq(old.nodes)
                    && roots.model() == old.roots
            };
            false
        }
    }
    #[check(terminates)]
    #[ensures((^self)@.nodes.len() == 0)]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn clear(&mut self) {
        self.nodes.clear();
        self.roots.clear();
        self.active = None;
        self.bookmarked.clear();
    }
}

impl<K, T, M, S> DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    /// Validates that the weave is internally consistent.
    #[check(terminates)]
    #[ensures(result == self@.valid())]
    pub fn validate(&self) -> bool {
        validate(
            &self.nodes,
            &self.roots,
            self.active.as_ref(),
            &self.bookmarked,
        )
    }
}

impl<K, T, M, S> MetadataWeave<K, DependentNode<K, T, S>, T, M> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    #[check(terminates)]
    #[ensures(*result == self.metadata)]
    #[inline]
    fn metadata(&self) -> &M {
        &self.metadata
    }
    #[requires(forall<m: &mut M> inv(m) ==> callback.precondition((m,)))]
    #[ensures((^self)@ == self@)]
    // Beyond the contract: the callback is called on the metadata, and leaves it as the weave's new
    // metadata.
    #[ensures(exists<m: &mut M> *m == self.metadata && ^m == (^self).metadata
        && callback.postcondition_once((m,), result))]
    #[inline]
    fn metadata_mut<O>(&mut self, callback: impl FnOnce(&mut M) -> O) -> O {
        callback(&mut self.metadata)
    }
}

impl<K, T, M, S> BookmarkableWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    type Bookmarks = IndexSet<K, S>;

    #[check(terminates)]
    #[ensures(result.model() == self@.bookmarked)]
    #[inline]
    fn bookmarks(&self) -> &Self::Bookmarks {
        &self.bookmarked
    }
    #[check(terminates)]
    #[ensures(result == self@.bookmarked.contains(id.deep_model()))]
    #[inline]
    fn contains_bookmark(&self, id: &K) -> bool {
        self.bookmarked.contains(id)
    }
    #[ensures(result ==> value == (^self)@.bookmarked.contains(id.deep_model()))]
    #[ensures(!result ==> (^self)@.bookmarked == self@.bookmarked)]
    #[ensures(result == self@.nodes.contains(&id.deep_model()))]
    #[ensures((^self)@.same_keys(self@))]
    #[ensures((^self)@.roots == self@.roots)]
    #[ensures((^self)@.active == self@.active)]
    #[ensures(forall<k> k != id.deep_model() ==> (^self)@.nodes.lookup(k) == self@.nodes.lookup(k))]
    #[ensures(result ==> (^self)@.nodes[id.deep_model()].contents == self@.nodes[id.deep_model()].contents
        && (^self)@.nodes[id.deep_model()]@.to == self@.nodes[id.deep_model()]@.to
        && (^self)@.nodes[id.deep_model()]@.from == self@.nodes[id.deep_model()]@.from)]
    // Beyond the contract: a new bookmark comes last, and the other bookmarks keep their order.
    #[ensures(result
        ==> bookmark_set((^self)@.bookmarked, self@.bookmarked, id.deep_model(), value))]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    #[check(terminates)]
    fn set_bookmarked(&mut self, id: &K, value: bool) -> bool {
        snapshot! { lemma_reflagged((*self)@) };

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

impl<K, T, M, S> SortableWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    #[requires(forall<f> cmp.hist_inv(f)
        ==> forall<a: &DependentNode<K, T, S>, b: &DependentNode<K, T, S>>
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
    #[ensures(result ==> forall<le: Mapping<DependentNode<K, T, S>, Mapping<DependentNode<K, T, S>, bool>>>
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
        mut cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
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
            proof_assert!(forall<le: Mapping<DependentNode<K, T, S>, Mapping<DependentNode<K, T, S>, bool>>>
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
        ==> forall<a: &DependentNode<K, T, S>, b: &DependentNode<K, T, S>>
            inv(a) && inv(b) ==> f.precondition((a, b)))]
    #[ensures(same_elements((^self)@.roots, self@.roots))]
    #[ensures((^self)@.nodes == self@.nodes)]
    #[ensures((^self)@.active == self@.active)]
    #[ensures((^self)@.bookmarked == self@.bookmarked)]
    // Beyond the contract: the roots are sorted stably.
    #[ensures(forall<le: Mapping<DependentNode<K, T, S>, Mapping<DependentNode<K, T, S>, bool>>>
        total_preorder(le) && compares_by(cmp, identity(), le)
        ==> sorted_stably((^self)@.roots, self@.roots, order_by(le, value_of(self@.nodes))))]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn sort_roots_by(
        &mut self,
        mut cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
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
    #[inline]
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.roots.sort_by(cmp);
    }
}

impl<K, T, M, S> SortableBookmarkableWeave<K, DependentNode<K, T, S>, T>
    for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    #[requires(forall<f> cmp.hist_inv(f)
        ==> forall<a: &DependentNode<K, T, S>, b: &DependentNode<K, T, S>>
            inv(a) && inv(b) ==> f.precondition((a, b)))]
    #[ensures(same_elements((^self)@.bookmarked, self@.bookmarked))]
    #[ensures((^self)@.nodes == self@.nodes)]
    #[ensures((^self)@.roots == self@.roots)]
    #[ensures((^self)@.active == self@.active)]
    // Beyond the contract: the bookmarks are sorted stably.
    #[ensures(forall<le: Mapping<DependentNode<K, T, S>, Mapping<DependentNode<K, T, S>, bool>>>
        total_preorder(le) && compares_by(cmp, identity(), le)
        ==> sorted_stably((^self)@.bookmarked, self@.bookmarked, order_by(le, value_of(self@.nodes))))]
    // Beyond the contract: the metadata is unchanged.
    #[ensures((^self).metadata == self.metadata)]
    fn sort_bookmarks_by(
        &mut self,
        mut cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
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
    #[inline]
    fn sort_bookmarks_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.bookmarked.sort_by(cmp);
    }
}

impl<K, T, M, S> ActiveSingularWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    #[check(terminates)]
    #[ensures(result.deep_model() == self@.active)]
    #[inline]
    fn active(&self) -> Option<K> {
        self.active
    }
}

impl<K, T, M, S> DiscreteWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    #[ensures(result ==> (^self)@.nodes.len() == self@.nodes.len() + 1)]
    #[ensures(result ==> (^self)@.nodes.contains(&id.deep_model()))]
    #[ensures(result ==> (^self)@.nodes.contains(&new_id.deep_model()))]
    #[ensures(result ==> !self@.nodes.contains(&new_id.deep_model()))]
    #[ensures(result ==> (^self)@.nodes[id.deep_model()]@.to.contains(new_id.deep_model())
        && (^self)@.nodes[id.deep_model()]@.to.len() == 1)]
    #[ensures(result ==> (^self)@.nodes[new_id.deep_model()]@.from == Some(id.deep_model()))]
    #[ensures(result ==> same_elements(
        self@.nodes[id.deep_model()]@.to,
        (^self)@.nodes[new_id.deep_model()]@.to,
    ))]
    #[ensures(!result ==> (^self)@.same_keys(self@))]
    #[ensures(same_elements((^self)@.roots, self@.roots))]
    #[ensures((^self)@.active == self@.active)]
    #[ensures(same_elements((^self)@.bookmarked, self@.bookmarked))]
    // Beyond the contract: how the node is split, that no other contents change, and that a failed
    // split changes nothing.
    #[ensures(result ==> split_shape(self@, (^self)@, id.deep_model(), new_id.deep_model()))]
    #[ensures(result ==> forall<k> self@.nodes.contains(&k) && k != id.deep_model()
        ==> (^self)@.nodes[k].contents == self@.nodes[k].contents)]
    #[ensures(!result ==> (^self)@ == self@)]
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

        if self.nodes.contains_key(&new_id) || *id == new_id {
            return false;
        }

        if let Some(mut node) = self.nodes.remove(id) {
            match node.contents.split(at) {
                DiscreteContentResult::Two(left, right) => {
                    let left_node = DependentNode {
                        id: node.id,
                        from: node.from,
                        to: {
                            let mut to = IndexSet::with_capacity_and_hasher(1, S::default());
                            to.insert(new_id);
                            to
                        },
                        active: node.active,
                        bookmarked: node.bookmarked,
                        contents: left,
                    };

                    node.from = Some(node.id);
                    node.id = new_id;
                    node.contents = right;
                    node.active = false;
                    node.bookmarked = false;

                    proof_assert!(linked(old.nodes, id.deep_model()));
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
                            && self.nodes.model()[k]@.from
                                == if node.to.model().subsequence(0, produced.len()).contains(k) {
                                    Some(new_id.deep_model())
                                } else {
                                    old.nodes[k]@.from
                                })]
                    for i in 0..node.to.len() {
                        proof_assert!(old.nodes[id.deep_model()]@.to.contains(node.to.model()[i@]));
                        let child = self.nodes.get_mut(&node.to[i]).unwrap();
                        child.from = Some(node.id);
                    }

                    self.nodes.insert(left_node.id, left_node);
                    proof! {
                        let view = snapshot!(DependentWeaveView {
                            nodes: self.nodes.model().add(new_id.deep_model(), node),
                            roots: self.roots.model(),
                            active: self.active.deep_model(),
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
                    self.nodes.insert(node.id, node);
                    proof_assert!(self.nodes.model().ext_eq(old.nodes));
                    false
                }
            }
        } else {
            proof_assert!(self.nodes.model().ext_eq(old.nodes));
            false
        }
    }
    #[ensures(result != None ==> (^self)@.nodes.len() == self@.nodes.len() - 1)]
    #[ensures(result != None ==> !(^self)@.nodes.contains(&id.deep_model()))]
    #[ensures(result != None ==> self@.nodes.contains(&id.deep_model()))]
    #[ensures(match result {
        Some(r) => self@.active == Some(id.deep_model()) ==> (^self)@.active == Some(r.deep_model()),
        None => true,
    })]
    #[ensures(match result {
        Some(r) => self@.active != Some(id.deep_model())
            ==> (^self)@.nodes[r.deep_model()]@.active == self@.nodes[r.deep_model()]@.active,
        None => true,
    })]
    #[ensures(match result {
        Some(r) => (^self)@.nodes[r.deep_model()]@.from == self@.nodes[r.deep_model()]@.from,
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
        Some(r) => self@.nodes[id.deep_model()]@.from == Some(r.deep_model()),
        None => true,
    })]
    #[ensures(result == None ==> (^self)@.same_keys(self@))]
    #[ensures(result == None ==> (^self)@.active == self@.active)]
    #[ensures(result == None ==> same_elements((^self)@.bookmarked, self@.bookmarked))]
    #[ensures(same_elements((^self)@.roots, self@.roots))]
    // Beyond the contract: the merged node is active exactly when either node was.
    #[ensures(match result {
        Some(r) => (^self)@.active
            == if self@.active == Some(id.deep_model()) { Some(r.deep_model()) } else { self@.active },
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
            && self@.nodes[id.deep_model()]@.from == Some(p)
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

        if let Some(mut node) = self.nodes.remove(id) {
            proof! {
                let p = snapshot!(match old.nodes[id.deep_model()]@.from {
                    Some(p) => p,
                    None => id.deep_model(),
                });
            }
            snapshot! { lemma_merge(*old, id.deep_model(), *p) };
            if let Some(mut parent) = node.from.as_ref().and_then(|id| self.nodes.remove(id)) {
                if parent.to.len() > 1 {
                    self.nodes.insert(parent.id, parent);
                    self.nodes.insert(node.id, node);
                    proof_assert!(self.nodes.model().ext_eq(old.nodes));
                    return None;
                }

                match parent.contents.merge(node.contents) {
                    DiscreteContentResult::Two(left, right) => {
                        parent.contents = left;
                        node.contents = right;
                        self.nodes.insert(parent.id, parent);
                        self.nodes.insert(node.id, node);
                        proof_assert!(self.nodes.model().ext_eq(old.nodes));
                        None
                    }
                    DiscreteContentResult::One(content) => {
                        parent.contents = content;
                        parent.to = node.to;

                        proof_assert!(linked(old.nodes, id.deep_model()) && linked(old.nodes, *p));
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
                                && self.nodes.model()[k]@.from
                                    == if parent.to.model().subsequence(0, produced.len()).contains(k) {
                                        Some(*p)
                                    } else {
                                        old.nodes[k]@.from
                                    })]
                        for i in 0..parent.to.len() {
                            proof_assert!(old.nodes[id.deep_model()]@.to.contains(parent.to.model()[i@]));
                            let child = self.nodes.get_mut(&parent.to[i]).unwrap();
                            child.from = Some(parent.id);
                        }

                        if node.active {
                            parent.active = true;
                            self.active = Some(parent.id);
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

                        Some(parent_id)
                    }
                }
            } else {
                self.nodes.insert(node.id, node);
                proof_assert!(self.nodes.model().ext_eq(old.nodes));
                None
            }
        } else {
            proof_assert!(self.nodes.model().ext_eq(old.nodes));
            None
        }
    }
}

impl<K, T, M, S> SemiIndependentWeave<K, DependentNode<K, T, S>, T> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
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
    #[inline]
    fn get_contents_mut<O>(&mut self, id: &K, callback: impl FnOnce(&mut T) -> O) -> Option<O> {
        snapshot! { lemma_reflagged((*self)@) };

        match self.nodes.get_mut(id) {
            Some(node) => Some(callback(&mut node.contents)),
            None => None,
        }
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, S> ArchivedDependentNode<K, T, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn validate(&self) -> bool {
        (if let ArchivedOption::Some(from) = &self.from {
            !self.to.contains(from)
        } else {
            true
        }) && self.from != Some(self.id)
            && !self.to.contains(&self.id)
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, S> Node<K::Archived, T::Archived> for ArchivedDependentNode<K, T, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
    S: BuildHasher + Default + Clone,
{
    type From = ArchivedOption<K::Archived>;
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
impl<K, T, M, S> ArchivedDependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
    M: Archive,
    S: BuildHasher + Default + Clone,
{
    fn validate(&self) -> bool {
        self.roots
            .iter()
            .all(move |value| self.nodes.contains_key(value))
            && self
                .active
                .as_ref()
                .is_none_or(|active| self.nodes.contains_key(active))
            && self
                .bookmarked
                .iter()
                .all(move |value| self.nodes.contains_key(value))
            && self.nodes.iter().all(|(key, value)| {
                value.validate()
                    && value.id == *key
                    && value
                        .from
                        .as_ref()
                        .is_none_or(|v| self.nodes.get(v).is_some_and(|p| p.to.contains(key)))
                    && value.to.iter().all(|v| {
                        self.nodes
                            .get(v)
                            .is_some_and(|p| p.from.as_ref() == Some(key))
                    })
                    && value.from.is_none() == self.roots.contains(key)
                    && value.active == (self.active == Some(*key))
                    && value.bookmarked == self.bookmarked.contains(key)
            })
            && !archived_detect_cycles(
                &self.nodes,
                self.roots.iter().copied(),
                &mut Vec::with_capacity(self.roots.len()),
                &mut HashSet::with_capacity(self.nodes.len()),
            )
    }
}

#[cfg(feature = "rkyv")]
#[allow(unsafe_code, reason = "rkyv limitation")]
// SAFETY:
// All fields are safe to access and no unsafe functions are called
unsafe impl<K, T, M, S, C> Verify<C> for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
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
impl<K, T, M, S> ImmutableWeave<K::Archived, ArchivedDependentNode<K, T, S>, T::Archived>
    for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
    M: Archive,
    S: BuildHasher + Default + Clone,
{
    type Nodes = ArchivedHashMap<K::Archived, ArchivedDependentNode<K, T, S>>;
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
        self.active == Some(*id)
    }
    #[inline]
    fn get(&self, id: &K::Archived) -> Option<&ArchivedDependentNode<K, T, S>> {
        self.nodes.get(id)
    }
    #[inline]
    fn get_parents(&self, id: &K::Archived) -> Option<&ArchivedOption<K::Archived>> {
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

        let mut scratchpad = Vec::new();

        for root in self.roots.iter() {
            archived_topological_sort(&self.nodes, *root, &mut scratchpad, output);
        }
    }
    fn get_ordered_identifiers_from(&self, id: &K::Archived, output: &mut Vec<K::Archived>) {
        output.clear();

        if self.nodes.contains_key(id) {
            let mut scratchpad = Vec::new();

            archived_topological_sort(&self.nodes, *id, &mut scratchpad, output);
        }
    }
    fn get_active_path(&self, output: &mut Vec<K::Archived>) {
        output.clear();

        if let ArchivedOption::Some(active) = self.active {
            archived_path_to_root(&self.nodes, active, output);
        }
    }
    fn get_path_from(&self, id: &K::Archived, output: &mut Vec<K::Archived>) {
        output.clear();

        if self.nodes.contains_key(id) {
            archived_path_to_root(&self.nodes, *id, output);
        }
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, M, S>
    ImmutableMetadataWeave<K::Archived, ArchivedDependentNode<K, T, S>, T::Archived, M::Archived>
    for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
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
    ImmutableBookmarkableWeave<K::Archived, ArchivedDependentNode<K, T, S>, T::Archived>
    for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
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
    ImmutableActiveSingularWeave<K::Archived, ArchivedDependentNode<K, T, S>, T::Archived>
    for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
    M: Archive,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn active(&self) -> Option<K::Archived> {
        match self.active {
            ArchivedOption::Some(active) => Some(active),
            ArchivedOption::None => None,
        }
    }
}

#[check(terminates)]
#[requires(valid_tree(nodes.model()))]
#[requires(nodes.model().contains(&id.deep_model()))]
#[ensures(exists<path: Seq<Id<K>>> ids((^thread)@) == ids(thread@).concat(path)
    && 0 < path.len()
    && path[0] == id.deep_model()
    && valid_path(nodes.model(), path)
    && no_dup(path))]
fn path_to_root<K, T, S>(
    nodes: &HashMap<K, DependentNode<K, T, S>, S>,
    mut id: K,
    thread: &mut Vec<K>,
) where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    snapshot! { lemma_ids_snoc::<K> };
    snapshot! { lemma_concat_snoc::<Id<K>> };
    proof! {
        let rank = snapshot!(rank_of(nodes.model()));
        let pre = snapshot!(ids(thread@));
        let start = snapshot!(id.deep_model());
        let mut path = snapshot!(Seq::<Id<K>>::empty());
    }
    thread.push(id);
    proof! {
        path = snapshot!(path.snoc(id.deep_model()));
    }

    #[invariant(ids(thread@) == pre.concat(*path))]
    #[invariant(0 < path.len() && path[0] == *start && path[path.len() - 1] == id.deep_model())]
    #[invariant(nodes.model().contains(&id.deep_model()))]
    #[invariant(forall<i> 0 <= i && i < path.len() ==> nodes.model().contains(&path[i]))]
    #[invariant(forall<i> 0 <= i && i + 1 < path.len()
        ==> parent(nodes.model(), path[i]) == Some(path[i + 1]))]
    #[invariant(forall<i, j> 0 <= i && i < j && j < path.len() ==> rank[path[j]] < rank[path[i]])]
    #[variant(rank[id.deep_model()])]
    while let Some(parent) = nodes[&id].from {
        thread.push(parent);
        proof! {
            path = snapshot!(path.snoc(parent.deep_model()));
        }
        id = parent;
    }
    proof_assert!(no_dup(*path) && valid_path(nodes.model(), *path));
}

#[cfg(feature = "rkyv")]
fn archived_path_to_root<K, T, S>(
    nodes: &ArchivedHashMap<K::Archived, ArchivedDependentNode<K, T, S>>,
    mut id: K::Archived,
    thread: &mut Vec<K::Archived>,
) where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord,
    T: Archive,
    S: BuildHasher + Default + Clone,
{
    thread.push(id);

    while let ArchivedOption::Some(parent) = nodes[&id].from {
        thread.push(parent);
        id = parent;
    }
}

#[check(terminates)]
#[requires(valid_tree(nodes.model()))]
#[requires(nodes.model().contains(&id.deep_model()))]
#[requires(stack.model().len() == 0)]
#[ensures((^stack).model().len() == 0)]
#[ensures(exists<added: Seq<Id<K>>> ids((^identifiers)@) == ids(identifiers@).concat(added)
    && traversal(nodes.model(), id.deep_model(), added)
    && added == preorder(nodes.model(), Seq::singleton(id.deep_model())))]
fn topological_sort<K, T, S>(
    nodes: &HashMap<K, DependentNode<K, T, S>, S>,
    id: K,
    stack: &mut ScratchpadVec<'_, K>,
    identifiers: &mut Vec<K>,
) where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    snapshot! { lemma_ids_snoc::<K> };
    snapshot! { lemma_concat_snoc::<Id<K>> };
    proof! {
        let sources = snapshot!(Seq::singleton(id.deep_model()));
        let pre = snapshot!(ids(identifiers@));
        let mut added = snapshot!(Seq::<Id<K>>::empty());
    }
    proof! {
        let empty = snapshot!(stack.model());
    }
    stack.push(id);
    proof_assert! {
        lemma_single_source(nodes.model(), id.deep_model());
        lemma_dfs_init(nodes.model(), *sources);
        lemma_ids_singleton(*empty, id);
        dfs(nodes.model(), *sources, *added, ids(stack.model()))
    };
    proof_assert!(sources.reverse().ext_eq(*sources));
    proof_assert! {
        preorder(nodes.model(), *sources).concat_empty();
        added.concat(preorder(nodes.model(), ids(stack.model()).reverse()))
            == preorder(nodes.model(), *sources)
    };

    #[invariant(dfs(nodes.model(), *sources, *added, ids(stack.model())))]
    #[invariant(ids(identifiers@) == pre.concat(*added))]
    #[invariant(added.concat(preorder(nodes.model(), ids(stack.model()).reverse()))
        == preorder(nodes.model(), *sources))]
    #[variant(nodes.model().len() - added.len())]
    while let Some(id) = stack.pop() {
        proof! {
            let rest = snapshot!(ids(stack.model()));
        }
        identifiers.push(id);
        extend_from_set_rev(stack, nodes[&id].to());
        proof_assert! {
            lemma_reverse(nodes.model()[id.deep_model()]@.to);
            lemma_dfs_step(
                nodes.model(),
                *sources,
                *added,
                *rest,
                id.deep_model(),
                nodes.model()[id.deep_model()]@.to.reverse(),
            );
            lemma_dfs_len(nodes.model(), *sources, added.snoc(id.deep_model()), ids(stack.model()));
            dfs(nodes.model(), *sources, added.snoc(id.deep_model()), ids(stack.model()))
                && added.len() + 1 <= nodes.model().len()
        };
        proof! {
            let after = snapshot!(ids(stack.model()));
        }
        proof_assert!(*after == rest.concat(nodes.model()[id.deep_model()]@.to.reverse()));
        proof_assert!(forall<i> 0 <= i && i < rest.len() ==> after[i] == rest[i]);
        proof_assert!(forall<i> 0 <= i && i < after.len() ==> nodes.model().contains(&after[i]));
        proof_assert!(forall<i> 0 <= i && i < rest.len() ==> nodes.model().contains(&rest[i]));
        proof_assert!(
            added.concat(preorder(
                nodes.model(),
                rest.snoc(id.deep_model()).reverse()
            )) == preorder(nodes.model(), *sources)
        );
        proof_assert! {
            lemma_preorder_step(nodes.model(), *rest, id.deep_model());
            preorder(nodes.model(), rest.snoc(id.deep_model()).reverse())
                == Seq::singleton(id.deep_model()).concat(preorder(nodes.model(), after.reverse()))
        };
        proof_assert!(
            added
                .snoc(id.deep_model())
                .ext_eq(added.concat(Seq::singleton(id.deep_model())))
        );
        proof_assert! {
            added.concat_assoc(
                Seq::singleton(id.deep_model()),
                preorder(nodes.model(), after.reverse()),
            );
            added.snoc(id.deep_model()).concat(preorder(nodes.model(), after.reverse()))
                == preorder(nodes.model(), *sources)
        };
        proof! {
            added = snapshot!(added.snoc(id.deep_model()));
        }
    }
    proof_assert! {
        lemma_traversal(nodes.model(), sources[0], *added);
        traversal(nodes.model(), sources[0], *added)
    };
    proof_assert! { lemma_ids_empty(stack.model()); ids(stack.model()) == Seq::empty() };
    proof_assert! {
        Seq::<Id<K>>::reverse_empty();
        added.concat_empty();
        *added == preorder(nodes.model(), *sources)
    };
}

#[cfg(feature = "rkyv")]
fn archived_topological_sort<K, N, T>(
    nodes: &ArchivedHashMap<K, N>,
    id: K,
    stack: &mut Vec<K>,
    identifiers: &mut Vec<K>,
) where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T, From = ArchivedOption<K>, To = ArchivedIndexSet<K>>,
{
    stack.push(id);

    while let Some(id) = stack.pop() {
        identifiers.push(id);
        stack.extend(archived_set_reverse_order(nodes[&id].to()).copied());
    }
}

/// Checks that the given parts form a valid [`DependentWeave`].
#[check(terminates)]
#[ensures(result == DependentWeaveView {
    nodes: nodes.model(),
    roots: roots.model(),
    active: active.deep_model(),
    bookmarked: bookmarked.model(),
}.valid())]
#[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
fn validate<K, T, S>(
    nodes: &HashMap<K, DependentNode<K, T, S>, S>,
    roots: &IndexSet<K, S>,
    active: Option<&K>,
    bookmarked: &IndexSet<K, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    let active_present = match active {
        Some(active) => nodes.contains_key(active),
        None => true,
    };

    contains_all(roots, nodes)
        && active_present
        && contains_all(bookmarked, nodes)
        && all_entries(nodes, |key, value| {
            validate_node(nodes, roots, active, bookmarked, key, value)
        })
        && !detect_cycles(nodes, roots, &mut Vec::with_capacity(roots.len()))
}

/// Checks that the node `value`, stored at `key`, is consistent with the given parts of a weave.
#[check(terminates)]
#[requires(nodes.model().lookup(key.deep_model()) == Some(*value))]
#[ensures(result == (linked(nodes.model(), key.deep_model())
    && (value@.from == None) == roots.model().contains(key.deep_model())
    && value@.active == (active.deep_model() == Some(key.deep_model()))
    && value@.bookmarked == bookmarked.model().contains(key.deep_model())))]
#[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
fn validate_node<K, T, S>(
    nodes: &HashMap<K, DependentNode<K, T, S>, S>,
    roots: &IndexSet<K, S>,
    active: Option<&K>,
    bookmarked: &IndexSet<K, S>,
    key: &K,
    value: &DependentNode<K, T, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    let linked_to_parent = match value.from {
        Some(from) => match nodes.get(&from) {
            Some(parent) => parent.to.contains(key),
            None => false,
        },
        None => true,
    };
    let is_active = match active {
        Some(active) => same_id(active, key),
        None => false,
    };

    value.validate()
        && same_id(&value.id, key)
        && linked_to_parent
        && links_back(nodes, &value.to, key)
        && value.from.is_none() == roots.contains(key)
        && value.active == is_active
        && value.bookmarked == bookmarked.contains(key)
}

/// Checks that every identifier in `ids` is a key of `nodes`.
#[check(terminates)]
#[ensures(result == forall<k> ids.model().contains(k) ==> nodes.model().contains(&k))]
pub(crate) fn contains_all<K, V, S>(ids: &IndexSet<K, S>, nodes: &HashMap<K, V, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    let mut i = 0;

    #[invariant(i@ <= ids.model().len())]
    #[invariant(forall<j> 0 <= j && j < i@ ==> nodes.model().contains(&ids.model()[j]))]
    #[variant(ids.model().len() - i@)]
    while i < ids.len() {
        if !nodes.contains_key(&ids[i]) {
            return false;
        }
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            i += 1;
        }
    }

    true
}

/// Checks that every identifier in `children` is a node whose parent is `key`.
#[check(terminates)]
#[ensures(result == forall<c> children.model().contains(c)
    ==> nodes.model().contains(&c) && nodes.model()[c]@.from == Some(key.deep_model()))]
#[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
fn links_back<K, T, S>(
    nodes: &HashMap<K, DependentNode<K, T, S>, S>,
    children: &IndexSet<K, S>,
    key: &K,
) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    let mut i = 0;

    #[invariant(i@ <= children.model().len())]
    #[invariant(forall<j> 0 <= j && j < i@ ==> nodes.model().contains(&children.model()[j])
        && nodes.model()[children.model()[j]]@.from == Some(key.deep_model()))]
    #[variant(children.model().len() - i@)]
    while i < children.len() {
        let linked = match nodes.get(&children[i]) {
            Some(child) => match child.from {
                Some(from) => same_id(&from, key),
                None => false,
            },
            None => false,
        };
        if !linked {
            return false;
        }
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            i += 1;
        }
    }

    true
}

#[check(terminates)]
#[requires(all_linked(nodes.model()))]
#[requires(forall<r> roots.model().contains(r) ==> nodes.model().contains(&r))]
#[requires(forall<k> nodes.model().contains(&k)
    ==> (parent(nodes.model(), k) == None) == roots.model().contains(k))]
#[requires(stack@.len() == 0)]
#[ensures(result == !acyclic(nodes.model()))]
#[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
fn detect_cycles<K, T, S>(
    nodes: &HashMap<K, DependentNode<K, T, S>, S>,
    roots: &IndexSet<K, S>,
    stack: &mut Vec<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    snapshot! { lemma_ids_snoc::<K> };
    proof! {
        let sources = snapshot!(roots.model());
        let mut added = snapshot!(Seq::<Id<K>>::empty());
    }
    let mut count = 0;

    proof_assert! { lemma_ids_empty(stack@); true };
    proof! {
        let empty = snapshot!(stack@);
    }
    extend_vec_from_set(stack, roots);
    proof_assert! { lemma_ids_empty(*empty); ids(stack@) == *sources };
    proof_assert! {
        lemma_roots_closed(nodes.model(), *sources);
        lemma_dfs_init(nodes.model(), *sources);
        dfs(nodes.model(), *sources, *added, ids(stack@))
    };

    #[invariant(dfs(nodes.model(), *sources, *added, ids(stack@)))]
    #[invariant(count@ == added.len())]
    #[variant(nodes.model().len() - added.len())]
    while let Some(id) = stack.pop() {
        proof! {
            let rest = snapshot!(ids(stack@));
        }
        proof_assert! {
            lemma_dfs_len(nodes.model(), *sources, *added, rest.snoc(id.deep_model()));
            added.len() < nodes.model().len()
        };
        count += 1;
        extend_vec_from_set(stack, nodes[&id].to());
        proof_assert! {
            lemma_dfs_step(
                nodes.model(),
                *sources,
                *added,
                *rest,
                id.deep_model(),
                nodes.model()[id.deep_model()]@.to,
            );
            dfs(nodes.model(), *sources, added.snoc(id.deep_model()), ids(stack@))
        };
        proof! {
            added = snapshot!(added.snoc(id.deep_model()));
        }
    }

    proof_assert! {
        lemma_detect_cycles(nodes.model(), *sources, *added);
        (added.len() == nodes.model().len()) == acyclic(nodes.model())
    };
    count != nodes.len()
}

#[cfg(feature = "rkyv")]
fn archived_detect_cycles<K, N, T, S>(
    nodes: &ArchivedHashMap<K, N>,
    roots: impl Iterator<Item = K>,
    stack: &mut Vec<K>,
    scratchpad_set: &mut HashSet<K, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T, From = ArchivedOption<K>, To = ArchivedIndexSet<K>>,
    S: BuildHasher + Default + Clone,
{
    stack.extend(roots);

    while let Some(id) = stack.pop() {
        if !scratchpad_set.insert(id) {
            return true;
        }
        stack.extend(archived_set_reverse_order(nodes[&id].to()).copied());
    }

    scratchpad_set.len() != nodes.len()
}

/// Pushes the identifiers in `set` onto `stack`, in reverse order.
#[trusted]
#[check(terminates)]
#[ensures(ids((^stack).model()) == ids(stack.model()).concat(set.model().reverse()))]
fn extend_from_set_rev<K, S>(stack: &mut ScratchpadVec<'_, K>, set: &IndexSet<K, S>)
where
    K: Copy + Verifiable,
{
    stack.extend(set.iter().rev().copied());
}

/// Sorts the identifiers in `set` by comparing their nodes with `cmp`.
#[trusted]
#[requires(forall<i> 0 <= i && i < set.model().len() ==> nodes.model().contains(&set.model()[i]))]
#[requires(forall<f> (*cmp).hist_inv(f)
    ==> forall<a: &DependentNode<K, T, S>, b: &DependentNode<K, T, S>>
        inv(a) && inv(b) ==> f.precondition((a, b)))]
#[ensures((^set).model().len() == set.model().len())]
#[ensures(same_elements((^set).model(), set.model()))]
#[ensures(forall<le: Mapping<DependentNode<K, T, S>, Mapping<DependentNode<K, T, S>, bool>>>
    total_preorder(le) && compares_by(*cmp, identity(), le)
    ==> sorted_stably((^set).model(), set.model(), order_by(le, value_of(nodes.model()))))]
fn sort_by_nodes<K, T, S, F>(
    set: &mut IndexSet<K, S>,
    nodes: &HashMap<K, DependentNode<K, T, S>, S>,
    scratchpad: &mut Scratchpad,
    cmp: &mut F,
) where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
    F: FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
{
    if set.len() > 20 {
        let guard = scratchpad.guard();
        let mut sorted = guard
            .arena()
            .alloc_iter_exact(set.drain(..).map(|id| &nodes[&id]));
        sorted.sort_by(|a, b| cmp(*a, *b));

        set.extend(sorted.into_iter().map(|node| node.id));
    } else {
        set.sort_by(|a, b| cmp(&nodes[a], &nodes[b]));
    }
}

/// Pushes the identifiers in `set` onto `stack`, in order.
#[trusted]
#[check(terminates)]
#[ensures(ids((^stack).model()) == ids(stack.model()).concat(set.model()))]
fn extend_from_set<K, S>(stack: &mut ScratchpadVec<'_, K>, set: &IndexSet<K, S>)
where
    K: Copy + Verifiable,
{
    stack.extend(set.iter().copied());
}

/// Pushes the identifiers in `set` onto `stack`, in order.
#[trusted]
#[check(terminates)]
#[ensures(ids((^stack)@) == ids(stack@).concat(set.model()))]
fn extend_vec_from_set<K, S>(stack: &mut Vec<K>, set: &IndexSet<K, S>)
where
    K: Copy + Verifiable,
{
    stack.extend(set.iter().copied());
}

/// Checks that `f` holds for every entry of `map`.
#[trusted]
#[check(terminates)]
#[requires(forall<k: &K, v: &V> inv(k) && inv(v) && map.model().lookup(k.deep_model()) == Some(*v)
    ==> f.precondition((k, v)))]
#[ensures(result ==> forall<k: Id<K>> map.model().contains(&k)
    ==> exists<key: &K, value: &V> key.deep_model() == k && map.model().lookup(k) == Some(*value)
        && f.postcondition((key, value), true))]
#[ensures(!result ==> exists<key: &K, value: &V>
    map.model().lookup(key.deep_model()) == Some(*value) && f.postcondition((key, value), false))]
pub(crate) fn all_entries<K, V, S>(map: &HashMap<K, V, S>, f: impl Fn(&K, &V) -> bool) -> bool
where
    K: Verifiable,
{
    map.iter().all(|(key, value)| f(key, value))
}
