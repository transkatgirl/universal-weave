//! Specifications and lemmas for [`DependentWeave`].
//!
//! This module only exists while Creusot is verifying the crate. See `VERIFICATION.md`.

use core::hash::{BuildHasher, Hash};

use creusot_std::{
    logic::{FMap, Mapping, such_that, try_such_that},
    prelude::*,
};

use super::{DependentNode, DependentWeave};
use crate::verification::{
    Id, Model, lemma_concat_contains, lemma_no_dup_concat, lemma_no_dup_snoc, lemma_snoc_contains,
    no_dup, replaced_in_place, set_bound, shift_removed,
};

/// The nodes of a weave, by identifier.
pub type Nodes<K, T, S> = FMap<Id<K>, DependentNode<K, T, S>>;

/// The view of a [`DependentNode`], without its contents.
pub struct NodeView<I> {
    /// The node's identifier.
    pub id: I,
    /// The identifier of the node's parent.
    pub from: Option<I>,
    /// The identifiers of the node's children, in order.
    pub to: Seq<I>,
    /// If the node is active.
    pub active: bool,
    /// If the node is bookmarked.
    pub bookmarked: bool,
}

impl<K, T, S> View for DependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    type ViewTy = NodeView<Id<K>>;

    #[logic(open)]
    fn view(self) -> NodeView<Id<K>> {
        NodeView {
            id: self.id.deep_model(),
            from: self.from.deep_model(),
            to: self.to.model(),
            active: self.active,
            bookmarked: self.bookmarked,
        }
    }
}

impl<I> NodeView<I> {
    /// The node's own links are consistent: spec version of `DependentNode::validate()`.
    #[logic(open)]
    pub fn valid(self) -> bool {
        pearlite! {
            (match self.from { Some(from) => !self.to.contains(from), None => true })
                && self.from != Some(self.id)
                && !self.to.contains(self.id)
        }
    }
}

/// The view of a [`DependentWeave`], without its metadata.
pub struct DependentWeaveView<K, T, S>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    /// The nodes, by identifier.
    pub nodes: Nodes<K, T, S>,
    /// The identifiers of the root nodes, in order.
    pub roots: Seq<Id<K>>,
    /// The identifier of the active node.
    pub active: Option<Id<K>>,
    /// The identifiers of the bookmarked nodes, in order.
    pub bookmarked: Seq<Id<K>>,
}

impl<K, T, M, S> View for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    type ViewTy = DependentWeaveView<K, T, S>;

    #[logic(open(crate))]
    fn view(self) -> DependentWeaveView<K, T, S> {
        DependentWeaveView {
            nodes: self.nodes.model(),
            roots: self.roots.model(),
            active: self.active.deep_model(),
            bookmarked: self.bookmarked.model(),
        }
    }
}

impl<K, T, M, S> Invariant for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    #[logic(open(crate))]
    fn invariant(self) -> bool {
        pearlite! { self@.valid() }
    }
}

/// The parent of `k`, if `k` is a node.
#[logic(open)]
pub fn parent<K, T, S>(nodes: Nodes<K, T, S>, k: Id<K>) -> Option<Id<K>>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    match nodes.lookup(k) {
        Some(node) => node.view().from,
        None => None,
    }
}

/// The links of node `k` are consistent with the rest of `nodes`.
#[logic(open)]
pub fn linked<K, T, S>(nodes: Nodes<K, T, S>, k: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = nodes[k]@;
        n.valid()
            && no_dup(n.to)
            && n.id == k
            && match n.from { Some(p) => nodes.contains(&p) && nodes[p]@.to.contains(k), None => true }
            && forall<c> n.to.contains(c) ==> nodes.contains(&c) && nodes[c]@.from == Some(k)
    }
}

/// `rank` strictly increases from parent to child.
#[logic(open)]
pub fn ranked<K, T, S>(nodes: Nodes<K, T, S>, rank: Mapping<Id<K>, Int>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<k> nodes.contains(&k) ==> 0 <= rank[k]
            && match nodes[k]@.from { Some(p) => rank[p] < rank[k], None => true }
    }
}

/// Following parent links never loops.
#[logic(open)]
pub fn acyclic<K, T, S>(nodes: Nodes<K, T, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! { exists<rank: Mapping<Id<K>, Int>> ranked(nodes, rank) }
}

/// A rank function of an acyclic forest.
#[logic(open)]
#[requires(acyclic(nodes))]
#[ensures(ranked(nodes, result))]
pub fn rank_of<K, T, S>(nodes: Nodes<K, T, S>) -> Mapping<Id<K>, Int>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    such_that(|rank| ranked(nodes, rank))
}

/// The nodes form a forest whose links are consistent.
#[logic(open)]
pub fn valid_tree<K, T, S>(nodes: Nodes<K, T, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! { (forall<k> nodes.contains(&k) ==> linked(nodes, k)) && acyclic(nodes) }
}

impl<K, T, S> DependentWeaveView<K, T, S>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    /// The weave is internally consistent: spec version of `DependentWeave::validate()`.
    #[logic(open)]
    pub fn valid(self) -> bool {
        pearlite! {
            valid_tree(self.nodes)
                && no_dup(self.roots)
                && no_dup(self.bookmarked)
                && (forall<k> self.roots.contains(k) ==> self.nodes.contains(&k))
                && match self.active { Some(a) => self.nodes.contains(&a), None => true }
                && (forall<k> self.bookmarked.contains(k) ==> self.nodes.contains(&k))
                && forall<k> self.nodes.contains(&k) ==> self.flagged(k)
        }
    }

    /// Whether node `k` is a root, is active and is bookmarked agrees with the rest of the weave.
    #[logic(open)]
    pub fn flagged(self, k: Id<K>) -> bool {
        pearlite! {
            let n = self.nodes[k]@;
            (n.from == None) == self.roots.contains(k)
                && n.active == (self.active == Some(k))
                && n.bookmarked == self.bookmarked.contains(k)
        }
    }

    /// `self` and `other` contain nodes with the same identifiers.
    #[logic(open)]
    pub fn same_keys(self, other: Self) -> bool {
        pearlite! { forall<k> self.nodes.contains(&k) == other.nodes.contains(&k) }
    }
}

/// The ancestor of `d` which is `n` links up from it, if any.
#[logic(open)]
#[variant(n)]
pub fn ancestor<K, T, S>(nodes: Nodes<K, T, S>, d: Id<K>, n: Int) -> Option<Id<K>>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    if n <= 0 {
        Some(d)
    } else {
        match parent(nodes, d) {
            Some(p) => ancestor(nodes, p, n - 1),
            None => None,
        }
    }
}

/// `d` is `a` or one of its descendants.
#[logic(open)]
pub fn descends<K, T, S>(nodes: Nodes<K, T, S>, d: Id<K>, a: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! { exists<n> 0 <= n && ancestor(nodes, d, n) == Some(a) }
}

/// The child of `c` descends from whatever `c` descends from.
#[logic]
#[requires(nodes.contains(&c))]
#[requires(parent(nodes, c) == Some(p))]
#[requires(descends(nodes, p, a))]
#[ensures(descends(nodes, c, a))]
pub fn lemma_descends_step<K, T, S>(nodes: Nodes<K, T, S>, c: Id<K>, p: Id<K>, a: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = such_that(|n: Int| 0 <= n && ancestor(nodes, p, n) == Some(a));
        proof_assert!(ancestor(nodes, c, n + 1) == Some(a))
    }
}

/// The empty forest is acyclic.
#[logic]
#[ensures(acyclic(FMap::<Id<K>, DependentNode<K, T, S>>::empty()))]
pub fn lemma_empty_acyclic<K, T, S>()
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    proof_assert!(ranked(
        FMap::<Id<K>, DependentNode<K, T, S>>::empty(),
        |_| 0
    ))
}

/// `new` has the same nodes, roots and links as `old`, and only its active node, bookmarks and
/// node contents may differ, which it flags consistently.
#[logic(open)]
pub fn reflagged<K, T, S>(
    old: DependentWeaveView<K, T, S>,
    new: DependentWeaveView<K, T, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        old.same_keys(new)
            && (forall<k> old.nodes.contains(&k) ==> new.nodes[k]@.id == old.nodes[k]@.id
                && new.nodes[k]@.from == old.nodes[k]@.from
                && crate::verification::same_elements(new.nodes[k]@.to, old.nodes[k]@.to)
                && no_dup(new.nodes[k]@.to))
            && crate::verification::same_elements(new.roots, old.roots)
            && no_dup(new.roots)
            && no_dup(new.bookmarked)
            && match new.active { Some(a) => new.nodes.contains(&a), None => true }
            && (forall<k> new.bookmarked.contains(k) ==> new.nodes.contains(&k))
            && forall<k> new.nodes.contains(&k) ==> new.flagged(k)
    }
}

/// Changing only flags and contents preserves validity, as long as the new flags are consistent.
#[logic]
#[requires(old.valid())]
#[ensures(forall<new: DependentWeaveView<K, T, S>> #[trigger(new.valid())]
    reflagged(old, new) ==> new.valid())]
pub fn lemma_reflagged<K, T, S>(old: DependentWeaveView<K, T, S>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<new: DependentWeaveView<K, T, S>> reflagged(old, new)
            ==> ranked(new.nodes, rank_of(old.nodes)));
        proof_assert!(forall<new: DependentWeaveView<K, T, S>> reflagged(old, new)
            ==> forall<k> new.nodes.contains(&k) ==> linked(new.nodes, k))
    }
}

/// `new` is `old` with node `id`'s bookmark set to `value`.
#[logic(open)]
pub fn rebookmarked<K, T, S>(
    old: DependentWeaveView<K, T, S>,
    new: DependentWeaveView<K, T, S>,
    id: Id<K>,
    value: bool,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = old.nodes[id]@;
        old.nodes.contains(&id)
            && (forall<k> new.nodes.contains(&k) == old.nodes.contains(&k))
            && (forall<k> k != id ==> new.nodes[k]@ == old.nodes[k]@)
            && new.nodes[id]@ == NodeView {
                id: n.id,
                from: n.from,
                to: n.to,
                active: n.active,
                bookmarked: value,
            }
            && new.roots == old.roots
            && new.active == old.active
            && no_dup(new.bookmarked)
            && forall<k> new.bookmarked.contains(k)
                == if k == id { value } else { old.bookmarked.contains(k) }
    }
}

/// Changing one node's bookmark preserves validity.
#[logic]
#[requires(old.valid())]
#[requires(rebookmarked(old, new, id, value))]
#[ensures(new.valid())]
pub fn lemma_rebookmarked_valid<K, T, S>(
    old: DependentWeaveView<K, T, S>,
    new: DependentWeaveView<K, T, S>,
    id: Id<K>,
    value: bool,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> new.nodes.contains(&k) ==> new.nodes[k]@.from == old.nodes[k]@.from
            && new.nodes[k]@.to == old.nodes[k]@.to && new.nodes[k]@.id == old.nodes[k]@.id
            && new.nodes[k]@.active == old.nodes[k]@.active);
        proof_assert!(ranked(new.nodes, rank_of(old.nodes)));
        proof_assert!(forall<k> new.nodes.contains(&k) ==> linked(old.nodes, k) && linked(new.nodes, k));
        proof_assert!(forall<k> new.nodes.contains(&k) ==> new.flagged(k))
    }
}

/// Changing one node's bookmark preserves validity.
#[logic]
#[requires(old.valid())]
#[ensures(forall<new: DependentWeaveView<K, T, S>> #[trigger(new.valid())]
    rebookmarked(old, new, id, value) ==> new.valid())]
pub fn lemma_rebookmarked<K, T, S>(old: DependentWeaveView<K, T, S>, id: Id<K>, value: bool)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_rebookmarked_valid::<K, T, S>;
}

/// Every node of `nodes` is linked to the rest of it.
#[logic(open)]
pub fn all_linked<K, T, S>(nodes: Nodes<K, T, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! { forall<k> nodes.contains(&k) ==> linked(nodes, k) }
}

/// `d` descends from itself.
#[logic]
#[ensures(descends(nodes, d, d))]
pub fn lemma_descends_refl<K, T, S>(nodes: Nodes<K, T, S>, d: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    proof_assert!(ancestor(nodes, d, 0) == Some(d))
}

/// Ancestors compose.
#[logic]
#[requires(0 <= n && 0 <= m)]
#[requires(ancestor(nodes, x, n) == Some(y))]
#[ensures(ancestor(nodes, x, n + m) == ancestor(nodes, y, m))]
#[variant(n)]
pub fn lemma_ancestor_add<K, T, S>(nodes: Nodes<K, T, S>, x: Id<K>, y: Id<K>, n: Int, m: Int)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    if n > 0 {
        match parent(nodes, x) {
            Some(p) => lemma_ancestor_add(nodes, p, y, n - 1, m),
            None => (),
        }
    }
}

/// Descent is transitive.
#[logic]
#[requires(descends(nodes, x, y))]
#[requires(descends(nodes, y, z))]
#[ensures(descends(nodes, x, z))]
pub fn lemma_descends_trans<K, T, S>(nodes: Nodes<K, T, S>, x: Id<K>, y: Id<K>, z: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = such_that(|n: Int| 0 <= n && ancestor(nodes, x, n) == Some(y));
        let m = such_that(|m: Int| 0 <= m && ancestor(nodes, y, m) == Some(z));
        lemma_ancestor_add(nodes, x, y, n, m)
    }
}

/// A node `n >= 1` links below `s` descends from a child of `s`.
#[logic]
#[requires(all_linked(nodes))]
#[requires(nodes.contains(&d))]
#[requires(1 <= n)]
#[requires(ancestor(nodes, d, n) == Some(s))]
#[ensures(exists<c> nodes[s]@.to.contains(c) && descends(nodes, d, c))]
#[variant(n)]
pub fn lemma_descends_child_n<K, T, S>(nodes: Nodes<K, T, S>, d: Id<K>, s: Id<K>, n: Int)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    match parent(nodes, d) {
        Some(p) => {
            if n == 1 {
                lemma_descends_refl(nodes, d)
            } else {
                lemma_descends_child_n(nodes, p, s, n - 1);
                pearlite! {
                    let c = such_that(|c| nodes[s]@.to.contains(c) && descends(nodes, p, c));
                    lemma_descends_step(nodes, d, p, c)
                }
            }
        }
        None => (),
    }
}

/// A proper descendant of `s` descends from a child of `s`.
#[logic]
#[requires(all_linked(nodes))]
#[requires(nodes.contains(&d))]
#[requires(descends(nodes, d, s))]
#[requires(d != s)]
#[ensures(exists<c> nodes[s]@.to.contains(c) && descends(nodes, d, c))]
pub fn lemma_descends_child<K, T, S>(nodes: Nodes<K, T, S>, d: Id<K>, s: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = such_that(|n: Int| 0 <= n && ancestor(nodes, d, n) == Some(s));
        lemma_descends_child_n(nodes, d, s, n)
    }
}

/// Ranks don't decrease from an ancestor to its descendants, which are all nodes.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(nodes.contains(&d))]
#[requires(0 <= n)]
#[requires(ancestor(nodes, d, n) == Some(a))]
#[ensures(nodes.contains(&a))]
#[ensures(rank[a] + n <= rank[d])]
#[variant(n)]
pub fn lemma_rank_ancestor<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    d: Id<K>,
    a: Id<K>,
    n: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    if n > 0 {
        match parent(nodes, d) {
            Some(p) => lemma_rank_ancestor(nodes, rank, p, a, n - 1),
            None => (),
        }
    }
}

/// Descendants of a node are nodes, ranked no lower than it.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(nodes.contains(&d))]
#[requires(descends(nodes, d, a))]
#[ensures(nodes.contains(&a))]
#[ensures(rank[a] <= rank[d])]
#[ensures(d != a ==> rank[a] < rank[d])]
pub fn lemma_rank_descends<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    d: Id<K>,
    a: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = such_that(|n: Int| 0 <= n && ancestor(nodes, d, n) == Some(a));
        lemma_rank_ancestor(nodes, rank, d, a, n)
    }
}

/// In a valid forest, a node is never a proper descendant of itself or of its descendants.
#[logic]
#[requires(valid_tree(nodes))]
#[requires(nodes.contains(&d))]
#[requires(descends(nodes, d, a))]
#[requires(descends(nodes, a, d))]
#[ensures(d == a)]
pub fn lemma_descends_antisym<K, T, S>(nodes: Nodes<K, T, S>, d: Id<K>, a: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    lemma_rank_descends(nodes, rank_of(nodes), d, a);
    lemma_rank_descends(nodes, rank_of(nodes), a, d)
}

/// Every node of an acyclic forest descends from a root.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(nodes.contains(&k))]
#[ensures(exists<r> nodes.contains(&r) && parent(nodes, r) == None && descends(nodes, k, r))]
#[variant(rank[k])]
pub fn lemma_has_root<K, T, S>(nodes: Nodes<K, T, S>, rank: Mapping<Id<K>, Int>, k: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    match parent(nodes, k) {
        Some(p) => {
            lemma_has_root(nodes, rank, p);
            pearlite! {
                let r = such_that(|r| nodes.contains(&r) && parent(nodes, r) == None
                    && descends(nodes, p, r));
                lemma_descends_step(nodes, k, p, r)
            }
        }
        None => lemma_descends_refl(nodes, k),
    }
}

/// A node descends from at most one root.
#[logic]
#[requires(0 <= n && 0 <= m)]
#[requires(ancestor(nodes, x, n) == Some(r1))]
#[requires(ancestor(nodes, x, m) == Some(r2))]
#[requires(parent(nodes, r1) == None)]
#[requires(parent(nodes, r2) == None)]
#[ensures(r1 == r2)]
#[variant(n)]
pub fn lemma_root_unique_n<K, T, S>(
    nodes: Nodes<K, T, S>,
    x: Id<K>,
    r1: Id<K>,
    r2: Id<K>,
    n: Int,
    m: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    if n > 0 && m > 0 {
        match parent(nodes, x) {
            Some(p) => lemma_root_unique_n(nodes, p, r1, r2, n - 1, m - 1),
            None => (),
        }
    }
}

/// A node descends from at most one root.
#[logic]
#[requires(descends(nodes, x, r1))]
#[requires(descends(nodes, x, r2))]
#[requires(parent(nodes, r1) == None)]
#[requires(parent(nodes, r2) == None)]
#[ensures(r1 == r2)]
pub fn lemma_root_unique<K, T, S>(nodes: Nodes<K, T, S>, x: Id<K>, r1: Id<K>, r2: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = such_that(|n: Int| 0 <= n && ancestor(nodes, x, n) == Some(r1));
        let m = such_that(|m: Int| 0 <= m && ancestor(nodes, x, m) == Some(r2));
        lemma_root_unique_n(nodes, x, r1, r2, n, m)
    }
}

/// A sequence without duplicates of keys of `m` is no longer than `m`.
#[logic]
#[requires(no_dup(s))]
#[requires(forall<i> 0 <= i && i < s.len() ==> m.contains(&s[i]))]
#[ensures(s.len() <= m.len())]
#[variant(s.len())]
pub fn lemma_no_dup_len<I, V>(s: Seq<I>, m: FMap<I, V>) {
    if s.len() > 0 {
        let x = s[s.len() - 1];
        lemma_no_dup_len(s.subsequence(0, s.len() - 1), m.remove(x))
    }
}

/// A sequence without duplicates of exactly the keys of `m` is as long as `m`.
#[logic]
#[requires(no_dup(s))]
#[requires(forall<x> s.contains(x) == m.contains(&x))]
#[ensures(s.len() == m.len())]
#[variant(s.len())]
pub fn lemma_lists_len<I, V>(s: Seq<I>, m: FMap<I, V>) {
    if s.len() > 0 {
        let x = s[s.len() - 1];
        let t = s.subsequence(0, s.len() - 1);
        pearlite! {
            proof_assert!(forall<y> t.contains(y) == (s.contains(y) && y != x));
            lemma_lists_len(t, m.remove(x))
        }
    } else {
        let _ = m.is_empty_logic();
    }
}

/// `x` descends from one of `sources`.
#[logic(open)]
pub fn from_sources<K, T, S>(nodes: Nodes<K, T, S>, sources: Seq<Id<K>>, x: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! { exists<src> sources.contains(src) && descends(nodes, x, src) }
}

/// No source is a child of a node which descends from a source.
#[logic(open)]
pub fn sources_closed<K, T, S>(nodes: Nodes<K, T, S>, sources: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<src> sources.contains(src) ==> match parent(nodes, src) {
            Some(p) => !from_sources(nodes, sources, p),
            None => true,
        }
    }
}

/// The state of a depth-first traversal of the descendants of `sources`: `added` lists the
/// visited nodes, in order, each after its parent unless it is a source, and `stack` the nodes
/// left to visit, below which lie all the descendants which haven't been visited.
#[logic(open)]
pub fn dfs<K, T, S>(
    nodes: Nodes<K, T, S>,
    sources: Seq<Id<K>>,
    added: Seq<Id<K>>,
    stack: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        no_dup(added)
            && no_dup(stack)
            && (forall<x> added.contains(x) ==> !stack.contains(x))
            && (forall<x> added.contains(x) || stack.contains(x)
                ==> nodes.contains(&x) && from_sources(nodes, sources, x))
            && (forall<x> stack.contains(x) ==> sources.contains(x) || match parent(nodes, x) {
                Some(p) => added.contains(p),
                None => false,
            })
            && (forall<j> 0 <= j && j < added.len() ==> sources.contains(added[j])
                || match parent(nodes, added[j]) {
                    Some(p) => added.subsequence(0, j).contains(p),
                    None => false,
                })
            && (forall<d> nodes.contains(&d) && from_sources(nodes, sources, d)
                ==> added.contains(d) || exists<s> stack.contains(s) && descends(nodes, d, s))
    }
}

/// A traversal starts with the sources on the stack.
#[logic]
#[requires(no_dup(sources))]
#[requires(forall<x> sources.contains(x) ==> nodes.contains(&x))]
#[ensures(dfs(nodes, sources, Seq::empty(), sources))]
pub fn lemma_dfs_init<K, T, S>(nodes: Nodes<K, T, S>, sources: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_descends_refl::<K, T, S>;
}

/// Visiting the node `s` at the top of the stack replaces it with its children `cs`, in any order.
#[logic]
#[requires(all_linked(nodes))]
#[requires(sources_closed(nodes, sources))]
#[requires(dfs(nodes, sources, added, rest.snoc(s)))]
#[requires(no_dup(cs))]
#[requires(crate::verification::same_elements(cs, nodes[s]@.to))]
#[ensures(dfs(nodes, sources, added.snoc(s), rest.concat(cs)))]
pub fn lemma_dfs_step<K, T, S>(
    nodes: Nodes<K, T, S>,
    sources: Seq<Id<K>>,
    added: Seq<Id<K>>,
    rest: Seq<Id<K>>,
    s: Id<K>,
    cs: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_descends_step::<K, T, S>;
    let _ = lemma_descends_child::<K, T, S>;
    let _ = lemma_descends_refl::<K, T, S>;
    let _ = lemma_snoc_contains::<Id<K>>;
    let _ = lemma_concat_contains::<Id<K>>;
    lemma_no_dup_snoc(rest, s);
    pearlite! {
        let stack = rest.snoc(s);
        let added2 = added.snoc(s);
        let stack2 = rest.concat(cs);
        proof_assert!(stack.contains(s) && !added.contains(s));
        proof_assert!(linked(nodes, s));
        proof_assert!(forall<c> cs.contains(c) ==> nodes.contains(&c) && parent(nodes, c) == Some(s));
        proof_assert!(forall<c> cs.contains(c) ==> !sources.contains(c));
        proof_assert!(forall<c> cs.contains(c) ==> !added.contains(c));
        proof_assert!(forall<c> cs.contains(c) ==> !rest.contains(c));
        proof_assert!(forall<c> cs.contains(c) ==> c != s);
        proof_assert!(forall<x> stack2.contains(x) == (rest.contains(x) || cs.contains(x)));
        proof_assert!(forall<x> added2.contains(x) == (added.contains(x) || x == s));
        proof_assert!(forall<x> rest.contains(x) ==> stack.contains(x) && x != s);
        lemma_no_dup_concat(rest, cs);
        lemma_no_dup_snoc(added, s);
        proof_assert!(forall<c> cs.contains(c) ==> from_sources(nodes, sources, c));
        proof_assert!(forall<j> 0 <= j && j < added.len() ==> added2.subsequence(0, j) == added.subsequence(0, j));
        proof_assert!(added2.subsequence(0, added.len()) == added);
        proof_assert!(forall<d> nodes.contains(&d) && from_sources(nodes, sources, d)
            && !added2.contains(d) && descends(nodes, d, s)
            ==> exists<c> cs.contains(c) && descends(nodes, d, c))
    }
}

/// When the stack is empty, the traversal has visited exactly the descendants of the sources.
#[logic]
#[requires(dfs(nodes, sources, added, Seq::empty()))]
#[ensures(forall<d> added.contains(d) == (nodes.contains(&d) && from_sources(nodes, sources, d)))]
pub fn lemma_dfs_done<K, T, S>(nodes: Nodes<K, T, S>, sources: Seq<Id<K>>, added: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
}

/// A traversal never visits more nodes than there are.
#[logic]
#[requires(dfs(nodes, sources, added, stack))]
#[ensures(added.len() + stack.len() <= nodes.len())]
pub fn lemma_dfs_len<K, T, S>(
    nodes: Nodes<K, T, S>,
    sources: Seq<Id<K>>,
    added: Seq<Id<K>>,
    stack: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let both = added.concat(stack);
        proof_assert!(forall<i> 0 <= i && i < both.len() ==> both.contains(both[i]));
        proof_assert!(no_dup(both));
        lemma_no_dup_len(both, nodes)
    }
}

/// `added` lists `id` and its descendants without duplicates, starting with `id`, with every
/// other node after its parent.
#[logic(open)]
pub fn traversal<K, T, S>(nodes: Nodes<K, T, S>, id: Id<K>, added: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        no_dup(added)
            && 0 < added.len()
            && added[0] == id
            && (forall<d> added.contains(d) == (nodes.contains(&d) && descends(nodes, d, id)))
            && forall<j> 1 <= j && j < added.len() ==> match parent(nodes, added[j]) {
                Some(p) => added.subsequence(0, j).contains(p),
                None => false,
            }
    }
}

/// `s` lists nodes without duplicates, each after its parent: spec version of the former
/// `valid_topological_sort`.
#[logic(open)]
pub fn topological<K, T, S>(nodes: Nodes<K, T, S>, s: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        no_dup(s)
            && forall<i> 0 <= i && i < s.len() ==> nodes.contains(&s[i])
                && match parent(nodes, s[i]) {
                    Some(p) => s.subsequence(0, i).contains(p),
                    None => true,
                }
    }
}

/// `s` is empty or the path from its first node up to a root: spec version of the former
/// `valid_path`.
#[logic(open)]
pub fn valid_path<K, T, S>(nodes: Nodes<K, T, S>, s: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        s.len() == 0 || (
            (forall<i> 0 <= i && i < s.len() ==> nodes.contains(&s[i]))
                && (forall<i> 0 <= i && i + 1 < s.len() ==> parent(nodes, s[i]) == Some(s[i + 1]))
                && parent(nodes, s[s.len() - 1]) == None
        )
    }
}

/// A single node is a closed set of sources.
#[logic]
#[requires(valid_tree(nodes))]
#[requires(nodes.contains(&id))]
#[ensures(sources_closed(nodes, Seq::singleton(id)))]
pub fn lemma_single_source<K, T, S>(nodes: Nodes<K, T, S>, id: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    match parent(nodes, id) {
        Some(p) => {
            lemma_descends_refl(nodes, p);
            lemma_descends_step(nodes, id, p, p);
            pearlite! {
                proof_assert!(linked(nodes, id));
                proof_assert!(from_sources(nodes, Seq::singleton(id), p) ==> descends(nodes, p, id));
                proof_assert!(descends(nodes, p, id) ==> {
                    lemma_descends_antisym(nodes, id, p);
                    false
                })
            }
        }
        None => (),
    }
}

/// A finished traversal from a single node lists its descendants in order.
#[logic]
#[requires(nodes.contains(&id))]
#[requires(dfs(nodes, Seq::singleton(id), added, Seq::empty()))]
#[ensures(traversal(nodes, id, added))]
pub fn lemma_traversal<K, T, S>(nodes: Nodes<K, T, S>, id: Id<K>, added: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    lemma_descends_refl(nodes, id);
    pearlite! {
        proof_assert!(forall<x> Seq::singleton(id).contains(x) == (x == id));
        proof_assert!(forall<d> from_sources(nodes, Seq::singleton(id), d) == descends(nodes, d, id));
        proof_assert!(added.contains(id));
        proof_assert!(added[0] == id);
        proof_assert!(forall<j> 1 <= j && j < added.len() ==> added[j] != id)
    }
}

/// A traversal from a root is topological.
#[logic]
#[requires(traversal(nodes, r, added))]
#[requires(parent(nodes, r) == None)]
#[ensures(topological(nodes, added))]
pub fn lemma_traversal_topological<K, T, S>(nodes: Nodes<K, T, S>, r: Id<K>, added: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_descends_refl::<K, T, S>;
    pearlite! {
        proof_assert!(forall<i> 0 <= i && i < added.len() ==> added.contains(added[i]))
    }
}

/// Concatenating disjoint topological sequences is topological.
#[logic]
#[requires(topological(nodes, a))]
#[requires(topological(nodes, b))]
#[requires(forall<x> a.contains(x) ==> !b.contains(x))]
#[ensures(topological(nodes, a.concat(b)))]
pub fn lemma_topological_concat<K, T, S>(nodes: Nodes<K, T, S>, a: Seq<Id<K>>, b: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    lemma_no_dup_concat(a, b);
    pearlite! {
        let ab = a.concat(b);
        proof_assert!(forall<i> 0 <= i && i < a.len() ==> ab[i] == a[i]
            && ab.subsequence(0, i) == a.subsequence(0, i));
        proof_assert!(forall<i> 0 <= i && i < b.len() ==> ab[a.len() + i] == b[i]
            && ab.subsequence(0, a.len() + i) == a.concat(b.subsequence(0, i)));
        let _ = lemma_concat_contains::<Id<K>>;
    }
}

/// `out` lists, in topological order, exactly the descendants of the first `i` roots of `view`.
#[logic(open)]
pub fn ordered_prefix<K, T, S>(view: DependentWeaveView<K, T, S>, i: Int, out: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        topological(view.nodes, out)
            && forall<x> out.contains(x) == (view.nodes.contains(&x)
                && exists<j> 0 <= j && j < i && descends(view.nodes, x, view.roots[j]))
    }
}

/// Appending the traversal of the next root extends the ordered prefix.
#[logic]
#[requires(view.valid())]
#[requires(0 <= i && i < view.roots.len())]
#[requires(ordered_prefix(view, i, out))]
#[requires(traversal(view.nodes, view.roots[i], added))]
#[ensures(ordered_prefix(view, i + 1, out.concat(added)))]
pub fn lemma_ordered_step<K, T, S>(
    view: DependentWeaveView<K, T, S>,
    i: Int,
    out: Seq<Id<K>>,
    added: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_concat_contains::<Id<K>>;
    let _ = lemma_root_unique::<K, T, S>;
    pearlite! {
        let r = view.roots[i];
        proof_assert!(view.roots.contains(r) && view.nodes.contains(&r) && view.flagged(r));
        proof_assert!(parent(view.nodes, r) == None);
        lemma_traversal_topological(view.nodes, r, added);
        proof_assert!(forall<j> 0 <= j && j < i ==> view.roots.contains(view.roots[j])
            && parent(view.nodes, view.roots[j]) == None && view.roots[j] != r);
        proof_assert!(forall<x> out.contains(x) ==> !added.contains(x));
        lemma_topological_concat(view.nodes, out, added)
    }
}

/// Once every root's traversal is appended, every node is listed.
#[logic]
#[requires(view.valid())]
#[requires(ordered_prefix(view, view.roots.len(), out))]
#[ensures(out.len() == view.nodes.len())]
pub fn lemma_ordered_done<K, T, S>(view: DependentWeaveView<K, T, S>, out: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_has_root::<K, T, S>;
    pearlite! {
        let _rank = rank_of(view.nodes);
        proof_assert!(forall<k> view.nodes.contains(&k) ==> exists<r> view.nodes.contains(&r)
            && parent(view.nodes, r) == None && descends(view.nodes, k, r));
        proof_assert!(forall<r> view.nodes.contains(&r) && parent(view.nodes, r) == None
            ==> view.flagged(r) && view.roots.contains(r));
        proof_assert!(forall<k> out.contains(k) == view.nodes.contains(&k));
        lemma_lists_len(out, view.nodes)
    }
}

/// The nodes a node descends from are nodes.
#[logic]
#[requires(valid_tree(nodes))]
#[requires(nodes.contains(&d))]
#[requires(descends(nodes, d, a))]
#[ensures(nodes.contains(&a))]
pub fn lemma_ancestor_node<K, T, S>(nodes: Nodes<K, T, S>, d: Id<K>, a: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    lemma_rank_descends(nodes, rank_of(nodes), d, a)
}

/// `new` is `old` once node `id` is split into `id`, whose only child is `new_id`, and `new_id`,
/// which takes `id`'s children.
#[logic(open)]
pub fn split_shape<K, T, S>(
    old: DependentWeaveView<K, T, S>,
    new: DependentWeaveView<K, T, S>,
    id: Id<K>,
    new_id: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = old.nodes[id]@;
        old.nodes.contains(&id)
            && !old.nodes.contains(&new_id)
            && id != new_id
            && (forall<k> new.nodes.contains(&k) == (old.nodes.contains(&k) || k == new_id))
            && new.roots == old.roots
            && new.active == old.active
            && new.bookmarked == old.bookmarked
            && new.nodes[id]@ == NodeView {
                id,
                from: n.from,
                to: Seq::singleton(new_id),
                active: n.active,
                bookmarked: n.bookmarked,
            }
            && new.nodes[new_id]@ == NodeView {
                id: new_id,
                from: Some(id),
                to: n.to,
                active: false,
                bookmarked: false,
            }
            && (forall<c> n.to.contains(c) ==> new.nodes[c]@ == NodeView {
                id: old.nodes[c]@.id,
                from: Some(new_id),
                to: old.nodes[c]@.to,
                active: old.nodes[c]@.active,
                bookmarked: old.nodes[c]@.bookmarked,
            })
            && (forall<k> old.nodes.contains(&k) && k != id && !n.to.contains(k)
                ==> new.nodes[k]@ == old.nodes[k]@)
    }
}

/// Splitting a node keeps the weave valid.
#[logic]
#[requires(old.valid())]
#[requires(split_shape(old, new, id, new_id))]
#[ensures(new.valid())]
pub fn lemma_split_valid<K, T, S>(
    old: DependentWeaveView<K, T, S>,
    new: DependentWeaveView<K, T, S>,
    id: Id<K>,
    new_id: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = old.nodes[id]@;
        let rank = rank_of(old.nodes);
        let rank2 = |k| if k == new_id { 2 * rank[id] + 1 } else { 2 * rank[k] };
        proof_assert!(linked(old.nodes, id));
        proof_assert!(forall<c> n.to.contains(c) ==> old.nodes.contains(&c)
            && old.nodes[c]@.from == Some(id) && c != id && c != new_id && rank[id] < rank[c]);
        proof_assert!(match n.from {
            Some(p) => old.nodes.contains(&p) && p != id && p != new_id && !n.to.contains(p)
                && rank[p] < rank[id],
            None => true,
        });
        proof_assert!(forall<k> old.nodes.contains(&k) && k != id && !n.to.contains(k)
            ==> linked(old.nodes, k) && old.nodes[k]@.from != Some(id));
        proof_assert!(forall<k> old.nodes.contains(&k) ==> old.nodes[k]@.from != Some(new_id)
            && !old.nodes[k]@.to.contains(new_id));
        proof_assert!(ranked(new.nodes, rank2));
        proof_assert!(linked(new.nodes, id));
        proof_assert!(linked(new.nodes, new_id));
        proof_assert!(forall<c> n.to.contains(c) ==> linked(new.nodes, c));
        proof_assert!(forall<k> old.nodes.contains(&k) && k != id && !n.to.contains(k)
            ==> linked(new.nodes, k));
        proof_assert!(forall<k> new.nodes.contains(&k) ==> new.flagged(k))
    }
}

/// `new` is `old` once node `id` is merged into its parent `p`, which was `id`'s only parent and
/// takes its children, active status and bookmark: if only `id` was bookmarked, `p` takes its place
/// among the bookmarks.
#[logic(open)]
pub fn merge_shape<K, T, S>(
    old: DependentWeaveView<K, T, S>,
    new: DependentWeaveView<K, T, S>,
    id: Id<K>,
    p: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = old.nodes[id]@;
        let pn = old.nodes[p]@;
        old.nodes.contains(&id)
            && n.from == Some(p)
            && pn.to.len() <= 1
            && (forall<k> new.nodes.contains(&k) == (old.nodes.contains(&k) && k != id))
            && new.roots == old.roots
            && new.active == (if old.active == Some(id) { Some(p) } else { old.active })
            && no_dup(new.bookmarked)
            && (forall<k> new.bookmarked.contains(k) == if k == p {
                old.bookmarked.contains(p) || old.bookmarked.contains(id)
            } else {
                k != id && old.bookmarked.contains(k)
            })
            && (if n.bookmarked && !pn.bookmarked {
                replaced_in_place(new.bookmarked, old.bookmarked, id, p)
            } else if n.bookmarked {
                shift_removed(new.bookmarked, old.bookmarked, id)
            } else {
                new.bookmarked == old.bookmarked
            })
            && new.nodes[p]@ == NodeView {
                id: p,
                from: pn.from,
                to: n.to,
                active: pn.active || n.active,
                bookmarked: pn.bookmarked || n.bookmarked,
            }
            && (forall<c> n.to.contains(c) ==> new.nodes[c]@ == NodeView {
                id: old.nodes[c]@.id,
                from: Some(p),
                to: old.nodes[c]@.to,
                active: old.nodes[c]@.active,
                bookmarked: old.nodes[c]@.bookmarked,
            })
            && (forall<k> old.nodes.contains(&k) && k != id && k != p && !n.to.contains(k)
                ==> new.nodes[k]@ == old.nodes[k]@)
    }
}

/// Merging a node into its parent keeps the weave valid.
#[logic]
#[requires(old.valid())]
#[requires(merge_shape(old, new, id, p))]
#[ensures(new.valid())]
pub fn lemma_merge_valid<K, T, S>(
    old: DependentWeaveView<K, T, S>,
    new: DependentWeaveView<K, T, S>,
    id: Id<K>,
    p: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = old.nodes[id]@;
        let pn = old.nodes[p]@;
        let rank = rank_of(old.nodes);
        proof_assert!(linked(old.nodes, id) && linked(old.nodes, p));
        proof_assert!(old.nodes.contains(&p) && pn.to.contains(id) && rank[p] < rank[id] && p != id);
        proof_assert!(forall<k> pn.to.contains(k) == (k == id));
        proof_assert!(forall<c> n.to.contains(c) ==> old.nodes.contains(&c)
            && old.nodes[c]@.from == Some(id) && rank[id] < rank[c] && c != p && c != id);
        proof_assert!(match pn.from {
            Some(q) => old.nodes.contains(&q) && rank[q] < rank[p] && q != id && !n.to.contains(q),
            None => true,
        });
        proof_assert!(forall<k> old.nodes.contains(&k) && k != id && k != p && !n.to.contains(k)
            ==> linked(old.nodes, k) && old.nodes[k]@.from != Some(id));
        proof_assert!(ranked(new.nodes, rank));
        proof_assert!(linked(new.nodes, p));
        proof_assert!(forall<c> n.to.contains(c) ==> linked(new.nodes, c));
        proof_assert!(forall<k> old.nodes.contains(&k) && k != id && k != p && !n.to.contains(k)
            ==> linked(new.nodes, k));
        proof_assert!(forall<k> new.nodes.contains(&k) ==> new.flagged(k))
    }
}

/// A proper descendant has a parent, from which it descends.
#[logic]
#[requires(descends(nodes, d, a))]
#[requires(d != a)]
#[ensures(match parent(nodes, d) { Some(p) => descends(nodes, p, a), None => false })]
pub fn lemma_descends_proper<K, T, S>(nodes: Nodes<K, T, S>, d: Id<K>, a: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = such_that(|n: Int| 0 <= n && ancestor(nodes, d, n) == Some(a));
        proof_assert!(n >= 1);
        match parent(nodes, d) {
            Some(p) => proof_assert!(ancestor(nodes, p, n - 1) == Some(a)),
            None => (),
        }
    }
}

/// `d` is `id` or one of its descendants in `nodes`.
#[logic(open)]
pub fn in_subtree<K, T, S>(nodes: Nodes<K, T, S>, id: Id<K>, d: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! { nodes.contains(&d) && descends(nodes, d, id) }
}

impl<K, T, S> DependentWeaveView<K, T, S>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    /// The active node once the subtree rooted at `id` is removed: if the active node is removed,
    /// the parent of `id` becomes active instead.
    #[logic(open)]
    pub fn active_after_removal(self, id: Id<K>) -> Option<Id<K>> {
        pearlite! {
            match self.active {
                Some(a) => if in_subtree(self.nodes, id, a) { self.nodes[id]@.from } else { self.active },
                None => None,
            }
        }
    }

    /// The active node once `set_active(id, value)` succeeds: `id` when it is activated, its parent
    /// when it was the active node and is deactivated, and the same node otherwise.
    #[logic(open)]
    pub fn active_after_set(self, id: Id<K>, value: bool) -> Option<Id<K>> {
        pearlite! {
            if value {
                Some(id)
            } else if self.active == Some(id) {
                self.nodes[id]@.from
            } else {
                self.active
            }
        }
    }
}

/// `new` has the same nodes, links, bookmarks and contents as `old`: only which node is active may
/// differ.
#[logic(open)]
pub fn same_graph<K, T, S>(new: Nodes<K, T, S>, old: Nodes<K, T, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (forall<k> new.contains(&k) == old.contains(&k))
            && forall<k> old.contains(&k) ==> new[k]@.id == old[k]@.id
                && new[k]@.from == old[k]@.from
                && new[k]@.to == old[k]@.to
                && new[k]@.bookmarked == old[k]@.bookmarked
                && new[k].contents == old[k].contents
    }
}

/// `new` is `old` once the subtree rooted at `id` is removed.
#[logic(open)]
pub fn removal_shape<K, T, S>(
    old: DependentWeaveView<K, T, S>,
    new: DependentWeaveView<K, T, S>,
    id: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        old.nodes.contains(&id)
            && (forall<k> new.nodes.contains(&k)
                == (old.nodes.contains(&k) && !in_subtree(old.nodes, id, k)))
            && no_dup(new.roots)
            && (forall<k> new.roots.contains(k) == (old.roots.contains(k) && k != id))
            && new.active == old.active_after_removal(id)
            && no_dup(new.bookmarked)
            && (forall<k> new.bookmarked.contains(k)
                == (old.bookmarked.contains(k) && !in_subtree(old.nodes, id, k)))
            && forall<k> new.nodes.contains(&k) ==> new.nodes[k]@.id == old.nodes[k]@.id
                && new.nodes[k]@.from == old.nodes[k]@.from
                && new.nodes[k]@.bookmarked == old.nodes[k]@.bookmarked
                && new.nodes[k]@.active == (new.active == Some(k))
                && no_dup(new.nodes[k]@.to)
                && forall<c> new.nodes[k]@.to.contains(c) == (old.nodes[k]@.to.contains(c) && c != id)
    }
}

/// Removing a subtree keeps the weave valid.
#[logic]
#[requires(old.valid())]
#[requires(removal_shape(old, new, id))]
#[ensures(new.valid())]
pub fn lemma_remove_valid<K, T, S>(
    old: DependentWeaveView<K, T, S>,
    new: DependentWeaveView<K, T, S>,
    id: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_descends_step::<K, T, S>;
    let _ = lemma_descends_refl::<K, T, S>;
    let _ = lemma_descends_proper::<K, T, S>;
    pearlite! {
        let rank = rank_of(old.nodes);
        proof_assert!(forall<k> old.roots.contains(k) && k != id ==> !in_subtree(old.nodes, id, k));
        proof_assert!(forall<k> new.nodes.contains(&k) ==> old.nodes.contains(&k) && linked(old.nodes, k));
        proof_assert!(forall<k> new.nodes.contains(&k) ==> match old.nodes[k]@.from {
            Some(p) => !in_subtree(old.nodes, id, p),
            None => true,
        });
        proof_assert!(forall<k, c> new.nodes.contains(&k) && old.nodes[k]@.to.contains(c) && c != id
            ==> !in_subtree(old.nodes, id, c) && new.nodes.contains(&c));
        proof_assert!(ranked(new.nodes, rank));
        proof_assert!(forall<k> new.nodes.contains(&k) ==> linked(new.nodes, k));
        proof_assert!(in_subtree(old.nodes, id, id));
        proof_assert!(linked(old.nodes, id));
        match old.nodes[id]@.from {
            Some(p) => {
                lemma_descends_refl(old.nodes, p);
                lemma_descends_step(old.nodes, id, p, p);
                proof_assert!(descends(old.nodes, p, id) ==> {
                    lemma_descends_antisym(old.nodes, id, p);
                    false
                });
                proof_assert!(new.nodes.contains(&p))
            }
            None => proof_assert!(old.roots.contains(id)),
        };
        proof_assert!(forall<k> new.nodes.contains(&k) ==> new.flagged(k))
    }
}

/// `node` is what remains of `old` after the subtree rooted at `id` is removed: at most its link to
/// `id` is gone, and only its active status may change.
#[logic(open)]
pub fn survives_removal<K, T, S>(
    node: DependentNode<K, T, S>,
    old: DependentNode<K, T, S>,
    id: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        node@.id == old@.id
            && node@.from == old@.from
            && (forall<c> node@.to.contains(c) == (old@.to.contains(c) && c != id))
            && node@.bookmarked == old@.bookmarked
            && node.contents == old.contents
    }
}

/// The parent of a node isn't in the node's subtree.
#[logic]
#[requires(valid_tree(nodes))]
#[requires(nodes.contains(&id))]
#[ensures(match parent(nodes, id) {
    Some(p) => nodes.contains(&p) && !in_subtree(nodes, id, p) && nodes[p]@.to.contains(id),
    None => true,
})]
pub fn lemma_parent_outside<K, T, S>(nodes: Nodes<K, T, S>, id: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    proof_assert!(linked(nodes, id));
    match parent(nodes, id) {
        Some(p) => {
            lemma_descends_refl(nodes, p);
            lemma_descends_step(nodes, id, p, p);
            pearlite! {
                proof_assert!(descends(nodes, p, id) ==> {
                    lemma_descends_antisym(nodes, id, p);
                    false
                })
            }
        }
        None => (),
    }
}

/// A sequence without duplicates of keys of `m`, as long as `m`, contains all its keys.
#[logic]
#[requires(no_dup(s))]
#[requires(forall<i> 0 <= i && i < s.len() ==> m.contains(&s[i]))]
#[requires(s.len() == m.len())]
#[ensures(forall<k> m.contains(&k) ==> s.contains(k))]
pub fn lemma_full_cover<I, V>(s: Seq<I>, m: FMap<I, V>) {
    pearlite! {
        proof_assert!(forall<k> m.contains(&k) && !s.contains(k) ==> {
            lemma_no_dup_len(s, m.remove(k));
            false
        })
    }
}

/// The position of `x` in `s`, if any.
#[logic(open)]
pub fn index_of<I>(s: Seq<I>, x: I) -> Int {
    pearlite! {
        match try_such_that(|i: Int| 0 <= i && i < s.len() && s[i] == x) {
            Some(i) => i,
            None => 0,
        }
    }
}

/// A traversal from the roots which visits every node gives a rank function: the order of visit.
#[logic]
#[requires(all_linked(nodes))]
#[requires(forall<r> roots.contains(r) ==> parent(nodes, r) == None)]
#[requires(dfs(nodes, roots, added, Seq::empty()))]
#[requires(forall<k> nodes.contains(&k) ==> added.contains(k))]
#[ensures(acyclic(nodes))]
pub fn lemma_visit_rank<K, T, S>(nodes: Nodes<K, T, S>, roots: Seq<Id<K>>, added: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let rank = |k| index_of(added, k);
        proof_assert!(forall<j> 0 <= j && j < added.len() ==> index_of(added, added[j]) == j);
        proof_assert!(forall<k> nodes.contains(&k) ==> 0 <= rank[k] && rank[k] < added.len()
            && added[rank[k]] == k);
        proof_assert!(forall<k> nodes.contains(&k) ==> match parent(nodes, k) {
            Some(p) => added.subsequence(0, rank[k]).contains(p),
            None => true,
        });
        proof_assert!(ranked(nodes, rank))
    }
}

/// A traversal from the roots visits as many nodes as there are exactly when they are acyclic.
#[logic]
#[requires(all_linked(nodes))]
#[requires(no_dup(roots))]
#[requires(forall<r> roots.contains(r) ==> nodes.contains(&r))]
#[requires(forall<k> nodes.contains(&k) ==> (parent(nodes, k) == None) == roots.contains(k))]
#[requires(dfs(nodes, roots, added, Seq::empty()))]
#[ensures((added.len() == nodes.len()) == acyclic(nodes))]
pub fn lemma_detect_cycles<K, T, S>(nodes: Nodes<K, T, S>, roots: Seq<Id<K>>, added: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    lemma_dfs_done(nodes, roots, added);
    if added.len() == nodes.len() {
        lemma_full_cover(added, nodes);
        lemma_visit_rank(nodes, roots, added)
    } else {
        pearlite! {
            proof_assert!(acyclic(nodes) ==> {
                let _ = lemma_has_root::<K, T, S>;
                let _rank = rank_of(nodes);
                proof_assert!(forall<k> nodes.contains(&k) ==> exists<r> nodes.contains(&r)
                    && parent(nodes, r) == None && descends(nodes, k, r));
                proof_assert!(forall<k> nodes.contains(&k) ==> from_sources(nodes, roots, k));
                proof_assert!(forall<k> added.contains(k) == nodes.contains(&k));
                lemma_lists_len(added, nodes);
                false
            })
        }
    }
}

/// Roots are closed sources.
#[logic]
#[requires(forall<r> roots.contains(r) ==> parent(nodes, r) == None)]
#[ensures(sources_closed(nodes, roots))]
pub fn lemma_roots_closed<K, T, S>(nodes: Nodes<K, T, S>, roots: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
}

/// The smallest of `b` and of the values of `f` over `s`.
#[logic(open)]
#[variant(s.len())]
#[ensures(result <= b)]
#[ensures(forall<i> 0 <= i && i < s.len() ==> result <= f[s[i]])]
#[ensures(result == b || exists<i> 0 <= i && i < s.len() && result == f[s[i]])]
pub fn min_over<T>(s: Seq<T>, f: Mapping<T, Int>, b: Int) -> Int {
    pearlite! {
        if s.len() == 0 {
            b
        } else {
            let m = min_over(s.tail(), f, b);
            if f[s[0]] < m { f[s[0]] } else { m }
        }
    }
}

/// The pre-order traversal of the subtrees rooted at the nodes of `s`, in order: each node,
/// followed by the traversals of its children's subtrees, in order. `rank`, which increases from
/// parent to child and stays below `bound`, shows that the traversal ends.
#[logic(open)]
#[requires(all_linked(nodes) && ranked(nodes, rank))]
#[requires(forall<k> nodes.contains(&k) ==> rank[k] < bound)]
#[requires(forall<i> 0 <= i && i < s.len() ==> nodes.contains(&s[i]))]
#[variant((bound - min_over(s, rank, bound), s.len()))]
pub fn preorder_by<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    bound: Int,
    s: Seq<Id<K>>,
) -> Seq<Id<K>>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        if s.len() == 0 {
            Seq::empty()
        } else {
            let to = nodes[s[0]]@.to;
            proof_assert!(linked(nodes, s[0]));
            proof_assert!(forall<i> 0 <= i && i < to.len() ==> nodes.contains(&to[i])
                && nodes[to[i]]@.from == Some(s[0]) && rank[s[0]] + 1 <= rank[to[i]]);
            proof_assert!(rank[s[0]] + 1 <= min_over(to, rank, bound));
            proof_assert!(min_over(s, rank, bound) <= rank[s[0]]);
            Seq::singleton(s[0])
                .concat(preorder_by(nodes, rank, bound, to))
                .concat(preorder_by(nodes, rank, bound, s.tail()))
        }
    }
}

/// The pre-order traversal of the subtrees rooted at the nodes of `s`, in order: each node,
/// followed by the traversals of its children's subtrees, in order.
///
/// This is only open in this module: the lemmas here give what the code needs
/// (`lemma_preorder_empty`, `lemma_preorder_concat`, `lemma_preorder_step` and
/// `lemma_preorder_pop`), so that the provers don't unfold it in the methods' proofs.
#[logic(open(self))]
#[requires(valid_tree(nodes))]
#[requires(forall<i> 0 <= i && i < s.len() ==> nodes.contains(&s[i]))]
pub fn preorder<K, T, S>(nodes: Nodes<K, T, S>, s: Seq<Id<K>>) -> Seq<Id<K>>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! { preorder_by(nodes, rank_of(nodes), set_bound(nodes.keys(), rank_of(nodes)), s) }
}

/// The traversal of two sequences of subtrees is the concatenation of their traversals.
#[logic]
#[requires(all_linked(nodes) && ranked(nodes, rank))]
#[requires(forall<k> nodes.contains(&k) ==> rank[k] < bound)]
#[requires(forall<i> 0 <= i && i < a.len() ==> nodes.contains(&a[i]))]
#[requires(forall<i> 0 <= i && i < b.len() ==> nodes.contains(&b[i]))]
#[ensures(preorder_by(nodes, rank, bound, a.concat(b))
    == preorder_by(nodes, rank, bound, a).concat(preorder_by(nodes, rank, bound, b)))]
#[variant(a.len())]
pub fn lemma_preorder_by_concat<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    bound: Int,
    a: Seq<Id<K>>,
    b: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        if a.len() == 0 {
            proof_assert!(a.concat(b).ext_eq(b));
            preorder_by(nodes, rank, bound, b).concat_empty()
        } else {
            let t = a.tail();
            proof_assert!(forall<i> 0 <= i && i < t.len() ==> t[i] == a[i + 1]);
            lemma_preorder_by_concat(nodes, rank, bound, t, b);
            proof_assert!(a.concat(b).len() > 0 && a.concat(b)[0] == a[0]);
            proof_assert!(a.concat(b).tail().ext_eq(t.concat(b)));
            let head = Seq::singleton(a[0]).concat(preorder_by(nodes, rank, bound, nodes[a[0]]@.to));
            head.concat_assoc(preorder_by(nodes, rank, bound, t), preorder_by(nodes, rank, bound, b))
        }
    }
}

/// The traversal of no subtrees is empty.
#[logic]
#[requires(valid_tree(nodes))]
#[ensures(preorder(nodes, Seq::empty()) == Seq::empty())]
pub fn lemma_preorder_empty<K, T, S>(nodes: Nodes<K, T, S>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
}

/// The traversal of two sequences of subtrees is the concatenation of their traversals.
#[logic]
#[requires(valid_tree(nodes))]
#[requires(forall<i> 0 <= i && i < a.len() ==> nodes.contains(&a[i]))]
#[requires(forall<i> 0 <= i && i < b.len() ==> nodes.contains(&b[i]))]
#[ensures(preorder(nodes, a.concat(b)) == preorder(nodes, a).concat(preorder(nodes, b)))]
pub fn lemma_preorder_concat<K, T, S>(nodes: Nodes<K, T, S>, a: Seq<Id<K>>, b: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let rank = rank_of(nodes);
        proof_assert!(forall<i> 0 <= i && i < a.concat(b).len() ==> nodes.contains(&a.concat(b)[i]));
        lemma_preorder_by_concat(nodes, rank, set_bound(nodes.keys(), rank), a, b)
    }
}

/// Visiting the node `s` at the top of a depth-first search's stack, and replacing it with its
/// children in reverse order, leaves the same traversal to do after `s`.
#[logic]
#[requires(valid_tree(nodes))]
#[requires(nodes.contains(&s))]
#[requires(forall<i> 0 <= i && i < rest.len() ==> nodes.contains(&rest[i]))]
#[ensures(preorder(nodes, rest.snoc(s).reverse()) == Seq::singleton(s)
    .concat(preorder(nodes, rest.concat(nodes[s]@.to.reverse()).reverse())))]
pub fn lemma_preorder_step<K, T, S>(nodes: Nodes<K, T, S>, rest: Seq<Id<K>>, s: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let to = nodes[s]@.to;
        let r = rest.reverse();
        proof_assert!(linked(nodes, s));
        proof_assert!(forall<i> 0 <= i && i < r.len() ==> r[i] == rest[rest.len() - 1 - i]);
        proof_assert!(forall<i> 0 <= i && i < r.len() ==> nodes.contains(&r[i]));
        proof_assert!(forall<i> 0 <= i && i < to.len() ==> nodes.contains(&to[i]));
        proof_assert!(rest.snoc(s).reverse().ext_eq(Seq::singleton(s).concat(r)));
        proof_assert!(Seq::singleton(s).concat(r).tail().ext_eq(r));
        rest.reverse_concat(to.reverse());
        proof_assert!(to.reverse().reverse().ext_eq(to));
        lemma_preorder_concat(nodes, to, r);
        Seq::singleton(s).concat_assoc(preorder(nodes, to), preorder(nodes, r))
    }
}

/// Visiting the node `s` at the top of a depth-first search's stack, after `added`, and replacing
/// it with its children in reverse order, keeps `added` followed by the traversal of the stack
/// equal to the traversal of the sources.
#[logic]
#[requires(valid_tree(nodes))]
#[requires(nodes.contains(&s))]
#[requires(forall<i> 0 <= i && i < sources.len() ==> nodes.contains(&sources[i]))]
#[requires(forall<i> 0 <= i && i < rest.len() ==> nodes.contains(&rest[i]))]
#[requires(added.concat(preorder(nodes, rest.snoc(s).reverse())) == preorder(nodes, sources))]
#[ensures(added.snoc(s).concat(preorder(nodes, rest.concat(nodes[s]@.to.reverse()).reverse()))
    == preorder(nodes, sources))]
pub fn lemma_preorder_pop<K, T, S>(
    nodes: Nodes<K, T, S>,
    sources: Seq<Id<K>>,
    added: Seq<Id<K>>,
    rest: Seq<Id<K>>,
    s: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let to = nodes[s]@.to;
        let next = rest.concat(to.reverse());
        proof_assert!(linked(nodes, s));
        proof_assert!(forall<i> 0 <= i && i < to.len() ==> nodes.contains(&to[i]));
        proof_assert!(forall<i> 0 <= i && i < to.len() ==> to.reverse()[i] == to[to.len() - 1 - i]);
        proof_assert!(forall<i> 0 <= i && i < next.len() ==> nodes.contains(&next[i]));
        proof_assert!(forall<i> 0 <= i && i < next.len() ==> next.reverse()[i] == next[next.len() - 1 - i]);
        proof_assert!(forall<i> 0 <= i && i < next.len() ==> nodes.contains(&next.reverse()[i]));
        let after = preorder(nodes, next.reverse());
        lemma_preorder_step(nodes, rest, s);
        proof_assert!(added.snoc(s).ext_eq(added.concat(Seq::singleton(s))));
        added.concat_assoc(Seq::singleton(s), after)
    }
}
