//! Specifications and lemmas for [`IndependentWeave`].
//!
//! This module only exists while Creusot is verifying the crate. See `VERIFICATION.md`.

use core::hash::{BuildHasher, Hash};

use creusot_std::{
    logic::{FMap, FSet, Mapping, such_that},
    prelude::*,
};

use super::{IndependentNode, IndependentWeave};
use crate::{
    IndependentContents,
    verification::{Id, Model, no_dup, same_elements},
};

/// The nodes of a weave, by identifier.
pub type Nodes<K, T, S> = FMap<Id<K>, IndependentNode<K, T, S>>;

/// The view of an [`IndependentNode`], without its contents.
pub struct NodeView<I> {
    /// The node's identifier.
    pub id: I,
    /// The identifiers of the node's parents, in order.
    pub from: Seq<I>,
    /// The identifiers of the node's children, in order.
    pub to: Seq<I>,
    /// If the node is active.
    pub active: bool,
    /// If the node is bookmarked.
    pub bookmarked: bool,
}

impl<K, T, S> View for IndependentNode<K, T, S>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type ViewTy = NodeView<Id<K>>;

    #[logic(open)]
    fn view(self) -> NodeView<Id<K>> {
        NodeView {
            id: self.id.deep_model(),
            from: self.from.model(),
            to: self.to.model(),
            active: self.active,
            bookmarked: self.bookmarked,
        }
    }
}

impl<I> NodeView<I> {
    /// The node's own links are consistent: spec version of `IndependentNode::validate()`.
    #[logic(open)]
    pub fn valid(self) -> bool {
        pearlite! {
            (forall<k> self.from.contains(k) ==> !self.to.contains(k))
                && !self.from.contains(self.id)
                && !self.to.contains(self.id)
        }
    }
}

/// The view of an [`IndependentWeave`], without its metadata.
pub struct IndependentWeaveView<K, T, S>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    /// The nodes, by identifier.
    pub nodes: Nodes<K, T, S>,
    /// The identifiers of the root nodes, in order.
    pub roots: Seq<Id<K>>,
    /// The identifiers of the active nodes.
    pub active: FSet<Id<K>>,
    /// The identifiers of the bookmarked nodes, in order.
    pub bookmarked: Seq<Id<K>>,
}

impl<K, T, M, S> View for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type ViewTy = IndependentWeaveView<K, T, S>;

    #[logic(open(crate))]
    fn view(self) -> IndependentWeaveView<K, T, S> {
        IndependentWeaveView {
            nodes: self.nodes.model(),
            roots: self.roots.model(),
            active: self.active.model(),
            bookmarked: self.bookmarked.model(),
        }
    }
}

impl<K, T, M, S> Invariant for IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    #[logic(open(crate))]
    fn invariant(self) -> bool {
        pearlite! { self@.valid() }
    }
}

/// The links of node `k` are consistent with the rest of `nodes`.
#[logic(open)]
pub fn linked<K, T, S>(nodes: Nodes<K, T, S>, k: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = nodes[k]@;
        n.valid()
            && no_dup(n.from)
            && no_dup(n.to)
            && n.id == k
            && (forall<p> n.from.contains(p) ==> nodes.contains(&p) && nodes[p]@.to.contains(k))
            && forall<c> n.to.contains(c) ==> nodes.contains(&c) && nodes[c]@.from.contains(k)
    }
}

/// Every node of `nodes` is linked to the rest of it.
#[logic(open)]
pub fn all_linked<K, T, S>(nodes: Nodes<K, T, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { forall<k> nodes.contains(&k) ==> linked(nodes, k) }
}

/// `rank` strictly increases from parent to child.
#[logic(open)]
pub fn ranked<K, T, S>(nodes: Nodes<K, T, S>, rank: Mapping<Id<K>, Int>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<k> nodes.contains(&k) ==> 0 <= rank[k]
            && forall<p> nodes[k]@.from.contains(p) ==> rank[p] < rank[k]
    }
}

/// No node is its own ancestor.
#[logic(open)]
pub fn acyclic<K, T, S>(nodes: Nodes<K, T, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { exists<rank: Mapping<Id<K>, Int>> ranked(nodes, rank) }
}

/// A rank function of an acyclic graph.
#[logic(open)]
#[requires(acyclic(nodes))]
#[ensures(ranked(nodes, result))]
pub fn rank_of<K, T, S>(nodes: Nodes<K, T, S>) -> Mapping<Id<K>, Int>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    such_that(|rank| ranked(nodes, rank))
}

/// Every identifier in `path` is a node, each node's successor is one of its parents, and the last
/// node has no parents.
#[logic(open)]
pub fn valid_path<K, T, S>(nodes: Nodes<K, T, S>, path: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (forall<i> 0 <= i && i < path.len() ==> nodes.contains(&path[i]))
            && (forall<i> 0 <= i && i + 1 < path.len() ==> nodes[path[i]]@.from.contains(path[i + 1]))
            && (0 < path.len() ==> nodes[path[path.len() - 1]]@.from.len() == 0)
    }
}

/// `path` is a valid path without duplicates whose nodes are exactly `set`.
#[logic(open)]
pub fn path_of<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>, path: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        valid_path(nodes, path) && no_dup(path) && forall<k> path.contains(k) == set.contains(&k)
    }
}

/// `set` holds exactly the nodes of a path from some node to a root.
#[logic(open)]
pub fn forms_path<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { exists<path: Seq<Id<K>>> path_of(nodes, set, path) }
}

/// `id` has a child in `set`: `nodes[id].to.iter().any(|c| set.contains(c))`.
#[logic(open)]
pub fn has_child_in<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>, id: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { exists<c> nodes[id]@.to.contains(c) && set.contains(&c) }
}

impl<K, T, S> IndependentWeaveView<K, T, S>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    /// The weave is internally consistent: spec version of `IndependentWeave::validate()`.
    ///
    /// `valid_topology` checks two things: that the links form no cycles, which is [`acyclic`]
    /// here, and that the active nodes are exactly the nodes of a path from some node to a root,
    /// which is [`forms_path`].
    #[logic(open)]
    pub fn valid(self) -> bool {
        pearlite! { self.valid_structure() && forms_path(self.nodes, self.active) }
    }

    /// The weave is consistent, except that its active nodes may not form a path.
    #[logic(open)]
    pub fn valid_structure(self) -> bool {
        pearlite! {
            structure(self.nodes, self.roots, self.active)
                && no_dup(self.bookmarked)
                && (forall<k> self.bookmarked.contains(k) ==> self.nodes.contains(&k))
                && forall<k> self.nodes.contains(&k)
                    ==> self.nodes[k]@.bookmarked == self.bookmarked.contains(k)
        }
    }

    /// Whether node `k` is a root, is active and is bookmarked agrees with the rest of the weave.
    #[logic(open)]
    pub fn flagged(self, k: Id<K>) -> bool {
        pearlite! {
            let n = self.nodes[k]@;
            (n.from.len() == 0) == self.roots.contains(k)
                && n.active == self.active.contains(&k)
                && n.bookmarked == self.bookmarked.contains(k)
        }
    }

    /// `self` and `other` contain nodes with the same identifiers.
    #[logic(open)]
    pub fn same_keys(self, other: Self) -> bool {
        pearlite! { forall<k> self.nodes.contains(&k) == other.nodes.contains(&k) }
    }
}

/// The nodes, roots and active nodes of a weave are consistent, except that the active nodes may
/// not form a path.
#[logic(open)]
pub fn structure<K, T, S>(nodes: Nodes<K, T, S>, roots: Seq<Id<K>>, active: FSet<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        all_linked(nodes)
            && acyclic(nodes)
            && no_dup(roots)
            && (forall<k> roots.contains(k) ==> nodes.contains(&k))
            && (forall<k> active.contains(&k) ==> nodes.contains(&k))
            && forall<k> nodes.contains(&k) ==> (nodes[k]@.from.len() == 0) == roots.contains(k)
                && nodes[k]@.active == active.contains(&k)
    }
}

/// `new` has the same nodes and links as `old`: only which nodes are active may differ.
#[logic(open)]
pub fn same_graph<K, T, S>(new: Nodes<K, T, S>, old: Nodes<K, T, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
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

/// Activating `id` only needs `id` itself to be activated: it has no parents and no node is
/// active, or one of its parents is active and has no active children.
#[logic(open)]
pub fn extends_path<K, T, S>(nodes: Nodes<K, T, S>, active: FSet<Id<K>>, id: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (nodes[id]@.from.len() == 0 && active.len() == 0)
            || exists<p> nodes[id]@.from.contains(p) && active.contains(&p)
                && !has_child_in(nodes, active, p)
    }
}

/// The active nodes form a path whenever `update_node_activity_in_place(id, value)` relies on
/// them doing so: when it doesn't change them, or adds `id` to or removes it from the end of the
/// path. Otherwise it recomputes the active path from scratch.
#[logic(open)]
pub fn ready_to_activate<K, T, S>(
    nodes: Nodes<K, T, S>,
    active: FSet<Id<K>>,
    id: Id<K>,
    value: bool,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (!nodes.contains(&id) ==> forms_path(nodes, active))
            && (nodes.contains(&id) && nodes[id]@.active == value ==> forms_path(nodes, active))
            && (nodes.contains(&id) && value && !nodes[id]@.active && extends_path(nodes, active, id)
                ==> forms_path(nodes, active.add(id)))
            && (nodes.contains(&id) && !value && nodes[id]@.active && !has_child_in(nodes, active, id)
                ==> forms_path(nodes, active.remove(id)))
    }
}

/// The number of elements of `s` in `set`.
#[logic(open)]
#[variant(s.len())]
pub fn count_in<I>(set: FSet<I>, s: Seq<I>) -> Int {
    pearlite! {
        if s.len() == 0 {
            0
        } else {
            (if set.contains(&s[0]) { 1 } else { 0 }) + count_in(set, s.tail())
        }
    }
}

/// `path` leads from `id` up to a root: it lists distinct nodes, starting at `id`, each followed by
/// one of its parents, and ending at a node without parents.
#[logic(open)]
pub fn path_up<K, T, S>(nodes: Nodes<K, T, S>, id: Id<K>, path: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { valid_path(nodes, path) && no_dup(path) && 0 < path.len() && path[0] == id }
}

/// `path` leads from `id` down to a leaf: it lists distinct nodes, starting at `id`, each followed
/// by one of its children, and ending at a node without children.
#[logic(open)]
pub fn path_down<K, T, S>(nodes: Nodes<K, T, S>, id: Id<K>, path: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        no_dup(path)
            && 0 < path.len()
            && path[0] == id
            && (forall<i> 0 <= i && i < path.len() ==> nodes.contains(&path[i]))
            && (forall<i> 0 <= i && i + 1 < path.len() ==> nodes[path[i]]@.to.contains(path[i + 1]))
            && nodes[path[path.len() - 1]]@.to.len() == 0
    }
}

/// As part of an active path which should keep as many nodes of `set` as possible, and add as few
/// other nodes as possible, `q` is no better than `p`: it has fewer nodes in `set`, or as many and
/// at least as many nodes in total.
#[logic(open)]
pub fn no_better<I>(set: FSet<I>, q: Seq<I>, p: Seq<I>) -> bool {
    pearlite! {
        count_in(set, q) < count_in(set, p)
            || (count_in(set, q) == count_in(set, p) && p.len() <= q.len())
    }
}

/// `path` is one of the best paths from `id` up to a root, by [`no_better`].
#[logic(open)]
pub fn best_path_up<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    id: Id<K>,
    path: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        path_up(nodes, id, path) && forall<q> path_up(nodes, id, q) ==> no_better(set, q, path)
    }
}

/// `path` is one of the best paths from `id` down to a leaf, by [`no_better`].
#[logic(open)]
pub fn best_path_down<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    id: Id<K>,
    path: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        path_down(nodes, id, path) && forall<q> path_down(nodes, id, q) ==> no_better(set, q, path)
    }
}

/// No path down from `id` reaches a node of `set` other than `id`.
#[logic(open)]
pub fn nothing_below<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>, id: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<q, i> path_down(nodes, id, q) && 0 < i && i < q.len() ==> !set.contains(&q[i])
    }
}

/// `below` is the part below `id` of an active path recomputed through `id`, when the nodes of
/// `set` were active: just `id` if no path down from `id` reaches a node of `set`, and otherwise
/// one of the best paths from `id` down to a leaf, cut after its last node in `set`.
#[logic(open)]
pub fn kept_below<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    id: Id<K>,
    below: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (below == Seq::singleton(id) && nothing_below(nodes, set, id))
            || exists<path> best_path_down(nodes, set, id, path)
                && 1 < below.len()
                && below.len() <= path.len()
                && (forall<i> 0 <= i && i < below.len() ==> below[i] == path[i])
                && set.contains(&below[below.len() - 1])
                && forall<i> below.len() <= i && i < path.len() ==> !set.contains(&path[i])
    }
}

/// `after` is the active path recomputed through `id`, when the nodes of `before` were active: one
/// of the best paths from `id` up to a root, extended below `id` as [`kept_below`] describes.
#[logic(open)]
pub fn rerouted<K, T, S>(
    nodes: Nodes<K, T, S>,
    before: FSet<Id<K>>,
    id: Id<K>,
    after: FSet<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        exists<up, below> best_path_up(nodes, before, id, up)
            && kept_below(nodes, before, id, below)
            && forall<k> after.contains(&k) == (up.contains(k) || below.contains(k))
    }
}

/// `after` is the active path once the inactive node `id` is activated, when the nodes of `before`
/// were active: `id` is added to the end of the active path when it extends it, and otherwise the
/// active path is [`rerouted`] through it.
#[logic(open)]
pub fn activated<K, T, S>(
    nodes: Nodes<K, T, S>,
    before: FSet<Id<K>>,
    id: Id<K>,
    after: FSet<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        if extends_path(nodes, before, id) {
            after == before.add(id)
        } else {
            rerouted(nodes, before, id, after)
        }
    }
}

/// `after` is the active path once `id` is activated with [`DependentWeave`]'s semantics, which
/// make it the end of the active path, when the nodes of `before` were active: unchanged if `id`
/// already ends it, `id` added to the end of the path when it extends it, and otherwise the nodes
/// of one of the best paths from `id` up to a root.
///
/// [`DependentWeave`]: crate::dependent::DependentWeave
#[logic(open)]
pub fn activated_at_end<K, T, S>(
    nodes: Nodes<K, T, S>,
    before: FSet<Id<K>>,
    id: Id<K>,
    after: FSet<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        if before.contains(&id) && !has_child_in(nodes, before, id) {
            after == before
        } else if !before.contains(&id) && extends_path(nodes, before, id) {
            after == before.add(id)
        } else {
            exists<up> best_path_up(nodes, before, id, up)
                && forall<k> after.contains(&k) == up.contains(k)
        }
    }
}

/// Inserting the node `n` into a weave whose view is `old` activates it: it is active, or one of
/// its children is an active root, which it becomes the parent of.
#[logic(open)]
pub fn inserted_active<K, T, S>(old: IndependentWeaveView<K, T, S>, n: NodeView<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        n.active
            || exists<c> n.to.contains(c) && old.nodes[c]@.from.len() == 0 && old.active.contains(&c)
    }
}

/// `after` holds the nodes of one of the longest paths to a root among those whose nodes are all
/// in `within`.
#[logic(open)]
pub fn longest_path_within<K, T, S>(
    nodes: Nodes<K, T, S>,
    within: FSet<Id<K>>,
    after: FSet<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        exists<path> path_of(nodes, after, path)
            && (forall<i> 0 <= i && i < path.len() ==> within.contains(&path[i]))
            && forall<q> valid_path(nodes, q) && no_dup(q)
                && (forall<i> 0 <= i && i < q.len() ==> within.contains(&q[i]))
                ==> q.len() <= path.len()
    }
}

/// The empty graph is acyclic.
#[logic]
#[ensures(acyclic(FMap::<Id<K>, IndependentNode<K, T, S>>::empty()))]
#[ensures(forms_path(FMap::<Id<K>, IndependentNode<K, T, S>>::empty(), FSet::empty()))]
pub fn lemma_empty<K, T, S>()
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    proof_assert!(ranked(
        FMap::<Id<K>, IndependentNode<K, T, S>>::empty(),
        |_| 0
    ));
    proof_assert!(path_of(
        FMap::<Id<K>, IndependentNode<K, T, S>>::empty(),
        FSet::empty(),
        Seq::empty()
    ));
}

/// `new` has the same nodes, roots, links and active nodes as `old`, and only its bookmarks and
/// node contents may differ, which it flags consistently.
#[logic(open)]
pub fn reflagged<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        old.same_keys(new)
            && (forall<k> old.nodes.contains(&k) ==> new.nodes[k]@.id == old.nodes[k]@.id
                && same_elements(new.nodes[k]@.from, old.nodes[k]@.from)
                && same_elements(new.nodes[k]@.to, old.nodes[k]@.to)
                && no_dup(new.nodes[k]@.from)
                && no_dup(new.nodes[k]@.to))
            && same_elements(new.roots, old.roots)
            && no_dup(new.roots)
            && no_dup(new.bookmarked)
            && new.active == old.active
            && (forall<k> new.bookmarked.contains(k) ==> new.nodes.contains(&k))
            && forall<k> new.nodes.contains(&k) ==> new.flagged(k)
    }
}

/// Changing only bookmarks, contents and the order of links preserves validity, as long as the new
/// flags are consistent.
#[logic]
#[requires(old.valid())]
#[ensures(forall<new: IndependentWeaveView<K, T, S>> #[trigger(new.valid())]
    reflagged(old, new) ==> new.valid())]
pub fn lemma_reflagged<K, T, S>(old: IndependentWeaveView<K, T, S>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<new: IndependentWeaveView<K, T, S>> reflagged(old, new)
            ==> ranked(new.nodes, rank_of(old.nodes)));
        proof_assert!(forall<new: IndependentWeaveView<K, T, S>> reflagged(old, new)
            ==> forall<k> new.nodes.contains(&k) ==> linked(new.nodes, k));
        proof_assert!(forall<new: IndependentWeaveView<K, T, S>, path: Seq<Id<K>>>
            reflagged(old, new) && path_of(old.nodes, old.active, path)
            ==> path_of(new.nodes, new.active, path))
    }
}

/// `new` is `old` with node `id`'s bookmark set to `value`.
#[logic(open)]
pub fn rebookmarked<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    value: bool,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
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
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    value: bool,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let path = such_that(|path| path_of(old.nodes, old.active, path));
        proof_assert!(forall<k> new.nodes.contains(&k) ==> new.nodes[k]@.from == old.nodes[k]@.from
            && new.nodes[k]@.to == old.nodes[k]@.to && new.nodes[k]@.id == old.nodes[k]@.id
            && new.nodes[k]@.active == old.nodes[k]@.active);
        proof_assert!(ranked(new.nodes, rank_of(old.nodes)));
        proof_assert!(forall<k> new.nodes.contains(&k) ==> linked(old.nodes, k) && linked(new.nodes, k));
        proof_assert!(structure(new.nodes, new.roots, new.active));
        proof_assert!(forall<k> new.nodes.contains(&k) ==> new.flagged(k));
        proof_assert!(new.valid_structure());
        proof_assert!(path_of(new.nodes, new.active, path))
    }
}

/// Changing one node's bookmark preserves validity.
#[logic]
#[requires(old.valid())]
#[ensures(forall<new: IndependentWeaveView<K, T, S>> #[trigger(new.valid())]
    rebookmarked(old, new, id, value) ==> new.valid())]
pub fn lemma_rebookmarked<K, T, S>(old: IndependentWeaveView<K, T, S>, id: Id<K>, value: bool)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_rebookmarked_valid::<K, T, S>;
}

pub mod activation;
pub mod choice;
pub mod conversion;
pub mod edit;
pub mod graph;
pub mod kahn;
pub mod paths;
pub mod removal;
pub mod routes;
pub mod search;
pub mod traversal;
pub mod validation;

#[allow(unused_imports)]
pub use activation::*;
#[allow(unused_imports)]
pub use choice::*;
#[allow(unused_imports)]
pub use conversion::*;
pub use edit::*;
pub use graph::*;
#[allow(unused_imports)]
pub use kahn::*;
#[allow(unused_imports)]
pub use paths::*;
#[allow(unused_imports)]
pub use removal::*;
#[allow(unused_imports)]
pub use routes::*;
#[allow(unused_imports)]
pub use search::*;
pub use traversal::*;
#[allow(unused_imports)]
pub use validation::*;

/// In a path through `set`, the only element without a child in `set` is the first one.
#[logic]
#[requires(all_linked(nodes))]
#[requires(path_of(nodes, set, path))]
#[requires(set.contains(&x))]
#[requires(!has_child_in(nodes, set, x))]
#[ensures(0 < path.len() && path[0] == x)]
pub fn lemma_path_end<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>, path: Seq<Id<K>>, x: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<i> 0 < i && i < path.len() ==> linked(nodes, path[i - 1])
            && nodes[path[i]]@.to.contains(path[i - 1]) && set.contains(&path[i - 1]));
        proof_assert!(forall<i> 0 < i && i < path.len() ==> path[i] != x)
    }
}

/// A valid weave is ready to activate or deactivate any node.
#[logic]
#[requires(view.valid())]
#[ensures(ready_to_activate(view.nodes, view.active, id, value))]
pub fn lemma_ready<K, T, S>(view: IndependentWeaveView<K, T, S>, id: Id<K>, value: bool)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    lemma_ready_parts(view.nodes, view.roots, view.active, id, value)
}

/// Consistent nodes whose active nodes form a path are ready to activate or deactivate any node.
#[logic]
#[requires(structure(nodes, roots, active))]
#[requires(forms_path(nodes, active))]
#[ensures(ready_to_activate(nodes, active, id, value))]
pub fn lemma_ready_parts<K, T, S>(
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    active: FSet<Id<K>>,
    id: Id<K>,
    value: bool,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let path = such_that(|path| path_of(nodes, active, path));
        proof_assert!(nodes.contains(&id) ==> nodes[id]@.active == active.contains(&id) && linked(nodes, id));
        proof_assert!(nodes.contains(&id) && value && !nodes[id]@.active && extends_path(nodes, active, id)
            ==> {
                if nodes[id]@.from.len() == 0 && active.len() == 0 {
                    proof_assert!(forall<k> !active.contains(&k));
                    proof_assert!(path_of(nodes, active.add(id), Seq::singleton(id)));
                    true
                } else {
                    let p = such_that(|p| nodes[id]@.from.contains(p) && active.contains(&p)
                        && !has_child_in(nodes, active, p));
                    lemma_path_end(nodes, active, path, p);
                    let new = Seq::singleton(id).concat(path);
                    proof_assert!(new.len() == path.len() + 1 && new[0] == id);
                    proof_assert!(forall<i> 0 <= i && i < path.len() ==> new[i + 1] == path[i]);
                    proof_assert!(!active.contains(&id) && !path.contains(id));
                    proof_assert!(forall<i> 0 <= i && i < new.len() ==> nodes.contains(&new[i]));
                    proof_assert!(nodes[new[0]]@.from.contains(new[1]));
                    proof_assert!(forall<i> 1 <= i && i + 1 < new.len()
                        ==> nodes[new[i]]@.from.contains(new[i + 1]));
                    proof_assert!(nodes[new[new.len() - 1]]@.from.len() == 0);
                    proof_assert!(valid_path(nodes, new));
                    proof_assert!(forall<i, j> 1 <= i && i < j && j < new.len() ==> new[i] != new[j]);
                    proof_assert!(forall<j> 1 <= j && j < new.len() ==> new[0] != new[j]);
                    proof_assert!(no_dup(new));
                    proof_assert!(forall<k> new.contains(k) == (path.contains(k) || k == id));
                    proof_assert!(path_of(nodes, active.add(id), new));
                    true
                }
            } && forms_path(nodes, active.add(id)));
        proof_assert!(nodes.contains(&id) && !value && nodes[id]@.active && !has_child_in(nodes, active, id)
            ==> {
                lemma_path_end(nodes, active, path, id);
                let new = path.subsequence(1, path.len());
                proof_assert!(forall<i> 0 <= i && i < new.len() ==> new[i] == path[i + 1]);
                proof_assert!(path_of(nodes, active.remove(id), new));
                true
            } && forms_path(nodes, active.remove(id)))
    }
}

/// `new` is `old` with its active nodes recomputed, consistently and forming a path.
#[logic(open)]
pub fn reactivated<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        same_graph(new.nodes, old.nodes)
            && new.roots == old.roots
            && new.bookmarked == old.bookmarked
            && structure(new.nodes, new.roots, new.active)
            && forms_path(new.nodes, new.active)
    }
}

/// Recomputing the active nodes of a weave which is valid apart from them makes it valid.
#[logic]
#[requires(old.valid_structure())]
#[ensures(forall<new: IndependentWeaveView<K, T, S>> #[trigger(new.valid())]
    reactivated(old, new) ==> new.valid())]
pub fn lemma_reactivated<K, T, S>(old: IndependentWeaveView<K, T, S>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// Changing only which nodes are active preserves the structure, if the flags stay consistent.
#[logic]
#[requires(structure(old, roots, old_active))]
#[requires(same_graph(new, old))]
#[requires(forall<k> active.contains(&k) ==> new.contains(&k))]
#[requires(forall<k> new.contains(&k) ==> new[k]@.active == active.contains(&k))]
#[ensures(structure(new, roots, active))]
pub fn lemma_same_graph_structure<K, T, S>(
    new: Nodes<K, T, S>,
    old: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    old_active: FSet<Id<K>>,
    active: FSet<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> new.contains(&k) ==> linked(old, k) && linked(new, k));
        proof_assert!(ranked(new, rank_of(old)))
    }
}
