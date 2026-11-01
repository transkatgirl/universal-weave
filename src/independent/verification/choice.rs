//! Which of several equally good active paths the weave chooses.
//!
//! When it recomputes the active path, `IndependentWeave` breaks ties by the order of the roots
//! and of each node's parents and children, which are `IndexSet`s, so the path it chooses is
//! determined by the weave. It doesn't depend on the order in which the active `HashSet` is
//! iterated: `fix_orphaned_activations` only uses that order to collect the ancestors of the
//! active nodes, which it then lists from the roots, in order.
//!
//! [`ancestry`] and [`reactivated_within`] are only open in this module, so that the provers don't
//! unfold them in the methods' proofs: the facts below give what those need.

use core::hash::{BuildHasher, Hash};

use creusot_std::logic::{FSet, Mapping, such_that};

#[allow(unused_imports)]
use super::*;
use crate::{
    IndependentContents,
    verification::{Id, no_dup, set_bound},
};

/// `set` contains the nodes of `within`, and the parents of each of its nodes.
#[logic(open)]
pub fn covers<K, T, S>(nodes: Nodes<K, T, S>, within: FSet<Id<K>>, set: FSet<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (forall<a> within.contains(&a) && nodes.contains(&a) ==> set.contains(&a))
            && forall<x, p> set.contains(&x) && nodes.contains(&x) && nodes[x]@.from.contains(p)
                ==> set.contains(&p)
    }
}

/// The nodes of `within` and their ancestors: the nodes in every set which [`covers`] `within`.
#[logic(open(self))]
pub fn ancestry<K, T, S>(nodes: Nodes<K, T, S>, within: FSet<Id<K>>) -> Mapping<Id<K>, bool>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        |k| nodes.contains(&k) && forall<set: FSet<Id<K>>> covers(nodes, within, set) ==> set.contains(&k)
    }
}

/// The nodes of `nodes` in `within`.
#[logic(open)]
pub fn member_within<K, T, S>(nodes: Nodes<K, T, S>, within: FSet<Id<K>>) -> Mapping<Id<K>, bool>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { |k| nodes.contains(&k) && within.contains(&k) }
}

/// The elements of `within` which are nodes of `nodes`.
#[logic(open)]
pub fn present<K, T, S>(nodes: Nodes<K, T, S>, within: FSet<Id<K>>) -> FSet<Id<K>>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { within.filter(|k: Id<K>| nodes.contains(&k)) }
}

/// `after` is the active path recomputed through the nodes of `within`, as
/// `fix_orphaned_activations` recomputes it: those nodes, if they form a path to a root, and
/// otherwise the nodes of the path which [`first_longest`] describes, through the nodes of
/// `within`, when their ancestors are listed in the order of Kahn's algorithm from each of `roots`
/// in turn ([`roots_kahn`]).
///
/// It determines `after` ([`lemma_reactivated_within_unique`]).
#[logic(open(self))]
pub fn reactivated_within<K, T, S>(
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    within: FSet<Id<K>>,
    after: FSet<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        if forms_path(nodes, present(nodes, within)) {
            after == present(nodes, within)
        } else {
            no_dup(roots_kahn(nodes, ancestry(nodes, within), roots))
                && exists<path> path_of(nodes, after, path)
                    && first_longest(
                        nodes,
                        roots_kahn(nodes, ancestry(nodes, within), roots),
                        member_within(nodes, within),
                        path,
                    )
        }
    }
}

/// [`reactivated_within`] determines the active path.
#[logic]
#[requires(reactivated_within(nodes, roots, within, a))]
#[requires(reactivated_within(nodes, roots, within, b))]
#[ensures(a == b)]
pub fn lemma_reactivated_within_unique<K, T, S>(
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    within: FSet<Id<K>>,
    a: FSet<Id<K>>,
    b: FSet<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(!forms_path(nodes, present(nodes, within)) ==> {
            let order = roots_kahn(nodes, ancestry(nodes, within), roots);
            let candidate = member_within(nodes, within);
            let p = such_that(|p| path_of(nodes, a, p) && first_longest(nodes, order, candidate, p));
            let q = such_that(|q| path_of(nodes, b, q) && first_longest(nodes, order, candidate, q));
            lemma_first_longest_unique(nodes, order, candidate, p, q);
            proof_assert!(forall<k> a.contains(&k) == b.contains(&k));
            true
        } && a.ext_eq(b));
    }
}

/// In a set of nodes whose elements other than `id` all have a child in it, every element is in
/// every set which contains `id` and the parents of its nodes.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(forall<x> part.contains(&x) ==> nodes.contains(&x))]
#[requires(forall<x> part.contains(&x) && x != id ==> has_child_in(nodes, part, x))]
#[requires(part.contains(&k))]
#[requires(set.contains(&id))]
#[requires(forall<x, p> set.contains(&x) && nodes.contains(&x) && nodes[x]@.from.contains(p)
    ==> set.contains(&p))]
#[requires(forall<x> part.contains(&x) ==> rank[x] < bound)]
#[ensures(set.contains(&k))]
#[variant(bound - rank[k])]
pub fn lemma_part_ancestor<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    part: FSet<Id<K>>,
    id: Id<K>,
    k: Id<K>,
    set: FSet<Id<K>>,
    bound: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if k != id {
        pearlite! {
            let c = such_that(|c| nodes[k]@.to.contains(c) && part.contains(&c));
            proof_assert!(linked(nodes, k) && nodes.contains(&c) && nodes[c]@.from.contains(k));
            proof_assert!(rank[k] < rank[c]);
            lemma_part_ancestor(nodes, rank, part, id, c, set, bound)
        }
    }
}

/// [`lemma_part_ancestor`] as a fact, which applies wherever it is asserted.
#[logic]
#[ensures(result == (all_linked(nodes)
    && ranked(nodes, rank)
    && (forall<x> part.contains(&x) ==> nodes.contains(&x))
    && (forall<x> part.contains(&x) && x != id ==> has_child_in(nodes, part, x))
    && part.contains(&k)
    && set.contains(&id)
    && (forall<x, p> set.contains(&x) && nodes.contains(&x) && nodes[x]@.from.contains(p)
        ==> set.contains(&p))
    && forall<x> part.contains(&x) ==> rank[x] < bound))]
#[ensures(result ==> set.contains(&k))]
pub fn part_ancestor_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    part: FSet<Id<K>>,
    id: Id<K>,
    k: Id<K>,
    set: FSet<Id<K>>,
    bound: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = all_linked(nodes)
            && ranked(nodes, rank)
            && (forall<x> part.contains(&x) ==> nodes.contains(&x))
            && (forall<x> part.contains(&x) && x != id ==> has_child_in(nodes, part, x))
            && part.contains(&k)
            && set.contains(&id)
            && (forall<x, p> set.contains(&x) && nodes.contains(&x) && nodes[x]@.from.contains(p)
                ==> set.contains(&p))
            && forall<x> part.contains(&x) ==> rank[x] < bound;
        proof_assert!(pre ==> {
            lemma_part_ancestor(nodes, rank, part, id, k, set, bound);
            true
        } && set.contains(&k));
        pre
    }
}

/// Searching for the ancestors of a node `id` of `within`, from those found in `old`, finds only
/// nodes of [`ancestry`]: the ones it adds are ancestors of `id`.
#[logic]
#[ensures(result == (all_linked(nodes)
    && ranked(nodes, rank)
    && (forall<k> new.contains(&k) ==> nodes.contains(&k))
    && (forall<k> new.contains(&k) && !old.contains(&k) && k != id
        ==> has_child_in(nodes, new.difference(old), k))
    && within.contains(&id)
    && nodes.contains(&id)
    && forall<k> old.contains(&k) ==> ancestry(nodes, within)[k]))]
#[ensures(result ==> forall<k> new.contains(&k) ==> ancestry(nodes, within)[k])]
pub fn ancestry_step_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    old: FSet<Id<K>>,
    new: FSet<Id<K>>,
    within: FSet<Id<K>>,
    id: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = all_linked(nodes)
            && ranked(nodes, rank)
            && (forall<k> new.contains(&k) ==> nodes.contains(&k))
            && (forall<k> new.contains(&k) && !old.contains(&k) && k != id
                ==> has_child_in(nodes, new.difference(old), k))
            && within.contains(&id)
            && nodes.contains(&id)
            && forall<k> old.contains(&k) ==> ancestry(nodes, within)[k];
        let part = new.difference(old);
        proof_assert!(pre ==> {
            proof_assert!(forall<x> part.contains(&x) == (new.contains(&x) && !old.contains(&x)));
            proof_assert!(forall<x> part.contains(&x) && x != id ==> has_child_in(nodes, part, x));
            proof_assert!(forall<x> part.contains(&x) ==> rank[x] < set_bound(part, rank));
            proof_assert!(forall<k, set: FSet<Id<K>>> part.contains(&k) && covers(nodes, within, set)
                ==> set.contains(&id)
                    && part_ancestor_ok(nodes, rank, part, id, k, set, set_bound(part, rank)));
            true
        } && forall<k> new.contains(&k) ==> ancestry(nodes, within)[k]);
        pre
    }
}

/// A set which [`covers`] `within` contains all of its [`ancestry`].
#[logic]
#[ensures(result == covers(nodes, within, set))]
#[ensures(result ==> forall<k> ancestry(nodes, within)[k] ==> set.contains(&k))]
pub fn ancestry_closed_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    within: FSet<Id<K>>,
    set: FSet<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    covers(nodes, within, set)
}

/// The active path is unchanged when it is already a path to a root through the nodes of
/// `within`.
#[logic]
#[ensures(result == (forms_path(nodes, after)
    && (forall<k> after.contains(&k) ==> nodes.contains(&k) && within.contains(&k))
    && forall<k> nodes.contains(&k) && within.contains(&k) ==> after.contains(&k)))]
#[ensures(result ==> reactivated_within(nodes, roots, within, after))]
pub fn reactivated_whole_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    within: FSet<Id<K>>,
    after: FSet<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = forms_path(nodes, after)
            && (forall<k> after.contains(&k) ==> nodes.contains(&k) && within.contains(&k))
            && forall<k> nodes.contains(&k) && within.contains(&k) ==> after.contains(&k);
        proof_assert!(pre ==> present(nodes, within).ext_eq(after));
        pre
    }
}

/// Otherwise, it is the path which `longest_candidate_path_to_root` finds.
#[logic]
#[ensures(result == (!forms_path(nodes, present(nodes, within))
    && order == roots_kahn(nodes, ancestry(nodes, within), roots)
    && candidate == member_within(nodes, within)
    && no_dup(order)
    && path_of(nodes, after, path)
    && first_longest(nodes, order, candidate, path)))]
#[ensures(result ==> reactivated_within(nodes, roots, within, after))]
#[allow(
    clippy::too_many_arguments,
    reason = "The path and what it is found from"
)]
pub fn reactivated_route_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    within: FSet<Id<K>>,
    after: FSet<Id<K>>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    path: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        !forms_path(nodes, present(nodes, within))
            && order == roots_kahn(nodes, ancestry(nodes, within), roots)
            && candidate == member_within(nodes, within)
            && no_dup(order)
            && path_of(nodes, after, path)
            && first_longest(nodes, order, candidate, path)
    }
}

/// [`reactivated_within`] only depends on the nodes of `within`.
#[logic]
#[ensures(result == (reactivated_within(nodes, roots, within, after)
    && forall<k> nodes.contains(&k) ==> within.contains(&k) == wider.contains(&k)))]
#[ensures(result ==> reactivated_within(nodes, roots, wider, after))]
pub fn reactivated_widen_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    within: FSet<Id<K>>,
    wider: FSet<Id<K>>,
    after: FSet<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = reactivated_within(nodes, roots, within, after)
            && forall<k> nodes.contains(&k) ==> within.contains(&k) == wider.contains(&k);
        proof_assert!(pre ==> {
            proof_assert!(present(nodes, within).ext_eq(present(nodes, wider)));
            proof_assert!(member_within(nodes, within).ext_eq(member_within(nodes, wider)));
            proof_assert!(forall<set> covers(nodes, within, set) == covers(nodes, wider, set));
            proof_assert!(ancestry(nodes, within).ext_eq(ancestry(nodes, wider)));
            true
        } && reactivated_within(nodes, roots, wider, after));
        pre
    }
}

/// [`reactivated_within`] is the same in graphs which differ only by which nodes are active.
#[logic]
#[ensures(result == (same_graph(new, old) && all_linked(old)))]
#[ensures(result ==> forall<roots, within, after> reactivated_within(new, roots, within, after)
    == reactivated_within(old, roots, within, after))]
pub fn same_graph_reactivated_ok<K, T, S>(new: Nodes<K, T, S>, old: Nodes<K, T, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = same_graph(new, old) && all_linked(old);
        proof_assert!(pre ==> {
            lemma_same_graph_paths(new, old);
            proof_assert!(forall<within> present(new, within).ext_eq(present(old, within)));
            proof_assert!(forall<within> member_within(new, within).ext_eq(member_within(old, within)));
            proof_assert!(forall<within, set> covers(new, within, set) == covers(old, within, set));
            proof_assert!(forall<within> ancestry(new, within).ext_eq(ancestry(old, within)));
            proof_assert!(forall<within> forms_path(new, present(old, within))
                == forms_path(old, present(old, within)));
            proof_assert!(forall<within, roots> {
                lemma_roots_kahn_same_graph(new, old, ancestry(old, within), roots);
                true
            } && roots_kahn(new, ancestry(old, within), roots)
                == roots_kahn(old, ancestry(old, within), roots));
            proof_assert!(forall<order, candidate, q> candidate_root_path(new, order, candidate, q)
                == candidate_root_path(old, order, candidate, q));
            proof_assert!(forall<order, candidate, k, p, len> old.contains(&k)
                ==> first_longest_parent(new, order, candidate, k, p, len)
                    == first_longest_parent(old, order, candidate, k, p, len));
            proof_assert!(forall<order, candidate, path> first_longest(new, order, candidate, path)
                == first_longest(old, order, candidate, path));
            true
        } && forall<roots, within, after> reactivated_within(new, roots, within, after)
            == reactivated_within(old, roots, within, after));
        pre
    }
}

/// What `fix_orphaned_activations` establishes gives [`reactivated_within`]: it lists the nodes of
/// `anc`, which are the [`ancestry`] of `within`, in the order `order` of [`roots_kahn`], and
/// `longest_candidate_path_to_root` finds `path` through the nodes of `within` in that order.
#[logic]
#[ensures(result == ((forall<k> within.contains(&k) ==> nodes.contains(&k))
    && covers(nodes, within, anc)
    && (forall<k> anc.contains(&k) ==> ancestry(nodes, within)[k])
    && (forall<k> admitted[k] == anc.contains(&k))
    && order == roots_kahn(nodes, admitted, roots)
    && (forall<k> candidate[k] == within.contains(&k))
    && no_dup(order)
    && candidates_ordered(nodes, order, candidate)
    && (forall<k> within.contains(&k) ==> order.contains(k))
    && (forall<chain> candidate_chain(nodes, order, candidate, chain) ==> path == chain)
    && first_longest(nodes, order, candidate, path)
    && path_of(nodes, after, path)))]
#[ensures(result ==> reactivated_within(nodes, roots, within, after))]
#[allow(clippy::too_many_arguments, reason = "What the search establishes")]
pub fn fix_reactivated_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    within: FSet<Id<K>>,
    after: FSet<Id<K>>,
    anc: FSet<Id<K>>,
    admitted: Mapping<Id<K>, bool>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    path: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (forall<k> within.contains(&k) ==> nodes.contains(&k))
            && covers(nodes, within, anc)
            && (forall<k> anc.contains(&k) ==> ancestry(nodes, within)[k])
            && (forall<k> admitted[k] == anc.contains(&k))
            && order == roots_kahn(nodes, admitted, roots)
            && (forall<k> candidate[k] == within.contains(&k))
            && no_dup(order)
            && candidates_ordered(nodes, order, candidate)
            && (forall<k> within.contains(&k) ==> order.contains(k))
            && (forall<chain> candidate_chain(nodes, order, candidate, chain) ==> path == chain)
            && first_longest(nodes, order, candidate, path)
            && path_of(nodes, after, path);
        proof_assert!(pre ==> {
            proof_assert!(admitted.ext_eq(ancestry(nodes, within)));
            proof_assert!(candidate.ext_eq(member_within(nodes, within)));
            proof_assert!(present(nodes, within).ext_eq(within));
            proof_assert!(forms_path(nodes, within) ==> {
                let whole = such_that(|w| path_of(nodes, within, w));
                path_of(nodes, within, whole)
                    && (forall<j> 0 <= j && j < whole.len()
                        ==> candidate[whole[j]] && order.contains(whole[j]))
                    && (forall<j> 0 <= j && j < order.len() && candidate[order[j]]
                        ==> whole.contains(order[j]))
                    && candidate_chain(nodes, order, candidate, whole)
                    && path == whole
                    && after.ext_eq(within)
            });
            true
        } && reactivated_within(nodes, roots, within, after));
        pre
    }
}

/// As part of an active path which should keep as many nodes of `set` as possible, and add as few
/// other nodes as possible, `q` is worse than `p`: it has fewer nodes in `set`, or as many and more
/// nodes in total.
#[logic(open)]
pub fn worse<I>(set: FSet<I>, q: Seq<I>, p: Seq<I>) -> bool {
    pearlite! {
        count_in(set, q) < count_in(set, p)
            || (count_in(set, q) == count_in(set, p) && p.len() < q.len())
    }
}

/// `p` is the first parent of `k` from which one of the best paths up to a root starts: no path up
/// from a parent of `k` is better than it, and every one from an earlier parent is worse.
#[logic(open)]
pub fn first_best_parent<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    k: Id<K>,
    p: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        exists<b, best> 0 <= b && b < nodes[k]@.from.len() && nodes[k]@.from[b] == p
            && best_path_up(nodes, set, p, best)
            && forall<j, r> 0 <= j && j < nodes[k]@.from.len() && path_up(nodes, nodes[k]@.from[j], r)
                ==> no_better(set, r, best) && (j < b ==> worse(set, r, best))
    }
}

/// `c` is the first child of `k` from which one of the best paths down to a leaf starts: no path
/// down from a child of `k` is better than it, and every one from an earlier child is worse.
#[logic(open)]
pub fn first_best_child<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    k: Id<K>,
    c: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        exists<b, best> 0 <= b && b < nodes[k]@.to.len() && nodes[k]@.to[b] == c
            && best_path_down(nodes, set, c, best)
            && forall<j, r> 0 <= j && j < nodes[k]@.to.len() && path_down(nodes, nodes[k]@.to[j], r)
                ==> no_better(set, r, best) && (j < b ==> worse(set, r, best))
    }
}

/// Each node of `path` but the last is followed by [its first best parent](first_best_parent).
#[logic(open)]
pub fn first_best_up<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>, path: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<i> 0 <= i && i + 1 < path.len() ==> first_best_parent(nodes, set, path[i], path[i + 1])
    }
}

/// Each node of `path` but the last is followed by [its first best child](first_best_child).
#[logic(open)]
pub fn first_best_down<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>, path: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<i> 0 <= i && i + 1 < path.len() ==> first_best_child(nodes, set, path[i], path[i + 1])
    }
}

/// [`rerouted`], choosing among equally good paths as the weave does: the path up from `id`, and
/// the part kept below it, continue from each node to its first parent or child from which one of
/// the best paths starts. Each of those steps is determined ([`lemma_first_best_parent_unique`] and
/// [`lemma_first_best_child_unique`]), and so is the path up ([`lemma_first_best_up_agree`]).
#[logic(open)]
pub fn rerouted_first<K, T, S>(
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
            && first_best_up(nodes, before, up)
            && kept_below(nodes, before, id, below)
            && first_best_down(nodes, before, below)
            && forall<k> after.contains(&k) == (up.contains(k) || below.contains(k))
    }
}

/// [`activated`], choosing among equally good paths as the weave does ([`rerouted_first`]).
#[logic(open)]
pub fn activated_first<K, T, S>(
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
            rerouted_first(nodes, before, id, after)
        }
    }
}

/// [`activated_at_end`], choosing among equally good paths up as the weave does
/// ([`first_best_up`]).
#[logic(open)]
pub fn activated_at_end_first<K, T, S>(
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
                && first_best_up(nodes, before, up)
                && forall<k> after.contains(&k) == up.contains(k)
        }
    }
}

/// The first best parents and children are the same in graphs which differ only by which nodes
/// are active.
#[logic]
#[ensures(result == same_graph(new, old))]
#[ensures(result ==> forall<before, id, after> rerouted_first(new, before, id, after)
    == rerouted_first(old, before, id, after))]
#[ensures(result ==> forall<before, id, after> activated_first(new, before, id, after)
    == activated_first(old, before, id, after))]
#[ensures(result ==> forall<before, id, after> activated_at_end_first(new, before, id, after)
    == activated_at_end_first(old, before, id, after))]
pub fn same_graph_first_ok<K, T, S>(new: Nodes<K, T, S>, old: Nodes<K, T, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(same_graph(new, old) ==> {
            lemma_same_graph_paths(new, old);
            proof_assert!(forall<set, k, p> old.contains(&k)
                ==> first_best_parent(new, set, k, p) == first_best_parent(old, set, k, p));
            proof_assert!(forall<set, k, c> old.contains(&k)
                ==> first_best_child(new, set, k, c) == first_best_child(old, set, k, c));
            proof_assert!(forall<set, id, up> path_up(old, id, up)
                ==> first_best_up(new, set, up) == first_best_up(old, set, up));
            proof_assert!(forall<set, id, below> kept_below(old, set, id, below)
                ==> (forall<i> 0 <= i && i + 1 < below.len() ==> old.contains(&below[i]))
                    && first_best_down(new, set, below) == first_best_down(old, set, below));
            proof_assert!(forall<before, id, after> rerouted_first(new, before, id, after)
                == rerouted_first(old, before, id, after));
            true
        } && (forall<before, id, after> rerouted_first(new, before, id, after)
                == rerouted_first(old, before, id, after))
            && (forall<before, id, after> activated_first(new, before, id, after)
                == activated_first(old, before, id, after))
            && forall<before, id, after> activated_at_end_first(new, before, id, after)
                == activated_at_end_first(old, before, id, after));
        same_graph(new, old)
    }
}

/// A node has at most one [first best parent](first_best_parent).
#[logic]
#[requires(first_best_parent(nodes, set, k, p))]
#[requires(first_best_parent(nodes, set, k, q))]
#[ensures(p == q)]
pub fn lemma_first_best_parent_unique<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    k: Id<K>,
    p: Id<K>,
    q: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let from = nodes[k]@.from;
        let wp = such_that(|w: (Int, Seq<Id<K>>)| 0 <= w.0 && w.0 < from.len() && from[w.0] == p
            && best_path_up(nodes, set, p, w.1)
            && forall<j, r> 0 <= j && j < from.len() && path_up(nodes, from[j], r)
                ==> no_better(set, r, w.1) && (j < w.0 ==> worse(set, r, w.1)));
        let wq = such_that(|w: (Int, Seq<Id<K>>)| 0 <= w.0 && w.0 < from.len() && from[w.0] == q
            && best_path_up(nodes, set, q, w.1)
            && forall<j, r> 0 <= j && j < from.len() && path_up(nodes, from[j], r)
                ==> no_better(set, r, w.1) && (j < w.0 ==> worse(set, r, w.1)));
        proof_assert!(!(wp.0 < wq.0));
        proof_assert!(!(wq.0 < wp.0));
    }
}

/// A node has at most one [first best child](first_best_child).
#[logic]
#[requires(first_best_child(nodes, set, k, c))]
#[requires(first_best_child(nodes, set, k, d))]
#[ensures(c == d)]
pub fn lemma_first_best_child_unique<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    k: Id<K>,
    c: Id<K>,
    d: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let to = nodes[k]@.to;
        let wc = such_that(|w: (Int, Seq<Id<K>>)| 0 <= w.0 && w.0 < to.len() && to[w.0] == c
            && best_path_down(nodes, set, c, w.1)
            && forall<j, r> 0 <= j && j < to.len() && path_down(nodes, to[j], r)
                ==> no_better(set, r, w.1) && (j < w.0 ==> worse(set, r, w.1)));
        let wd = such_that(|w: (Int, Seq<Id<K>>)| 0 <= w.0 && w.0 < to.len() && to[w.0] == d
            && best_path_down(nodes, set, d, w.1)
            && forall<j, r> 0 <= j && j < to.len() && path_down(nodes, to[j], r)
                ==> no_better(set, r, w.1) && (j < w.0 ==> worse(set, r, w.1)));
        proof_assert!(!(wc.0 < wd.0));
        proof_assert!(!(wd.0 < wc.0));
    }
}

/// Two paths up from the same node, each to its [first best parent](first_best_parent) at each
/// step, which agree up to their `i`-th node, agree on the rest.
#[logic]
#[requires(path_up(nodes, id, u) && first_best_up(nodes, set, u))]
#[requires(path_up(nodes, id, v) && first_best_up(nodes, set, v))]
#[requires(0 <= i && i < u.len() && i < v.len())]
#[requires(forall<j> 0 <= j && j <= i ==> u[j] == v[j])]
#[ensures(u == v)]
#[variant(u.len() - i)]
pub fn lemma_first_best_up_agree<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    id: Id<K>,
    u: Seq<Id<K>>,
    v: Seq<Id<K>>,
    i: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        if i + 1 < u.len() && i + 1 < v.len() {
            lemma_first_best_parent_unique(nodes, set, u[i], u[i + 1], v[i + 1]);
            lemma_first_best_up_agree(nodes, set, id, u, v, i + 1)
        } else {
            proof_assert!(i + 1 == u.len() ==> nodes[u[i]]@.from.len() == 0);
            proof_assert!(i + 1 == v.len() ==> nodes[v[i]]@.from.len() == 0);
            proof_assert!(i + 1 < u.len() ==> nodes[u[i]]@.from.contains(u[i + 1]));
            proof_assert!(i + 1 < v.len() ==> nodes[v[i]]@.from.contains(v[i + 1]));
            proof_assert!(u.len() == v.len());
            proof_assert!(u.ext_eq(v))
        }
    }
}

/// [`activated_at_end_first`] determines the active path.
#[logic]
#[requires(activated_at_end_first(nodes, before, id, a))]
#[requires(activated_at_end_first(nodes, before, id, b))]
#[ensures(a == b)]
pub fn lemma_activated_at_end_first_unique<K, T, S>(
    nodes: Nodes<K, T, S>,
    before: FSet<Id<K>>,
    id: Id<K>,
    a: FSet<Id<K>>,
    b: FSet<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(!(before.contains(&id) && !has_child_in(nodes, before, id))
            && !(!before.contains(&id) && extends_path(nodes, before, id)) ==> {
                let u = such_that(|u| best_path_up(nodes, before, id, u) && first_best_up(nodes, before, u)
                    && forall<k> a.contains(&k) == u.contains(k));
                let v = such_that(|v| best_path_up(nodes, before, id, v) && first_best_up(nodes, before, v)
                    && forall<k> b.contains(&k) == v.contains(k));
                lemma_first_best_up_agree(nodes, before, id, u, v, 0);
                true
            } && a.ext_eq(b));
    }
}
