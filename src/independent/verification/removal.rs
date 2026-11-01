//! Lemmas about removing a node and its orphaned descendants.
//!
//! `remove` deletes the node `t`, then every node all of whose parents have been deleted, in the
//! order of a Kahn-style traversal: `remaining_parents` counts, for each child of a deleted node,
//! its parents which haven't been deleted yet, and a node is pushed onto the stack when its count
//! reaches zero. The deleted nodes are listed in `done`, starting with `t`.

use core::hash::{BuildHasher, Hash};

use creusot_std::logic::{FMap, FSet, Mapping, such_that};

#[allow(unused_imports)]
use super::*;
use crate::{
    IndependentContents,
    verification::{
        Id, in_order, lemma_in_order_refl, lemma_in_order_removed, lemma_in_order_trans,
        lemma_no_dup_snoc, lemma_snoc_contains, no_dup, set_bound,
    },
};

/// `nodes` are linked to each other: [`all_linked`], hidden from the callers of the lemmas below.
#[logic(open(self))]
pub fn cascade_linked<K, T, S>(nodes: Nodes<K, T, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    all_linked(nodes)
}

/// `rank` ranks `nodes`: [`ranked`], hidden from the callers of the lemmas below.
#[logic(open(self))]
pub fn cascade_ranked<K, T, S>(nodes: Nodes<K, T, S>, rank: Mapping<Id<K>, Int>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    ranked(nodes, rank)
}

/// `old` is valid, hidden from the callers of the lemmas below.
#[logic(open(self))]
pub fn cascade_valid<K, T, S>(old: IndependentWeaveView<K, T, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    old.valid()
}

/// The number of parents of `k` which aren't in `done`.
#[logic(open(self))]
pub fn left_parents<K, T, S>(nodes: Nodes<K, T, S>, done: Seq<Id<K>>, k: Id<K>) -> Int
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pending_parents(nodes, admit_all(), done, k)
}

/// `k` may be deleted along with `t`: it is a node and, unless it is `t`, it ranks above `t`, has
/// parents, and all of them are in `done`.
#[logic(open(self))]
pub fn cascade_removable<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    done: Seq<Id<K>>,
    k: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        nodes.contains(&k)
            && (k != t ==> rank[t] < rank[k]
                && 0 < nodes[k]@.from.len()
                && forall<p> nodes[k]@.from.contains(p) ==> done.contains(p))
    }
}

/// The state of the removal between two deleted nodes: `done` lists the deleted nodes, `stack`
/// the nodes about to be deleted, and `counts` the number of parents not yet deleted of every
/// child of a deleted node.
#[logic(open(self))]
pub fn cascade_outer<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    done: Seq<Id<K>>,
    stack: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        no_dup(done)
            && no_dup(stack)
            && 0 < done.len()
            && done[0] == t
            && (forall<k> done.contains(k) ==> cascade_removable(nodes, rank, t, done, k))
            && (forall<k> stack.contains(k) ==> !done.contains(k) && cascade_removable(nodes, rank, t, done, k))
            && (forall<k> counts.contains(&k) ==> nodes.contains(&k)
                && counts[k]@ == left_parents(nodes, done, k))
            && (forall<k> counts.contains(&k) && counts[k]@ == 0 ==> done.contains(k) || stack.contains(k))
            && forall<k, p> nodes.contains(&k) && nodes[k]@.from.contains(p) && done.contains(p)
                ==> counts.contains(&k)
    }
}

/// The state of the removal while the children of `cur`, which has just been deleted after
/// `prev`, are visited: the counts of the children in `unvisited` don't account for `cur` yet.
#[logic(open(self))]
pub fn cascade_inner<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    prev: Seq<Id<K>>,
    cur: Id<K>,
    unvisited: Seq<Id<K>>,
    stack: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        no_dup(prev.snoc(cur))
            && no_dup(stack)
            && prev.snoc(cur)[0] == t
            && (forall<k> prev.snoc(cur).contains(k) ==> cascade_removable(nodes, rank, t, prev.snoc(cur), k))
            && (forall<k> nodes[cur]@.to.contains(k) ==> !prev.snoc(cur).contains(k))
            && (forall<k> stack.contains(k) ==> !prev.snoc(cur).contains(k)
                && cascade_removable(nodes, rank, t, prev.snoc(cur), k))
            && (forall<k> counts.contains(&k) ==> nodes.contains(&k)
                && counts[k]@ == left_parents(nodes, prev.snoc(cur), k)
                    + if unvisited.contains(k) { 1 } else { 0 })
            && (forall<k> counts.contains(&k) && counts[k]@ == 0
                ==> prev.snoc(cur).contains(k) || stack.contains(k))
            && (forall<k> stack.contains(k) ==> !unvisited.contains(k))
            && forall<k, p> nodes.contains(&k) && nodes[k]@.from.contains(p)
                && (prev.contains(p) || (p == cur && !unvisited.contains(k))) ==> counts.contains(&k)
    }
}

/// `done` is what the removal of `t` deletes: `t`, and nodes whose parents were all deleted, so
/// that every other node with parents keeps one of them.
#[logic(open(self))]
pub fn cascade_done<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    done: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        done.contains(t)
            && (forall<k> done.contains(k) ==> cascade_removable(nodes, rank, t, done, k))
            && forall<k> nodes.contains(&k) && !done.contains(k) && 0 < nodes[k]@.from.len()
                ==> exists<p> nodes[k]@.from.contains(p) && !done.contains(p)
    }
}

/// `n1` is `nodes` without `t`, whose parents no longer list it as a child.
#[logic(open)]
pub fn detached<K, T, S>(nodes: Nodes<K, T, S>, n1: Nodes<K, T, S>, t: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (forall<k> n1.contains(&k) == (nodes.contains(&k) && k != t))
            && forall<k> n1.contains(&k) ==> n1[k]@.id == nodes[k]@.id
                && n1[k]@.from == nodes[k]@.from
                && n1[k]@.active == nodes[k]@.active
                && n1[k]@.bookmarked == nodes[k]@.bookmarked
                && (forall<c> n1[k]@.to.contains(c) == (nodes[k]@.to.contains(c) && c != t))
                && (!nodes[t]@.from.contains(k) ==> n1[k]@.to == nodes[k]@.to)
    }
}

/// `new` holds the nodes of `n1` which aren't in `done`, without their parents in `done`.
#[logic(open(self))]
pub fn pruned<K, T, S>(n1: Nodes<K, T, S>, new: Nodes<K, T, S>, done: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (forall<k> new.contains(&k) == (n1.contains(&k) && !done.contains(k)))
            && forall<k> new.contains(&k) ==> new[k]@.id == n1[k]@.id
                && new[k]@.to == n1[k]@.to
                && new[k]@.active == n1[k]@.active
                && new[k]@.bookmarked == n1[k]@.bookmarked
                && forall<p> new[k]@.from.contains(p) == (n1[k]@.from.contains(p) && !done.contains(p))
    }
}

/// `nodes`, `roots` and `active` are those of `old` without the nodes in `done`.
#[logic(open(self))]
pub fn cascade_shape<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    active: FSet<Id<K>>,
    done: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (forall<k> nodes.contains(&k) == (old.nodes.contains(&k) && !done.contains(k)))
            && (forall<k> nodes.contains(&k) ==> nodes[k]@.id == old.nodes[k]@.id
                && nodes[k]@.active == old.nodes[k]@.active
                && nodes[k]@.bookmarked == old.nodes[k]@.bookmarked
                && (forall<p> nodes[k]@.from.contains(p) == (old.nodes[k]@.from.contains(p) && !done.contains(p)))
                && forall<c> nodes[k]@.to.contains(c) == (old.nodes[k]@.to.contains(c) && !done.contains(c)))
            && no_dup(roots)
            && (forall<k> roots.contains(k) == (old.roots.contains(k) && !done.contains(k)))
            && forall<k> active.contains(&k) == (old.active.contains(&k) && !done.contains(k))
    }
}

/// `done` lists what removing `t` from `nodes` deletes: `t`, and the nodes whose parents are all
/// deleted. Every other deleted node has parents, all of them deleted, and every kept node which
/// has parents keeps one of them. In an acyclic graph, this determines the elements of `done`
/// ([`lemma_removal_unique`]).
#[logic(open)]
pub fn removal<K, T, S>(nodes: Nodes<K, T, S>, t: Id<K>, done: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        done.contains(t)
            && (forall<k> done.contains(k) ==> nodes.contains(&k)
                && (k != t ==> 0 < nodes[k]@.from.len()
                    && forall<p> nodes[k]@.from.contains(p) ==> done.contains(p)))
            && forall<k> nodes.contains(&k) && !done.contains(k) && 0 < nodes[k]@.from.len()
                ==> exists<p> nodes[k]@.from.contains(p) && !done.contains(p)
    }
}

/// After its first element, `done` lists each node after all of its parents.
///
/// This is only open in this module, like [`removal_order`].
#[logic(open(self))]
pub fn parents_first<K, T, S>(nodes: Nodes<K, T, S>, done: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<i, p> 1 <= i && i < done.len() && nodes[done[i]]@.from.contains(p)
            ==> done.subsequence(0, i).contains(p)
    }
}

/// Appending a node whose parents are all listed keeps [`parents_first`].
#[logic]
#[requires(parents_first(nodes, done))]
#[requires(0 < done.len())]
#[requires(forall<p> nodes[x]@.from.contains(p) ==> done.contains(p))]
#[ensures(parents_first(nodes, done.snoc(x)))]
pub fn lemma_parents_first_snoc<K, T, S>(nodes: Nodes<K, T, S>, done: Seq<Id<K>>, x: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<i> 0 <= i && i <= done.len()
            ==> done.snoc(x).subsequence(0, i).ext_eq(done.subsequence(0, i)));
        proof_assert!(done.subsequence(0, done.len()).ext_eq(done));
        proof_assert!(forall<i> 1 <= i && i < done.len() ==> done.snoc(x)[i] == done[i]);
        proof_assert!(done.snoc(x)[done.len()] == x)
    }
}

/// `done` lists `t` first, and then each node after all of its parents ([`parents_first`]).
///
/// This is only open in this module, so that the existential postcondition of
/// `remove_tracked`, which states it, stays easy to instantiate.
#[logic(open(self))]
pub fn removal_order<K, T, S>(nodes: Nodes<K, T, S>, t: Id<K>, done: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { 0 < done.len() && done[0] == t && parents_first(nodes, done) }
}

/// A single node is listed after its parents.
#[logic]
#[ensures(parents_first(nodes, Seq::singleton(t)))]
pub fn lemma_parents_first_single<K, T, S>(nodes: Nodes<K, T, S>, t: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// Deleting the node `x` at the top of the removal's stack lists it after its parents.
#[logic]
#[requires(cascade_outer(nodes, rank, t, prev, rest.snoc(x), counts))]
#[requires(parents_first(nodes, prev))]
#[ensures(parents_first(nodes, prev.snoc(x)))]
pub fn lemma_cascade_parents_first<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    prev: Seq<Id<K>>,
    rest: Seq<Id<K>>,
    x: Id<K>,
    counts: FMap<Id<K>, usize>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(rest.snoc(x).contains(x));
        proof_assert!(!prev.contains(x) && cascade_removable(nodes, rank, t, prev, x));
        proof_assert!(x != t);
        lemma_parents_first_snoc(nodes, prev, x)
    }
}

/// The removal lists `t` first, so it lists the nodes in [`removal_order`] once they are listed
/// after their parents.
#[logic]
#[requires(cascade_outer(nodes, rank, t, done, stack, counts))]
#[requires(parents_first(nodes, done))]
#[ensures(removal_order(nodes, t, done))]
pub fn lemma_cascade_removal_order<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    done: Seq<Id<K>>,
    stack: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// Gives [`removal_order`].
#[logic]
#[requires(0 < done.len() && done[0] == t && parents_first(nodes, done))]
#[ensures(removal_order(nodes, t, done))]
pub fn lemma_removal_order<K, T, S>(nodes: Nodes<K, T, S>, t: Id<K>, done: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// `passed` lists the nodes of `nodes` whose identifiers `done` lists, in the same order, as they
/// are in `nodes`: with the same links, flags and contents.
#[logic(open)]
pub fn as_removed<K, T, S>(
    nodes: Nodes<K, T, S>,
    done: Seq<Id<K>>,
    passed: Seq<IndependentNode<K, T, S>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        passed.len() == done.len()
            && forall<i> 0 <= i && i < done.len()
                ==> passed[i]@ == nodes[done[i]]@ && passed[i].contents == nodes[done[i]].contents
    }
}

/// Passing one more removed node, as it was, keeps [`as_removed`].
#[logic]
#[requires(as_removed(nodes, done, passed))]
#[requires(n@ == nodes[k]@ && n.contents == nodes[k].contents)]
#[ensures(as_removed(nodes, done.snoc(k), passed.snoc(n)))]
pub fn lemma_as_removed_snoc<K, T, S>(
    nodes: Nodes<K, T, S>,
    done: Seq<Id<K>>,
    passed: Seq<IndependentNode<K, T, S>>,
    k: Id<K>,
    n: IndependentNode<K, T, S>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<i> 0 <= i && i < done.len()
            ==> done.snoc(k)[i] == done[i] && passed.snoc(n)[i] == passed[i])
    }
}

/// `new` is `old` without the elements of `done`, in the same order.
#[logic(open)]
pub fn filtered<I>(new: Seq<I>, old: Seq<I>, done: Seq<I>) -> bool {
    pearlite! { in_order(new, old) && forall<x> new.contains(x) == (old.contains(x) && !done.contains(x)) }
}

/// Every node of `new` has the contents it has in `old`, and lists its parents and children in
/// the same order.
#[logic(open)]
pub fn kept_in_order<K, T, S>(new: Nodes<K, T, S>, old: Nodes<K, T, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<k> new.contains(&k) ==> new[k].contents == old[k].contents
            && in_order(new[k]@.from, old[k]@.from)
            && in_order(new[k]@.to, old[k]@.to)
    }
}

/// `new` is `old` once the nodes in `done` are removed: the other nodes keep their identifiers,
/// contents and bookmarks, and their parents and children lose the removed nodes and stay in
/// order, as do the roots and the bookmarks. Which nodes are active isn't described.
#[logic(open)]
pub fn removal_shape<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    done: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (forall<k> new.nodes.contains(&k) == (old.nodes.contains(&k) && !done.contains(k)))
            && (forall<k> new.nodes.contains(&k) ==> new.nodes[k]@.id == old.nodes[k]@.id
                && new.nodes[k].contents == old.nodes[k].contents
                && new.nodes[k]@.bookmarked == old.nodes[k]@.bookmarked
                && filtered(new.nodes[k]@.from, old.nodes[k]@.from, done)
                && filtered(new.nodes[k]@.to, old.nodes[k]@.to, done))
            && filtered(new.roots, old.roots, done)
            && filtered(new.bookmarked, old.bookmarked, done)
    }
}

/// Every active child of a deleted active node is deleted.
#[logic(open(self))]
pub fn active_kept<K, T, S>(nodes: Nodes<K, T, S>, active: FSet<Id<K>>, done: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<c, k> nodes.contains(&c) && active.contains(&c) && active.contains(&k) && done.contains(k)
            && nodes[c]@.from.contains(k) ==> done.contains(c)
    }
}

/// The removal starts with `t` deleted and its children about to be visited.
#[logic]
#[requires(cascade_linked(nodes))]
#[requires(nodes.contains(&t))]
#[ensures(cascade_inner(nodes, rank, t, Seq::empty(), t, nodes[t]@.to, Seq::empty(), FMap::empty()))]
pub fn lemma_cascade_start<K, T, S>(nodes: Nodes<K, T, S>, rank: Mapping<Id<K>, Int>, t: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_snoc_contains::<Id<K>>;
    pearlite! {
        let out = Seq::<Id<K>>::empty().snoc(t);
        lemma_no_dup_snoc(Seq::<Id<K>>::empty(), t);
        proof_assert!(forall<k> !Seq::<Id<K>>::empty().contains(k));
        proof_assert!(forall<k> out.contains(k) == (k == t));
        proof_assert!(out[0] == t);
        proof_assert!(linked(nodes, t));
        proof_assert!(forall<k> !FMap::<Id<K>, usize>::empty().contains(&k));
        proof_assert!(forall<k> nodes.contains(&k) && nodes[k]@.from.contains(t)
            ==> linked(nodes, k) && nodes[t]@.to.contains(k))
    }
}

/// Deleting the node `cur` at the top of the stack starts visiting its children.
#[logic]
#[requires(cascade_linked(nodes))]
#[requires(cascade_ranked(nodes, rank))]
#[requires(cascade_outer(nodes, rank, t, prev, rest.snoc(cur), counts))]
#[ensures(cascade_inner(nodes, rank, t, prev, cur, nodes[cur]@.to, rest, counts))]
#[ensures(cur != t && nodes.contains(&cur) && rank[t] < rank[cur])]
#[ensures(!prev.contains(cur) && prev.contains(t))]
#[ensures(!nodes[t]@.from.contains(cur))]
pub fn lemma_cascade_pop<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    prev: Seq<Id<K>>,
    rest: Seq<Id<K>>,
    cur: Id<K>,
    counts: FMap<Id<K>, usize>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_snoc_contains::<Id<K>>;
    let _ = lemma_pending_snoc::<K, T, S>;
    lemma_no_dup_snoc(rest, cur);
    lemma_no_dup_snoc(prev, cur);
    pearlite! {
        let out = prev.snoc(cur);
        let to = nodes[cur]@.to;
        proof_assert!(rest.snoc(cur).contains(cur));
        proof_assert!(prev.contains(t));
        proof_assert!(!prev.contains(cur) && cascade_removable(nodes, rank, t, prev, cur) && cur != t);
        proof_assert!(nodes.contains(&t) && forall<p> nodes[t]@.from.contains(p) ==> rank[p] < rank[t]);
        proof_assert!(out[0] == prev[0]);
        proof_assert!(linked(nodes, cur));
        proof_assert!(forall<k> out.contains(k) == (prev.contains(k) || k == cur));
        proof_assert!(forall<k> prev.contains(k) ==> cascade_removable(nodes, rank, t, out, k));
        proof_assert!(forall<k> out.contains(k) ==> cascade_removable(nodes, rank, t, out, k));
        proof_assert!(forall<k> to.contains(k) ==> nodes.contains(&k) && linked(nodes, k)
            && nodes[k]@.from.contains(cur) && rank[cur] < rank[k] && k != t && k != cur);
        proof_assert!(forall<k> to.contains(k) ==> !prev.contains(k));
        proof_assert!(forall<k> to.contains(k) ==> !out.contains(k));
        proof_assert!(forall<k> rest.contains(k) ==> rest.snoc(cur).contains(k) && k != cur);
        proof_assert!(forall<k> rest.contains(k) ==> !out.contains(k) && cascade_removable(nodes, rank, t, out, k));
        proof_assert!(forall<k> rest.contains(k) ==> !to.contains(k));
        proof_assert!(forall<k> nodes.contains(&k) ==> linked(nodes, k)
            && nodes[k]@.from.contains(cur) == to.contains(k));
        proof_assert!(forall<k> counts.contains(&k) ==> counts[k]@
            == left_parents(nodes, out, k) + if to.contains(k) { 1 } else { 0 });
        proof_assert!(forall<k> counts.contains(&k) && counts[k]@ == 0 ==> out.contains(k) || rest.contains(k))
    }
}

/// The count of the child `nodes[cur]@.to[j]` before it is decremented.
#[logic]
#[requires(cascade_linked(nodes))]
#[requires(0 <= j && j < nodes[cur]@.to.len())]
#[requires(cascade_inner(nodes, rank, t, prev, cur, nodes[cur]@.to.subsequence(0, j + 1), stack, counts))]
#[ensures(nodes.contains(&nodes[cur]@.to[j]))]
#[ensures(!prev.snoc(cur).contains(nodes[cur]@.to[j]))]
#[ensures(nodes[cur]@.to[j] != t)]
#[ensures(0 <= left_parents(nodes, prev.snoc(cur), nodes[cur]@.to[j]))]
#[ensures(counts.contains(&nodes[cur]@.to[j]) ==> counts[nodes[cur]@.to[j]]@
    == left_parents(nodes, prev.snoc(cur), nodes[cur]@.to[j]) + 1)]
#[ensures(!counts.contains(&nodes[cur]@.to[j]) ==> nodes[nodes[cur]@.to[j]]@.from.len()
    == left_parents(nodes, prev.snoc(cur), nodes[cur]@.to[j]) + 1)]
pub fn lemma_cascade_count<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    prev: Seq<Id<K>>,
    cur: Id<K>,
    j: Int,
    stack: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_snoc_contains::<Id<K>>;
    pearlite! {
        let to = nodes[cur]@.to;
        let child = to[j];
        let out = prev.snoc(cur);
        lemma_prefix_step(to, j);
        lemma_no_dup_snoc(prev, cur);
        proof_assert!(to.subsequence(0, j + 1).contains(child));
        proof_assert!(to.contains(child));
        proof_assert!(out.contains(cur) && out.contains(t));
        proof_assert!(!prev.contains(cur));
        proof_assert!(linked(nodes, cur));
        proof_assert!(nodes.contains(&child) && linked(nodes, child));
        proof_assert!(nodes[child]@.from.contains(cur));
        proof_assert!(admitted_parent(nodes, admit_all(), child, cur));
        lemma_pending_zero(nodes, admit_all(), out, child);
        lemma_pending_snoc(nodes, admit_all(), prev, cur, child);
        proof_assert!(!counts.contains(&child)
            ==> forall<p> admitted_parent(nodes, admit_all(), child, p) ==> !prev.contains(p));
        proof_assert!(!counts.contains(&child) ==> {
            lemma_pending_fresh(nodes, admit_all(), prev, child);
            lemma_count_full(nodes[child]@.from, admit_all());
            left_parents(nodes, prev, child) == nodes[child]@.from.len()
        })
    }
}

/// Decrementing the count of the child `nodes[cur]@.to[j]` to `n`, and pushing it onto the stack
/// if `n` is zero, visits it.
#[logic]
#[requires(cascade_linked(nodes))]
#[requires(cascade_ranked(nodes, rank))]
#[requires(0 <= j && j < nodes[cur]@.to.len())]
#[requires(cascade_inner(nodes, rank, t, prev, cur, nodes[cur]@.to.subsequence(0, j + 1), stack, counts))]
#[requires(n@ == left_parents(nodes, prev.snoc(cur), nodes[cur]@.to[j]))]
#[ensures(cascade_inner(
    nodes,
    rank,
    t,
    prev,
    cur,
    nodes[cur]@.to.subsequence(0, j),
    if n@ == 0 { stack.snoc(nodes[cur]@.to[j]) } else { stack },
    counts.add(nodes[cur]@.to[j], n),
))]
pub fn lemma_cascade_visit<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    prev: Seq<Id<K>>,
    cur: Id<K>,
    j: Int,
    stack: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
    n: usize,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_snoc_contains::<Id<K>>;
    pearlite! {
        let to = nodes[cur]@.to;
        let child = to[j];
        let out = prev.snoc(cur);
        let u = to.subsequence(0, j + 1);
        let u2 = to.subsequence(0, j);
        let counts2 = counts.add(child, n);
        let stack2 = if n@ == 0 { stack.snoc(child) } else { stack };
        lemma_prefix_step(to, j);
        proof_assert!(u.contains(child));
        proof_assert!(linked(nodes, cur));
        proof_assert!(!u2.contains(child));
        proof_assert!(to.contains(child));
        proof_assert!(nodes.contains(&child) && linked(nodes, child) && nodes[child]@.from.contains(cur));
        proof_assert!(nodes.contains(&cur) && rank[cur] < rank[child]);
        proof_assert!(out.contains(cur));
        proof_assert!(cascade_removable(nodes, rank, t, out, cur));
        proof_assert!(cur == t || rank[t] < rank[cur]);
        proof_assert!(rank[t] < rank[child]);
        proof_assert!(nodes[child]@.from.contains(nodes[child]@.from[0]) || 0 < nodes[child]@.from.len());
        proof_assert!(0 < nodes[child]@.from.len());
        proof_assert!(!stack.contains(child) && !out.contains(child));
        proof_assert!(child != t);
        lemma_pending_zero(nodes, admit_all(), out, child);
        proof_assert!(forall<p> admitted_parent(nodes, admit_all(), child, p) == nodes[child]@.from.contains(p));
        proof_assert!(n@ == 0 ==> forall<p> nodes[child]@.from.contains(p) ==> out.contains(p));
        proof_assert!(n@ == 0 ==> cascade_removable(nodes, rank, t, out, child));
        lemma_no_dup_snoc(stack, child);
        proof_assert!(no_dup(stack2));
        proof_assert!(forall<k> stack2.contains(k) == (stack.contains(k) || (n@ == 0 && k == child)));
        proof_assert!(forall<k> stack2.contains(k) ==> !out.contains(k) && cascade_removable(nodes, rank, t, out, k));
        proof_assert!(forall<k> counts2.contains(&k) == (counts.contains(&k) || k == child));
        proof_assert!(forall<k> k != child ==> counts2[k] == counts[k]);
        proof_assert!(counts2[child] == n);
        proof_assert!(forall<k> k != child ==> u2.contains(k) == u.contains(k));
        proof_assert!(forall<k> counts2.contains(&k) ==> nodes.contains(&k)
            && counts2[k]@ == left_parents(nodes, out, k)
                + if u2.contains(k) { 1 } else { 0 });
        proof_assert!(forall<k> counts2.contains(&k) && counts2[k]@ == 0 ==> out.contains(k) || stack2.contains(k));
        proof_assert!(forall<k> stack2.contains(k) ==> !u2.contains(k));
        proof_assert!(forall<k, p> nodes.contains(&k) && nodes[k]@.from.contains(p)
            && (prev.contains(p) || (p == cur && !u2.contains(k))) ==> counts2.contains(&k))
    }
}

/// Once every child of `cur` has been visited, `cur` is deleted.
#[logic]
#[requires(cascade_inner(nodes, rank, t, prev, cur, nodes[cur]@.to.subsequence(0, 0), stack, counts))]
#[ensures(cascade_outer(nodes, rank, t, prev.snoc(cur), stack, counts))]
pub fn lemma_cascade_next<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    prev: Seq<Id<K>>,
    cur: Id<K>,
    stack: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_snoc_contains::<Id<K>>;
    pearlite! {
        proof_assert!(forall<k> !nodes[cur]@.to.subsequence(0, 0).contains(k));
        proof_assert!(0 < prev.snoc(cur).len());
        proof_assert!(forall<k, p> nodes.contains(&k) && nodes[k]@.from.contains(p) && prev.snoc(cur).contains(p)
            ==> prev.contains(p) || p == cur)
    }
}

/// When the stack is empty, the removal is complete.
#[logic]
#[requires(cascade_linked(nodes))]
#[requires(cascade_outer(nodes, rank, t, done, Seq::empty(), counts))]
#[ensures(cascade_done(nodes, rank, t, done))]
#[ensures(done.contains(t))]
#[ensures(done.len() == 1 ==> forall<k> done.contains(k) == (k == t))]
#[ensures(forall<k> counts.contains(&k) && counts[k]@ == 0 ==> done.contains(k))]
#[ensures(forall<k, p> nodes.contains(&k) && nodes[k]@.from.contains(p) && done.contains(p)
    ==> counts.contains(&k))]
pub fn lemma_cascade_end<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    done: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_pending_zero::<K, T, S>;
    pearlite! {
        proof_assert!(forall<k> !Seq::<Id<K>>::empty().contains(k));
        proof_assert!(done.contains(done[0]));
        proof_assert!(done.len() == 1 ==> forall<k> done.contains(k) == (k == done[0]));
        proof_assert!(forall<k, p> admitted_parent(nodes, admit_all(), k, p) == nodes[k]@.from.contains(p));
        proof_assert!(forall<k> nodes.contains(&k) && 0 < nodes[k]@.from.len()
            ==> nodes[k]@.from.contains(nodes[k]@.from[0]));
        proof_assert!(forall<k> nodes.contains(&k) && 0 < nodes[k]@.from.len()
            && (forall<p> nodes[k]@.from.contains(p) ==> done.contains(p))
            ==> counts.contains(&k));
        proof_assert!(forall<k> nodes.contains(&k) && 0 < nodes[k]@.from.len()
            && (forall<p> nodes[k]@.from.contains(p) ==> done.contains(p))
            ==> {
                lemma_pending_zero(nodes, admit_all(), done, k);
                counts[k]@ == 0
            });
        proof_assert!(forall<k> nodes.contains(&k) && 0 < nodes[k]@.from.len()
            && (forall<p> nodes[k]@.from.contains(p) ==> done.contains(p))
            ==> done.contains(k))
    }
}

/// When `t` has no children, it is deleted alone.
#[logic]
#[requires(cascade_linked(nodes))]
#[requires(nodes.contains(&t))]
#[requires(nodes[t]@.to.len() == 0)]
#[ensures(forall<k> Seq::singleton(t).contains(k) == (k == t))]
#[ensures(cascade_done(nodes, rank, t, Seq::singleton(t)))]
#[ensures(active_kept(nodes, active, Seq::singleton(t)))]
#[ensures(forall<k> nodes.contains(&k) ==> !nodes[k]@.from.contains(t))]
pub fn lemma_cascade_leaf<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    active: FSet<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(Seq::singleton(t).len() == 1 && Seq::singleton(t)[0] == t);
        proof_assert!(forall<k> Seq::singleton(t).contains(k) == (k == t));
        proof_assert!(forall<k> !nodes[t]@.to.contains(k));
        proof_assert!(forall<k> nodes.contains(&k) ==> linked(nodes, k) && !nodes[k]@.from.contains(t));
        proof_assert!(forall<k> nodes.contains(&k) && 0 < nodes[k]@.from.len()
            ==> nodes[k]@.from.contains(nodes[k]@.from[0]) && nodes[k]@.from[0] != t)
    }
}

/// Facts about what the removal of `t` deletes.
#[logic]
#[requires(cascade_linked(nodes))]
#[requires(cascade_ranked(nodes, rank))]
#[requires(cascade_done(nodes, rank, t, done))]
#[ensures(forall<p> nodes[t]@.from.contains(p) ==> !done.contains(p))]
#[ensures(forall<k, c> nodes.contains(&k) && !done.contains(k) && nodes[k]@.to.contains(c)
    && done.contains(c) ==> c == t)]
#[ensures(forall<k> done.contains(k) && k != t ==> 0 < nodes[k]@.from.len())]
pub fn lemma_cascade_facts<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    done: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(nodes.contains(&t) && linked(nodes, t));
        proof_assert!(forall<p> nodes[t]@.from.contains(p) ==> p != t && nodes.contains(&p) && rank[p] < rank[t]);
        proof_assert!(forall<k, c> nodes.contains(&k) && nodes[k]@.to.contains(c)
            ==> linked(nodes, k) && nodes[c]@.from.contains(k))
    }
}

/// The parts of the weave once the nodes in `done` are deleted, from the intermediate states of
/// the removal.
#[logic]
#[requires(cascade_valid(old))]
#[requires(cascade_ranked(old.nodes, rank))]
#[requires(cascade_done(old.nodes, rank, t, done))]
#[requires(detached(old.nodes, n1, t))]
#[requires(pruned(n1, nodes, done))]
#[requires(no_dup(roots))]
#[requires(forall<k> roots.contains(k) == (old.roots.contains(k) && k != t))]
#[requires(forall<k> active.contains(&k) == (old.active.contains(&k) && !done.contains(k)))]
#[ensures(cascade_shape(old, nodes, roots, active, done))]
pub fn lemma_cascade_shape<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    n1: Nodes<K, T, S>,
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    active: FSet<Id<K>>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    done: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    lemma_cascade_facts(old.nodes, rank, t, done);
    pearlite! {
        proof_assert!(forall<k> old.nodes.contains(&k) ==> old.flagged(k));
        proof_assert!(forall<k> nodes.contains(&k) == (old.nodes.contains(&k) && !done.contains(k)));
        proof_assert!(forall<k> nodes.contains(&k) ==> n1.contains(&k) && old.nodes.contains(&k)
            && !done.contains(k) && k != t);
        proof_assert!(forall<k, c> nodes.contains(&k) ==> nodes[k]@.to.contains(c)
            == (old.nodes[k]@.to.contains(c) && c != t));
        proof_assert!(forall<k, c> nodes.contains(&k) ==> nodes[k]@.to.contains(c)
            == (old.nodes[k]@.to.contains(c) && !done.contains(c)));
        proof_assert!(forall<k> done.contains(k) && k != t ==> old.nodes.contains(&k) && !old.roots.contains(k));
        proof_assert!(forall<k> roots.contains(k) == (old.roots.contains(k) && !done.contains(k)))
    }
}

/// Deleting the nodes in `done` keeps the weave's structure valid.
#[logic]
#[requires(cascade_valid(old))]
#[requires(cascade_ranked(old.nodes, rank))]
#[requires(cascade_done(old.nodes, rank, t, done))]
#[requires(cascade_shape(old, nodes, roots, active, done))]
#[ensures(structure(nodes, roots, active))]
#[ensures(forall<k> nodes.contains(&k) == (old.nodes.contains(&k) && !done.contains(k)))]
#[ensures(forall<k> nodes.contains(&k) ==> nodes[k]@.bookmarked == old.bookmarked.contains(k))]
#[ensures(forall<k> old.bookmarked.contains(k) && !done.contains(k) ==> nodes.contains(&k))]
pub fn lemma_cascade_structure<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    active: FSet<Id<K>>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    done: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> old.nodes.contains(&k) ==> linked(old.nodes, k) && old.flagged(k));
        proof_assert!(forall<k> nodes.contains(&k) ==> old.nodes.contains(&k) && !done.contains(k));
        proof_assert!(forall<k> nodes.contains(&k) ==> nodes[k]@.valid());
        proof_assert!(forall<k> nodes.contains(&k) ==> nodes[k]@.id == k);
        proof_assert!(forall<k, p> nodes.contains(&k) && nodes[k]@.from.contains(p)
            ==> nodes.contains(&p) && nodes[p]@.to.contains(k));
        proof_assert!(forall<k, c> nodes.contains(&k) && nodes[k]@.to.contains(c)
            ==> nodes.contains(&c) && nodes[c]@.from.contains(k));
        proof_assert!(forall<k> nodes.contains(&k) ==> linked(nodes, k));
        proof_assert!(all_linked(nodes));
        proof_assert!(ranked(nodes, rank));
        proof_assert!(acyclic(nodes));
        proof_assert!(forall<k> nodes.contains(&k) && 0 < nodes[k]@.from.len()
            ==> nodes[k]@.from.contains(nodes[k]@.from[0]));
        proof_assert!(forall<k> nodes.contains(&k) && 0 < old.nodes[k]@.from.len()
            ==> exists<p> old.nodes[k]@.from.contains(p) && !done.contains(p));
        proof_assert!(forall<k> nodes.contains(&k)
            ==> (nodes[k]@.from.len() == 0) == (old.nodes[k]@.from.len() == 0));
        proof_assert!(forall<k> nodes.contains(&k) ==> (nodes[k]@.from.len() == 0) == roots.contains(k));
        proof_assert!(forall<k> nodes.contains(&k) ==> nodes[k]@.active == active.contains(&k))
    }
}

/// The index of the first element of `s` from `i` on which isn't in `done`, or the length of `s`.
#[logic]
#[requires(0 <= i && i <= s.len())]
#[ensures(i <= result && result <= s.len())]
#[ensures(forall<j> i <= j && j < result ==> done.contains(s[j]))]
#[ensures(result < s.len() ==> !done.contains(s[result]))]
#[variant(s.len() - i)]
pub fn first_kept<I>(s: Seq<I>, done: Seq<I>, i: Int) -> Int {
    if i == s.len() {
        i
    } else if done.contains(s[i]) {
        first_kept(s, done, i + 1)
    } else {
        i
    }
}

/// If whenever an element of `s` is in `done`, so is the previous one, then the elements of `s`
/// in `done` form a prefix.
#[logic]
#[requires(forall<i> 0 < i && i < s.len() && done.contains(s[i]) ==> done.contains(s[i - 1]))]
#[requires(0 <= a && a <= b && b < s.len())]
#[requires(done.contains(s[b]))]
#[ensures(done.contains(s[a]))]
#[variant(b - a)]
pub fn lemma_done_down<I>(s: Seq<I>, done: Seq<I>, a: Int, b: Int) {
    if a < b {
        lemma_done_down(s, done, a, b - 1)
    }
}

/// The suffix of `s` from `m` on contains exactly the elements of `s` from index `m` on.
#[logic]
#[requires(0 <= m && m <= s.len())]
#[ensures(s.subsequence(m, s.len()).len() == s.len() - m)]
#[ensures(forall<i> 0 <= i && i < s.len() - m ==> s.subsequence(m, s.len())[i] == s[m + i])]
#[ensures(forall<x> s.subsequence(m, s.len()).contains(x)
    == exists<j> m <= j && j < s.len() && s[j] == x)]
pub fn lemma_suffix_contains<T>(s: Seq<T>, m: Int) {
    pearlite! {
        let q = s.subsequence(m, s.len());
        proof_assert!(forall<i> 0 <= i && i < s.len() - m ==> q[i] == s[m + i]);
        proof_assert!(forall<j> m <= j && j < s.len() ==> q[j - m] == s[j]);
        proof_assert!(forall<x, j> m <= j && j < s.len() && s[j] == x ==> q[j - m] == x
            && 0 <= j - m && j - m < q.len());
        proof_assert!(forall<x> (exists<j> m <= j && j < s.len() && s[j] == x) ==> q.contains(x))
    }
}

/// The suffix of the old active path from its first node which isn't deleted is a path through
/// the remaining active nodes.
#[logic]
#[requires(cascade_valid(old))]
#[requires(cascade_shape(old, nodes, roots, active, done))]
#[requires(path_of(old.nodes, old.active, path))]
#[requires(0 <= m && m <= path.len())]
#[requires(forall<j> 0 <= j && j < m ==> done.contains(path[j]))]
#[requires(forall<j> m <= j && j < path.len() ==> !done.contains(path[j]))]
#[ensures(path_of(nodes, active, path.subsequence(m, path.len())))]
pub fn lemma_cascade_suffix<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    active: FSet<Id<K>>,
    done: Seq<Id<K>>,
    path: Seq<Id<K>>,
    m: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let q = path.subsequence(m, path.len());
        lemma_suffix_contains(path, m);
        proof_assert!(forall<i> 0 <= i && i < q.len() ==> q[i] == path[m + i]);
        proof_assert!(forall<i> 0 <= i && i < q.len() ==> nodes.contains(&q[i]));
        proof_assert!(forall<i> 0 <= i && i + 1 < q.len() ==> nodes[q[i]]@.from.contains(q[i + 1]));
        proof_assert!(0 < q.len() ==> old.nodes[q[q.len() - 1]]@.from.len() == 0);
        proof_assert!(0 < q.len() && 0 < nodes[q[q.len() - 1]]@.from.len()
            ==> nodes[q[q.len() - 1]]@.from.contains(nodes[q[q.len() - 1]]@.from[0]));
        proof_assert!(0 < q.len() ==> nodes[q[q.len() - 1]]@.from.len() == 0);
        proof_assert!(valid_path(nodes, q));
        proof_assert!(no_dup(q));
        proof_assert!(forall<k> q.contains(k) == (path.contains(k) && !done.contains(k)));
        proof_assert!(forall<k> q.contains(k) == active.contains(&k))
    }
}

/// Deleting the nodes in `done` keeps the active nodes a path, if every active child of a
/// deleted active node is deleted.
#[logic]
#[requires(cascade_valid(old))]
#[requires(cascade_shape(old, nodes, roots, active, done))]
#[requires(active_kept(old.nodes, old.active, done))]
#[ensures(forms_path(nodes, active))]
pub fn lemma_cascade_path<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    active: FSet<Id<K>>,
    done: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_done_down::<Id<K>>;
    pearlite! {
        let path = such_that(|path| path_of(old.nodes, old.active, path));
        proof_assert!(forall<i> 0 <= i && i < path.len() ==> old.nodes.contains(&path[i])
            && old.active.contains(&path[i]));
        proof_assert!(forall<i> 0 < i && i < path.len() && done.contains(path[i])
            ==> old.nodes[path[i - 1]]@.from.contains(path[i]) && done.contains(path[i - 1]));
        let m = first_kept(path, done, 0);
        proof_assert!(forall<j> 0 <= j && j < m ==> done.contains(path[j]));
        proof_assert!(forall<j> m <= j && j < path.len() ==> !done.contains(path[j]));
        lemma_cascade_suffix(old, nodes, roots, active, done, path, m)
    }
}

/// Every active child of a deleted active node is deleted, unless an active node was deleted and
/// an active node keeps a parent.
#[logic]
#[requires(forall<k> active.contains(&k) ==> nodes.contains(&k))]
#[requires(forall<k> counts.contains(&k) && counts[k]@ == 0 ==> done.contains(k))]
#[requires(forall<k, p> nodes.contains(&k) && nodes[k]@.from.contains(p) && done.contains(p)
    ==> counts.contains(&k))]
#[requires((forall<k> done.contains(k) ==> !active.contains(&k))
    || !exists<k> counts.contains(&k) && counts[k]@ > 0 && active.contains(&k) && !done.contains(k))]
#[ensures(active_kept(nodes, active, done))]
pub fn lemma_active_kept<K, T, S>(
    nodes: Nodes<K, T, S>,
    active: FSet<Id<K>>,
    done: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// Deleting `t`, which has no children, keeps the weave valid, with the bookmarks `bm`.
#[logic]
#[requires(cascade_valid(old))]
#[requires(cascade_ranked(old.nodes, rank))]
#[requires(old.nodes.contains(&t))]
#[requires(old.nodes[t]@.to.len() == 0)]
#[requires(detached(old.nodes, n1, t))]
#[requires(no_dup(roots))]
#[requires(forall<k> roots.contains(k) == (old.roots.contains(k) && k != t))]
#[requires(forall<k> active.contains(&k) == (old.active.contains(&k) && k != t))]
#[requires(no_dup(bm))]
#[requires(forall<k> bm.contains(k) == (old.bookmarked.contains(k) && k != t))]
#[ensures(forall<new: IndependentWeaveView<K, T, S>> #[trigger(new.valid())]
    new.nodes == n1 && new.roots == roots && new.active == active && new.bookmarked == bm
        ==> new.valid())]
pub fn lemma_cascade_leaf_valid<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    n1: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    active: FSet<Id<K>>,
    bm: Seq<Id<K>>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let done = Seq::singleton(t);
        lemma_cascade_leaf(old.nodes, rank, t, old.active);
        proof_assert!(forall<k> n1.contains(&k) ==> old.nodes.contains(&k) && !n1[k]@.from.contains(t));
        proof_assert!(pruned(n1, n1, done));
        proof_assert!(forall<k> active.contains(&k) == (old.active.contains(&k) && !done.contains(k)));
        lemma_cascade_shape(old, n1, n1, roots, active, rank, t, done);
        lemma_cascade_structure(old, n1, roots, active, rank, t, done);
        lemma_cascade_path(old, n1, roots, active, done);
        proof_assert!(structure(n1, roots, active) && forms_path(n1, active));
        proof_assert!(forall<k> bm.contains(k) ==> n1.contains(&k));
        proof_assert!(forall<k> n1.contains(&k) ==> k != t && n1[k]@.bookmarked == bm.contains(k));
        proof_assert!(IndependentWeaveView { nodes: n1, roots, active, bookmarked: bm }.valid())
    }
}

/// Removing the element at index `i` of a sequence without duplicates removes exactly that
/// element.
#[logic]
#[requires(no_dup(s))]
#[requires(0 <= i && i < s.len())]
#[ensures(no_dup(s.removed(i)))]
#[ensures(forall<x> s.removed(i).contains(x) == (s.contains(x) && x != s[i]))]
pub fn lemma_removed_contains<I>(s: Seq<I>, i: Int) {
    pearlite! {
        let r = s.removed(i);
        proof_assert!(r.len() == s.len() - 1);
        proof_assert!(forall<j> 0 <= j && j < i ==> r[j] == s[j]);
        proof_assert!(forall<j> i <= j && j < r.len() ==> r[j] == s[j + 1]);
        proof_assert!(forall<j> 0 <= j && j < i ==> r.contains(s[j]));
        proof_assert!(forall<j> i < j && j < s.len() ==> r[j - 1] == s[j] && r.contains(s[j]));
        proof_assert!(forall<x> r.contains(x) ==> s.contains(x) && x != s[i]);
        proof_assert!(forall<x> s.contains(x) && x != s[i] ==> r.contains(x))
    }
}

/// Boolean form of [`lemma_removed_contains`].
#[logic]
#[ensures(result == (no_dup(s) && 0 <= i && i < s.len()))]
#[ensures(result ==> no_dup(s.removed(i)))]
#[ensures(result ==> forall<x> s.removed(i).contains(x) == (s.contains(x) && x != s[i]))]
pub fn removed_ok<I>(s: Seq<I>, i: Int) -> bool {
    pearlite! {
        let pre = no_dup(s) && 0 <= i && i < s.len();
        proof_assert!(pre ==> { lemma_removed_contains(s, i); true }
            && no_dup(s.removed(i))
            && forall<x> s.removed(i).contains(x) == (s.contains(x) && x != s[i]));
        pre
    }
}

/// Facts about the node `t` of a valid weave.
#[logic]
#[requires(old.valid())]
#[requires(old.nodes.contains(&t))]
#[requires(ranked(old.nodes, rank))]
#[ensures(cascade_linked(old.nodes) && cascade_ranked(old.nodes, rank) && cascade_valid(old))]
#[ensures(old.nodes[t]@.id == t)]
#[ensures((old.nodes[t]@.from.len() == 0) == old.roots.contains(t))]
#[ensures(no_dup(old.nodes[t]@.from) && no_dup(old.nodes[t]@.to) && !old.nodes[t]@.to.contains(t))]
#[ensures(forall<k> old.nodes[t]@.to.contains(k) ==> old.nodes.contains(&k))]
#[ensures(forall<k> old.nodes.contains(&k) ==> old.nodes[k]@.active == old.active.contains(&k)
    && old.nodes[k]@.bookmarked == old.bookmarked.contains(k))]
#[ensures(forall<k> old.active.contains(&k) ==> old.nodes.contains(&k))]
#[ensures(forall<p> old.nodes[t]@.from.contains(p) ==> rank[p] < rank[t])]
pub fn lemma_cascade_init<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    t: Id<K>,
    rank: Mapping<Id<K>, Int>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(linked(old.nodes, t) && old.flagged(t))
    }
}

/// `new` is `nodes` without `t`, and the nodes in `processed` no longer list `t` as a child.
#[logic(open(self))]
pub fn detaching<K, T, S>(
    nodes: Nodes<K, T, S>,
    new: Nodes<K, T, S>,
    t: Id<K>,
    processed: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (forall<k> new.contains(&k) == (nodes.contains(&k) && k != t))
            && forall<k> new.contains(&k) ==> new[k]@.id == nodes[k]@.id
                && new[k]@.from == nodes[k]@.from
                && new[k]@.active == nodes[k]@.active
                && new[k]@.bookmarked == nodes[k]@.bookmarked
                && (processed.contains(k)
                    ==> forall<c> new[k]@.to.contains(c) == (nodes[k]@.to.contains(c) && c != t))
                && (!processed.contains(k) ==> new[k]@.to == nodes[k]@.to)
    }
}

/// Removing `t` starts detaching it.
#[logic]
#[requires(processed.len() == 0)]
#[ensures(detaching(nodes, nodes.remove(t), t, processed))]
pub fn lemma_detaching_start<K, T, S>(nodes: Nodes<K, T, S>, t: Id<K>, processed: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> !processed.contains(k))
    }
}

/// Removing `t` from the children of `key` detaches it from one more node.
#[logic]
#[requires(detaching(nodes, before, t, processed))]
#[requires(forall<x> processed2.contains(x) == (processed.contains(x) || x == key))]
#[requires(!processed.contains(key))]
#[requires(forall<k> k != key ==> after.lookup(k) == before.lookup(k))]
#[requires(after.contains(&key) == before.contains(&key))]
#[requires(before.contains(&key) ==> after[key]@.id == before[key]@.id
    && after[key]@.from == before[key]@.from
    && after[key]@.active == before[key]@.active
    && after[key]@.bookmarked == before[key]@.bookmarked
    && forall<c> after[key]@.to.contains(c) == (before[key]@.to.contains(c) && c != t))]
#[ensures(detaching(nodes, after, t, processed2))]
pub fn lemma_detaching_step<K, T, S>(
    nodes: Nodes<K, T, S>,
    before: Nodes<K, T, S>,
    after: Nodes<K, T, S>,
    t: Id<K>,
    processed: Seq<Id<K>>,
    processed2: Seq<Id<K>>,
    key: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> k != key ==> after.contains(&k) == before.contains(&k));
        proof_assert!(forall<k> k != key && after.contains(&k) ==> after[k] == before[k])
    }
}

/// Once `t` is detached from all of its parents, the nodes are `detached`.
#[logic]
#[requires(cascade_linked(nodes))]
#[requires(nodes.contains(&t))]
#[requires(forall<x> processed.contains(x) == nodes[t]@.from.contains(x))]
#[requires(detaching(nodes, new, t, processed))]
#[ensures(detached(nodes, new, t))]
pub fn lemma_detaching_done<K, T, S>(
    nodes: Nodes<K, T, S>,
    new: Nodes<K, T, S>,
    t: Id<K>,
    processed: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> nodes.contains(&k) && nodes[k]@.to.contains(t)
            ==> linked(nodes, k) && nodes[t]@.from.contains(k))
    }
}

/// `new` is `n2` where the nodes in `processed` no longer list the nodes in `done` as parents.
#[logic(open(self))]
pub fn unparenting<K, T, S>(
    n2: Nodes<K, T, S>,
    new: Nodes<K, T, S>,
    done: Seq<Id<K>>,
    processed: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (forall<k> new.contains(&k) == n2.contains(&k))
            && forall<k> n2.contains(&k) ==> new[k]@.id == n2[k]@.id
                && new[k]@.to == n2[k]@.to
                && new[k]@.active == n2[k]@.active
                && new[k]@.bookmarked == n2[k]@.bookmarked
                && forall<p> new[k]@.from.contains(p)
                    == (n2[k]@.from.contains(p) && !(done.contains(p) && processed.contains(k)))
    }
}

/// Unparenting starts with no node processed.
#[logic]
#[requires(processed.len() == 0)]
#[ensures(unparenting(n2, n2, done, processed))]
pub fn lemma_unparenting_start<K, T, S>(n2: Nodes<K, T, S>, done: Seq<Id<K>>, processed: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> !processed.contains(k))
    }
}

/// Removing the nodes in `done` from the parents of `key` processes one more node.
#[logic]
#[requires(unparenting(n2, before, done, processed))]
#[requires(forall<x> processed2.contains(x) == (processed.contains(x) || x == key))]
#[requires(forall<k> k != key ==> after.lookup(k) == before.lookup(k))]
#[requires(after.contains(&key) == before.contains(&key))]
#[requires(before.contains(&key) ==> after[key]@.id == before[key]@.id
    && after[key]@.to == before[key]@.to
    && after[key]@.active == before[key]@.active
    && after[key]@.bookmarked == before[key]@.bookmarked
    && forall<p> after[key]@.from.contains(p) == (before[key]@.from.contains(p) && !done.contains(p)))]
#[ensures(unparenting(n2, after, done, processed2))]
pub fn lemma_unparenting_step<K, T, S>(
    n2: Nodes<K, T, S>,
    before: Nodes<K, T, S>,
    after: Nodes<K, T, S>,
    done: Seq<Id<K>>,
    processed: Seq<Id<K>>,
    processed2: Seq<Id<K>>,
    key: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> k != key ==> after.contains(&k) == before.contains(&k));
        proof_assert!(forall<k> k != key && after.contains(&k) ==> after[k] == before[k])
    }
}

/// Once every remaining node with a deleted parent is processed, the nodes are `pruned`.
#[logic]
#[requires(forall<k> n2.contains(&k) == (n1.contains(&k) && !done.contains(k)))]
#[requires(forall<k> n2.contains(&k) ==> n2[k] == n1[k])]
#[requires(unparenting(n2, new, done, processed))]
#[requires(forall<k, p> n2.contains(&k) && n2[k]@.from.contains(p) && done.contains(p)
    ==> processed.contains(k))]
#[ensures(pruned(n1, new, done))]
pub fn lemma_unparenting_done<K, T, S>(
    n1: Nodes<K, T, S>,
    n2: Nodes<K, T, S>,
    new: Nodes<K, T, S>,
    done: Seq<Id<K>>,
    processed: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// Replacing the node `key` by `v`, which no longer lists the nodes in `done` as parents, processes
/// one more node.
#[logic]
#[requires(unparenting(n2, before, done, processed))]
#[requires(forall<x> processed2.contains(x) == (processed.contains(x) || x == key))]
#[requires(before.contains(&key) ==> after == before.add(key, v))]
#[requires(before.contains(&key) ==> v@.id == before[key]@.id
    && v@.to == before[key]@.to
    && v@.active == before[key]@.active
    && v@.bookmarked == before[key]@.bookmarked
    && forall<p> v@.from.contains(p) == (before[key]@.from.contains(p) && !done.contains(p)))]
#[requires(!before.contains(&key) ==> after == before)]
#[ensures(unparenting(n2, after, done, processed2))]
pub fn lemma_unparenting_set<K, T, S>(
    n2: Nodes<K, T, S>,
    before: Nodes<K, T, S>,
    after: Nodes<K, T, S>,
    done: Seq<Id<K>>,
    processed: Seq<Id<K>>,
    processed2: Seq<Id<K>>,
    key: Id<K>,
    v: IndependentNode<K, T, S>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> k != key ==> after.lookup(k) == before.lookup(k));
        proof_assert!(after.contains(&key) == before.contains(&key));
        proof_assert!(before.contains(&key) ==> after[key] == v);
        lemma_unparenting_step(n2, before, after, done, processed, processed2, key)
    }
}

/// A list of what the removal deletes, as the removal builds it, is a [`removal`].
#[logic]
#[requires(cascade_done(nodes, rank, t, done))]
#[ensures(removal(nodes, t, done))]
pub fn lemma_removal_cascade<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    done: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// Two lists of what removing `t` deletes agree on the nodes which rank below `n`.
#[logic]
#[requires(ranked(nodes, rank))]
#[requires(removal(nodes, t, d1))]
#[requires(removal(nodes, t, d2))]
#[requires(0 <= n)]
#[ensures(forall<k> nodes.contains(&k) && rank[k] < n ==> d1.contains(k) == d2.contains(k))]
#[variant(n)]
pub fn lemma_removal_unique_below<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    d1: Seq<Id<K>>,
    d2: Seq<Id<K>>,
    n: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if n > 0 {
        lemma_removal_unique_below(nodes, rank, t, d1, d2, n - 1);
        pearlite! {
            proof_assert!(forall<k, p> nodes.contains(&k) && rank[k] == n - 1 && nodes[k]@.from.contains(p)
                && (d1.contains(p) || d2.contains(p))
                ==> nodes.contains(&p) && rank[p] < n - 1 && d1.contains(p) == d2.contains(p));
            proof_assert!(forall<k> nodes.contains(&k) && rank[k] == n - 1 && k != t && d1.contains(k)
                ==> (forall<p> nodes[k]@.from.contains(p) ==> d2.contains(p)) && d2.contains(k));
            proof_assert!(forall<k> nodes.contains(&k) && rank[k] == n - 1 && k != t && d2.contains(k)
                ==> (forall<p> nodes[k]@.from.contains(p) ==> d1.contains(p)) && d1.contains(k))
        }
    }
}

/// Only one set of nodes is what removing `t` from an acyclic graph deletes.
#[logic]
#[requires(acyclic(nodes))]
#[requires(removal(nodes, t, d1))]
#[requires(removal(nodes, t, d2))]
#[ensures(forall<k> d1.contains(k) == d2.contains(k))]
pub fn lemma_removal_unique<K, T, S>(
    nodes: Nodes<K, T, S>,
    t: Id<K>,
    d1: Seq<Id<K>>,
    d2: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let rank = rank_of(nodes);
    let bound = set_bound(nodes.keys(), rank);
    let n = if bound < 0 { 0 } else { bound };
    lemma_removal_unique_below(nodes, rank, t, d1, d2, n);
    pearlite! {
        proof_assert!(forall<k> nodes.contains(&k) ==> nodes.keys().contains(&k) && rank[k] < n)
    }
}

/// What `IndexSet::shift_remove` leaves keeps the order of the other elements.
#[logic]
#[requires(forall<i> 0 <= i && i < s.len() && s[i] == x ==> new == s.removed(i))]
#[requires(!s.contains(x) ==> new == s)]
#[ensures(in_order(new, s))]
pub fn lemma_shift_removed<I>(new: Seq<I>, s: Seq<I>, x: I) {
    pearlite! {
        if s.contains(x) {
            let i = such_that(|i: Int| 0 <= i && i < s.len() && s[i] == x);
            lemma_in_order_removed(s, i)
        } else {
            lemma_in_order_refl(s)
        }
    }
}

/// Nodes unchanged from `old` are kept in order.
#[logic]
#[requires(forall<k> new.contains(&k) ==> old.contains(&k) && new[k] == old[k])]
#[ensures(kept_in_order(new, old))]
pub fn lemma_kept_sub<K, T, S>(new: Nodes<K, T, S>, old: Nodes<K, T, S>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_in_order_refl::<Id<K>>;
}

/// Removing children of `key` while keeping the others in order keeps the nodes in order.
#[logic]
#[requires(kept_in_order(before, old))]
#[requires(forall<k> k != key ==> after.lookup(k) == before.lookup(k))]
#[requires(after.contains(&key) == before.contains(&key))]
#[requires(before.contains(&key) ==> after[key].contents == before[key].contents
    && after[key]@.from == before[key]@.from
    && in_order(after[key]@.to, before[key]@.to))]
#[ensures(kept_in_order(after, old))]
pub fn lemma_kept_children_step<K, T, S>(
    old: Nodes<K, T, S>,
    before: Nodes<K, T, S>,
    after: Nodes<K, T, S>,
    key: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> k != key && after.contains(&k) ==> before.contains(&k) && after[k] == before[k]);
        proof_assert!(before.contains(&key) ==> {
            lemma_in_order_trans(after[key]@.to, before[key]@.to, old[key]@.to);
            true
        })
    }
}

/// Replacing the node `key` by `v`, which lists some of its parents in order, keeps the nodes in
/// order.
#[logic]
#[requires(kept_in_order(before, old))]
#[requires(before.contains(&key) ==> after == before.add(key, v)
    && v.contents == before[key].contents
    && v@.to == before[key]@.to
    && in_order(v@.from, before[key]@.from))]
#[requires(!before.contains(&key) ==> after == before)]
#[ensures(kept_in_order(after, old))]
pub fn lemma_kept_parents_set<K, T, S>(
    old: Nodes<K, T, S>,
    before: Nodes<K, T, S>,
    after: Nodes<K, T, S>,
    key: Id<K>,
    v: IndependentNode<K, T, S>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> k != key && after.contains(&k) ==> before.contains(&k) && after[k] == before[k]);
        proof_assert!(before.contains(&key) ==> after.contains(&key) && after[key] == v && {
            lemma_in_order_trans(v@.from, before[key]@.from, old[key]@.from);
            true
        })
    }
}

/// Deleting `t`, which has no children, deletes it alone, in the shape [`removal_shape`]
/// describes.
#[logic]
#[requires(cascade_linked(old.nodes))]
#[requires(old.nodes.contains(&t))]
#[requires(old.nodes[t]@.to.len() == 0)]
#[requires(detached(old.nodes, new.nodes, t))]
#[requires(kept_in_order(new.nodes, old.nodes))]
#[requires(filtered(new.roots, old.roots, Seq::singleton(t)))]
#[requires(filtered(new.bookmarked, old.bookmarked, Seq::singleton(t)))]
#[ensures(removal(old.nodes, t, Seq::singleton(t)))]
#[ensures(removal_shape(old, new, Seq::singleton(t)))]
pub fn lemma_removal_leaf<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    t: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let done = Seq::singleton(t);
        proof_assert!(forall<k> done.contains(k) == (k == t));
        proof_assert!(forall<k> !old.nodes[t]@.to.contains(k));
        proof_assert!(forall<k> old.nodes.contains(&k) ==> linked(old.nodes, k) && !old.nodes[k]@.from.contains(t));
        proof_assert!(forall<k> old.nodes.contains(&k) && 0 < old.nodes[k]@.from.len()
            ==> old.nodes[k]@.from.contains(old.nodes[k]@.from[0]) && old.nodes[k]@.from[0] != t);
        proof_assert!(removal(old.nodes, t, done));
        proof_assert!(forall<k> new.nodes.contains(&k) ==> old.nodes.contains(&k) && k != t
            && filtered(new.nodes[k]@.from, old.nodes[k]@.from, done)
            && filtered(new.nodes[k]@.to, old.nodes[k]@.to, done))
    }
}

/// Deleting the nodes in `done`, in the shape the removal builds, while keeping the rest in order,
/// is in the shape [`removal_shape`] describes.
#[logic]
#[requires(cascade_shape(old, new.nodes, new.roots, new.active, done))]
#[requires(kept_in_order(new.nodes, old.nodes))]
#[requires(in_order(new.roots, old.roots))]
#[requires(filtered(new.bookmarked, old.bookmarked, done))]
#[ensures(removal_shape(old, new, done))]
pub fn lemma_removal_cascade_shape<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    done: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// Recomputing which nodes are active keeps the shape of a removal.
#[logic]
#[requires(removal_shape(old, mid, done))]
#[requires(same_graph(new.nodes, mid.nodes))]
#[requires(new.roots == mid.roots)]
#[requires(new.bookmarked == mid.bookmarked)]
#[ensures(removal_shape(old, new, done))]
pub fn lemma_removal_reactivated<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    mid: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    done: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

// Boolean forms of the lemmas used by `remove` and `remove_tracked`: asserting `x_ok(args)` puts
// that one instance of the lemma in the context, instead of a global axiom over all arguments.

/// Boolean form of [`lemma_cascade_init`].
#[logic]
#[ensures(result == ((old.valid())
    && (old.nodes.contains(&t))
    && (ranked(old.nodes, rank))))]
#[ensures(result ==> cascade_linked(old.nodes) && cascade_ranked(old.nodes, rank) && cascade_valid(old))]
#[ensures(result ==> old.nodes[t]@.id == t)]
#[ensures(result ==> (old.nodes[t]@.from.len() == 0) == old.roots.contains(t))]
#[ensures(result ==> no_dup(old.nodes[t]@.from) && no_dup(old.nodes[t]@.to) && !old.nodes[t]@.to.contains(t))]
#[ensures(result ==> forall<k> old.nodes[t]@.to.contains(k) ==> old.nodes.contains(&k))]
#[ensures(result ==> forall<k> old.nodes.contains(&k) ==> old.nodes[k]@.active == old.active.contains(&k) && old.nodes[k]@.bookmarked == old.bookmarked.contains(k))]
#[ensures(result ==> forall<k> old.active.contains(&k) ==> old.nodes.contains(&k))]
#[ensures(result ==> forall<p> old.nodes[t]@.from.contains(p) ==> rank[p] < rank[t])]
pub fn cascade_init_ok<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    t: Id<K>,
    rank: Mapping<Id<K>, Int>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (old.valid())
            && (old.nodes.contains(&t))
            && (ranked(old.nodes, rank));
        proof_assert!(pre ==> {
            lemma_cascade_init(old, t, rank);
            true
        } && (cascade_linked(old.nodes) && cascade_ranked(old.nodes, rank) && cascade_valid(old))
            && (old.nodes[t]@.id == t)
            && ((old.nodes[t]@.from.len() == 0) == old.roots.contains(t))
            && (no_dup(old.nodes[t]@.from) && no_dup(old.nodes[t]@.to) && !old.nodes[t]@.to.contains(t))
            && (forall<k> old.nodes[t]@.to.contains(k) ==> old.nodes.contains(&k))
            && (forall<k> old.nodes.contains(&k) ==> old.nodes[k]@.active == old.active.contains(&k) && old.nodes[k]@.bookmarked == old.bookmarked.contains(k))
            && (forall<k> old.active.contains(&k) ==> old.nodes.contains(&k))
            && (forall<p> old.nodes[t]@.from.contains(p) ==> rank[p] < rank[t]));
        pre
    }
}

/// Boolean form of [`lemma_cascade_start`].
#[logic]
#[ensures(result == ((cascade_linked(nodes))
    && (nodes.contains(&t))))]
#[ensures(result ==> cascade_inner(nodes, rank, t, Seq::empty(), t, nodes[t]@.to, Seq::empty(), FMap::empty()))]
pub fn cascade_start_ok<K, T, S>(nodes: Nodes<K, T, S>, rank: Mapping<Id<K>, Int>, t: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (cascade_linked(nodes))
            && (nodes.contains(&t));
        proof_assert!(pre ==> {
            lemma_cascade_start(nodes, rank, t);
            true
        } && (cascade_inner(nodes, rank, t, Seq::empty(), t, nodes[t]@.to, Seq::empty(), FMap::empty())));
        pre
    }
}

/// Boolean form of [`lemma_cascade_pop`].
#[logic]
#[ensures(result == ((cascade_linked(nodes))
    && (cascade_ranked(nodes, rank))
    && (cascade_outer(nodes, rank, t, prev, rest.snoc(cur), counts))))]
#[ensures(result ==> cascade_inner(nodes, rank, t, prev, cur, nodes[cur]@.to, rest, counts))]
#[ensures(result ==> cur != t && nodes.contains(&cur) && rank[t] < rank[cur])]
#[ensures(result ==> !prev.contains(cur) && prev.contains(t))]
#[ensures(result ==> !nodes[t]@.from.contains(cur))]
pub fn cascade_pop_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    prev: Seq<Id<K>>,
    rest: Seq<Id<K>>,
    cur: Id<K>,
    counts: FMap<Id<K>, usize>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (cascade_linked(nodes))
            && (cascade_ranked(nodes, rank))
            && (cascade_outer(nodes, rank, t, prev, rest.snoc(cur), counts));
        proof_assert!(pre ==> {
            lemma_cascade_pop(nodes, rank, t, prev, rest, cur, counts);
            true
        } && (cascade_inner(nodes, rank, t, prev, cur, nodes[cur]@.to, rest, counts))
            && (cur != t && nodes.contains(&cur) && rank[t] < rank[cur])
            && (!prev.contains(cur) && prev.contains(t))
            && (!nodes[t]@.from.contains(cur)));
        pre
    }
}

/// Boolean form of [`lemma_cascade_count`].
#[logic]
#[ensures(result == ((cascade_linked(nodes))
    && (0 <= j && j < nodes[cur]@.to.len())
    && (cascade_inner(nodes, rank, t, prev, cur, nodes[cur]@.to.subsequence(0, j + 1), stack, counts))))]
#[ensures(result ==> nodes.contains(&nodes[cur]@.to[j]))]
#[ensures(result ==> !prev.snoc(cur).contains(nodes[cur]@.to[j]))]
#[ensures(result ==> nodes[cur]@.to[j] != t)]
#[ensures(result ==> 0 <= left_parents(nodes, prev.snoc(cur), nodes[cur]@.to[j]))]
#[ensures(result ==> counts.contains(&nodes[cur]@.to[j]) ==> counts[nodes[cur]@.to[j]]@ == left_parents(nodes, prev.snoc(cur), nodes[cur]@.to[j]) + 1)]
#[ensures(result ==> !counts.contains(&nodes[cur]@.to[j]) ==> nodes[nodes[cur]@.to[j]]@.from.len() == left_parents(nodes, prev.snoc(cur), nodes[cur]@.to[j]) + 1)]
pub fn cascade_count_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    prev: Seq<Id<K>>,
    cur: Id<K>,
    j: Int,
    stack: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (cascade_linked(nodes))
            && (0 <= j && j < nodes[cur]@.to.len())
            && (cascade_inner(nodes, rank, t, prev, cur, nodes[cur]@.to.subsequence(0, j + 1), stack, counts));
        proof_assert!(pre ==> {
            lemma_cascade_count(nodes, rank, t, prev, cur, j, stack, counts);
            true
        } && (nodes.contains(&nodes[cur]@.to[j]))
            && (!prev.snoc(cur).contains(nodes[cur]@.to[j]))
            && (nodes[cur]@.to[j] != t)
            && (0 <= left_parents(nodes, prev.snoc(cur), nodes[cur]@.to[j]))
            && (counts.contains(&nodes[cur]@.to[j]) ==> counts[nodes[cur]@.to[j]]@ == left_parents(nodes, prev.snoc(cur), nodes[cur]@.to[j]) + 1)
            && (!counts.contains(&nodes[cur]@.to[j]) ==> nodes[nodes[cur]@.to[j]]@.from.len() == left_parents(nodes, prev.snoc(cur), nodes[cur]@.to[j]) + 1));
        pre
    }
}

/// Boolean form of [`lemma_cascade_visit`].
#[logic]
#[ensures(result == ((cascade_linked(nodes))
    && (cascade_ranked(nodes, rank))
    && (0 <= j && j < nodes[cur]@.to.len())
    && (cascade_inner(nodes, rank, t, prev, cur, nodes[cur]@.to.subsequence(0, j + 1), stack, counts))
    && (n@ == left_parents(nodes, prev.snoc(cur), nodes[cur]@.to[j]))))]
#[ensures(result ==> cascade_inner(nodes, rank, t, prev, cur, nodes[cur]@.to.subsequence(0, j), if n@ == 0 { stack.snoc(nodes[cur]@.to[j]) } else { stack }, counts.add(nodes[cur]@.to[j], n)))]
pub fn cascade_visit_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    prev: Seq<Id<K>>,
    cur: Id<K>,
    j: Int,
    stack: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
    n: usize,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (cascade_linked(nodes))
            && (cascade_ranked(nodes, rank))
            && (0 <= j && j < nodes[cur]@.to.len())
            && (cascade_inner(nodes, rank, t, prev, cur, nodes[cur]@.to.subsequence(0, j + 1), stack, counts))
            && (n@ == left_parents(nodes, prev.snoc(cur), nodes[cur]@.to[j]));
        proof_assert!(pre ==> {
            lemma_cascade_visit(nodes, rank, t, prev, cur, j, stack, counts, n);
            true
        } && (cascade_inner(nodes, rank, t, prev, cur, nodes[cur]@.to.subsequence(0, j), if n@ == 0 { stack.snoc(nodes[cur]@.to[j]) } else { stack }, counts.add(nodes[cur]@.to[j], n))));
        pre
    }
}

/// Boolean form of [`lemma_cascade_next`].
#[logic]
#[ensures(result == (cascade_inner(nodes, rank, t, prev, cur, nodes[cur]@.to.subsequence(0, 0), stack, counts)))]
#[ensures(result ==> cascade_outer(nodes, rank, t, prev.snoc(cur), stack, counts))]
pub fn cascade_next_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    prev: Seq<Id<K>>,
    cur: Id<K>,
    stack: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = cascade_inner(nodes, rank, t, prev, cur, nodes[cur]@.to.subsequence(0, 0), stack, counts);
        proof_assert!(pre ==> {
            lemma_cascade_next(nodes, rank, t, prev, cur, stack, counts);
            true
        } && (cascade_outer(nodes, rank, t, prev.snoc(cur), stack, counts)));
        pre
    }
}

/// Boolean form of [`lemma_cascade_end`].
#[logic]
#[ensures(result == ((cascade_linked(nodes))
    && (cascade_outer(nodes, rank, t, done, Seq::empty(), counts))))]
#[ensures(result ==> cascade_done(nodes, rank, t, done))]
#[ensures(result ==> done.contains(t))]
#[ensures(result ==> done.len() == 1 ==> forall<k> done.contains(k) == (k == t))]
#[ensures(result ==> forall<k> counts.contains(&k) && counts[k]@ == 0 ==> done.contains(k))]
#[ensures(result ==> forall<k, p> nodes.contains(&k) && nodes[k]@.from.contains(p) && done.contains(p) ==> counts.contains(&k))]
#[ensures(result ==> no_dup(done))]
pub fn cascade_end_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    done: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (cascade_linked(nodes))
            && (cascade_outer(nodes, rank, t, done, Seq::empty(), counts));
        proof_assert!(pre ==> {
            lemma_cascade_end(nodes, rank, t, done, counts);
            true
        } && (cascade_done(nodes, rank, t, done))
            && (done.contains(t))
            && (done.len() == 1 ==> forall<k> done.contains(k) == (k == t))
            && (forall<k> counts.contains(&k) && counts[k]@ == 0 ==> done.contains(k))
            && (forall<k, p> nodes.contains(&k) && nodes[k]@.from.contains(p) && done.contains(p) ==> counts.contains(&k)));
        pre
    }
}

/// Boolean form of [`lemma_cascade_shape`].
#[logic]
#[ensures(result == ((cascade_valid(old))
    && (cascade_ranked(old.nodes, rank))
    && (cascade_done(old.nodes, rank, t, done))
    && (detached(old.nodes, n1, t))
    && (pruned(n1, nodes, done))
    && (no_dup(roots))
    && (forall<k> roots.contains(k) == (old.roots.contains(k) && k != t))
    && (forall<k> active.contains(&k) == (old.active.contains(&k) && !done.contains(k)))))]
#[ensures(result ==> cascade_shape(old, nodes, roots, active, done))]
pub fn cascade_shape_ok<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    n1: Nodes<K, T, S>,
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    active: FSet<Id<K>>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    done: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (cascade_valid(old))
            && (cascade_ranked(old.nodes, rank))
            && (cascade_done(old.nodes, rank, t, done))
            && (detached(old.nodes, n1, t))
            && (pruned(n1, nodes, done))
            && (no_dup(roots))
            && (forall<k> roots.contains(k) == (old.roots.contains(k) && k != t))
            && (forall<k> active.contains(&k) == (old.active.contains(&k) && !done.contains(k)));
        proof_assert!(pre ==> {
            lemma_cascade_shape(old, n1, nodes, roots, active, rank, t, done);
            true
        } && (cascade_shape(old, nodes, roots, active, done)));
        pre
    }
}

/// Boolean form of [`lemma_cascade_structure`].
#[logic]
#[ensures(result == ((cascade_valid(old))
    && (cascade_ranked(old.nodes, rank))
    && (cascade_done(old.nodes, rank, t, done))
    && (cascade_shape(old, nodes, roots, active, done))))]
#[ensures(result ==> structure(nodes, roots, active))]
#[ensures(result ==> forall<k> nodes.contains(&k) == (old.nodes.contains(&k) && !done.contains(k)))]
#[ensures(result ==> forall<k> nodes.contains(&k) ==> nodes[k]@.bookmarked == old.bookmarked.contains(k))]
#[ensures(result ==> forall<k> old.bookmarked.contains(k) && !done.contains(k) ==> nodes.contains(&k))]
pub fn cascade_structure_ok<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    active: FSet<Id<K>>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    done: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (cascade_valid(old))
            && (cascade_ranked(old.nodes, rank))
            && (cascade_done(old.nodes, rank, t, done))
            && (cascade_shape(old, nodes, roots, active, done));
        proof_assert!(pre ==> {
            lemma_cascade_structure(old, nodes, roots, active, rank, t, done);
            true
        } && (structure(nodes, roots, active))
            && (forall<k> nodes.contains(&k) == (old.nodes.contains(&k) && !done.contains(k)))
            && (forall<k> nodes.contains(&k) ==> nodes[k]@.bookmarked == old.bookmarked.contains(k))
            && (forall<k> old.bookmarked.contains(k) && !done.contains(k) ==> nodes.contains(&k)));
        pre
    }
}

/// Boolean form of [`lemma_cascade_path`].
#[logic]
#[ensures(result == ((cascade_valid(old))
    && (cascade_shape(old, nodes, roots, active, done))
    && (active_kept(old.nodes, old.active, done))))]
#[ensures(result ==> forms_path(nodes, active))]
pub fn cascade_path_ok<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    active: FSet<Id<K>>,
    done: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (cascade_valid(old))
            && (cascade_shape(old, nodes, roots, active, done))
            && (active_kept(old.nodes, old.active, done));
        proof_assert!(pre ==> {
            lemma_cascade_path(old, nodes, roots, active, done);
            true
        } && (forms_path(nodes, active)));
        pre
    }
}

/// Boolean form of [`lemma_active_kept`].
#[logic]
#[ensures(result == ((forall<k> active.contains(&k) ==> nodes.contains(&k))
    && (forall<k> counts.contains(&k) && counts[k]@ == 0 ==> done.contains(k))
    && (forall<k, p> nodes.contains(&k) && nodes[k]@.from.contains(p) && done.contains(p) ==> counts.contains(&k))
    && ((forall<k> done.contains(k) ==> !active.contains(&k)) || !exists<k> counts.contains(&k) && counts[k]@ > 0 && active.contains(&k) && !done.contains(k))))]
#[ensures(result ==> active_kept(nodes, active, done))]
pub fn active_kept_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    active: FSet<Id<K>>,
    done: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (forall<k> active.contains(&k) ==> nodes.contains(&k))
            && (forall<k> counts.contains(&k) && counts[k]@ == 0 ==> done.contains(k))
            && (forall<k, p> nodes.contains(&k) && nodes[k]@.from.contains(p) && done.contains(p) ==> counts.contains(&k))
            && ((forall<k> done.contains(k) ==> !active.contains(&k)) || !exists<k> counts.contains(&k) && counts[k]@ > 0 && active.contains(&k) && !done.contains(k));
        proof_assert!(pre ==> {
            lemma_active_kept(nodes, active, done, counts);
            true
        } && (active_kept(nodes, active, done)));
        pre
    }
}

/// Boolean form of [`lemma_cascade_leaf_valid`].
#[logic]
#[ensures(result == ((cascade_valid(old))
    && (cascade_ranked(old.nodes, rank))
    && (old.nodes.contains(&t))
    && (old.nodes[t]@.to.len() == 0)
    && (detached(old.nodes, n1, t))
    && (no_dup(roots))
    && (forall<k> roots.contains(k) == (old.roots.contains(k) && k != t))
    && (forall<k> active.contains(&k) == (old.active.contains(&k) && k != t))
    && (no_dup(bm))
    && (forall<k> bm.contains(k) == (old.bookmarked.contains(k) && k != t))))]
#[ensures(result ==> forall<new: IndependentWeaveView<K, T, S>> #[trigger(new.valid())] new.nodes == n1 && new.roots == roots && new.active == active && new.bookmarked == bm ==> new.valid())]
pub fn cascade_leaf_valid_ok<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    n1: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    active: FSet<Id<K>>,
    bm: Seq<Id<K>>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (cascade_valid(old))
            && (cascade_ranked(old.nodes, rank))
            && (old.nodes.contains(&t))
            && (old.nodes[t]@.to.len() == 0)
            && (detached(old.nodes, n1, t))
            && (no_dup(roots))
            && (forall<k> roots.contains(k) == (old.roots.contains(k) && k != t))
            && (forall<k> active.contains(&k) == (old.active.contains(&k) && k != t))
            && (no_dup(bm))
            && (forall<k> bm.contains(k) == (old.bookmarked.contains(k) && k != t));
        proof_assert!(pre ==> {
            lemma_cascade_leaf_valid(old, n1, roots, active, bm, rank, t);
            true
        } && (forall<new: IndependentWeaveView<K, T, S>> #[trigger(new.valid())] new.nodes == n1 && new.roots == roots && new.active == active && new.bookmarked == bm ==> new.valid()));
        pre
    }
}

/// Boolean form of [`lemma_detaching_start`].
#[logic]
#[ensures(result == (processed.len() == 0))]
#[ensures(result ==> detaching(nodes, nodes.remove(t), t, processed))]
pub fn detaching_start_ok<K, T, S>(nodes: Nodes<K, T, S>, t: Id<K>, processed: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = processed.len() == 0;
        proof_assert!(pre ==> {
            lemma_detaching_start(nodes, t, processed);
            true
        } && (detaching(nodes, nodes.remove(t), t, processed)));
        pre
    }
}

/// Boolean form of [`lemma_detaching_step`].
#[logic]
#[ensures(result == ((detaching(nodes, before, t, processed))
    && (forall<x> processed2.contains(x) == (processed.contains(x) || x == key))
    && (!processed.contains(key))
    && (forall<k> k != key ==> after.lookup(k) == before.lookup(k))
    && (after.contains(&key) == before.contains(&key))
    && (before.contains(&key) ==> after[key]@.id == before[key]@.id && after[key]@.from == before[key]@.from && after[key]@.active == before[key]@.active && after[key]@.bookmarked == before[key]@.bookmarked && forall<c> after[key]@.to.contains(c) == (before[key]@.to.contains(c) && c != t))))]
#[ensures(result ==> detaching(nodes, after, t, processed2))]
pub fn detaching_step_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    before: Nodes<K, T, S>,
    after: Nodes<K, T, S>,
    t: Id<K>,
    processed: Seq<Id<K>>,
    processed2: Seq<Id<K>>,
    key: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (detaching(nodes, before, t, processed))
            && (forall<x> processed2.contains(x) == (processed.contains(x) || x == key))
            && (!processed.contains(key))
            && (forall<k> k != key ==> after.lookup(k) == before.lookup(k))
            && (after.contains(&key) == before.contains(&key))
            && (before.contains(&key) ==> after[key]@.id == before[key]@.id && after[key]@.from == before[key]@.from && after[key]@.active == before[key]@.active && after[key]@.bookmarked == before[key]@.bookmarked && forall<c> after[key]@.to.contains(c) == (before[key]@.to.contains(c) && c != t));
        proof_assert!(pre ==> {
            lemma_detaching_step(nodes, before, after, t, processed, processed2, key);
            true
        } && (detaching(nodes, after, t, processed2)));
        pre
    }
}

/// Boolean form of [`lemma_detaching_done`].
#[logic]
#[ensures(result == ((cascade_linked(nodes))
    && (nodes.contains(&t))
    && (forall<x> processed.contains(x) == nodes[t]@.from.contains(x))
    && (detaching(nodes, new, t, processed))))]
#[ensures(result ==> detached(nodes, new, t))]
pub fn detaching_done_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    new: Nodes<K, T, S>,
    t: Id<K>,
    processed: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (cascade_linked(nodes))
            && (nodes.contains(&t))
            && (forall<x> processed.contains(x) == nodes[t]@.from.contains(x))
            && (detaching(nodes, new, t, processed));
        proof_assert!(pre ==> {
            lemma_detaching_done(nodes, new, t, processed);
            true
        } && (detached(nodes, new, t)));
        pre
    }
}

/// Boolean form of [`lemma_unparenting_start`].
#[logic]
#[ensures(result == (processed.len() == 0))]
#[ensures(result ==> unparenting(n2, n2, done, processed))]
pub fn unparenting_start_ok<K, T, S>(
    n2: Nodes<K, T, S>,
    done: Seq<Id<K>>,
    processed: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = processed.len() == 0;
        proof_assert!(pre ==> {
            lemma_unparenting_start(n2, done, processed);
            true
        } && (unparenting(n2, n2, done, processed)));
        pre
    }
}

/// Boolean form of [`lemma_unparenting_set`].
#[logic]
#[ensures(result == ((unparenting(n2, before, done, processed))
    && (forall<x> processed2.contains(x) == (processed.contains(x) || x == key))
    && (before.contains(&key) ==> after == before.add(key, v))
    && (before.contains(&key) ==> v@.id == before[key]@.id && v@.to == before[key]@.to && v@.active == before[key]@.active && v@.bookmarked == before[key]@.bookmarked && forall<p> v@.from.contains(p) == (before[key]@.from.contains(p) && !done.contains(p)))
    && (!before.contains(&key) ==> after == before)))]
#[ensures(result ==> unparenting(n2, after, done, processed2))]
pub fn unparenting_set_ok<K, T, S>(
    n2: Nodes<K, T, S>,
    before: Nodes<K, T, S>,
    after: Nodes<K, T, S>,
    done: Seq<Id<K>>,
    processed: Seq<Id<K>>,
    processed2: Seq<Id<K>>,
    key: Id<K>,
    v: IndependentNode<K, T, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (unparenting(n2, before, done, processed))
            && (forall<x> processed2.contains(x) == (processed.contains(x) || x == key))
            && (before.contains(&key) ==> after == before.add(key, v))
            && (before.contains(&key) ==> v@.id == before[key]@.id && v@.to == before[key]@.to && v@.active == before[key]@.active && v@.bookmarked == before[key]@.bookmarked && forall<p> v@.from.contains(p) == (before[key]@.from.contains(p) && !done.contains(p)))
            && (!before.contains(&key) ==> after == before);
        proof_assert!(pre ==> {
            lemma_unparenting_set(n2, before, after, done, processed, processed2, key, v);
            true
        } && (unparenting(n2, after, done, processed2)));
        pre
    }
}

/// Boolean form of [`lemma_unparenting_done`].
#[logic]
#[ensures(result == ((forall<k> n2.contains(&k) == (n1.contains(&k) && !done.contains(k)))
    && (forall<k> n2.contains(&k) ==> n2[k] == n1[k])
    && (unparenting(n2, new, done, processed))
    && (forall<k, p> n2.contains(&k) && n2[k]@.from.contains(p) && done.contains(p) ==> processed.contains(k))))]
#[ensures(result ==> pruned(n1, new, done))]
pub fn unparenting_done_ok<K, T, S>(
    n1: Nodes<K, T, S>,
    n2: Nodes<K, T, S>,
    new: Nodes<K, T, S>,
    done: Seq<Id<K>>,
    processed: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (forall<k> n2.contains(&k) == (n1.contains(&k) && !done.contains(k)))
            && (forall<k> n2.contains(&k) ==> n2[k] == n1[k])
            && (unparenting(n2, new, done, processed))
            && (forall<k, p> n2.contains(&k) && n2[k]@.from.contains(p) && done.contains(p) ==> processed.contains(k));
        proof_assert!(pre ==> {
            lemma_unparenting_done(n1, n2, new, done, processed);
            true
        } && (pruned(n1, new, done)));
        pre
    }
}

/// Boolean form of [`kahn::lemma_prefix_step`].
#[logic]
#[ensures(result == (0 <= i && i < s.len()))]
#[ensures(result ==> forall<x> s.subsequence(0, i + 1).contains(x)
    == (s.subsequence(0, i).contains(x) || x == s[i]))]
#[ensures(result ==> no_dup(s) ==> !s.subsequence(0, i).contains(s[i]))]
pub fn prefix_step_ok<I>(s: Seq<I>, i: Int) -> bool {
    pearlite! {
        let pre = 0 <= i && i < s.len();
        proof_assert!(pre ==> { lemma_prefix_step(s, i); true }
            && (forall<x> s.subsequence(0, i + 1).contains(x)
                == (s.subsequence(0, i).contains(x) || x == s[i]))
            && (no_dup(s) ==> !s.subsequence(0, i).contains(s[i])));
        pre
    }
}

/// Boolean form of [`crate::verification::lemma_keys_subset_len`].
#[logic]
#[ensures(result == forall<k> a.contains(&k) ==> b.contains(&k))]
#[ensures(result ==> a.len() <= b.len())]
pub fn keys_subset_len_ok<I, V, W>(a: FMap<I, V>, b: FMap<I, W>) -> bool {
    pearlite! {
        let pre = forall<k> a.contains(&k) ==> b.contains(&k);
        proof_assert!(pre ==> { crate::verification::lemma_keys_subset_len(a, b); true } && a.len() <= b.len());
        pre
    }
}

/// Boolean form of [`crate::verification::lemma_same_keys_len`].
#[logic]
#[ensures(result == forall<k> a.contains(&k) == b.contains(&k))]
#[ensures(result ==> a.len() == b.len())]
pub fn same_keys_len_ok<I, V, W>(a: FMap<I, V>, b: FMap<I, W>) -> bool {
    pearlite! {
        let pre = forall<k> a.contains(&k) == b.contains(&k);
        proof_assert!(pre ==> { crate::verification::lemma_same_keys_len(a, b); true } && a.len() == b.len());
        pre
    }
}

/// Boolean form of [`crate::verification::lemma_fset_subset_len`].
#[logic]
#[ensures(result == forall<k> a.contains(&k) ==> b.contains(&k))]
#[ensures(result ==> a.len() <= b.len())]
pub fn fset_subset_len_ok<I>(a: FSet<I>, b: FSet<I>) -> bool {
    pearlite! {
        let pre = forall<k> a.contains(&k) ==> b.contains(&k);
        proof_assert!(pre ==> { crate::verification::lemma_fset_subset_len(a, b); true } && a.len() <= b.len());
        pre
    }
}

/// Boolean form of [`crate::verification::lemma_subset_len`].
#[logic]
#[ensures(result == (no_dup(a) && forall<x> a.contains(x) ==> b.contains(x)))]
#[ensures(result ==> a.len() <= b.len())]
pub fn seq_subset_len_ok<I>(a: Seq<I>, b: Seq<I>) -> bool {
    pearlite! {
        let pre = no_dup(a) && forall<x> a.contains(x) ==> b.contains(x);
        proof_assert!(pre ==> { crate::verification::lemma_subset_len(a, b); true } && a.len() <= b.len());
        pre
    }
}

/// Boolean form of [`crate::dependent::verification::lemma_no_dup_len`].
#[logic]
#[ensures(result == (no_dup(s) && forall<i> 0 <= i && i < s.len() ==> m.contains(&s[i])))]
#[ensures(result ==> s.len() <= m.len())]
pub fn no_dup_len_ok<I, V>(s: Seq<I>, m: FMap<I, V>) -> bool {
    pearlite! {
        let pre = no_dup(s) && forall<i> 0 <= i && i < s.len() ==> m.contains(&s[i]);
        proof_assert!(pre ==> { crate::dependent::verification::lemma_no_dup_len(s, m); true }
            && s.len() <= m.len());
        pre
    }
}

/// Boolean form of [`lemma_reactivated`].
#[logic]
#[ensures(result == old.valid_structure())]
#[ensures(result ==> forall<new: IndependentWeaveView<K, T, S>> #[trigger(new.valid())]
    reactivated(old, new) ==> new.valid())]
pub fn reactivated_ok<K, T, S>(old: IndependentWeaveView<K, T, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = old.valid_structure();
        proof_assert!(pre ==> { lemma_reactivated(old); true }
            && forall<new: IndependentWeaveView<K, T, S>> reactivated(old, new) ==> new.valid());
        pre
    }
}

/// Boolean form of [`lemma_removal_cascade`].
#[logic]
#[ensures(result == cascade_done(nodes, rank, t, done))]
#[ensures(result ==> removal(nodes, t, done))]
pub fn removal_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    t: Id<K>,
    done: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = cascade_done(nodes, rank, t, done);
        proof_assert!(pre ==> { lemma_removal_cascade(nodes, rank, t, done); true }
            && removal(nodes, t, done));
        pre
    }
}

/// Boolean form of [`lemma_in_order_refl`].
#[logic]
#[ensures(result)]
#[ensures(in_order(s, s))]
pub fn in_order_refl_ok<I>(s: Seq<I>) -> bool {
    lemma_in_order_refl(s);
    true
}

/// Boolean form of [`lemma_shift_removed`].
#[logic]
#[ensures(result == ((forall<i> 0 <= i && i < s.len() && s[i] == x ==> new == s.removed(i))
    && (!s.contains(x) ==> new == s)))]
#[ensures(result ==> in_order(new, s))]
pub fn shift_removed_ok<I>(new: Seq<I>, s: Seq<I>, x: I) -> bool {
    pearlite! {
        let pre = (forall<i> 0 <= i && i < s.len() && s[i] == x ==> new == s.removed(i))
            && (!s.contains(x) ==> new == s);
        proof_assert!(pre ==> { lemma_shift_removed(new, s, x); true } && in_order(new, s));
        pre
    }
}

/// Boolean form of [`lemma_kept_sub`].
#[logic]
#[ensures(result == forall<k> new.contains(&k) ==> old.contains(&k) && new[k] == old[k])]
#[ensures(result ==> kept_in_order(new, old))]
pub fn kept_sub_ok<K, T, S>(new: Nodes<K, T, S>, old: Nodes<K, T, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = forall<k> new.contains(&k) ==> old.contains(&k) && new[k] == old[k];
        proof_assert!(pre ==> { lemma_kept_sub(new, old); true } && kept_in_order(new, old));
        pre
    }
}

/// Boolean form of [`lemma_kept_children_step`].
#[logic]
#[ensures(result == ((kept_in_order(before, old))
    && (forall<k> k != key ==> after.lookup(k) == before.lookup(k))
    && (after.contains(&key) == before.contains(&key))
    && (before.contains(&key) ==> after[key].contents == before[key].contents
        && after[key]@.from == before[key]@.from
        && in_order(after[key]@.to, before[key]@.to))))]
#[ensures(result ==> kept_in_order(after, old))]
pub fn kept_children_step_ok<K, T, S>(
    old: Nodes<K, T, S>,
    before: Nodes<K, T, S>,
    after: Nodes<K, T, S>,
    key: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (kept_in_order(before, old))
            && (forall<k> k != key ==> after.lookup(k) == before.lookup(k))
            && (after.contains(&key) == before.contains(&key))
            && (before.contains(&key) ==> after[key].contents == before[key].contents
                && after[key]@.from == before[key]@.from
                && in_order(after[key]@.to, before[key]@.to));
        proof_assert!(pre ==> { lemma_kept_children_step(old, before, after, key); true }
            && kept_in_order(after, old));
        pre
    }
}

/// Boolean form of [`lemma_kept_parents_set`].
#[logic]
#[ensures(result == ((kept_in_order(before, old))
    && (before.contains(&key) ==> after == before.add(key, v)
        && v.contents == before[key].contents
        && v@.to == before[key]@.to
        && in_order(v@.from, before[key]@.from))
    && (!before.contains(&key) ==> after == before)))]
#[ensures(result ==> kept_in_order(after, old))]
pub fn kept_parents_set_ok<K, T, S>(
    old: Nodes<K, T, S>,
    before: Nodes<K, T, S>,
    after: Nodes<K, T, S>,
    key: Id<K>,
    v: IndependentNode<K, T, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (kept_in_order(before, old))
            && (before.contains(&key) ==> after == before.add(key, v)
                && v.contents == before[key].contents
                && v@.to == before[key]@.to
                && in_order(v@.from, before[key]@.from))
            && (!before.contains(&key) ==> after == before);
        proof_assert!(pre ==> { lemma_kept_parents_set(old, before, after, key, v); true }
            && kept_in_order(after, old));
        pre
    }
}

/// Boolean form of [`lemma_removal_leaf`].
#[logic]
#[ensures(result == ((cascade_linked(old.nodes))
    && (old.nodes.contains(&t))
    && (old.nodes[t]@.to.len() == 0)
    && (detached(old.nodes, new.nodes, t))
    && (kept_in_order(new.nodes, old.nodes))
    && (filtered(new.roots, old.roots, Seq::singleton(t)))
    && (filtered(new.bookmarked, old.bookmarked, Seq::singleton(t)))))]
#[ensures(result ==> removal(old.nodes, t, Seq::singleton(t)))]
#[ensures(result ==> removal_shape(old, new, Seq::singleton(t)))]
pub fn removal_leaf_ok<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    t: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (cascade_linked(old.nodes))
            && (old.nodes.contains(&t))
            && (old.nodes[t]@.to.len() == 0)
            && (detached(old.nodes, new.nodes, t))
            && (kept_in_order(new.nodes, old.nodes))
            && (filtered(new.roots, old.roots, Seq::singleton(t)))
            && (filtered(new.bookmarked, old.bookmarked, Seq::singleton(t)));
        proof_assert!(pre ==> { lemma_removal_leaf(old, new, t); true }
            && removal(old.nodes, t, Seq::singleton(t))
            && removal_shape(old, new, Seq::singleton(t)));
        pre
    }
}

/// Boolean form of [`lemma_removal_cascade_shape`].
#[logic]
#[ensures(result == ((cascade_shape(old, new.nodes, new.roots, new.active, done))
    && (kept_in_order(new.nodes, old.nodes))
    && (in_order(new.roots, old.roots))
    && (filtered(new.bookmarked, old.bookmarked, done))))]
#[ensures(result ==> removal_shape(old, new, done))]
pub fn removal_cascade_shape_ok<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    done: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (cascade_shape(old, new.nodes, new.roots, new.active, done))
            && (kept_in_order(new.nodes, old.nodes))
            && (in_order(new.roots, old.roots))
            && (filtered(new.bookmarked, old.bookmarked, done));
        proof_assert!(pre ==> { lemma_removal_cascade_shape(old, new, done); true }
            && removal_shape(old, new, done));
        pre
    }
}

/// Boolean form of [`lemma_removal_reactivated`].
#[logic]
#[ensures(result == ((removal_shape(old, mid, done))
    && (same_graph(new.nodes, mid.nodes))
    && (new.roots == mid.roots)
    && (new.bookmarked == mid.bookmarked)))]
#[ensures(result ==> removal_shape(old, new, done))]
pub fn removal_reactivated_ok<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    mid: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    done: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = (removal_shape(old, mid, done))
            && (same_graph(new.nodes, mid.nodes))
            && (new.roots == mid.roots)
            && (new.bookmarked == mid.bookmarked);
        proof_assert!(pre ==> { lemma_removal_reactivated(old, mid, new, done); true }
            && removal_shape(old, new, done));
        pre
    }
}
