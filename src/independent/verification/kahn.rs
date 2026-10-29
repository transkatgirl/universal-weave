//! Lemmas used to verify `crate::independent::graph::kahn`.

use core::hash::{BuildHasher, Hash};

use creusot_std::logic::{FMap, Mapping};

#[allow(unused_imports)]
use super::*;
#[allow(unused_imports)]
use crate::{
    IndependentContents,
    independent::IndependentNode,
    verification::{Id, Model, lemma_no_dup_snoc, lemma_snoc_contains, no_dup},
};

/// The predicate which accepts every identifier, used by the topological sort of the whole graph.
#[logic(open)]
pub fn admit_all<I>() -> Mapping<I, bool> {
    |_| true
}

/// The elements of a non-empty sequence are those of its prefix without the last element, and the
/// last element.
#[logic]
#[requires(0 < s.len())]
#[ensures(forall<x> s.contains(x) == (s.subsequence(0, s.len() - 1).contains(x) || x == s[s.len() - 1]))]
#[ensures(no_dup(s) ==> no_dup(s.subsequence(0, s.len() - 1)))]
#[ensures(no_dup(s) ==> !s.subsequence(0, s.len() - 1).contains(s[s.len() - 1]))]
pub fn lemma_init_contains<T>(s: Seq<T>) {
    pearlite! {
        let t = s.subsequence(0, s.len() - 1);
        proof_assert!(forall<i> 0 <= i && i < t.len() ==> t[i] == s[i]);
        proof_assert!(s[s.len() - 1] == s[s.len() - 1]);
        proof_assert!(s.contains(s[s.len() - 1]))
    }
}

/// Prefixes of a sequence grow one element at a time.
#[logic]
#[requires(0 <= i && i < s.len())]
#[ensures(forall<x> s.subsequence(0, i + 1).contains(x)
    == (s.subsequence(0, i).contains(x) || x == s[i]))]
#[ensures(no_dup(s) ==> !s.subsequence(0, i).contains(s[i]))]
pub fn lemma_prefix_step<T>(s: Seq<T>, i: Int) {
    pearlite! {
        proof_assert!(forall<j> 0 <= j && j < i ==> s.subsequence(0, i + 1)[j] == s.subsequence(0, i)[j]);
        proof_assert!(forall<j> 0 <= j && j < i ==> s.subsequence(0, i)[j] == s[j]);
        proof_assert!(s.subsequence(0, i + 1)[i] == s[i])
    }
}

/// Counting gives at least zero and at most the length.
#[logic]
#[ensures(0 <= count_where(s, p) && count_where(s, p) <= s.len())]
#[variant(s.len())]
pub fn lemma_count_bounds<I>(s: Seq<I>, p: Mapping<I, bool>) {
    if s.len() > 0 {
        lemma_count_bounds(s.subsequence(0, s.len() - 1), p)
    }
}

/// Nothing is counted exactly when no element is accepted.
#[logic]
#[ensures((count_where(s, p) == 0) == (forall<x> s.contains(x) ==> !p[x]))]
#[variant(s.len())]
pub fn lemma_count_none<I>(s: Seq<I>, p: Mapping<I, bool>) {
    if s.len() > 0 {
        let t = s.subsequence(0, s.len() - 1);
        lemma_init_contains(s);
        lemma_count_bounds(t, p);
        lemma_count_none(t, p)
    }
}

/// Every element is counted when every element is accepted.
#[logic]
#[requires(forall<x> s.contains(x) ==> p[x])]
#[ensures(count_where(s, p) == s.len())]
#[variant(s.len())]
pub fn lemma_count_full<I>(s: Seq<I>, p: Mapping<I, bool>) {
    if s.len() > 0 {
        lemma_init_contains(s);
        lemma_count_full(s.subsequence(0, s.len() - 1), p)
    }
}

/// Counting only depends on what the predicate says about the elements.
#[logic]
#[requires(forall<x> s.contains(x) ==> p[x] == q[x])]
#[ensures(count_where(s, p) == count_where(s, q))]
#[variant(s.len())]
pub fn lemma_count_ext<I>(s: Seq<I>, p: Mapping<I, bool>, q: Mapping<I, bool>) {
    if s.len() > 0 {
        lemma_init_contains(s);
        lemma_count_ext(s.subsequence(0, s.len() - 1), p, q)
    }
}

/// Rejecting one more element `x`, which appears at most once, counts one less if it appears.
#[logic]
#[requires(no_dup(s))]
#[requires(forall<y> s.contains(y) && y != x ==> p[y] == q[y])]
#[requires(p[x] && !q[x])]
#[ensures(count_where(s, p) == count_where(s, q) + if s.contains(x) { 1 } else { 0 })]
#[variant(s.len())]
pub fn lemma_count_remove<I>(s: Seq<I>, p: Mapping<I, bool>, q: Mapping<I, bool>, x: I) {
    if s.len() > 0 {
        lemma_init_contains(s);
        lemma_count_remove(s.subsequence(0, s.len() - 1), p, q, x)
    }
}

/// Outputting the admitted node `x` decreases the pending count of its children by one, and
/// leaves the others unchanged.
#[logic]
#[requires(no_dup(nodes[k]@.from))]
#[requires(admitted[x] && !done.contains(x))]
#[ensures(pending_parents(nodes, admitted, done.snoc(x), k)
    + if nodes[k]@.from.contains(x) { 1 } else { 0 }
    == pending_parents(nodes, admitted, done, k))]
pub fn lemma_pending_snoc<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    done: Seq<Id<K>>,
    x: Id<K>,
    k: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_snoc_contains::<Id<K>>;
    pearlite! {
        proof_assert!(forall<y> done.snoc(x).contains(y) == (done.contains(y) || y == x));
        lemma_count_remove(
            nodes[k]@.from,
            |p| admitted[p] && !done.contains(p),
            |p| admitted[p] && !done.snoc(x).contains(p),
            x,
        )
    }
}

/// The pending count is nonnegative, and zero exactly when every admitted parent is done.
#[logic]
#[ensures(0 <= pending_parents(nodes, admitted, done, k))]
#[ensures((pending_parents(nodes, admitted, done, k) == 0)
    == (forall<p> admitted_parent(nodes, admitted, k, p) ==> done.contains(p)))]
pub fn lemma_pending_zero<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    done: Seq<Id<K>>,
    k: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        lemma_count_bounds(nodes[k]@.from, |p| admitted[p] && !done.contains(p));
        lemma_count_none(nodes[k]@.from, |p| admitted[p] && !done.contains(p))
    }
}

/// When no admitted parent is done, every admitted parent is pending.
#[logic]
#[requires(forall<p> admitted_parent(nodes, admitted, k, p) ==> !done.contains(p))]
#[ensures(pending_parents(nodes, admitted, done, k) == count_where(nodes[k]@.from, admitted))]
pub fn lemma_pending_fresh<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    done: Seq<Id<K>>,
    k: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        lemma_count_ext(nodes[k]@.from, |p| admitted[p] && !done.contains(p), admitted)
    }
}

/// Before anything is output, every parent is pending.
#[logic]
#[ensures(pending_parents(nodes, admit_all(), Seq::empty(), k) == nodes[k]@.from.len())]
pub fn lemma_pending_all<K, T, S>(nodes: Nodes<K, T, S>, k: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        lemma_count_full(
            nodes[k]@.from,
            |p| admit_all::<Id<K>>()[p] && !Seq::<Id<K>>::empty().contains(p),
        )
    }
}

/// The admitted parents of output nodes are output.
#[logic]
#[requires(kahn_order(nodes, admitted, output, stack))]
#[ensures(forall<k, p> output.contains(k) && admitted_parent(nodes, admitted, k, p) ==> output.contains(p))]
pub fn lemma_order_parents<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    output: Seq<Id<K>>,
    stack: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<i, j> 0 <= j && j < i && i <= output.len()
            ==> output.subsequence(0, i)[j] == output[j]);
        proof_assert!(forall<i, x> 0 <= i && i <= output.len() && output.subsequence(0, i).contains(x)
            ==> output.contains(x))
    }
}

/// The state of Kahn's algorithm between two nodes: `output` lists the nodes output so far,
/// `stack` the nodes ready to be output, and `counts` the number of admitted parents not yet output
/// of some admitted nodes, which are output or ready when it is zero.
#[logic(open)]
pub fn kahn_outer<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    output: Seq<Id<K>>,
    stack: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        kahn_order(nodes, admitted, output, stack)
            && (forall<k> counts.contains(&k) ==> nodes.contains(&k)
                && admitted[k]
                && counts[k]@ == pending_parents(nodes, admitted, output, k))
            && forall<k> counts.contains(&k) && counts[k]@ == 0 ==> output.contains(k) || stack.contains(k)
    }
}

/// Every admitted node with an admitted parent in `output` has a count.
#[logic(open)]
pub fn kahn_counted<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    output: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<k, p> nodes.contains(&k) && admitted[k] && admitted_parent(nodes, admitted, k, p)
            && output.contains(p) ==> counts.contains(&k)
    }
}

/// The state of Kahn's algorithm while the children of `id`, which has just been output after
/// `prev`, are visited: the counts of the children in `unvisited` don't account for `id` yet.
#[logic(open)]
pub fn kahn_inner<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    prev: Seq<Id<K>>,
    id: Id<K>,
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
        kahn_order(nodes, admitted, prev.snoc(id), stack)
            && !prev.contains(id)
            && nodes.contains(&id)
            && admitted[id]
            && (forall<k> nodes[id]@.to.contains(k) ==> !prev.snoc(id).contains(k))
            && (forall<k> counts.contains(&k) ==> nodes.contains(&k)
                && admitted[k]
                && counts[k]@ == pending_parents(nodes, admitted, prev.snoc(id), k)
                    + if unvisited.contains(k) { 1 } else { 0 })
            && (forall<k> counts.contains(&k) && counts[k]@ == 0
                ==> prev.snoc(id).contains(k) || stack.contains(k))
            && (forall<k> stack.contains(k) ==> !unvisited.contains(k))
            && forall<k, p> nodes.contains(&k) && admitted[k] && admitted_parent(nodes, admitted, k, p)
                && (prev.contains(p) || (p == id && !unvisited.contains(k))) ==> counts.contains(&k)
    }
}

/// Outputting the node `id` at the top of the stack starts visiting its children.
#[logic]
#[requires(all_linked(nodes))]
#[requires(kahn_outer(nodes, admitted, prev, rest.snoc(id), counts))]
#[requires(kahn_counted(nodes, admitted, prev, counts))]
#[ensures(kahn_inner(nodes, admitted, prev, id, nodes[id]@.to, rest, counts))]
pub fn lemma_kahn_pop<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    prev: Seq<Id<K>>,
    rest: Seq<Id<K>>,
    id: Id<K>,
    counts: FMap<Id<K>, usize>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_snoc_contains::<Id<K>>;
    let _ = lemma_pending_snoc::<K, T, S>;
    lemma_no_dup_snoc(rest, id);
    lemma_no_dup_snoc(prev, id);
    lemma_order_parents(nodes, admitted, prev, rest.snoc(id));
    crate::verification::lemma_full_prefix(prev);
    pearlite! {
        let out = prev.snoc(id);
        let to = nodes[id]@.to;
        proof_assert!(rest.snoc(id).contains(id));
        proof_assert!(!prev.contains(id) && nodes.contains(&id) && admitted[id]);
        proof_assert!(linked(nodes, id));
        proof_assert!(forall<k> to.contains(k) ==> nodes.contains(&k) && admitted_parent(nodes, admitted, k, id));
        proof_assert!(forall<k> nodes.contains(&k) ==> nodes[k]@.from.contains(id) == to.contains(k));
        proof_assert!(forall<k> to.contains(k) ==> !rest.contains(k));
        proof_assert!(forall<k> to.contains(k) ==> !out.contains(k));
        proof_assert!(forall<i> 0 <= i && i <= prev.len() ==> out.subsequence(0, i) == prev.subsequence(0, i));
        proof_assert!(forall<i> 0 <= i && i < out.len() ==> nodes.contains(&out[i]) && admitted[out[i]]);
        proof_assert!(forall<i, p> 0 <= i && i < out.len() && admitted_parent(nodes, admitted, out[i], p)
            ==> out.subsequence(0, i).contains(p));
        proof_assert!(forall<k> rest.contains(k) ==> !out.contains(k));
        proof_assert!(kahn_order(nodes, admitted, out, rest));
        proof_assert!(forall<k> counts.contains(&k) ==> nodes.contains(&k) && linked(nodes, k));
        proof_assert!(forall<k> counts.contains(&k) ==> counts[k]@
            == pending_parents(nodes, admitted, out, k) + if to.contains(k) { 1 } else { 0 })
    }
}

/// The count of the admitted child `nodes[id]@.to[j]` before it is decremented.
#[logic]
#[requires(all_linked(nodes))]
#[requires(0 <= j && j < nodes[id]@.to.len())]
#[requires(kahn_inner(nodes, admitted, prev, id, nodes[id]@.to.subsequence(0, j + 1), stack, counts))]
#[requires(admitted[nodes[id]@.to[j]])]
#[ensures(nodes.contains(&nodes[id]@.to[j]))]
#[ensures(0 <= pending_parents(nodes, admitted, prev.snoc(id), nodes[id]@.to[j]))]
#[ensures(counts.contains(&nodes[id]@.to[j]) ==> counts[nodes[id]@.to[j]]@
    == pending_parents(nodes, admitted, prev.snoc(id), nodes[id]@.to[j]) + 1)]
#[ensures(!counts.contains(&nodes[id]@.to[j]) ==> count_where(nodes[nodes[id]@.to[j]]@.from, admitted)
    == pending_parents(nodes, admitted, prev.snoc(id), nodes[id]@.to[j]) + 1)]
pub fn lemma_kahn_count<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    prev: Seq<Id<K>>,
    id: Id<K>,
    j: Int,
    stack: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let to = nodes[id]@.to;
        let child = to[j];
        lemma_prefix_step(to, j);
        proof_assert!(to.subsequence(0, j + 1).contains(child));
        proof_assert!(linked(nodes, id));
        proof_assert!(to.contains(child));
        proof_assert!(nodes.contains(&child) && linked(nodes, child));
        proof_assert!(admitted_parent(nodes, admitted, child, id));
        lemma_pending_zero(nodes, admitted, prev.snoc(id), child);
        lemma_pending_snoc(nodes, admitted, prev, id, child);
        proof_assert!(!counts.contains(&child)
            ==> forall<p> admitted_parent(nodes, admitted, child, p) ==> !prev.contains(p));
        proof_assert!(!counts.contains(&child) ==> {
            lemma_pending_fresh(nodes, admitted, prev, child);
            pending_parents(nodes, admitted, prev, child) == count_where(nodes[child]@.from, admitted)
        })
    }
}

/// Decrementing the count of the admitted child `nodes[id]@.to[j]` to `n`, and pushing it onto
/// the stack if `n` is zero, visits it.
#[logic]
#[requires(all_linked(nodes))]
#[requires(0 <= j && j < nodes[id]@.to.len())]
#[requires(kahn_inner(nodes, admitted, prev, id, nodes[id]@.to.subsequence(0, j + 1), stack, counts))]
#[requires(admitted[nodes[id]@.to[j]])]
#[requires(n@ == pending_parents(nodes, admitted, prev.snoc(id), nodes[id]@.to[j]))]
#[ensures(admitted_parent(nodes, admitted, nodes[id]@.to[j], id))]
#[ensures(kahn_inner(
    nodes,
    admitted,
    prev,
    id,
    nodes[id]@.to.subsequence(0, j),
    if n@ == 0 { stack.snoc(nodes[id]@.to[j]) } else { stack },
    counts.add(nodes[id]@.to[j], n),
))]
pub fn lemma_kahn_visit<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    prev: Seq<Id<K>>,
    id: Id<K>,
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
        let to = nodes[id]@.to;
        let child = to[j];
        let out = prev.snoc(id);
        let u = to.subsequence(0, j + 1);
        let u2 = to.subsequence(0, j);
        let counts2 = counts.add(child, n);
        let stack2 = if n@ == 0 { stack.snoc(child) } else { stack };
        lemma_prefix_step(to, j);
        proof_assert!(u.contains(child));
        proof_assert!(linked(nodes, id));
        proof_assert!(!u2.contains(child));
        proof_assert!(to.contains(child));
        proof_assert!(nodes.contains(&child) && admitted_parent(nodes, admitted, child, id));
        proof_assert!(!stack.contains(child) && !out.contains(child));
        lemma_pending_zero(nodes, admitted, out, child);
        lemma_no_dup_snoc(stack, child);
        proof_assert!(forall<k> stack2.contains(k) == (stack.contains(k) || (n@ == 0 && k == child)));
        proof_assert!(kahn_order(nodes, admitted, out, stack2));
        proof_assert!(forall<k> counts2.contains(&k) == (counts.contains(&k) || k == child));
        proof_assert!(forall<k> k != child ==> counts2[k] == counts[k]);
        proof_assert!(counts2[child] == n);
        proof_assert!(forall<k> counts2.contains(&k) ==> nodes.contains(&k)
            && admitted[k]
            && counts2[k]@ == pending_parents(nodes, admitted, out, k)
                + if u2.contains(k) { 1 } else { 0 });
        proof_assert!(forall<k> counts2.contains(&k) && counts2[k]@ == 0 ==> out.contains(k) || stack2.contains(k));
        proof_assert!(forall<k> stack2.contains(k) ==> !u2.contains(k))
    }
}

/// Skipping the child `nodes[id]@.to[j]`, which isn't admitted, visits it.
#[logic]
#[requires(0 <= j && j < nodes[id]@.to.len())]
#[requires(kahn_inner(nodes, admitted, prev, id, nodes[id]@.to.subsequence(0, j + 1), stack, counts))]
#[requires(!admitted[nodes[id]@.to[j]])]
#[ensures(kahn_inner(nodes, admitted, prev, id, nodes[id]@.to.subsequence(0, j), stack, counts))]
pub fn lemma_kahn_skip<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    prev: Seq<Id<K>>,
    id: Id<K>,
    j: Int,
    stack: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { lemma_prefix_step(nodes[id]@.to, j) }
}

/// Once every child of `id` has been visited, `id` is output.
#[logic]
#[requires(kahn_inner(nodes, admitted, prev, id, nodes[id]@.to.subsequence(0, 0), stack, counts))]
#[ensures(kahn_outer(nodes, admitted, prev.snoc(id), stack, counts))]
#[ensures(kahn_counted(nodes, admitted, prev.snoc(id), counts))]
pub fn lemma_kahn_next<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    prev: Seq<Id<K>>,
    id: Id<K>,
    stack: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_snoc_contains::<Id<K>>;
    pearlite! {
        proof_assert!(forall<k> !nodes[id]@.to.subsequence(0, 0).contains(k))
    }
}

/// The topological sort of the whole graph starts with the roots on the stack and the number of
/// parents of every node as its count.
#[logic]
#[requires(all_linked(nodes))]
#[requires(no_dup(stack))]
#[requires(forall<k> stack.contains(k) == (nodes.contains(&k) && nodes[k]@.from.len() == 0))]
#[requires(forall<k> counts.contains(&k) == nodes.contains(&k))]
#[requires(forall<k> nodes.contains(&k) ==> counts[k]@ == nodes[k]@.from.len())]
#[ensures(kahn_outer(nodes, admit_all(), Seq::empty(), stack, counts))]
pub fn lemma_kahn_start<K, T, S>(
    nodes: Nodes<K, T, S>,
    stack: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_pending_all::<K, T, S>;
    pearlite! {
        proof_assert!(forall<k> stack.contains(k) ==> forall<p> !nodes[k]@.from.contains(p));
        proof_assert!(forall<k> !Seq::<Id<K>>::empty().contains(k));
        proof_assert!(kahn_order(nodes, admit_all(), Seq::empty(), stack))
    }
}

/// A subgraph's topological sort starts with its root on the stack.
#[logic]
#[requires(kahn_state(nodes, admitted, output, counts))]
#[requires(nodes.contains(&root))]
#[requires(admitted[root])]
#[requires(forall<p> !admitted_parent(nodes, admitted, root, p))]
#[requires(!output.contains(root))]
#[ensures(kahn_outer(nodes, admitted, output, Seq::singleton(root), counts))]
#[ensures(kahn_counted(nodes, admitted, output, counts))]
pub fn lemma_kahn_resume<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    output: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
    root: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> Seq::singleton(root).contains(k) == (k == root));
        proof_assert!(forall<k> !Seq::<Id<K>>::empty().contains(k))
    }
}

/// A subgraph's topological sort ends when the stack is empty.
#[logic]
#[requires(kahn_outer(nodes, admitted, output, Seq::empty(), counts))]
#[requires(kahn_counted(nodes, admitted, output, counts))]
#[ensures(kahn_state(nodes, admitted, output, counts))]
pub fn lemma_kahn_end<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    output: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> !Seq::<Id<K>>::empty().contains(k))
    }
}

/// If every node whose parents are all in `done` is in `done`, every node of rank at most `n` is.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(forall<k> nodes.contains(&k) && (forall<p> nodes[k]@.from.contains(p) ==> done.contains(p))
    ==> done.contains(k))]
#[requires(0 <= n)]
#[ensures(forall<k> nodes.contains(&k) && rank[k] <= n ==> done.contains(k))]
#[variant(n)]
pub fn lemma_rank_done<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    done: Seq<Id<K>>,
    n: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if n > 0 {
        lemma_rank_done(nodes, rank, done, n - 1)
    }
}

/// If every node whose parents are all in `done` is in `done`, so is the node `k`.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(forall<k> nodes.contains(&k) && (forall<p> nodes[k]@.from.contains(p) ==> done.contains(p))
    ==> done.contains(k))]
#[requires(nodes.contains(&k))]
#[ensures(done.contains(k))]
pub fn lemma_rank_done_node<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    done: Seq<Id<K>>,
    k: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    lemma_rank_done(nodes, rank, done, rank[k])
}

/// If every node whose parents are all in `done` is in `done`, every node of an acyclic graph is.
#[logic]
#[requires(all_linked(nodes))]
#[requires(acyclic(nodes))]
#[requires(forall<k> nodes.contains(&k) && (forall<p> nodes[k]@.from.contains(p) ==> done.contains(p))
    ==> done.contains(k))]
#[ensures(forall<k> nodes.contains(&k) ==> done.contains(k))]
pub fn lemma_all_done<K, T, S>(nodes: Nodes<K, T, S>, done: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_rank_done_node::<K, T, S>;
    pearlite! {
        let rank = rank_of(nodes);
        proof_assert!(ranked(nodes, rank))
    }
}

/// When the topological sort of the whole graph ends, it has output every node.
#[logic]
#[requires(all_linked(nodes))]
#[requires(acyclic(nodes))]
#[requires(kahn_outer(nodes, admit_all(), output, Seq::empty(), counts))]
#[requires(forall<k> nodes.contains(&k) ==> counts.contains(&k))]
#[ensures(output.len() == nodes.len())]
#[ensures(topological(nodes, output))]
pub fn lemma_kahn_sorted_all<K, T, S>(
    nodes: Nodes<K, T, S>,
    output: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_pending_zero::<K, T, S>;
    pearlite! {
        proof_assert!(forall<k> !Seq::<Id<K>>::empty().contains(k));
        proof_assert!(forall<k, p> admitted_parent(nodes, admit_all(), k, p) == nodes[k]@.from.contains(p));
        proof_assert!(forall<k> nodes.contains(&k)
            && (forall<p> nodes[k]@.from.contains(p) ==> output.contains(p))
            ==> {
                lemma_pending_zero(nodes, admit_all(), output, k);
                counts[k]@ == 0
            });
        lemma_all_done(nodes, output);
        proof_assert!(forall<x> output.contains(x) == nodes.contains(&x));
        crate::dependent::verification::lemma_lists_len(output, nodes)
    }
}
