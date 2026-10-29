//! Lemmas about the results of the graph searches and topological sorts, used by the operations
//! which list nodes.

use core::hash::{BuildHasher, Hash};

use creusot_std::logic::{FMap, FSet, Mapping, such_that};

use super::*;
use crate::{
    IndependentContents,
    verification::{Id, first, no_dup},
};

/// `count_where` is zero when the predicate rejects every element.
#[logic]
#[requires(forall<i> 0 <= i && i < s.len() ==> !p[s[i]])]
#[ensures(count_where(s, p) == 0)]
#[variant(s.len())]
pub fn lemma_count_zero<I>(s: Seq<I>, p: Mapping<I, bool>) {
    if s.len() > 0 {
        lemma_count_zero(s.subsequence(0, s.len() - 1), p)
    }
}

/// Every admitted node of rank below `n` has been output, as long as every admitted node which
/// hasn't been output has an admitted parent.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(kahn_state(nodes, admitted, output, counts))]
#[requires(forall<k> nodes.contains(&k) && admitted[k] && !output.contains(k)
    ==> exists<p> admitted_parent(nodes, admitted, k, p))]
#[ensures(forall<k> nodes.contains(&k) && admitted[k] && rank[k] < n ==> output.contains(k))]
#[variant(n)]
pub fn lemma_kahn_complete_below<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    admitted: Mapping<Id<K>, bool>,
    output: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
    n: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if n > 0 {
        lemma_kahn_complete_below(nodes, rank, admitted, output, counts, n - 1);
        pearlite! {
            proof_assert!(forall<k> nodes.contains(&k) && admitted[k] && rank[k] == n - 1
                && !output.contains(k) ==> {
                    let p = such_that(|p| admitted_parent(nodes, admitted, k, p));
                    proof_assert!(linked(nodes, k) && nodes.contains(&p));
                    proof_assert!(forall<q> admitted_parent(nodes, admitted, k, q)
                        ==> nodes.contains(&q) && rank[q] < n - 1 && output.contains(q));
                    lemma_count_zero(nodes[k]@.from, |q| admitted[q] && !output.contains(q));
                    proof_assert!(counts.contains(&k));
                    false
                })
        }
    }
}

/// The admitted node `k` has been output, as long as every admitted node which hasn't been output
/// has an admitted parent.
#[logic]
#[requires(all_linked(nodes) && acyclic(nodes))]
#[requires(kahn_state(nodes, admitted, output, counts))]
#[requires(forall<k> nodes.contains(&k) && admitted[k] && !output.contains(k)
    ==> exists<p> admitted_parent(nodes, admitted, k, p))]
#[ensures(nodes.contains(&k) && admitted[k] ==> output.contains(k))]
pub fn lemma_kahn_complete_at<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    output: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
    k: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let rank = rank_of(nodes);
        lemma_kahn_complete_below(nodes, rank, admitted, output, counts, rank[k] + 1)
    }
}

/// Every admitted node has been output, as long as every admitted node which hasn't been output
/// has an admitted parent.
#[logic]
#[requires(all_linked(nodes) && acyclic(nodes))]
#[requires(kahn_state(nodes, admitted, output, counts))]
#[requires(forall<k> nodes.contains(&k) && admitted[k] && !output.contains(k)
    ==> exists<p> admitted_parent(nodes, admitted, k, p))]
#[ensures(forall<k> nodes.contains(&k) && admitted[k] ==> output.contains(k))]
pub fn lemma_kahn_complete<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    output: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_kahn_complete_at::<K, T, S>;
}

/// The empty state of a topological sort.
#[logic]
#[ensures(kahn_state(nodes, admitted, Seq::empty(), FMap::empty()))]
pub fn lemma_kahn_empty<K, T, S>(nodes: Nodes<K, T, S>, admitted: Mapping<Id<K>, bool>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// In a set whose elements other than `id` all have a parent in it, `id` has the lowest rank.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(forall<x> set.contains(&x) ==> nodes.contains(&x))]
#[requires(forall<x> set.contains(&x) && x != id ==> has_parent_in(nodes, set, x))]
#[requires(set.contains(&k))]
#[ensures(rank[id] <= rank[k])]
#[variant(rank[k])]
pub fn lemma_closure_rank<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    set: FSet<Id<K>>,
    id: Id<K>,
    k: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if k != id {
        pearlite! {
            let q = such_that(|q| nodes[k]@.from.contains(q) && set.contains(&q));
            proof_assert!(rank[q] < rank[k]);
            lemma_closure_rank(nodes, rank, set, id, q)
        }
    }
}

/// In a set whose elements other than `id` all have a parent in it, every element descends
/// from `id`.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(forall<x> set.contains(&x) ==> nodes.contains(&x))]
#[requires(forall<x> set.contains(&x) && x != id ==> has_parent_in(nodes, set, x))]
#[requires(set.contains(&k))]
#[requires(other.contains(&id) && child_closed(nodes, other))]
#[ensures(other.contains(&k))]
#[variant(rank[k])]
pub fn lemma_closure_descends<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    set: FSet<Id<K>>,
    id: Id<K>,
    k: Id<K>,
    other: FSet<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if k != id {
        pearlite! {
            let q = such_that(|q| nodes[k]@.from.contains(q) && set.contains(&q));
            proof_assert!(rank[q] < rank[k] && linked(nodes, k) && nodes[q]@.to.contains(k));
            lemma_closure_descends(nodes, rank, set, id, q, other)
        }
    }
}

/// A sequence without duplicates of exactly the elements of `set` is as long as `set`.
#[logic]
#[requires(no_dup(s))]
#[requires(forall<x> s.contains(x) == set.contains(&x))]
#[ensures(s.len() == set.len())]
#[variant(s.len())]
pub fn lemma_lists_set_len<I>(s: Seq<I>, set: FSet<I>) {
    if s.len() > 0 {
        let x = s[s.len() - 1];
        let t = s.subsequence(0, s.len() - 1);
        pearlite! {
            proof_assert!(forall<y> t.contains(y) == (s.contains(y) && y != x));
            lemma_lists_set_len(t, set.remove(x))
        }
    } else {
        pearlite! { proof_assert!(set.ext_eq(FSet::empty())) }
    }
}

/// A topological sort of exactly the nodes of a path makes it the only candidate chain.
#[logic]
#[requires(all_linked(nodes) && acyclic(nodes))]
#[requires(path_of(nodes, set, path))]
#[requires(forall<k> admitted[k] == set.contains(&k))]
#[requires(kahn_order(nodes, admitted, order, Seq::empty()))]
#[requires(forall<k> set.contains(&k) ==> order.contains(k))]
#[ensures(candidate_chain(nodes, order, admitted, path))]
pub fn lemma_path_chain<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    admitted: Mapping<Id<K>, bool>,
    path: Seq<Id<K>>,
    order: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<i, j> 0 <= i && i < order.len() && 0 <= j && j < order.len()
            && set.contains(&order[i]) && set.contains(&order[j])
            && nodes[order[j]]@.from.contains(order[i])
            ==> order.subsequence(0, j).contains(order[i]) && {
                let i2 = such_that(|i2: Int| 0 <= i2 && i2 < j && order.subsequence(0, j)[i2] == order[i]);
                proof_assert!(order[i2] == order[i]);
                i2 == i
            } && i < j)
    }
}

/// A set closed under taking parents which contains `k` contains a root.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(forall<x> set.contains(x) ==> nodes.contains(&x))]
#[requires(forall<x, p> set.contains(x) && nodes[x]@.from.contains(p) ==> set.contains(p))]
#[requires(set.contains(k))]
#[ensures(exists<r> set.contains(r) && nodes[r]@.from.len() == 0)]
#[variant(rank[k])]
pub fn lemma_has_root<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    set: Seq<Id<K>>,
    k: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        if nodes[k]@.from.len() != 0 {
            let p = nodes[k]@.from[0];
            proof_assert!(nodes[k]@.from.contains(p) && rank[p] < rank[k]);
            lemma_has_root(nodes, rank, set, p)
        }
    }
}

/// In a set whose elements other than `id` all have a child in it, every element is an ancestor
/// of `id`: it belongs to every set closed under taking parents which contains `id`.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(forall<x> set.contains(&x) ==> nodes.contains(&x))]
#[requires(forall<x> set.contains(&x) && x != id ==> has_child_in(nodes, set, x))]
#[requires(set.contains(&k))]
#[requires(other.contains(id))]
#[requires(forall<x, p> other.contains(x) && nodes[x]@.from.contains(p) ==> other.contains(p))]
#[requires(forall<x> set.contains(&x) ==> rank[x] < bound)]
#[ensures(other.contains(k))]
#[variant(bound - rank[k])]
pub fn lemma_closure_ancestor<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    set: FSet<Id<K>>,
    id: Id<K>,
    k: Id<K>,
    other: Seq<Id<K>>,
    bound: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if k != id {
        pearlite! {
            let c = such_that(|c| nodes[k]@.to.contains(c) && set.contains(&c));
            proof_assert!(linked(nodes, k) && nodes[c]@.from.contains(k) && rank[k] < rank[c]);
            lemma_closure_ancestor(nodes, rank, set, id, c, other, bound)
        }
    }
}

/// `down` goes from parent to child and ends at `id`.
#[logic(open)]
pub fn descending_to<K, T, S>(nodes: Nodes<K, T, S>, down: Seq<Id<K>>, id: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        0 < down.len()
            && down[down.len() - 1] == id
            && no_dup(down)
            && (forall<i> 0 <= i && i < down.len() ==> nodes.contains(&down[i]))
            && forall<i> 0 <= i && i + 1 < down.len() ==> nodes[down[i + 1]]@.from.contains(down[i])
    }
}

/// Reversing a descent from a root to `id` gives a path from `id` to that root.
#[logic]
#[requires(descending_to(nodes, down, id))]
#[requires(nodes[down[0]]@.from.len() == 0)]
#[ensures(valid_path(nodes, down.reverse()))]
#[ensures(no_dup(down.reverse()))]
#[ensures(first(down.reverse()) == Some(id))]
pub fn lemma_reverse_descent<K, T, S>(nodes: Nodes<K, T, S>, down: Seq<Id<K>>, id: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let up = down.reverse();
        let n = down.len();
        proof_assert!(up.len() == n);
        proof_assert!(forall<i> 0 <= i && i < n ==> up[i] == down[n - 1 - i]);
        crate::verification::lemma_reverse(down);
        proof_assert!(forall<i> 0 <= i && i + 1 < n ==> nodes[up[i]]@.from.contains(up[i + 1]));
        proof_assert!(up[n - 1] == down[0])
    }
}

/// Joining a descent from the first node of a path to a root down to `id` with that path, without
/// repeating its first node, gives a path from `id` to the root.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(descending_to(nodes, down, id))]
#[requires(valid_path(nodes, up) && no_dup(up))]
#[requires(0 < up.len() && up[0] == down[0])]
#[ensures(valid_path(nodes, down.reverse().subsequence(0, down.len() - 1).concat(up)))]
#[ensures(no_dup(down.reverse().subsequence(0, down.len() - 1).concat(up)))]
#[ensures(first(down.reverse().subsequence(0, down.len() - 1).concat(up)) == Some(id))]
pub fn lemma_join_descent<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    down: Seq<Id<K>>,
    up: Seq<Id<K>>,
    id: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = down.len();
        let head = down.reverse().subsequence(0, n - 1);
        let joined = head.concat(up);
        proof_assert!(head.len() == n - 1);
        proof_assert!(forall<i> 0 <= i && i < n - 1 ==> head[i] == down[n - 1 - i]);
        proof_assert!(joined.len() == n - 1 + up.len());
        proof_assert!(forall<i> 0 <= i && i < n - 1 ==> joined[i] == down[n - 1 - i]);
        proof_assert!(forall<i> 0 <= i && i < up.len() ==> joined[n - 1 + i] == up[i]);
        lemma_path_ranks_all(nodes, rank, up);
        lemma_descent_ranks(nodes, rank, down, id);
        proof_assert!(forall<i> 0 <= i && i < up.len() ==> rank[up[i]] <= rank[down[0]]);
        proof_assert!(forall<j> 1 <= j && j < n ==> rank[down[0]] < rank[down[j]]);
        proof_assert!(forall<i, j> 0 <= i && i < n - 1 && 0 <= j && j < up.len() ==> head[i] != up[j]);
        proof_assert!(forall<i> 0 <= i && i < joined.len() ==> nodes.contains(&joined[i]));
        proof_assert!(forall<i> 0 <= i && i + 1 < n - 1 ==> nodes[joined[i]]@.from.contains(joined[i + 1]));
        proof_assert!(1 < n ==> nodes[joined[n - 2]]@.from.contains(joined[n - 1]));
        proof_assert!(forall<i> n - 1 <= i && i + 1 < joined.len() ==> nodes[joined[i]]@.from.contains(joined[i + 1]));
        proof_assert!(joined[joined.len() - 1] == up[up.len() - 1]);
        proof_assert!(valid_path(nodes, joined));
        proof_assert!(forall<i, j> 0 <= i && i < j && j < n - 1 ==> head[i] != head[j]);
        proof_assert!(no_dup(joined));
        proof_assert!(joined[0] == id)
    }
}

/// Ranks increase along a descent.
#[logic]
#[requires(ranked(nodes, rank))]
#[requires(descending_to(nodes, down, id))]
#[ensures(forall<i, j> 0 <= i && i < j && j < down.len() ==> rank[down[i]] < rank[down[j]])]
pub fn lemma_descent_ranks<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    down: Seq<Id<K>>,
    id: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(crate::verification::increasing_steps(down, rank));
        crate::verification::lemma_increasing(down, rank)
    }
}

/// [`lemma_closure_ancestor`] as a fact: asserting it puts the lemma's instance in context.
#[logic]
#[ensures(result == (all_linked(nodes)
    && ranked(nodes, rank)
    && (forall<x> set.contains(&x) ==> nodes.contains(&x))
    && (forall<x> set.contains(&x) && x != id ==> has_child_in(nodes, set, x))
    && set.contains(&k)
    && other.contains(id)
    && (forall<x, p> other.contains(x) && nodes[x]@.from.contains(p) ==> other.contains(p))
    && forall<x> set.contains(&x) ==> rank[x] < bound))]
#[ensures(result ==> other.contains(k))]
pub fn closure_ancestor_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    set: FSet<Id<K>>,
    id: Id<K>,
    k: Id<K>,
    other: Seq<Id<K>>,
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
            && (forall<x> set.contains(&x) ==> nodes.contains(&x))
            && (forall<x> set.contains(&x) && x != id ==> has_child_in(nodes, set, x))
            && set.contains(&k)
            && other.contains(id)
            && (forall<x, p> other.contains(x) && nodes[x]@.from.contains(p) ==> other.contains(p))
            && forall<x> set.contains(&x) ==> rank[x] < bound;
        proof_assert!(pre ==> {
            lemma_closure_ancestor(nodes, rank, set, id, k, other, bound);
            true
        } && other.contains(k));
        pre
    }
}

/// If `p` accepts the `i`th element of `s`, it accepts a first one.
#[logic]
#[requires(0 <= i && i < s.len() && p[s[i]])]
#[ensures(exists<j> 0 <= j && j <= i && p[s[j]] && forall<m> 0 <= m && m < j ==> !p[s[m]])]
#[variant(i)]
pub fn lemma_first_where<I>(s: Seq<I>, p: Mapping<I, bool>, i: Int) {
    pearlite! {
        if exists<m> 0 <= m && m < i && p[s[m]] {
            let m = such_that(|m: Int| 0 <= m && m < i && p[s[m]]);
            lemma_first_where(s, p, m)
        }
    }
}

/// Nodes closed under taking parents which include the first node of a path include all of it.
#[logic]
#[requires(valid_path(nodes, path))]
#[requires(forall<k, p> a[k] && nodes.contains(&k) && nodes[k]@.from.contains(p) ==> a[p])]
#[requires(0 <= i && i < path.len() && a[path[0]])]
#[ensures(a[path[i]])]
#[variant(i)]
pub fn lemma_closed_path_at<K, T, S>(
    nodes: Nodes<K, T, S>,
    a: Mapping<Id<K>, bool>,
    path: Seq<Id<K>>,
    i: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if i > 0 {
        lemma_closed_path_at(nodes, a, path, i - 1)
    }
}

/// When nodes `anc`, closed under taking parents, include an active node, the active nodes among
/// them form a path to a root: the end of the active path from its first node in `anc`. A
/// topological order of the active nodes in `anc` which includes its active roots makes that path
/// its only candidate chain.
#[logic]
#[requires(all_linked(nodes) && acyclic(nodes))]
#[requires(path_of(nodes, active, path))]
#[requires(parent_closed(nodes, anc))]
#[requires(forall<k> admitted[k] == (active.contains(&k) && anc.contains(&k)))]
#[requires(kahn_state(nodes, admitted, order, counts))]
#[requires(forall<k> admitted[k] && nodes[k]@.from.len() == 0 ==> order.contains(k))]
#[requires(exists<k> admitted[k])]
#[ensures(exists<q> path_of(nodes, active.intersection(anc), q)
    && candidate_chain(nodes, order, admitted, q))]
pub fn lemma_active_ancestors<K, T, S>(
    nodes: Nodes<K, T, S>,
    active: FSet<Id<K>>,
    anc: FSet<Id<K>>,
    admitted: Mapping<Id<K>, bool>,
    order: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
    path: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let k = such_that(|k| admitted[k]);
        proof_assert!(path.contains(k));
        let i = such_that(|i: Int| 0 <= i && i < path.len() && path[i] == k);
        let inside = crate::verification::member_of(anc);
        proof_assert!(forall<x> inside[x] == anc.contains(&x));
        proof_assert!(inside[path[i]]);
        lemma_first_where(path, inside, i);
        let j = such_that(|j: Int| 0 <= j && j <= i && inside[path[j]]
            && forall<m> 0 <= m && m < j ==> !inside[path[m]]);
        let q = path.subsequence(j, path.len());
        proof_assert!(forall<m> 0 <= m && m < q.len() ==> q[m] == path[j + m]);
        proof_assert!(valid_path(nodes, q));
        proof_assert!(forall<m> 0 <= m && m < q.len() ==> {
            lemma_closed_path_at(nodes, inside, q, m);
            anc.contains(&q[m])
        });
        let set = active.intersection(anc);
        proof_assert!(forall<x> set.contains(&x) == (active.contains(&x) && anc.contains(&x)));
        proof_assert!(forall<x> q.contains(x) ==> set.contains(&x));
        proof_assert!(forall<m> 0 <= m && m < path.len() && anc.contains(&path[m])
            ==> j <= m && q[m - j] == path[m]);
        proof_assert!(forall<x> set.contains(&x) ==> path.contains(x));
        proof_assert!(forall<x> set.contains(&x) ==> q.contains(x));
        proof_assert!(path_of(nodes, set, q));
        let r = q[q.len() - 1];
        proof_assert!(admitted[r] && nodes[r]@.from.len() == 0 && order.contains(r));
        proof_assert!(forall<m> 0 <= m && m + 1 < q.len()
            ==> nodes[q[m]]@.from.contains(q[m + 1]) && admitted[q[m + 1]]
                && admitted_parent(nodes, admitted, q[m], q[m + 1]));
        proof_assert!(forall<x> admitted[x] ==> exists<m> 0 <= m && m < q.len() && q[m] == x);
        proof_assert!(forall<x> admitted[x] && x != r ==> exists<m> 0 <= m && m + 1 < q.len() && q[m] == x);
        proof_assert!(forall<x> nodes.contains(&x) && admitted[x] && !order.contains(x)
            ==> x != r && exists<m> 0 <= m && m + 1 < q.len() && q[m] == x
                && admitted_parent(nodes, admitted, x, q[m + 1]));
        lemma_kahn_complete(nodes, admitted, order, counts);
        proof_assert!(forall<x> set.contains(&x) ==> order.contains(x));
        lemma_path_chain(nodes, set, admitted, q, order)
    }
}
