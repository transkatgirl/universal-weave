//! Definitions about the directed acyclic graph formed by
//! [`IndependentNode`](crate::independent::IndependentNode) links, used by the
//! specifications of the graph searches and topological sorts.

use core::hash::{BuildHasher, Hash};

use creusot_std::{
    logic::{FMap, FSet, Mapping},
    prelude::*,
};

use super::Nodes;
use crate::{
    IndependentContents,
    verification::{Id, no_dup},
};

/// Every parent of an element of `set` is in `set`.
#[logic(open)]
pub fn parent_closed<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<k, p> set.contains(&k) && nodes[k]@.from.contains(p) ==> set.contains(&p)
    }
}

/// Every child of an element of `set` is in `set`.
#[logic(open)]
pub fn child_closed<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<k, c> set.contains(&k) && nodes[k]@.to.contains(c) ==> set.contains(&c)
    }
}

/// `id` has a parent in `set`.
#[logic(open)]
pub fn has_parent_in<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>, id: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { exists<p> nodes[id]@.from.contains(p) && set.contains(&p) }
}

/// `roots` lists, without duplicates, exactly the elements of `set` which have no parents.
#[logic(open)]
pub fn roots_of<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>, roots: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        no_dup(roots)
            && forall<k> roots.contains(k) == (set.contains(&k) && nodes[k]@.from.len() == 0)
    }
}

/// `p` is a parent of `k` which `admitted` accepts.
#[logic(open)]
pub fn admitted_parent<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    k: Id<K>,
    p: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { nodes[k]@.from.contains(p) && admitted[p] }
}

/// The number of elements of `s` which `p` accepts.
#[logic(open)]
#[variant(s.len())]
pub fn count_where<I>(s: Seq<I>, p: Mapping<I, bool>) -> Int {
    pearlite! {
        if s.len() == 0 {
            0
        } else {
            count_where(s.subsequence(0, s.len() - 1), p) + if p[s[s.len() - 1]] { 1 } else { 0 }
        }
    }
}

/// The number of parents of `k` which `admitted` accepts and which aren't in `done`.
#[logic(open)]
pub fn pending_parents<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    done: Seq<Id<K>>,
    k: Id<K>,
) -> Int
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { count_where(nodes[k]@.from, |p| admitted[p] && !done.contains(p)) }
}

/// The nodes in `output` are admitted, each appears once and after its admitted parents, and the
/// nodes in `stack` are admitted, haven't been output, and have had their admitted parents output.
#[logic(open)]
pub fn kahn_order<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    output: Seq<Id<K>>,
    stack: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        no_dup(output)
            && no_dup(stack)
            && (forall<i> 0 <= i && i < output.len()
                ==> nodes.contains(&output[i]) && admitted[output[i]])
            && (forall<i, p> 0 <= i && i < output.len() && admitted_parent(nodes, admitted, output[i], p)
                ==> output.subsequence(0, i).contains(p))
            && forall<k> stack.contains(k) ==> !output.contains(k)
                && nodes.contains(&k)
                && admitted[k]
                && forall<p> admitted_parent(nodes, admitted, k, p) ==> output.contains(p)
    }
}

/// `value` lists each node once, every node after its parents: spec version of the former
/// `valid_topological_sort`.
#[logic(open)]
pub fn topological<K, T, S>(nodes: Nodes<K, T, S>, value: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        no_dup(value)
            && (forall<i> 0 <= i && i < value.len() ==> nodes.contains(&value[i]))
            && forall<i, p> 0 <= i && i < value.len() && nodes[value[i]]@.from.contains(p)
                ==> value.subsequence(0, i).contains(p)
    }
}

/// `d` is `a` or one of its descendants: it belongs to every child-closed set containing `a`.
#[logic(open)]
pub fn descends<K, T, S>(nodes: Nodes<K, T, S>, d: Id<K>, a: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<set: FSet<Id<K>>> set.contains(&a) && child_closed(nodes, set) ==> set.contains(&d)
    }
}

/// A node descends from itself.
#[logic]
#[ensures(descends(nodes, d, d))]
pub fn lemma_descends_refl<K, T, S>(nodes: Nodes<K, T, S>, d: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// A child of `x` descends from whatever `x` descends from.
#[logic]
#[requires(nodes[x]@.to.contains(c))]
#[requires(descends(nodes, x, a))]
#[ensures(descends(nodes, c, a))]
pub fn lemma_descends_child<K, T, S>(nodes: Nodes<K, T, S>, x: Id<K>, c: Id<K>, a: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// Whatever descends from a child `c` of `p` descends from `p`.
#[logic]
#[requires(nodes[p]@.to.contains(c))]
#[requires(descends(nodes, d, c))]
#[ensures(descends(nodes, d, p))]
pub fn lemma_descends_parent<K, T, S>(nodes: Nodes<K, T, S>, d: Id<K>, c: Id<K>, p: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// Descending is transitive.
#[logic]
#[requires(descends(nodes, x, y))]
#[requires(descends(nodes, y, z))]
#[ensures(descends(nodes, x, z))]
pub fn lemma_descends_trans<K, T, S>(nodes: Nodes<K, T, S>, x: Id<K>, y: Id<K>, z: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// Nodes closed under taking parents which include `d` include every node `d` descends from.
#[logic]
#[requires(super::all_linked(nodes))]
#[requires(forall<k, p> closed[k] && nodes.contains(&k) && nodes[k]@.from.contains(p) ==> closed[p])]
#[requires(closed[d])]
#[requires(nodes.contains(&a))]
#[requires(descends(nodes, d, a))]
#[ensures(closed[a])]
pub fn lemma_descends_closed<K, T, S>(
    nodes: Nodes<K, T, S>,
    closed: Mapping<Id<K>, bool>,
    d: Id<K>,
    a: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let open = nodes.keys().filter(|x: Id<K>| !closed[x]);
        proof_assert!(forall<x> open.contains(&x) == (nodes.contains(&x) && !closed[x]));
        proof_assert!(forall<x, c> open.contains(&x) && nodes[x]@.to.contains(c)
            ==> super::linked(nodes, x) && nodes.contains(&c) && super::linked(nodes, c)
                && nodes[c]@.from.contains(x));
        proof_assert!(child_closed(nodes, open));
        proof_assert!(!closed[a] ==> open.contains(&a) && open.contains(&d))
    }
}

/// A node only descends from nodes of at most its rank.
#[logic]
#[requires(super::all_linked(nodes))]
#[requires(super::ranked(nodes, rank))]
#[requires(nodes.contains(&a))]
#[requires(descends(nodes, d, a))]
#[ensures(nodes.contains(&d) && rank[a] <= rank[d])]
pub fn lemma_descends_rank<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    d: Id<K>,
    a: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let below = nodes.keys().filter(|x: Id<K>| rank[a] <= rank[x]);
        proof_assert!(forall<x> below.contains(&x) == (nodes.contains(&x) && rank[a] <= rank[x]));
        proof_assert!(forall<x, c> below.contains(&x) && nodes[x]@.to.contains(c)
            ==> super::linked(nodes, x) && nodes.contains(&c) && nodes[c]@.from.contains(x)
                && rank[x] < rank[c]);
        proof_assert!(child_closed(nodes, below));
        proof_assert!(below.contains(&a))
    }
}

/// The state of `topological_sort_subgraph` between calls, which outputs the nodes accepted by
/// `admitted` in topological order (Kahn's algorithm): `output` lists the nodes output so far, and
/// `counts` the number of accepted parents not yet output of each accepted node with an accepted
/// parent which has been output.
#[logic(open)]
pub fn kahn_state<K, T, S>(
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
        kahn_order(nodes, admitted, output, Seq::empty())
            && (forall<k> counts.contains(&k) ==> nodes.contains(&k)
                && admitted[k]
                && counts[k]@ == pending_parents(nodes, admitted, output, k))
            && (forall<k> counts.contains(&k) && counts[k]@ == 0 ==> output.contains(k))
            && forall<k, p> nodes.contains(&k) && admitted[k] && admitted_parent(nodes, admitted, k, p)
                && output.contains(p) ==> counts.contains(&k)
    }
}

/// `chain` is the only path through the nodes of `order` accepted by `candidate`: a path to a root
/// through exactly those nodes, which `order` lists in topological order.
#[logic(open)]
pub fn candidate_chain<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    chain: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        super::valid_path(nodes, chain)
            && no_dup(chain)
            && no_dup(order)
            && (forall<j> 0 <= j && j < chain.len() ==> candidate[chain[j]] && order.contains(chain[j]))
            && (forall<i> 0 <= i && i < order.len() && candidate[order[i]] ==> chain.contains(order[i]))
            && forall<i, j> 0 <= i && i < order.len() && 0 <= j && j < order.len()
                && candidate[order[i]] && candidate[order[j]] && nodes[order[j]]@.from.contains(order[i])
                ==> i < j
    }
}

/// Counting over one more element of a prefix.
#[logic]
#[requires(0 <= i && i < s.len())]
#[ensures(count_where(s.subsequence(0, i + 1), p)
    == count_where(s.subsequence(0, i), p) + if p[s[i]] { 1 } else { 0 })]
#[ensures(0 <= count_where(s.subsequence(0, i), p) && count_where(s.subsequence(0, i), p) <= i)]
#[variant(i)]
pub fn lemma_count_prefix<I>(s: Seq<I>, i: Int, p: Mapping<I, bool>) {
    pearlite! {
        proof_assert!(s.subsequence(0, i + 1).subsequence(0, i).ext_eq(s.subsequence(0, i)));
        if i > 0 {
            lemma_count_prefix(s, i - 1, p)
        }
    }
}
