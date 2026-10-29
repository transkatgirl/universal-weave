//! Lemmas used to verify the validation functions in `crate::independent::support`.
//!
//! `valid_topology` runs a depth-first search from every root, marking each node it enters as in
//! progress (`false`) and each node it exits as finished (`true`), and lists the finished nodes in
//! the order they were exited. [`vt_dfs`] describes the state of that search.

use core::hash::{BuildHasher, Hash};

use creusot_std::logic::{FMap, FSet, Mapping, such_that};

#[allow(unused_imports)]
use super::*;
#[allow(unused_imports)]
use crate::{
    IndependentContents, Step,
    independent::IndependentNode,
    verification::{Id, Model, no_dup},
};

/// `step` enters the node `k`.
#[logic(open(crate))]
pub(crate) fn vt_enters<K: DeepModel>(step: Step<K, K>, k: Id<K>) -> bool {
    pearlite! {
        match step {
            Step::Enter(e) => e.deep_model() == k,
            Step::Exit(_) => false,
        }
    }
}

/// `step` exits the node `k`.
#[logic(open(crate))]
pub(crate) fn vt_exits<K: DeepModel>(step: Step<K, K>, k: Id<K>) -> bool {
    pearlite! {
        match step {
            Step::Exit(e) => e.deep_model() == k,
            Step::Enter(_) => false,
        }
    }
}

/// Every child of a node of `order` comes before it in `order`.
#[logic(open)]
pub fn vt_children_first<K, T, S>(nodes: Nodes<K, T, S>, order: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<j, c> 0 <= j && j < order.len() && nodes[order[j]]@.to.contains(c)
            ==> order.subsequence(0, j).contains(c)
    }
}

/// The marks of the search: `map` marks nodes, `gray` lists, without duplicates, the nodes in
/// progress (marked `false`), and `finished` lists, without duplicates, the finished nodes
/// (marked `true`), each after its children.
#[logic(open)]
pub fn vt_marks<K, T, S>(
    nodes: Nodes<K, T, S>,
    map: FMap<Id<K>, bool>,
    finished: Seq<Id<K>>,
    gray: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (forall<k> map.contains(&k) ==> nodes.contains(&k))
            && no_dup(gray)
            && (forall<k> (map.contains(&k) && !map[k]) == gray.contains(k))
            && no_dup(finished)
            && (forall<k> (map.contains(&k) && map[k]) == finished.contains(k))
            && vt_children_first(nodes, finished)
    }
}

/// The shape of the search's stack: the nodes in progress `gray` form a chain from parent to
/// child, `gray[t]` is exited at position `exits[t]`, the only `Exit` steps are those, and every
/// node entered above `exits[t]` (and below `exits[t + 1]`) is a child of `gray[t]`.
#[logic(open(crate))]
pub(crate) fn vt_shape<K, T, S>(
    nodes: Nodes<K, T, S>,
    stack: Seq<Step<K, K>>,
    gray: Seq<Id<K>>,
    exits: Seq<Int>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        exits.len() == gray.len()
            && (forall<t> 0 <= t && t < exits.len()
                ==> 0 <= exits[t] && exits[t] < stack.len() && vt_exits(stack[exits[t]], gray[t]))
            && (forall<t, u> 0 <= t && t < u && u < exits.len() ==> exits[t] < exits[u])
            && (forall<q> 0 <= q && q < stack.len() ==> match stack[q] {
                Step::Exit(_) => exists<t> 0 <= t && t < exits.len() && exits[t] == q,
                Step::Enter(e) => nodes.contains(&e.deep_model())
                    && forall<t> 0 <= t && t < exits.len() && exits[t] < q
                        && (t + 1 == exits.len() || q < exits[t + 1])
                        ==> nodes[gray[t]]@.to.contains(e.deep_model()),
            })
            && forall<t> 0 <= t && t + 1 < gray.len() ==> nodes[gray[t]]@.to.contains(gray[t + 1])
    }
}

/// Every child of a node in progress `gray[t]` is finished, entered above `exits[t]`, or in
/// progress after `gray[t]`.
#[logic(open(crate))]
pub(crate) fn vt_pending<K, T, S>(
    nodes: Nodes<K, T, S>,
    map: FMap<Id<K>, bool>,
    stack: Seq<Step<K, K>>,
    gray: Seq<Id<K>>,
    exits: Seq<Int>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<t, c> 0 <= t && t < gray.len() && nodes[gray[t]]@.to.contains(c)
            ==> (map.contains(&c) && map[c])
                || (exists<q> exits[t] < q && q < stack.len() && vt_enters(stack[q], c))
                || exists<u> t < u && u < gray.len() && gray[u] == c
    }
}

/// The state of the depth-first search of `valid_topology`.
#[logic(open(crate))]
pub(crate) fn vt_dfs<K, T, S>(
    nodes: Nodes<K, T, S>,
    map: FMap<Id<K>, bool>,
    stack: Seq<Step<K, K>>,
    finished: Seq<Id<K>>,
    gray: Seq<Id<K>>,
    exits: Seq<Int>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        vt_marks(nodes, map, finished, gray)
            && vt_shape(nodes, stack, gray, exits)
            && vt_pending(nodes, map, stack, gray, exits)
    }
}

/// Starting a search from the node `root`.
#[logic]
#[requires(vt_dfs(nodes, map, stack, finished, Seq::empty(), Seq::empty()))]
#[requires(stack.len() == 0)]
#[requires(nodes.contains(&root.deep_model()))]
#[ensures(vt_dfs(nodes, map, stack.snoc(Step::Enter(root)), finished, Seq::empty(), Seq::empty()))]
pub(crate) fn lemma_vt_start<K, T, S>(
    nodes: Nodes<K, T, S>,
    map: FMap<Id<K>, bool>,
    stack: Seq<Step<K, K>>,
    finished: Seq<Id<K>>,
    root: K,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(stack.snoc(Step::Enter(root))[0] == Step::Enter(root));
        proof_assert!(stack.snoc(Step::Enter(root)).len() == 1)
    }
}

/// When the stack is empty, no node is in progress.
#[logic]
#[requires(vt_dfs(nodes, map, stack, finished, gray, exits))]
#[requires(stack.len() == 0)]
#[ensures(vt_dfs(nodes, map, stack, finished, Seq::empty(), Seq::empty()))]
#[ensures(forall<k> map.contains(&k) ==> map[k])]
pub(crate) fn lemma_vt_stop<K, T, S>(
    nodes: Nodes<K, T, S>,
    map: FMap<Id<K>, bool>,
    stack: Seq<Step<K, K>>,
    finished: Seq<Id<K>>,
    gray: Seq<Id<K>>,
    exits: Seq<Int>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(exits.len() == 0);
        proof_assert!(gray.ext_eq(Seq::empty()));
        proof_assert!(exits.ext_eq(Seq::empty()))
    }
}

/// Popping a step which enters a finished node.
#[logic]
#[requires(vt_dfs(nodes, map, rest.snoc(Step::Enter(c)), finished, gray, exits))]
#[requires(map.contains(&c.deep_model()) && map[c.deep_model()])]
#[ensures(vt_dfs(nodes, map, rest, finished, gray, exits))]
pub(crate) fn lemma_vt_skip<K, T, S>(
    nodes: Nodes<K, T, S>,
    map: FMap<Id<K>, bool>,
    rest: Seq<Step<K, K>>,
    c: K,
    finished: Seq<Id<K>>,
    gray: Seq<Id<K>>,
    exits: Seq<Int>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let stack = rest.snoc(Step::Enter(c));
        proof_assert!(forall<q> 0 <= q && q < rest.len() ==> stack[q] == rest[q]);
        proof_assert!(stack[rest.len()] == Step::Enter(c));
        proof_assert!(forall<t> 0 <= t && t < exits.len() ==> exits[t] != rest.len());
        proof_assert!(forall<t> 0 <= t && t < exits.len() ==> exits[t] < rest.len());
        proof_assert!(vt_shape(nodes, rest, gray, exits));
        proof_assert!(forall<t, d> 0 <= t && t < gray.len()
            && exits[t] < rest.len() && vt_enters(stack[rest.len()], d)
            ==> d == c.deep_model());
        proof_assert!(vt_pending(nodes, map, rest, gray, exits))
    }
}

/// Along a chain of children, ranks increase.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(forall<t> 0 <= t && t < gray.len() ==> nodes.contains(&gray[t]))]
#[requires(forall<t> 0 <= t && t + 1 < gray.len() ==> nodes[gray[t]]@.to.contains(gray[t + 1]))]
#[requires(0 <= s && s < gray.len())]
#[ensures(rank[gray[s]] <= rank[gray[gray.len() - 1]])]
#[variant(gray.len() - s)]
pub fn lemma_vt_chain_rank<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    gray: Seq<Id<K>>,
    s: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if s + 1 < gray.len() {
        pearlite! {
            proof_assert!(linked(nodes, gray[s]));
            proof_assert!(nodes[gray[s + 1]]@.from.contains(gray[s]));
            proof_assert!(rank[gray[s]] < rank[gray[s + 1]]);
            lemma_vt_chain_rank(nodes, rank, gray, s + 1)
        }
    }
}

/// A chain of children whose last element has its first element as a child contradicts any rank.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(forall<t> 0 <= t && t < gray.len() ==> nodes.contains(&gray[t]))]
#[requires(forall<t> 0 <= t && t + 1 < gray.len() ==> nodes[gray[t]]@.to.contains(gray[t + 1]))]
#[requires(0 < gray.len())]
#[requires(gray.contains(c))]
#[requires(nodes[gray[gray.len() - 1]]@.to.contains(c))]
#[ensures(false)]
pub fn lemma_vt_cycle<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    gray: Seq<Id<K>>,
    c: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let s = such_that(|s: Int| 0 <= s && s < gray.len() && gray[s] == c);
        lemma_vt_chain_rank(nodes, rank, gray, s);
        let last = gray[gray.len() - 1];
        proof_assert!(linked(nodes, last));
        proof_assert!(nodes.contains(&c) && nodes[c]@.from.contains(last));
        proof_assert!(rank[last] < rank[c])
    }
}

/// Entering a node in progress means the graph has a cycle.
#[logic]
#[requires(all_linked(nodes))]
#[requires(vt_dfs(nodes, map, rest.snoc(Step::Enter(c)), finished, gray, exits))]
#[requires(map.contains(&c.deep_model()) && !map[c.deep_model()])]
#[ensures(!acyclic(nodes))]
pub(crate) fn lemma_vt_back_edge<K, T, S>(
    nodes: Nodes<K, T, S>,
    map: FMap<Id<K>, bool>,
    rest: Seq<Step<K, K>>,
    c: K,
    finished: Seq<Id<K>>,
    gray: Seq<Id<K>>,
    exits: Seq<Int>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_vt_cycle::<K, T, S>;
    pearlite! {
        let stack = rest.snoc(Step::Enter(c));
        let m = gray.len() - 1;
        proof_assert!(gray.contains(c.deep_model()));
        proof_assert!(0 <= m);
        proof_assert!(stack[rest.len()] == Step::Enter(c));
        proof_assert!(exits[m] != rest.len());
        proof_assert!(exits[m] < rest.len());
        proof_assert!(nodes[gray[m]]@.to.contains(c.deep_model()));
        proof_assert!(forall<t> 0 <= t && t < gray.len() ==> nodes.contains(&gray[t]))
    }
}

/// Entering a new node `c`: marking it in progress and pushing its exit, then the entries of its
/// children.
#[logic]
#[requires(all_linked(nodes))]
#[requires(vt_dfs(nodes, map, rest.snoc(Step::Enter(c)), finished, gray, exits))]
#[requires(!map.contains(&c.deep_model()))]
#[requires(stack.len() == rest.len() + 1 + nodes[c.deep_model()]@.to.len())]
#[requires(forall<q> 0 <= q && q < rest.len() + 1 ==> stack[q] == rest.snoc(Step::Exit(c))[q])]
#[requires(forall<i> 0 <= i && i < nodes[c.deep_model()]@.to.len()
    ==> match stack[rest.len() + 1 + i] {
        Step::Enter(k) => k.deep_model()
            == nodes[c.deep_model()]@.to[nodes[c.deep_model()]@.to.len() - 1 - i],
        Step::Exit(_) => false,
    })]
#[ensures(vt_dfs(
    nodes,
    map.add(c.deep_model(), false),
    stack,
    finished,
    gray.snoc(c.deep_model()),
    exits.snoc(rest.len()),
))]
pub(crate) fn lemma_vt_enter<K, T, S>(
    nodes: Nodes<K, T, S>,
    map: FMap<Id<K>, bool>,
    rest: Seq<Step<K, K>>,
    c: K,
    stack: Seq<Step<K, K>>,
    finished: Seq<Id<K>>,
    gray: Seq<Id<K>>,
    exits: Seq<Int>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = crate::verification::lemma_snoc_contains::<Id<K>>;
    crate::verification::lemma_no_dup_snoc(gray, c.deep_model());
    pearlite! {
        let old = rest.snoc(Step::Enter(c));
        let id = c.deep_model();
        let to = nodes[id]@.to;
        let p = rest.len();
        let map2 = map.add(id, false);
        let gray2 = gray.snoc(id);
        let exits2 = exits.snoc(p);
        let m = gray.len() - 1;
        proof_assert!(forall<q> 0 <= q && q < p ==> old[q] == rest[q] && stack[q] == rest[q]);
        proof_assert!(old[p] == Step::Enter(c));
        proof_assert!(stack[p] == Step::Exit(c));
        proof_assert!(nodes.contains(&id));
        proof_assert!(linked(nodes, id));
        proof_assert!(forall<t> 0 <= t && t < exits.len() ==> exits[t] != p && exits[t] < p);
        proof_assert!(gray.len() > 0 ==> exits[m] < p && nodes[gray[m]]@.to.contains(id));
        proof_assert!(!gray.contains(id));
        proof_assert!(!finished.contains(id));
        proof_assert!(no_dup(gray2));
        proof_assert!(forall<k> gray2.contains(k) == (gray.contains(k) || k == id));
        proof_assert!(forall<k> (map2.contains(&k) && !map2[k]) == gray2.contains(k));
        proof_assert!(forall<k> (map2.contains(&k) && map2[k]) == finished.contains(k));
        proof_assert!(vt_marks(nodes, map2, finished, gray2));
        proof_assert!(forall<i> 0 <= i && i < to.len() ==> to.contains(to[to.len() - 1 - i]));
        proof_assert!(forall<q> p < q && q < stack.len() ==> 0 <= q - p - 1 && q - p - 1 < to.len()
            && stack[q] == stack[rest.len() + 1 + (q - p - 1)]);
        proof_assert!(forall<q> p < q && q < stack.len()
            ==> match stack[rest.len() + 1 + (q - p - 1)] {
                Step::Enter(k) => k.deep_model() == to[to.len() - 1 - (q - p - 1)],
                Step::Exit(_) => false,
            });
        proof_assert!(forall<q> p < q && q < stack.len() ==> match stack[q] {
            Step::Enter(k) => to.contains(k.deep_model()) && nodes.contains(&k.deep_model()),
            Step::Exit(_) => false,
        });
        // The shape of the new stack.
        proof_assert!(exits2.len() == gray2.len());
        proof_assert!(exits2[gray.len()] == p && gray2[gray.len()] == id);
        proof_assert!(forall<t> 0 <= t && t < gray.len() ==> exits2[t] == exits[t] && gray2[t] == gray[t]);
        proof_assert!(forall<t> 0 <= t && t < exits2.len()
            ==> 0 <= exits2[t] && exits2[t] < stack.len() && vt_exits(stack[exits2[t]], gray2[t]));
        proof_assert!(forall<t, u> 0 <= t && t < u && u < exits2.len() ==> exits2[t] < exits2[u]);
        proof_assert!(forall<q> 0 <= q && q < p ==> match stack[q] {
            Step::Exit(_) => exists<t> 0 <= t && t < exits2.len() && exits2[t] == q,
            Step::Enter(e) => nodes.contains(&e.deep_model())
                && forall<t> 0 <= t && t < exits2.len() && exits2[t] < q
                    && (t + 1 == exits2.len() || q < exits2[t + 1])
                    ==> nodes[gray2[t]]@.to.contains(e.deep_model()),
        });
        proof_assert!(forall<q> p < q && q < stack.len() ==> match stack[q] {
            Step::Exit(_) => false,
            Step::Enter(e) => nodes.contains(&e.deep_model())
                && forall<t> 0 <= t && t < exits2.len() && exits2[t] < q
                    && (t + 1 == exits2.len() || q < exits2[t + 1])
                    ==> t == gray.len() && nodes[gray2[t]]@.to.contains(e.deep_model()),
        });
        proof_assert!(forall<t> 0 <= t && t + 1 < gray2.len()
            ==> nodes[gray2[t]]@.to.contains(gray2[t + 1]));
        proof_assert!(vt_shape(nodes, stack, gray2, exits2));
        // The pending children.
        proof_assert!(forall<j> 0 <= j && j < to.len() ==> {
            let q = p + 1 + (to.len() - 1 - j);
            proof_assert!(stack[q] == stack[rest.len() + 1 + (to.len() - 1 - j)]);
            p < q && q < stack.len() && vt_enters(stack[q], to[j])
        });
        proof_assert!(forall<d> to.contains(d)
            ==> exists<q> exits2[gray.len()] < q && q < stack.len() && vt_enters(stack[q], d));
        proof_assert!(forall<t, d> 0 <= t && t < gray.len() && nodes[gray[t]]@.to.contains(d)
            ==> (map2.contains(&d) && map2[d])
                || (exists<q> exits2[t] < q && q < stack.len() && vt_enters(stack[q], d))
                || exists<u> t < u && u < gray2.len() && gray2[u] == d);
        proof_assert!(vt_pending(nodes, map2, stack, gray2, exits2))
    }
}

/// Exiting the node `x`: marking it finished and listing it.
#[logic]
#[requires(vt_dfs(nodes, map, rest.snoc(Step::Exit(x)), finished, gray, exits))]
#[ensures(0 < gray.len() && gray[gray.len() - 1] == x.deep_model())]
#[ensures(vt_dfs(
    nodes,
    map.add(x.deep_model(), true),
    rest,
    finished.snoc(x.deep_model()),
    gray.subsequence(0, gray.len() - 1),
    exits.subsequence(0, exits.len() - 1),
))]
pub(crate) fn lemma_vt_exit<K, T, S>(
    nodes: Nodes<K, T, S>,
    map: FMap<Id<K>, bool>,
    rest: Seq<Step<K, K>>,
    x: K,
    finished: Seq<Id<K>>,
    gray: Seq<Id<K>>,
    exits: Seq<Int>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let old = rest.snoc(Step::Exit(x));
        let id = x.deep_model();
        let p = rest.len();
        let m = gray.len() - 1;
        let map2 = map.add(id, true);
        let finished2 = finished.snoc(id);
        let gray2 = gray.subsequence(0, m);
        let exits2 = exits.subsequence(0, m);
        proof_assert!(forall<q> 0 <= q && q < p ==> old[q] == rest[q]);
        proof_assert!(old[p] == Step::Exit(x));
        proof_assert!(exists<t> 0 <= t && t < exits.len() && exits[t] == p);
        proof_assert!(0 <= m && exits[m] == p);
        proof_assert!(gray[m] == id);
        proof_assert!(forall<t> 0 <= t && t < m ==> exits[t] < p);
        proof_assert!(forall<k> gray2.contains(k) == (gray.contains(k) && k != id));
        proof_assert!(map.contains(&id) && !map[id] && !finished.contains(id));
        proof_assert!(forall<c> nodes[id]@.to.contains(c) ==> map.contains(&c) && map[c]);
        proof_assert!(forall<c> nodes[id]@.to.contains(c) ==> finished.contains(c));
        proof_assert!(finished2.subsequence(0, finished.len()) == finished);
        proof_assert!(forall<j> 0 <= j && j < finished.len()
            ==> finished2.subsequence(0, j) == finished.subsequence(0, j));
        proof_assert!(vt_marks(nodes, map2, finished2, gray2));
        proof_assert!(vt_shape(nodes, rest, gray2, exits2));
        proof_assert!(vt_pending(nodes, map2, rest, gray2, exits2))
    }
}

/// Every node whose rank is below `n` has been finished, if the roots have been and every child
/// of a finished node has been.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(forall<k> roots.contains(k) == (nodes.contains(&k) && nodes[k]@.from.len() == 0))]
#[requires(forall<k> roots.contains(k) ==> finished.contains(k))]
#[requires(vt_children_first(nodes, finished))]
#[ensures(forall<k> nodes.contains(&k) && rank[k] < n ==> finished.contains(k))]
#[variant(n)]
pub fn lemma_vt_reach_below<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    roots: Seq<Id<K>>,
    finished: Seq<Id<K>>,
    n: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if n > 0 {
        lemma_vt_reach_below(nodes, rank, roots, finished, n - 1);
        pearlite! {
            proof_assert!(forall<k> nodes.contains(&k) && rank[k] == n - 1
                && nodes[k]@.from.len() > 0 ==> {
                    let p = nodes[k]@.from[0];
                    proof_assert!(linked(nodes, k));
                    proof_assert!(nodes[k]@.from.contains(p));
                    proof_assert!(nodes.contains(&p) && nodes[p]@.to.contains(k));
                    proof_assert!(rank[p] < n - 1);
                    proof_assert!(finished.contains(p));
                    let j = such_that(|j: Int| 0 <= j && j < finished.len() && finished[j] == p);
                    proof_assert!(finished.subsequence(0, j).contains(k));
                    finished.contains(k)
                })
        }
    }
}

/// The node `k` of an acyclic graph has been finished, if the roots have been and every child of
/// a finished node has been.
#[logic]
#[requires(all_linked(nodes) && acyclic(nodes))]
#[requires(forall<k> roots.contains(k) == (nodes.contains(&k) && nodes[k]@.from.len() == 0))]
#[requires(forall<k> roots.contains(k) ==> finished.contains(k))]
#[requires(vt_children_first(nodes, finished))]
#[ensures(nodes.contains(&k) ==> finished.contains(k))]
pub fn lemma_vt_reach_at<K, T, S>(
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    finished: Seq<Id<K>>,
    k: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let rank = rank_of(nodes);
        lemma_vt_reach_below(nodes, rank, roots, finished, rank[k] + 1)
    }
}

/// Listing every node, each after its children, gives a rank function.
#[logic]
#[requires(all_linked(nodes))]
#[requires(no_dup(order))]
#[requires(vt_children_first(nodes, order))]
#[requires(forall<k> nodes.contains(&k) ==> order.contains(k))]
#[ensures(acyclic(nodes))]
pub fn lemma_vt_acyclic<K, T, S>(nodes: Nodes<K, T, S>, order: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let rank = |k| order.len() - crate::dependent::verification::index_of(order, k);
        proof_assert!(forall<j> 0 <= j && j < order.len()
            ==> crate::dependent::verification::index_of(order, order[j]) == j);
        proof_assert!(forall<k> order.contains(k) ==> {
            let j = crate::dependent::verification::index_of(order, k);
            0 <= j && j < order.len() && order[j] == k
        });
        proof_assert!(forall<k, p> nodes.contains(&k) && nodes[k]@.from.contains(p) ==> {
            let jk = crate::dependent::verification::index_of(order, k);
            let jp = crate::dependent::verification::index_of(order, p);
            proof_assert!(linked(nodes, k));
            proof_assert!(nodes.contains(&p) && nodes[p]@.to.contains(k));
            proof_assert!(order.subsequence(0, jp).contains(k));
            let i = such_that(|i: Int| 0 <= i && i < jp && order.subsequence(0, jp)[i] == k);
            proof_assert!(order[i] == k);
            jk < jp
        });
        proof_assert!(ranked(nodes, rank))
    }
}

/// A sequence without duplicates of elements of `set` is no longer than it.
#[logic]
#[requires(no_dup(s))]
#[requires(forall<i> 0 <= i && i < s.len() ==> set.contains(&s[i]))]
#[ensures(s.len() <= set.len())]
#[variant(s.len())]
pub fn lemma_vt_no_dup_set_len<I>(s: Seq<I>, set: FSet<I>) {
    if s.len() > 0 {
        let x = s[s.len() - 1];
        lemma_vt_no_dup_set_len(s.subsequence(0, s.len() - 1), set.remove(x))
    }
}

/// A sequence without duplicates of elements of `set`, as long as it, contains all of them.
#[logic]
#[requires(no_dup(s))]
#[requires(forall<i> 0 <= i && i < s.len() ==> set.contains(&s[i]))]
#[requires(s.len() == set.len())]
#[ensures(forall<k> set.contains(&k) ==> s.contains(k))]
pub fn lemma_vt_set_cover<I>(s: Seq<I>, set: FSet<I>) {
    pearlite! {
        proof_assert!(forall<k> set.contains(&k) && !s.contains(k) ==> {
            lemma_vt_no_dup_set_len(s, set.remove(k));
            false
        })
    }
}

/// `path` is as long as there are active nodes exactly when they form a path. (`_order` and
/// `_candidate` are unused: they only give [`lemma_vt_path`] a conclusion which mentions all of
/// its arguments, which makes it easy to apply.)
#[logic(open)]
pub fn vt_path_matches<K, T, S>(
    nodes: Nodes<K, T, S>,
    _order: Seq<Id<K>>,
    active: FSet<Id<K>>,
    _candidate: Mapping<Id<K>, bool>,
    path: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { (path.len() == active.len()) == forms_path(nodes, active) }
}

/// When `order` lists every node, each before its children, the longest path through the active
/// nodes is as long as there are active nodes exactly when they form a path.
#[logic]
#[requires(all_linked(nodes))]
#[requires(no_dup(order))]
#[requires(forall<k> order.contains(k) == nodes.contains(&k))]
#[requires(forall<i, j> 0 <= i && i < order.len() && 0 <= j && j < order.len()
    && nodes[order[j]]@.from.contains(order[i]) ==> i < j)]
#[requires(forall<k> candidate[k] == active.contains(&k))]
#[requires(valid_path(nodes, path))]
#[requires(no_dup(path))]
#[requires(forall<i> 0 <= i && i < path.len() ==> candidate[path[i]] && order.contains(path[i]))]
#[requires(forall<chain> candidate_chain(nodes, order, candidate, chain) ==> path == chain)]
#[ensures(vt_path_matches(nodes, order, active, candidate, path))]
pub fn lemma_vt_path<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    active: FSet<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    path: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(path.len() == active.len() ==> {
            lemma_vt_set_cover(path, active);
            path_of(nodes, active, path)
        });
        proof_assert!(forms_path(nodes, active) ==> {
            let chain = such_that(|chain| path_of(nodes, active, chain));
            proof_assert!(forall<j> 0 <= j && j < chain.len()
                ==> nodes.contains(&chain[j]) && chain.contains(chain[j]));
            proof_assert!(forall<i> 0 <= i && i < order.len() ==> order.contains(order[i]));
            proof_assert!(candidate_chain(nodes, order, candidate, chain));
            lemma_lists_set_len(chain, active);
            path.len() == active.len()
        })
    }
}

/// Every node of an acyclic graph has been finished, if the roots have been and every child of a
/// finished node has been.
#[logic]
#[requires(all_linked(nodes) && acyclic(nodes))]
#[requires(forall<k> roots.contains(k) == (nodes.contains(&k) && nodes[k]@.from.len() == 0))]
#[requires(forall<k> roots.contains(k) ==> finished.contains(k))]
#[requires(vt_children_first(nodes, finished))]
#[ensures(forall<k> nodes.contains(&k) ==> finished.contains(k))]
pub fn lemma_vt_reach_all<K, T, S>(nodes: Nodes<K, T, S>, roots: Seq<Id<K>>, finished: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_vt_reach_at::<K, T, S>;
}

/// Once every root has been searched, as many nodes have been marked as there are exactly when
/// the graph is acyclic, and then every node has been finished.
#[logic]
#[requires(all_linked(nodes))]
#[requires(forall<k> roots.contains(k) == (nodes.contains(&k) && nodes[k]@.from.len() == 0))]
#[requires(vt_marks(nodes, map, finished, Seq::empty()))]
#[requires(forall<k> roots.contains(k) ==> map.contains(&k))]
#[ensures((map.len() == nodes.len()) == acyclic(nodes))]
#[ensures(map.len() == nodes.len() ==> forall<k> nodes.contains(&k) ==> finished.contains(k))]
pub fn lemma_vt_check<K, T, S>(
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    map: FMap<Id<K>, bool>,
    finished: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> map.contains(&k) == finished.contains(k));
        proof_assert!(forall<k> finished.contains(k) ==> nodes.contains(&k));
        proof_assert!(forall<i> 0 <= i && i < finished.len()
            ==> finished.contains(finished[i]) && nodes.contains(&finished[i]));
        crate::dependent::verification::lemma_lists_len(finished, map);
        proof_assert!(finished.len() == map.len());
        proof_assert!(acyclic(nodes) ==> {
            lemma_vt_reach_all(nodes, roots, finished);
            crate::dependent::verification::lemma_lists_len(finished, nodes);
            map.len() == nodes.len()
        });
        proof_assert!(map.len() == nodes.len() ==> {
            crate::dependent::verification::lemma_full_cover(finished, nodes);
            lemma_vt_acyclic(nodes, finished);
            acyclic(nodes) && forall<k> nodes.contains(&k) ==> finished.contains(k)
        })
    }
}

/// Reversing a list of every node, each after its children, lists each node before its children.
#[logic]
#[requires(all_linked(nodes))]
#[requires(no_dup(finished))]
#[requires(vt_children_first(nodes, finished))]
#[requires(forall<k> finished.contains(k) == nodes.contains(&k))]
#[ensures(no_dup(finished.reverse()))]
#[ensures(forall<k> finished.reverse().contains(k) == nodes.contains(&k))]
#[ensures(forall<i, j> 0 <= i && i < finished.len() && 0 <= j && j < finished.len()
    && nodes[finished.reverse()[j]]@.from.contains(finished.reverse()[i]) ==> i < j)]
pub fn lemma_vt_order<K, T, S>(nodes: Nodes<K, T, S>, finished: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    crate::verification::lemma_reverse(finished);
    pearlite! {
        let order = finished.reverse();
        let n = finished.len();
        proof_assert!(forall<i> 0 <= i && i < n ==> order[i] == finished[n - 1 - i]);
        proof_assert!(forall<i> 0 <= i && i < n ==> finished.contains(finished[i]));
        proof_assert!(forall<i, j> 0 <= i && i < n && 0 <= j && j < n
            && nodes[order[j]]@.from.contains(order[i]) ==> {
                let c = order[j];
                let p = order[i];
                proof_assert!(nodes.contains(&c) && linked(nodes, c));
                proof_assert!(nodes[p]@.to.contains(c));
                proof_assert!(finished.subsequence(0, n - 1 - i).contains(c));
                let jj = such_that(|jj: Int| 0 <= jj && jj < n - 1 - i
                    && finished.subsequence(0, n - 1 - i)[jj] == c);
                proof_assert!(finished[jj] == c && finished[n - 1 - j] == c);
                proof_assert!(jj == n - 1 - j);
                i < j
            })
    }
}
