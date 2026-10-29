//! Lemmas used to verify `crate::independent::graph::paths`.

use core::hash::{BuildHasher, Hash};

use creusot_std::logic::{FMap, Mapping, such_that};

#[allow(unused_imports)]
use super::*;
#[allow(unused_imports)]
use crate::{
    IndependentContents,
    independent::IndependentNode,
    verification::{Id, Model, no_dup},
};

/// The distances and parents recorded by `longest_candidate_path_to_root`: each recorded node is a
/// candidate of `order`, and points either to itself, when it has no parents, or to one of its
/// parents which is also recorded.
#[logic(open)]
pub fn parent_pointers<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    map: FMap<Id<K>, (usize, K)>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<k> map.contains(&k) ==> nodes.contains(&k) && candidate[k] && order.contains(k)
            && (map[k].1.deep_model() == k ==> nodes[k]@.from.len() == 0)
            && (map[k].1.deep_model() != k ==> nodes[k]@.from.contains(map[k].1.deep_model())
                && map.contains(&map[k].1.deep_model()))
    }
}

/// The predecessors recorded by `shortest_path_to_ancestor` while searching up from `start`: each
/// recorded node is a node, and each one but `start` is a parent of its recorded predecessor.
#[logic(open)]
pub fn predecessors<K, T, S>(nodes: Nodes<K, T, S>, start: Id<K>, map: FMap<Id<K>, K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<k> map.contains(&k) ==> nodes.contains(&k)
            && (k != start ==> map.contains(&map[k].deep_model())
                && nodes[map[k].deep_model()]@.from.contains(k))
    }
}

/// `(d, p)` is what `longest_candidate_path_to_root` records for `k`: `k` itself at distance 0
/// when it has no parents, or else a recorded parent with the greatest distance, at one more than
/// that distance.
#[logic(open)]
pub fn recorded_step<K, T, S>(
    nodes: Nodes<K, T, S>,
    map: FMap<Id<K>, (usize, K)>,
    k: Id<K>,
    d: usize,
    p: K,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (p.deep_model() == k && d@ == 0 && nodes[k]@.from.len() == 0)
            || (nodes[k]@.from.contains(p.deep_model())
                && map.contains(&p.deep_model())
                && d@ == map[p.deep_model()].0@ + 1
                && forall<q> nodes[k]@.from.contains(q) && map.contains(&q)
                    ==> map[q].0@ <= map[p.deep_model()].0@)
    }
}

/// The longest distance after recording `x` at distance `d`.
#[logic(open)]
pub fn longest_after<K>(longest: Option<(usize, K)>, d: usize, x: K) -> Option<(usize, K)> {
    pearlite! {
        match longest {
            None => Some((d, x)),
            Some((v, _)) => if v@ < d@ { Some((d, x)) } else { longest },
        }
    }
}

/// The state of `longest_candidate_path_to_root` after processing the first `n` nodes of `order`,
/// when its candidates form the chain `chain`: a candidate is recorded once processed, the
/// recorded nodes are a suffix of the chain, each at its distance from the end of the chain and
/// pointing to the next node of the chain, and the longest distance is that of the first recorded
/// node.
#[logic(open)]
pub fn chain_progress<K: DeepModel>(
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    chain: Seq<Id<K>>,
    map: FMap<Id<K>, (usize, K)>,
    longest: Option<(usize, K)>,
    n: Int,
) -> bool {
    pearlite! {
        (forall<a> 0 <= a && a < order.len() && candidate[order[a]]
            ==> map.contains(&order[a]) == (a < n))
            && (forall<j1, j2> 0 <= j1 && j1 <= j2 && j2 < chain.len() && map.contains(&chain[j1])
                ==> map.contains(&chain[j2]))
            && (forall<j> 0 <= j && j < chain.len() && map.contains(&chain[j])
                ==> map[chain[j]].0@ == chain.len() - 1 - j
                    && map[chain[j]].1.deep_model()
                        == if j + 1 == chain.len() { chain[j] } else { chain[j + 1] })
            && match longest {
                None => forall<j> 0 <= j && j < chain.len() ==> !map.contains(&chain[j]),
                Some((d, k)) => d@ < chain.len()
                    && k.deep_model() == chain[chain.len() - 1 - d@]
                    && map.contains(&k.deep_model())
                    && forall<j> 0 <= j && j < chain.len() - 1 - d@ ==> !map.contains(&chain[j]),
            }
    }
}

/// [`chain_progress`] holds for every candidate chain.
#[logic(open)]
pub fn chain_invariant<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    map: FMap<Id<K>, (usize, K)>,
    longest: Option<(usize, K)>,
    n: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<chain> candidate_chain(nodes, order, candidate, chain)
            ==> chain_progress(order, candidate, chain, map, longest, n)
    }
}

/// Before anything is recorded.
#[logic]
#[requires(forall<k> !map.contains(&k))]
#[ensures(chain_invariant(nodes, order, candidate, map, None, 0))]
pub fn lemma_chain_init<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    map: FMap<Id<K>, (usize, K)>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// Skipping a node which isn't a candidate.
#[logic]
#[requires(chain_invariant(nodes, order, candidate, map, longest, n))]
#[requires(0 <= n && n < order.len())]
#[requires(!candidate[order[n]])]
#[ensures(chain_invariant(nodes, order, candidate, map, longest, n + 1))]
pub fn lemma_chain_skip<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    map: FMap<Id<K>, (usize, K)>,
    longest: Option<(usize, K)>,
    n: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// The next node of a candidate chain is processed: its successor in the chain, if any, has been
/// recorded, and its predecessors haven't.
#[logic]
#[requires(candidate_chain(nodes, order, candidate, chain))]
#[requires(parent_pointers(nodes, order, candidate, map))]
#[requires(chain_progress(order, candidate, chain, map, longest, n))]
#[requires(0 <= n && n < order.len())]
#[requires(candidate[order[n]])]
#[ensures(0 <= result && result < chain.len() && chain[result] == order[n])]
#[ensures(!map.contains(&order[n]))]
#[ensures(forall<j> 0 <= j && j <= result ==> !map.contains(&chain[j]))]
#[ensures(result + 1 < chain.len() ==> map.contains(&chain[result + 1])
    && nodes[order[n]]@.from.contains(chain[result + 1]))]
#[ensures(result + 1 == chain.len() ==> nodes[order[n]]@.from.len() == 0)]
#[ensures(forall<k> map.contains(&k) ==> exists<j> 0 <= j && j < chain.len() && chain[j] == k)]
pub fn lemma_chain_position<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    chain: Seq<Id<K>>,
    map: FMap<Id<K>, (usize, K)>,
    longest: Option<(usize, K)>,
    n: Int,
) -> Int
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let x = order[n];
        proof_assert!(chain.contains(x));
        let jx = such_that(|j: Int| 0 <= j && j < chain.len() && chain[j] == x);
        proof_assert!(!map.contains(&x));
        proof_assert!(forall<j> 0 <= j && j <= jx && map.contains(&chain[j])
            ==> map.contains(&chain[jx]));
        if jx + 1 < chain.len() {
            let y = chain[jx + 1];
            proof_assert!(nodes[x]@.from.contains(y));
            proof_assert!(order.contains(y));
            let a = such_that(|a: Int| 0 <= a && a < order.len() && order[a] == y);
            proof_assert!(candidate[order[a]] && candidate[order[n]]);
            proof_assert!(nodes[order[n]]@.from.contains(order[a]));
            proof_assert!(a < n);
            proof_assert!(map.contains(&y));
        } else {
            proof_assert!(nodes[x]@.from.len() == 0);
        }
        proof_assert!(forall<k> map.contains(&k) ==> order.contains(k) && candidate[k]);
        proof_assert!(forall<k> map.contains(&k) ==> exists<a> 0 <= a && a < order.len()
            && order[a] == k && candidate[order[a]] && chain.contains(order[a]));
        jx
    }
}

/// Recording a candidate of a chain.
#[logic]
#[requires(candidate_chain(nodes, order, candidate, chain))]
#[requires(parent_pointers(nodes, order, candidate, map))]
#[requires(chain_progress(order, candidate, chain, map, longest, n))]
#[requires(0 <= n && n < order.len())]
#[requires(candidate[order[n]])]
#[requires(x.deep_model() == order[n])]
#[requires(recorded_step(nodes, map, order[n], d, p))]
#[ensures(chain_progress(
    order,
    candidate,
    chain,
    map.add(order[n], (d, p)),
    longest_after(longest, d, x),
    n + 1,
))]
pub fn lemma_chain_record_one<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    chain: Seq<Id<K>>,
    map: FMap<Id<K>, (usize, K)>,
    longest: Option<(usize, K)>,
    n: Int,
    x: K,
    d: usize,
    p: K,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let len = chain.len();
        let jx = lemma_chain_position(nodes, order, candidate, chain, map, longest, n);
        let map2 = map.add(order[n], (d, p));
        let longest2 = longest_after(longest, d, x);
        if jx + 1 < len {
            // A recorded parent with the greatest distance: the next node of the chain.
            proof_assert!(nodes[order[n]]@.from.len() > 0);
            proof_assert!(p.deep_model() != order[n]);
            proof_assert!(map.contains(&p.deep_model()));
            proof_assert!(map[chain[jx + 1]].0@ == len - 2 - jx);
            proof_assert!(map[chain[jx + 1]].0@ <= map[p.deep_model()].0@);
            let jp = such_that(|j: Int| 0 <= j && j < len && chain[j] == p.deep_model());
            proof_assert!(jp > jx);
            proof_assert!(jp == jx + 1);
            proof_assert!(d@ == len - 1 - jx);
            proof_assert!(p.deep_model() == chain[jx + 1]);
            match longest {
                None => proof_assert!(false),
                Some((v, _)) => {
                    proof_assert!(len - 1 - v@ <= jx + 1);
                    proof_assert!(map.contains(&chain[len - 1 - v@]));
                    proof_assert!(len - 1 - v@ > jx);
                    proof_assert!(v@ < d@);
                }
            }
            proof_assert!(longest2 == Some((d, x)));
        } else {
            proof_assert!(jx + 1 == len);
            proof_assert!(p.deep_model() == order[n] && d@ == 0);
            match longest {
                None => (),
                Some((v, _)) => {
                    proof_assert!(map.contains(&chain[len - 1 - v@]));
                    proof_assert!(false);
                }
            }
            proof_assert!(longest2 == Some((d, x)));
        }
        proof_assert!(forall<j> 0 <= j && j < len && chain[j] == order[n] ==> j == jx);
        proof_assert!(forall<j> 0 <= j && j < len && map2.contains(&chain[j]) && j != jx
            ==> map.contains(&chain[j]) && map2[chain[j]] == map[chain[j]]);
        proof_assert!(map2[chain[jx]] == (d, p));
        proof_assert!(forall<a> 0 <= a && a < order.len() && candidate[order[a]] && a != n
            ==> order[a] != order[n]);
    }
}

/// Recording a candidate.
#[logic]
#[requires(chain_invariant(nodes, order, candidate, map, longest, n))]
#[requires(parent_pointers(nodes, order, candidate, map))]
#[requires(0 <= n && n < order.len())]
#[requires(candidate[order[n]])]
#[requires(x.deep_model() == order[n])]
#[requires(recorded_step(nodes, map, order[n], d, p))]
#[ensures(chain_invariant(
    nodes,
    order,
    candidate,
    map.add(order[n], (d, p)),
    longest_after(longest, d, x),
    n + 1,
))]
pub fn lemma_chain_record<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    map: FMap<Id<K>, (usize, K)>,
    longest: Option<(usize, K)>,
    n: Int,
    x: K,
    d: usize,
    p: K,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<chain> candidate_chain(nodes, order, candidate, chain) ==> {
            lemma_chain_record_one(nodes, order, candidate, chain, map, longest, n, x, d, p);
            chain_progress(order, candidate, chain, map.add(order[n], (d, p)),
                longest_after(longest, d, x), n + 1)
        })
    }
}

/// A candidate with parents, none of which is recorded, is never part of a chain.
#[logic]
#[requires(chain_invariant(nodes, order, candidate, map, longest, n))]
#[requires(parent_pointers(nodes, order, candidate, map))]
#[requires(0 <= n && n < order.len())]
#[requires(candidate[order[n]])]
#[requires(0 < nodes[order[n]]@.from.len())]
#[requires(forall<q> nodes[order[n]]@.from.contains(q) ==> !map.contains(&q))]
#[ensures(chain_invariant(nodes, order, candidate, map, longest, n + 1))]
pub fn lemma_chain_unrecorded<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    map: FMap<Id<K>, (usize, K)>,
    longest: Option<(usize, K)>,
    n: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        // The candidate's successor in a chain would be a recorded parent, or the candidate would
        // be the chain's root and have no parents.
        proof_assert!(forall<chain> candidate_chain(nodes, order, candidate, chain) ==> {
            let _position = lemma_chain_position(nodes, order, candidate, chain, map, longest, n);
            false
        })
    }
}

/// Once every node has been processed, the recorded parents lead from the node with the longest
/// distance along the chain.
#[logic]
#[requires(candidate_chain(nodes, order, candidate, chain))]
#[requires(chain_progress(order, candidate, chain, map, longest, order.len()))]
#[ensures(forall<j> 0 <= j && j < chain.len() ==> map.contains(&chain[j])
    && map[chain[j]].1.deep_model() == if j + 1 == chain.len() { chain[j] } else { chain[j + 1] })]
#[ensures(match longest {
    None => chain.len() == 0,
    Some((_, k)) => 0 < chain.len() && k.deep_model() == chain[0],
})]
pub fn lemma_chain_done<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    chain: Seq<Id<K>>,
    map: FMap<Id<K>, (usize, K)>,
    longest: Option<(usize, K)>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<j> 0 <= j && j < chain.len()
            ==> order.contains(chain[j]) && candidate[chain[j]]);
        proof_assert!(forall<j> 0 <= j && j < chain.len() ==> map.contains(&chain[j]));
    }
}

/// Candidates of `order` come after their candidate parents.
#[logic(open)]
pub fn candidates_ordered<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<i, j> 0 <= i && i < order.len() && 0 <= j && j < order.len()
            && candidate[order[i]] && candidate[order[j]] && nodes[order[j]]@.from.contains(order[i])
            ==> i < j
    }
}

/// `q` is a path to a root through candidates of `order`.
#[logic(open)]
pub fn candidate_root_path<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    q: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        valid_path(nodes, q)
            && no_dup(q)
            && forall<i> 0 <= i && i < q.len() ==> candidate[q[i]] && order.contains(q[i])
    }
}

/// Every [`candidate_root_path`] starting at one of the first `n` nodes of `order` starts at a
/// recorded node, whose distance is at least the path's length minus one.
///
/// Its definition is only visible in this module, so that it doesn't weigh on the goals of
/// `longest_candidate_path_to_root`, which uses the lemmas below instead.
#[logic]
pub fn distance_bound<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    map: FMap<Id<K>, (usize, K)>,
    n: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<q> candidate_root_path(nodes, order, candidate, q) && 0 < q.len()
            && order.subsequence(0, n).contains(q[0])
            ==> map.contains(&q[0]) && q.len() <= map[q[0]].0@ + 1
    }
}

/// Each recorded distance is one more than that of the recorded parent, or zero for a node
/// pointing to itself.
#[logic(open)]
pub fn exact_distances<K: DeepModel>(map: FMap<Id<K>, (usize, K)>) -> bool {
    pearlite! {
        forall<k> map.contains(&k)
            ==> if map[k].1.deep_model() == k {
                map[k].0@ == 0
            } else {
                map[k].0@ == map[map[k].1.deep_model()].0@ + 1
            }
    }
}

/// `longest` holds the greatest recorded distance and a node recorded at it, or nothing if no node
/// is recorded.
#[logic(open)]
pub fn longest_recorded<K: DeepModel>(
    map: FMap<Id<K>, (usize, K)>,
    longest: Option<(usize, K)>,
) -> bool {
    pearlite! {
        match longest {
            None => forall<k> !map.contains(&k),
            Some((d, k)) => map.contains(&k.deep_model())
                && map[k.deep_model()].0 == d
                && forall<j> map.contains(&j) ==> map[j].0@ <= d@,
        }
    }
}

/// Without its first node, a [`candidate_root_path`] of more than one node is a candidate path from
/// the first node's successor, one of its parents.
#[logic]
#[requires(candidate_root_path(nodes, order, candidate, q))]
#[requires(1 < q.len())]
#[ensures(candidate_root_path(nodes, order, candidate, q.tail()))]
#[ensures(q.tail().len() == q.len() - 1 && q.tail()[0] == q[1])]
#[ensures(nodes[q[0]]@.from.contains(q[1]))]
pub fn lemma_candidate_root_path_tail<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    q: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<i> 0 <= i && i < q.tail().len() ==> q.tail()[i] == q[i + 1]);
        proof_assert!(q.tail()[q.tail().len() - 1] == q[q.len() - 1]);
    }
}

/// A candidate path from the node at index `n` of `order` continues at a candidate parent, which
/// comes earlier in `order`.
#[logic]
#[requires(candidates_ordered(nodes, order, candidate))]
#[requires(candidate_root_path(nodes, order, candidate, q))]
#[requires(1 < q.len())]
#[requires(0 <= n && n < order.len() && q[0] == order[n])]
#[ensures(order.subsequence(0, n).contains(q[1]))]
#[ensures(candidate_root_path(nodes, order, candidate, q.tail()))]
#[ensures(q.tail().len() == q.len() - 1 && q.tail()[0] == q[1])]
#[ensures(nodes[q[0]]@.from.contains(q[1]))]
pub fn lemma_candidate_root_path_earlier<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    q: Seq<Id<K>>,
    n: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        lemma_candidate_root_path_tail(nodes, order, candidate, q);
        let a = such_that(|a: Int| 0 <= a && a < order.len() && order[a] == q[1]);
        proof_assert!(candidate[order[a]] && candidate[order[n]]);
        proof_assert!(nodes[order[n]]@.from.contains(order[a]));
        proof_assert!(a < n);
        proof_assert!(order.subsequence(0, n)[a] == q[1]);
    }
}

/// Skipping a node which isn't a candidate keeps the distances bounded.
#[logic]
#[requires(distance_bound(nodes, order, candidate, map, n))]
#[requires(0 <= n && n < order.len())]
#[requires(!candidate[order[n]])]
#[ensures(distance_bound(nodes, order, candidate, map, n + 1))]
pub fn lemma_distance_skip<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    map: FMap<Id<K>, (usize, K)>,
    n: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<x> order.subsequence(0, n + 1).contains(x)
            == (order.subsequence(0, n).contains(x) || x == order[n]) && {
                crate::verification::lemma_prefix_contains(order, n, x);
                true
            });
    }
}

/// A candidate with parents, none of which is recorded, starts no candidate path.
#[logic]
#[requires(distance_bound(nodes, order, candidate, map, n))]
#[requires(candidates_ordered(nodes, order, candidate))]
#[requires(0 <= n && n < order.len())]
#[requires(candidate[order[n]])]
#[requires(0 < nodes[order[n]]@.from.len())]
#[requires(forall<q> nodes[order[n]]@.from.contains(q) ==> !map.contains(&q))]
#[ensures(distance_bound(nodes, order, candidate, map, n + 1))]
pub fn lemma_distance_unrecorded<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    map: FMap<Id<K>, (usize, K)>,
    n: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<x> order.subsequence(0, n + 1).contains(x)
            == (order.subsequence(0, n).contains(x) || x == order[n]) && {
                crate::verification::lemma_prefix_contains(order, n, x);
                true
            });
        proof_assert!(forall<q> candidate_root_path(nodes, order, candidate, q) && 0 < q.len()
            && q[0] == order[n] ==> {
                if q.len() == 1 {
                    proof_assert!(nodes[q[0]]@.from.len() == 0);
                } else {
                    lemma_candidate_root_path_earlier(nodes, order, candidate, q, n);
                    proof_assert!(map.contains(&q.tail()[0]));
                }
                false
            });
    }
}

/// Recording a candidate at the distance `longest_candidate_path_to_root` computes for it keeps
/// the distances bounded.
#[logic]
#[requires(distance_bound(nodes, order, candidate, map, n))]
#[requires(candidates_ordered(nodes, order, candidate))]
#[requires(no_dup(order))]
#[requires(0 <= n && n < order.len())]
#[requires(candidate[order[n]])]
#[requires(!map.contains(&order[n]))]
#[requires(recorded_step(nodes, map, order[n], d, p))]
#[ensures(distance_bound(nodes, order, candidate, map.add(order[n], (d, p)), n + 1))]
pub fn lemma_distance_record<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    map: FMap<Id<K>, (usize, K)>,
    n: Int,
    d: usize,
    p: K,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let map2 = map.add(order[n], (d, p));
        proof_assert!(forall<x> order.subsequence(0, n + 1).contains(x)
            == (order.subsequence(0, n).contains(x) || x == order[n]) && {
                crate::verification::lemma_prefix_contains(order, n, x);
                true
            });
        proof_assert!(!order.subsequence(0, n).contains(order[n]));
        proof_assert!(forall<x> order.subsequence(0, n).contains(x)
            ==> x != order[n] && map2.contains(&x) == map.contains(&x) && map2[x] == map[x]);
        proof_assert!(forall<q> candidate_root_path(nodes, order, candidate, q) && 0 < q.len()
            && q[0] == order[n] ==> {
                if q.len() == 1 {
                    proof_assert!(nodes[q[0]]@.from.len() == 0);
                    proof_assert!(d@ == 0);
                } else {
                    lemma_candidate_root_path_earlier(nodes, order, candidate, q, n);
                    let y = q[1];
                    proof_assert!(map.contains(&y) && q.len() <= map[y].0@ + 2);
                    proof_assert!(nodes[order[n]]@.from.contains(y));
                    proof_assert!(map[y].0@ <= map[p.deep_model()].0@);
                    proof_assert!(d@ == map[p.deep_model()].0@ + 1);
                }
                map2.contains(&q[0]) && q.len() <= map2[q[0]].0@ + 1
            });
    }
}

/// A topological order of admitted nodes has no duplicates and orders any candidates.
#[logic]
#[requires(kahn_order(nodes, admitted, order, Seq::empty()))]
#[ensures(no_dup(order))]
#[ensures(candidates_ordered(nodes, order, candidate))]
pub fn lemma_kahn_candidates_ordered<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<i, j> 0 <= i && i < order.len() && 0 <= j && j < order.len()
            && nodes[order[j]]@.from.contains(order[i]) ==> {
                proof_assert!(admitted_parent(nodes, admitted, order[j], order[i]));
                lemma_parent_before(nodes, admitted, order, j, order[i]);
                i < j
            });
    }
}

/// Before any node is processed.
#[logic]
#[ensures(distance_bound(nodes, order, candidate, map, 0))]
pub fn lemma_distance_init<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    map: FMap<Id<K>, (usize, K)>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { proof_assert!(forall<x> !order.subsequence(0, 0).contains(x)) }
}

/// Recording a node keeps the recorded distances exact, if it is new and recorded at one more than
/// the distance of its recorded parent, or at zero when pointing to itself.
#[logic]
#[requires(exact_distances(map))]
#[requires(forall<k> map.contains(&k)
    ==> map[k].1.deep_model() == k || map.contains(&map[k].1.deep_model()))]
#[requires(!map.contains(&x))]
#[requires(p.deep_model() == x ==> d@ == 0)]
#[requires(p.deep_model() != x ==> map.contains(&p.deep_model()) && d@ == map[p.deep_model()].0@ + 1)]
#[ensures(exact_distances(map.add(x, (d, p))))]
pub fn lemma_exact_record<K: DeepModel>(map: FMap<Id<K>, (usize, K)>, x: Id<K>, d: usize, p: K) {
    pearlite! {
        proof_assert!(forall<k> map.contains(&k) ==> map[k].1.deep_model() != x);
    }
}

/// Recording a new node keeps track of the longest distance.
#[logic]
#[requires(longest_recorded(map, longest))]
#[requires(!map.contains(&x.deep_model()))]
#[ensures(longest_recorded(map.add(x.deep_model(), (d, p)), longest_after(longest, d, x)))]
pub fn lemma_longest_record<K: DeepModel>(
    map: FMap<Id<K>, (usize, K)>,
    longest: Option<(usize, K)>,
    x: K,
    d: usize,
    p: K,
) {
}

/// Once every node is processed, no [`candidate_root_path`] is longer than the longest distance
/// plus one, or than nothing when no node is recorded.
#[logic]
#[requires(distance_bound(nodes, order, candidate, map, order.len()))]
#[requires(longest_recorded(map, longest))]
#[requires(match longest { None => len == 0, Some((d, _)) => len == d@ + 1 })]
#[ensures(forall<q> candidate_root_path(nodes, order, candidate, q) ==> q.len() <= len)]
pub fn lemma_longest_done<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    map: FMap<Id<K>, (usize, K)>,
    longest: Option<(usize, K)>,
    len: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        crate::verification::lemma_full_prefix(order);
        proof_assert!(forall<q> candidate_root_path(nodes, order, candidate, q) && 0 < q.len()
            ==> order.subsequence(0, order.len()).contains(q[0]));
    }
}

/// A candidate chain lists the candidates of a topological order without duplicates.
#[logic]
#[requires(candidate_chain(nodes, order, candidate, chain))]
#[ensures(no_dup(order))]
#[ensures(candidates_ordered(nodes, order, candidate))]
pub fn lemma_chain_ordered<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    chain: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// [`lemma_longest_done`] as a fact, which applies wherever it is asserted.
#[logic]
#[ensures(result == (distance_bound(nodes, order, candidate, map, order.len())
    && longest_recorded(map, longest)
    && match longest { None => len == 0, Some((d, _)) => len == d@ + 1 }))]
#[ensures(result ==> forall<q> candidate_root_path(nodes, order, candidate, q) ==> q.len() <= len)]
pub fn longest_done_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    map: FMap<Id<K>, (usize, K)>,
    longest: Option<(usize, K)>,
    len: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = distance_bound(nodes, order, candidate, map, order.len())
            && longest_recorded(map, longest)
            && match longest { None => len == 0, Some((d, _)) => len == d@ + 1 };
        proof_assert!(pre ==> {
            lemma_longest_done(nodes, order, candidate, map, longest, len);
            true
        } && forall<q> candidate_root_path(nodes, order, candidate, q) ==> q.len() <= len);
        pre
    }
}

/// The levels of the breadth-first search of `shortest_path_to_ancestor` from `start`: `queue`
/// lists the found nodes from `start`, which is alone until it is visited, at levels which never
/// decrease, the nodes not yet visited (from `head` on) are at most one level above the last
/// visited one, and the recorded predecessor of each found node but `start` is one level below it.
#[logic(open)]
pub fn bfs_found<K: DeepModel>(
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    head: Int,
) -> bool {
    pearlite! {
        0 < queue.len()
            && queue[0] == start
            && level[start] == 0
            && 0 <= head && head <= queue.len()
            && (head == 0 ==> queue.len() == 1)
            && (forall<j, k> 0 <= j && j < k && k < queue.len() ==> level[queue[j]] <= level[queue[k]])
            && (0 < head ==> forall<k> head <= k && k < queue.len()
                ==> level[queue[k]] <= level[queue[head - 1]] + 1)
            && forall<k> map.contains(&k) && k != start ==> level[map[k].deep_model()] + 1 == level[k]
    }
}

/// The first `n` nodes of the search's `queue` are expanded: their parents are found, at most one
/// level above them.
#[logic(open)]
pub fn bfs_expanded<K, T, S>(
    nodes: Nodes<K, T, S>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    n: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<j, p> 0 <= j && j < n && nodes[queue[j]]@.from.contains(p)
            ==> map.contains(&p) && level[p] <= level[queue[j]] + 1
    }
}

/// Walking up `q` from `start`, each of its first `t` steps reaches a found node, at most at that
/// many levels, as long as `t` is no greater than the level of `queue[h]`, before which every node
/// is expanded.
#[logic]
#[requires(bfs_found(start, queue, map, level, h + 1))]
#[requires(bfs_expanded(nodes, queue, map, level, h))]
#[requires(forall<k> map.contains(&k) == queue.contains(k))]
#[requires(0 <= h && h < queue.len())]
#[requires(descending_to(nodes, q, start))]
#[requires(0 <= t && t < q.len() && t <= level[queue[h]])]
#[ensures(map.contains(&q[q.len() - 1 - t]) && level[q[q.len() - 1 - t]] <= t)]
#[variant(t)]
pub fn lemma_bfs_reach<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    h: Int,
    q: Seq<Id<K>>,
    t: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if t > 0 {
        lemma_bfs_reach(nodes, start, queue, map, level, h, q, t - 1);
        pearlite! {
            let v = q[q.len() - t];
            let k = such_that(|k: Int| 0 <= k && k < queue.len() && queue[k] == v);
            proof_assert!(level[queue[k]] < level[queue[h]]);
            proof_assert!(k < h);
            proof_assert!(nodes[v]@.from.contains(q[q.len() - 1 - t]))
        }
    } else {
        proof_assert!(q[q.len() - 1] == start && queue.contains(start))
    }
}

/// The breadth-first search finds a target at the fewest steps from `start`: if `queue[h]` is the
/// first target it visits, every descent from a target to `start` has at least one more node than
/// the level of `queue[h]`.
#[logic]
#[requires(bfs_found(start, queue, map, level, h + 1))]
#[requires(bfs_expanded(nodes, queue, map, level, h))]
#[requires(forall<k> map.contains(&k) == queue.contains(k))]
#[requires(0 <= h && h < queue.len())]
#[requires(forall<j> 0 <= j && j < h ==> !is_target[nodes[queue[j]]])]
#[requires(descending_to(nodes, q, start) && is_target[nodes[q[0]]])]
#[ensures(level[queue[h]] + 1 <= q.len())]
pub fn lemma_bfs_shortest<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    h: Int,
    is_target: Mapping<IndependentNode<K, T, S>, bool>,
    q: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        if q.len() < level[queue[h]] + 1 {
            lemma_bfs_reach(nodes, start, queue, map, level, h, q, q.len() - 1);
            let k = such_that(|k: Int| 0 <= k && k < queue.len() && queue[k] == q[0]);
            proof_assert!(level[queue[k]] < level[queue[h]]);
            proof_assert!(k < h);
            proof_assert!(false)
        }
    }
}

/// Every found node is at most one level above the last visited node.
#[logic]
#[requires(bfs_found(start, queue, map, level, head))]
#[requires(0 < head)]
#[requires(queue.contains(p))]
#[ensures(level[p] <= level[queue[head - 1]] + 1)]
pub fn lemma_bfs_found_level<K: DeepModel>(
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    head: Int,
    p: Id<K>,
) {
    pearlite! {
        let k = such_that(|k: Int| 0 <= k && k < queue.len() && queue[k] == p);
        proof_assert!(k < head ==> level[queue[k]] <= level[queue[head - 1]])
    }
}
