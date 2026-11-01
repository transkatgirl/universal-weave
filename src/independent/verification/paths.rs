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

/// `p` is the first parent of `k` from which a [`candidate_root_path`] of `len` nodes may start,
/// and no such path from any parent of `k` is longer: those from earlier parents are shorter.
#[logic(open)]
pub fn first_longest_parent<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    k: Id<K>,
    p: Id<K>,
    len: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        exists<b> 0 <= b && b < nodes[k]@.from.len() && nodes[k]@.from[b] == p
            && forall<j, q> 0 <= j && j < nodes[k]@.from.len()
                && candidate_root_path(nodes, order, candidate, q) && 0 < q.len()
                && q[0] == nodes[k]@.from[j]
                ==> q.len() <= len && (j < b ==> q.len() < len)
    }
}

/// `path` is the path `longest_candidate_path_to_root` finds: one of the longest
/// [`candidate_root_path`]s, starting at the first node of `order` from which one of them starts,
/// and continuing from each node to its first parent from which the rest of such a path starts.
///
/// No two paths satisfy it ([`lemma_first_longest_unique`]).
#[logic(open)]
pub fn first_longest<K, T, S>(
    nodes: Nodes<K, T, S>,
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
        candidate_root_path(nodes, order, candidate, path)
            && (forall<q> candidate_root_path(nodes, order, candidate, q) ==> q.len() <= path.len())
            && (forall<a, q> 0 <= a && a < order.len()
                && candidate_root_path(nodes, order, candidate, q) && 0 < q.len()
                && q.len() == path.len() && q[0] == order[a]
                ==> exists<b> 0 <= b && b <= a && order[b] == path[0])
            && forall<i> 0 <= i && i + 1 < path.len()
                ==> first_longest_parent(nodes, order, candidate, path[i], path[i + 1], path.len() - i - 1)
    }
}

/// The parent recorded for each node is the first of its parents with the greatest recorded
/// distance.
#[logic(open)]
pub fn first_pointers<K, T, S>(nodes: Nodes<K, T, S>, map: FMap<Id<K>, (usize, K)>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<k> map.contains(&k) && map[k].1.deep_model() != k
            ==> (forall<q> nodes[k]@.from.contains(q) && map.contains(&q)
                    ==> map[q].0@ <= map[map[k].1.deep_model()].0@)
                && exists<b> 0 <= b && b < nodes[k]@.from.len()
                    && nodes[k]@.from[b] == map[k].1.deep_model()
                    && forall<j> 0 <= j && j < b && map.contains(&nodes[k]@.from[j])
                        ==> map[nodes[k]@.from[j]].0@ < map[map[k].1.deep_model()].0@
    }
}

/// The longest distance is that of the first node of `order` recorded at it.
#[logic(open)]
pub fn first_longest_recorded<K: DeepModel>(
    order: Seq<Id<K>>,
    map: FMap<Id<K>, (usize, K)>,
    longest: Option<(usize, K)>,
) -> bool {
    pearlite! {
        match longest {
            None => true,
            Some((d, k)) => exists<b> 0 <= b && b < order.len() && order[b] == k.deep_model()
                && forall<a> 0 <= a && a < b && map.contains(&order[a]) ==> map[order[a]].0@ < d@,
        }
    }
}

/// Recording the candidate `order[n]` at `(d, p)`, where `p` is the node itself or the first of
/// its parents with the greatest recorded distance, keeps [`first_pointers`]: no recorded node has
/// it as a parent, since candidate parents come first in `order`.
#[logic]
#[ensures(result == (first_pointers(nodes, map)
    && parent_pointers(nodes, order, candidate, map)
    && candidates_ordered(nodes, order, candidate)
    && no_dup(order)
    && 0 <= n && n < order.len() && candidate[order[n]]
    && (forall<k> map.contains(&k) ==> order.subsequence(0, n).contains(k))
    && (p.deep_model() == order[n]
        || (recorded_step(nodes, map, order[n], d, p)
            && exists<b> 0 <= b && b < nodes[order[n]]@.from.len()
                && nodes[order[n]]@.from[b] == p.deep_model()
                && forall<j> 0 <= j && j < b && map.contains(&nodes[order[n]]@.from[j])
                    ==> map[nodes[order[n]]@.from[j]].0@ < map[p.deep_model()].0@))))]
#[ensures(result ==> first_pointers(nodes, map.add(order[n], (d, p))))]
#[allow(clippy::too_many_arguments, reason = "The state of the search")]
pub fn first_record_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    map: FMap<Id<K>, (usize, K)>,
    n: Int,
    d: usize,
    p: K,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = first_pointers(nodes, map)
            && parent_pointers(nodes, order, candidate, map)
            && candidates_ordered(nodes, order, candidate)
            && no_dup(order)
            && 0 <= n && n < order.len() && candidate[order[n]]
            && (forall<k> map.contains(&k) ==> order.subsequence(0, n).contains(k))
            && (p.deep_model() == order[n]
                || (recorded_step(nodes, map, order[n], d, p)
                    && exists<b> 0 <= b && b < nodes[order[n]]@.from.len()
                        && nodes[order[n]]@.from[b] == p.deep_model()
                        && forall<j> 0 <= j && j < b && map.contains(&nodes[order[n]]@.from[j])
                            ==> map[nodes[order[n]]@.from[j]].0@ < map[p.deep_model()].0@));
        let x = order[n];
        let m = map.add(x, (d, p));
        proof_assert!(pre ==> {
            proof_assert!(forall<k> map.contains(&k) ==> exists<a> 0 <= a && a < n && order[a] == k);
            proof_assert!(!map.contains(&x));
            proof_assert!(forall<k> map.contains(&k) ==> !nodes[k]@.from.contains(x));
            proof_assert!(!nodes[x]@.from.contains(x));
            proof_assert!(forall<k> m.contains(&k) == (map.contains(&k) || k == x));
            proof_assert!(forall<k> map.contains(&k) ==> m[k] == map[k]);
            proof_assert!(forall<k> map.contains(&k) && map[k].1.deep_model() != k
                ==> map.contains(&map[k].1.deep_model()) && map[k].1.deep_model() != x);
            proof_assert!(p.deep_model() != x ==> map.contains(&p.deep_model())
                && m[p.deep_model()] == map[p.deep_model()]);
            true
        } && first_pointers(nodes, m));
        pre
    }
}

/// Recording `order[n]` keeps the longest distance at the first node of `order` recorded at it.
#[logic]
#[ensures(result == (first_longest_recorded(order, map, longest)
    && longest_recorded(map, longest)
    && no_dup(order)
    && 0 <= n && n < order.len() && x.deep_model() == order[n]
    && !map.contains(&order[n])
    && (forall<k> map.contains(&k) ==> order.subsequence(0, n).contains(k))))]
#[ensures(result
    ==> first_longest_recorded(order, map.add(order[n], (d, p)), longest_after(longest, d, x)))]
#[allow(clippy::too_many_arguments, reason = "The state of the search")]
pub fn first_longest_record_ok<K: DeepModel>(
    order: Seq<Id<K>>,
    map: FMap<Id<K>, (usize, K)>,
    longest: Option<(usize, K)>,
    n: Int,
    x: K,
    d: usize,
    p: K,
) -> bool {
    pearlite! {
        let pre = first_longest_recorded(order, map, longest)
            && longest_recorded(map, longest)
            && no_dup(order)
            && 0 <= n && n < order.len() && x.deep_model() == order[n]
            && !map.contains(&order[n])
            && (forall<k> map.contains(&k) ==> order.subsequence(0, n).contains(k));
        let m = map.add(order[n], (d, p));
        proof_assert!(pre ==> {
            proof_assert!(forall<k> map.contains(&k) ==> exists<a> 0 <= a && a < n && order[a] == k);
            proof_assert!(forall<a> 0 <= a && a < n ==> order[a] != order[n]);
            proof_assert!(forall<a> 0 <= a && a < n && m.contains(&order[a])
                ==> map.contains(&order[a]) && m[order[a]] == map[order[a]]);
            proof_assert!(match longest {
                None => true,
                Some((_, k)) => exists<b> 0 <= b && b < n && order[b] == k.deep_model(),
            });
            true
        } && first_longest_recorded(order, m, longest_after(longest, d, x)));
        pre
    }
}

/// Once every node is processed, following the recorded parents from the first node of `order`
/// with the greatest distance gives [`first_longest`].
#[logic]
#[ensures(result == (distance_bound(nodes, order, candidate, map, order.len())
    && first_pointers(nodes, map)
    && first_longest_recorded(order, map, longest)
    && longest_recorded(map, longest)
    && candidate_root_path(nodes, order, candidate, path)
    && (forall<q> candidate_root_path(nodes, order, candidate, q) ==> q.len() <= path.len())
    && (match longest {
        None => path.len() == 0,
        Some((d, k)) => path.len() == d@ + 1 && path[0] == k.deep_model(),
    })
    && (forall<i> 0 <= i && i < path.len()
        ==> map.contains(&path[i]) && map[path[i]].0@ == path.len() - 1 - i)
    && forall<i> 0 <= i && i + 1 < path.len() ==> map[path[i]].1.deep_model() == path[i + 1]))]
#[ensures(result ==> first_longest(nodes, order, candidate, path))]
pub fn first_longest_done_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    map: FMap<Id<K>, (usize, K)>,
    longest: Option<(usize, K)>,
    path: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = distance_bound(nodes, order, candidate, map, order.len())
            && first_pointers(nodes, map)
            && first_longest_recorded(order, map, longest)
            && longest_recorded(map, longest)
            && candidate_root_path(nodes, order, candidate, path)
            && (forall<q> candidate_root_path(nodes, order, candidate, q) ==> q.len() <= path.len())
            && (match longest {
                None => path.len() == 0,
                Some((d, k)) => path.len() == d@ + 1 && path[0] == k.deep_model(),
            })
            && (forall<i> 0 <= i && i < path.len()
                ==> map.contains(&path[i]) && map[path[i]].0@ == path.len() - 1 - i)
            && forall<i> 0 <= i && i + 1 < path.len() ==> map[path[i]].1.deep_model() == path[i + 1];
        proof_assert!(pre ==> {
            crate::verification::lemma_full_prefix(order);
            proof_assert!(forall<q> candidate_root_path(nodes, order, candidate, q) && 0 < q.len()
                ==> order.subsequence(0, order.len()).contains(q[0])
                    && map.contains(&q[0]) && q.len() <= map[q[0]].0@ + 1);
            proof_assert!(forall<a, q> 0 <= a && a < order.len()
                && candidate_root_path(nodes, order, candidate, q) && 0 < q.len()
                && q.len() == path.len() && q[0] == order[a]
                ==> exists<b> 0 <= b && b <= a && order[b] == path[0]);
            proof_assert!(forall<i> 0 <= i && i + 1 < path.len()
                ==> path[i + 1] != path[i] && map[path[i]].1.deep_model() != path[i]
                    && map[path[i + 1]].0@ == path.len() - i - 2);
            proof_assert!(forall<i> 0 <= i && i + 1 < path.len()
                ==> first_longest_parent(nodes, order, candidate, path[i], path[i + 1], path.len() - i - 1));
            true
        } && first_longest(nodes, order, candidate, path));
        pre
    }
}

/// The nodes of a [`candidate_root_path`] from its `i`-th one form one.
#[logic]
#[requires(candidate_root_path(nodes, order, candidate, p))]
#[requires(0 <= i && i < p.len())]
#[ensures(candidate_root_path(nodes, order, candidate, p.subsequence(i, p.len())))]
#[ensures(p.subsequence(i, p.len()).len() == p.len() - i)]
#[ensures(p.subsequence(i, p.len())[0] == p[i])]
pub fn lemma_candidate_root_path_suffix<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    p: Seq<Id<K>>,
    i: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let s = p.subsequence(i, p.len());
        proof_assert!(forall<j> 0 <= j && j < s.len() ==> s[j] == p[i + j]);
        proof_assert!(forall<j> 0 <= j && j + 1 < s.len() ==> nodes[s[j]]@.from.contains(s[j + 1]));
        proof_assert!(nodes[s[s.len() - 1]]@.from.len() == 0);
        proof_assert!(no_dup(s))
    }
}

/// Two paths satisfying [`first_longest`] which agree up to their `i`-th node agree on the rest.
#[logic]
#[requires(first_longest(nodes, order, candidate, p))]
#[requires(first_longest(nodes, order, candidate, q))]
#[requires(p.len() == q.len())]
#[requires(0 <= i && i < p.len() && p[i] == q[i])]
#[ensures(forall<j> i <= j && j < p.len() ==> p[j] == q[j])]
#[variant(p.len() - i)]
pub fn lemma_first_longest_agree<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    p: Seq<Id<K>>,
    q: Seq<Id<K>>,
    i: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if i + 1 < p.len() {
        pearlite! {
            let len = p.len() - i - 1;
            let from = nodes[p[i]]@.from;
            lemma_candidate_root_path_suffix(nodes, order, candidate, p, i + 1);
            lemma_candidate_root_path_suffix(nodes, order, candidate, q, i + 1);
            let bp = such_that(|b| 0 <= b && b < from.len() && from[b] == p[i + 1]
                && forall<j, r> 0 <= j && j < from.len()
                    && candidate_root_path(nodes, order, candidate, r) && 0 < r.len() && r[0] == from[j]
                    ==> r.len() <= len && (j < b ==> r.len() < len));
            let bq = such_that(|b| 0 <= b && b < from.len() && from[b] == q[i + 1]
                && forall<j, r> 0 <= j && j < from.len()
                    && candidate_root_path(nodes, order, candidate, r) && 0 < r.len() && r[0] == from[j]
                    ==> r.len() <= len && (j < b ==> r.len() < len));
            proof_assert!(!(bq < bp));
            proof_assert!(!(bp < bq));
            proof_assert!(p[i + 1] == q[i + 1]);
            lemma_first_longest_agree(nodes, order, candidate, p, q, i + 1)
        }
    }
}

/// No two paths satisfy [`first_longest`].
#[logic]
#[requires(no_dup(order))]
#[requires(first_longest(nodes, order, candidate, p))]
#[requires(first_longest(nodes, order, candidate, q))]
#[ensures(p == q)]
pub fn lemma_first_longest_unique<K, T, S>(
    nodes: Nodes<K, T, S>,
    order: Seq<Id<K>>,
    candidate: Mapping<Id<K>, bool>,
    p: Seq<Id<K>>,
    q: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(p.len() == q.len());
        proof_assert!(0 < p.len() ==> {
            let bp = such_that(|b| 0 <= b && b < order.len() && order[b] == p[0]);
            let bq = such_that(|b| 0 <= b && b < order.len() && order[b] == q[0]);
            proof_assert!(exists<b> 0 <= b && b <= bq && order[b] == p[0]);
            proof_assert!(exists<b> 0 <= b && b <= bp && order[b] == q[0]);
            proof_assert!(bp <= bq && bq <= bp);
            lemma_first_longest_agree(nodes, order, candidate, p, q, 0);
            true
        } && forall<j> 0 <= j && j < p.len() ==> p[j] == q[j]);
        proof_assert!(p.ext_eq(q))
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

/// `d` comes before `e` in the order in which `shortest_path_to_ancestor` meets descents to the
/// same node, which compares them from that node up: they are as long, and at the last position
/// where they differ, `d`'s node comes before `e`'s among the parents of the node after it.
#[logic(open)]
pub fn precedes<K, T, S>(nodes: Nodes<K, T, S>, d: Seq<Id<K>>, e: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { d.len() == e.len() && exists<i, b, c> precedes_at(nodes, d, e, i, b, c) }
}

/// [`precedes`], where `d` and `e` differ last at position `i`, and `d[i]` and `e[i]` are at
/// positions `b` and `c` among the parents of `d[i + 1]`.
#[logic(open)]
pub fn precedes_at<K, T, S>(
    nodes: Nodes<K, T, S>,
    d: Seq<Id<K>>,
    e: Seq<Id<K>>,
    i: Int,
    b: Int,
    c: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        0 <= i && i + 1 < d.len() && i + 1 < e.len()
            && (forall<j> i < j && j < d.len() ==> d[j] == e[j])
            && 0 <= b && b < c && c < nodes[d[i + 1]]@.from.len()
            && nodes[d[i + 1]]@.from[b] == d[i]
            && nodes[d[i + 1]]@.from[c] == e[i]
    }
}

/// `down` is the first, in the order of [`precedes`], of the descents to `id` with as many nodes
/// which start at a node `is_target` accepts.
#[logic(open(self))]
pub fn first_descent<K, T, S>(
    nodes: Nodes<K, T, S>,
    down: Seq<Id<K>>,
    id: Id<K>,
    is_target: Mapping<IndependentNode<K, T, S>, bool>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<q> descending_to(nodes, q, id) && is_target[nodes[q[0]]] && q.len() == down.len()
            ==> q == down || precedes(nodes, down, q)
    }
}

/// `down` is the first, in the order of [`precedes`], of the descents to `id` with as many nodes
/// which start at `down[0]`.
#[logic(open(self))]
pub fn first_descent_from<K, T, S>(nodes: Nodes<K, T, S>, down: Seq<Id<K>>, id: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<q> descending_to(nodes, q, id) && q[0] == down[0] && q.len() == down.len()
            ==> q == down || precedes(nodes, down, q)
    }
}

/// `down` is the first, in the order of [`precedes`], of the descents to `id` with as many nodes
/// which start at a root.
#[logic(open(self))]
pub fn first_descent_from_root<K, T, S>(nodes: Nodes<K, T, S>, down: Seq<Id<K>>, id: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<q> descending_to(nodes, q, id) && nodes[q[0]]@.from.len() == 0 && q.len() == down.len()
            ==> q == down || precedes(nodes, down, q)
    }
}

/// The descent the search records from the found node `x`: `x`, the node it was found from, and
/// so on, `fuel` times.
#[logic(open(self))]
#[variant(fuel)]
pub fn tree_path<K: DeepModel>(map: FMap<Id<K>, K>, x: Id<K>, fuel: Int) -> Seq<Id<K>> {
    pearlite! {
        if fuel <= 0 {
            Seq::singleton(x)
        } else {
            Seq::singleton(x).concat(tree_path(map, map[x].deep_model(), fuel - 1))
        }
    }
}

/// The recorded descent from `x` lists `x`, and then follows the recorded predecessors.
#[logic]
#[requires(0 <= fuel)]
#[ensures(tree_path(map, x, fuel).len() == fuel + 1)]
#[ensures(tree_path(map, x, fuel)[0] == x)]
#[ensures(forall<i> 0 <= i && i < fuel
    ==> tree_path(map, x, fuel)[i + 1] == map[tree_path(map, x, fuel)[i]].deep_model())]
#[variant(fuel)]
pub fn lemma_tree_path_shape<K: DeepModel>(map: FMap<Id<K>, K>, x: Id<K>, fuel: Int) {
    if 0 < fuel {
        pearlite! {
            let rest = tree_path(map, map[x].deep_model(), fuel - 1);
            lemma_tree_path_shape(map, map[x].deep_model(), fuel - 1);
            let t = Seq::singleton(x).concat(rest);
            proof_assert!(tree_path(map, x, fuel) == t);
            proof_assert!(forall<i> 0 <= i && i < rest.len() ==> t[i + 1] == rest[i]);
            proof_assert!(t[1] == map[x].deep_model());
            proof_assert!(forall<i> 1 <= i && i < fuel ==> t[i + 1] == rest[i] && t[i] == rest[i - 1])
        }
    }
}

/// The levels of the found nodes: `start` is at level 0, every other found node is one level
/// above the node it was found from, which was found too, and no level is negative.
#[logic(open)]
pub fn bfs_levels<K: DeepModel>(start: Id<K>, map: FMap<Id<K>, K>, level: Mapping<Id<K>, Int>) -> bool {
    pearlite! {
        level[start] == 0
            && (forall<k> map.contains(&k) ==> 0 <= level[k])
            && forall<k> map.contains(&k) && k != start
                ==> map.contains(&map[k].deep_model()) && level[map[k].deep_model()] + 1 == level[k]
    }
}

/// The recorded descent from a found node `x` is a descent to `start` through found nodes, one
/// level lower at each step.
#[logic]
#[requires(predecessors(nodes, start, map))]
#[requires(bfs_levels(start, map, level))]
#[requires(map.contains(&x))]
#[ensures(descending_to(nodes, tree_path(map, x, level[x]), start))]
#[ensures(tree_path(map, x, level[x]).len() == level[x] + 1 && tree_path(map, x, level[x])[0] == x)]
#[ensures(forall<i> 0 <= i && i <= level[x]
    ==> map.contains(&tree_path(map, x, level[x])[i])
        && level[tree_path(map, x, level[x])[i]] == level[x] - i)]
#[variant(level[x])]
pub fn lemma_tree_path_descent<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    x: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        lemma_tree_path_shape(map, x, level[x]);
        if x == start {
            proof_assert!(tree_path(map, x, level[x]) == Seq::singleton(start));
        } else {
            let y = map[x].deep_model();
            lemma_tree_path_descent(nodes, start, map, level, y);
            let rest = tree_path(map, y, level[y]);
            let t = Seq::singleton(x).concat(rest);
            proof_assert!(level[y] == level[x] - 1 && 0 < level[x]);
            proof_assert!(tree_path(map, x, level[x]) == t);
            proof_assert!(forall<i> 0 <= i && i < rest.len() ==> t[i + 1] == rest[i]);
            proof_assert!(nodes[y]@.from.contains(x));
            proof_assert!(forall<i> 0 <= i && i < rest.len() ==> rest[i] != x);
            proof_assert!(no_dup(t));
        }
    }
}

/// Each found node above level 0 was found from a found node one level below it.
#[logic(open)]
pub fn levels_down<K: DeepModel>(map: FMap<Id<K>, K>, level: Mapping<Id<K>, Int>) -> bool {
    pearlite! {
        forall<k> map.contains(&k) && 0 < level[k]
            ==> map.contains(&map[k].deep_model()) && level[map[k].deep_model()] + 1 == level[k]
    }
}

/// Recording a new node leaves the recorded descents of the other found nodes unchanged.
#[logic]
#[requires(levels_down(map, level))]
#[requires(map.contains(&x))]
#[requires(!map.contains(&p))]
#[ensures(tree_path(map.add(p, v), x, level[x]) == tree_path(map, x, level[x]))]
#[variant(level[x])]
pub fn lemma_tree_path_add<K: DeepModel>(
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    x: Id<K>,
    p: Id<K>,
    v: K,
) {
    pearlite! {
        if 0 < level[x] {
            let y = map[x].deep_model();
            lemma_tree_path_add(map, level, y, p, v);
            proof_assert!(map.add(p, v)[x] == map[x]);
            proof_assert!(level[y] == level[x] - 1);
        }
    }
}

/// The recorded descent from a node found from `y` is that node followed by the recorded descent
/// from `y`.
#[logic]
#[requires(levels_down(map, level))]
#[requires(map.contains(&y) && 0 <= level[y])]
#[requires(!map.contains(&p))]
#[requires(yk.deep_model() == y)]
#[ensures(tree_path(map.add(p, yk), p, level[y] + 1)
    == Seq::singleton(p).concat(tree_path(map, y, level[y])))]
pub fn lemma_tree_path_push<K: DeepModel>(
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    y: Id<K>,
    p: Id<K>,
    yk: K,
) {
    pearlite! {
        lemma_tree_path_add(map, level, y, p, yk);
        proof_assert!(map.add(p, yk)[p] == yk)
    }
}

/// [`precedes`] is transitive.
#[logic]
#[requires(all_linked(nodes))]
#[requires(forall<j> 0 <= j && j < d.len() ==> nodes.contains(&d[j]))]
#[requires(precedes(nodes, d, e))]
#[requires(precedes(nodes, e, f))]
#[ensures(precedes(nodes, d, f))]
pub fn lemma_precedes_trans<K, T, S>(
    nodes: Nodes<K, T, S>,
    d: Seq<Id<K>>,
    e: Seq<Id<K>>,
    f: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<i, b1, c1, k, b2, c2> precedes_at(nodes, d, e, i, b1, c1)
            && precedes_at(nodes, e, f, k, b2, c2)
            ==> (i == k ==> linked(nodes, d[i + 1]) && e[i + 1] == d[i + 1] && c1 == b2
                    && precedes_at(nodes, d, f, i, b1, c2))
                && (i < k ==> d[k] == e[k] && d[k + 1] == e[k + 1]
                    && precedes_at(nodes, d, f, k, b2, c2))
                && (k < i ==> e[i] == f[i] && e[i + 1] == f[i + 1]
                    && precedes_at(nodes, d, f, i, b1, c1)))
    }
}

/// Descents which differ only below the node they end at keep their order once each is extended
/// by one node: they still differ last where they did.
#[logic]
#[requires(precedes(nodes, d, e))]
#[ensures(precedes(nodes, Seq::singleton(a).concat(d), Seq::singleton(b).concat(e)))]
pub fn lemma_precedes_cons<K, T, S>(
    nodes: Nodes<K, T, S>,
    d: Seq<Id<K>>,
    e: Seq<Id<K>>,
    a: Id<K>,
    b: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let x = Seq::singleton(a).concat(d);
        let y = Seq::singleton(b).concat(e);
        proof_assert!(forall<j> 0 <= j && j < d.len() ==> x[j + 1] == d[j] && y[j + 1] == e[j]);
        proof_assert!(x.len() == d.len() + 1 && y.len() == e.len() + 1);
        proof_assert!(forall<i, b1, c1> precedes_at(nodes, d, e, i, b1, c1)
            ==> (forall<j> i + 1 < j && j < x.len() ==> x[j] == y[j])
                && x[i + 2] == d[i + 1] && x[i + 1] == d[i] && y[i + 1] == e[i]
                && precedes_at(nodes, x, y, i + 1, b1, c1))
    }
}

/// Two descents which differ only in their first node are ordered by those nodes' positions among
/// the parents of the next one.
#[logic]
#[requires(0 < t.len())]
#[requires(0 <= b && b < c && c < nodes[t[0]]@.from.len())]
#[requires(nodes[t[0]]@.from[b] == a1 && nodes[t[0]]@.from[c] == a2)]
#[ensures(precedes(nodes, Seq::singleton(a1).concat(t), Seq::singleton(a2).concat(t)))]
pub fn lemma_precedes_head<K, T, S>(
    nodes: Nodes<K, T, S>,
    t: Seq<Id<K>>,
    a1: Id<K>,
    a2: Id<K>,
    b: Int,
    c: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let x = Seq::singleton(a1).concat(t);
        let y = Seq::singleton(a2).concat(t);
        proof_assert!(forall<j> 0 <= j && j < t.len() ==> x[j + 1] == t[j] && y[j + 1] == t[j]);
        proof_assert!(x[0] == a1 && y[0] == a2 && x[1] == t[0]);
        proof_assert!(precedes_at(nodes, x, y, 0, b, c))
    }
}

/// No two descents precede each other.
#[logic]
#[requires(all_linked(nodes))]
#[requires(forall<j> 0 <= j && j < d.len() ==> nodes.contains(&d[j]))]
#[requires(precedes(nodes, d, e))]
#[ensures(!precedes(nodes, e, d))]
pub fn lemma_precedes_asym<K, T, S>(nodes: Nodes<K, T, S>, d: Seq<Id<K>>, e: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<i, b1, c1, k, b2, c2> precedes_at(nodes, d, e, i, b1, c1)
            && precedes_at(nodes, e, d, k, b2, c2)
            ==> linked(nodes, d[i + 1]) && d[i] != e[i]
                && (i < k ==> d[k] == e[k])
                && (k < i ==> e[i] == d[i])
                && i == k && b2 == c1 && c2 == b1 && false)
    }
}

/// [`first_descent`] determines the descent among those to `id` with as many nodes from a node
/// `is_target` accepts.
#[logic]
#[requires(all_linked(nodes))]
#[requires(first_descent(nodes, d, id, is_target) && descending_to(nodes, d, id))]
#[requires(is_target[nodes[d[0]]])]
#[requires(first_descent(nodes, e, id, is_target) && descending_to(nodes, e, id))]
#[requires(is_target[nodes[e[0]]])]
#[requires(d.len() == e.len())]
#[ensures(d == e)]
pub fn lemma_first_descent_unique<K, T, S>(
    nodes: Nodes<K, T, S>,
    d: Seq<Id<K>>,
    e: Seq<Id<K>>,
    id: Id<K>,
    is_target: Mapping<IndependentNode<K, T, S>, bool>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(e == d || precedes(nodes, d, e));
        proof_assert!(d == e || precedes(nodes, e, d));
        proof_assert!(precedes(nodes, d, e) ==> {
            lemma_precedes_asym(nodes, d, e);
            d == e
        })
    }
}

/// Among the found nodes of each level, the search's queue lists first those whose recorded
/// descent comes first in the order of [`precedes`].
#[logic(open(self))]
pub fn bfs_ordered<K, T, S>(
    nodes: Nodes<K, T, S>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<j, k> 0 <= j && j < k && k < queue.len() && level[queue[j]] == level[queue[k]]
            ==> precedes(
                nodes,
                tree_path(map, queue[j], level[queue[j]]),
                tree_path(map, queue[k], level[queue[k]]),
            )
    }
}

/// The recorded descent from each found node comes first, in the order of [`precedes`], among the
/// descents from that node to `start` with as many nodes.
#[logic(open(self))]
pub fn bfs_least<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<x, d> queue.contains(x) && descending_to(nodes, d, start) && d[0] == x
            && d.len() == level[x] + 1
            ==> d == tree_path(map, x, level[x]) || precedes(nodes, tree_path(map, x, level[x]), d)
    }
}

/// Each found node but `start` was found from one of the first `head` nodes of the queue, and those
/// among the first `old` nodes of the queue from one of its first `head - 1`.
#[logic(open)]
pub fn bfs_discovered<K: DeepModel>(
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    head: Int,
    old: Int,
) -> bool {
    pearlite! {
        forall<k> 0 <= k && k < queue.len() && queue[k] != start
            ==> exists<j> 0 <= j && j < head && queue[j] == map[queue[k]].deep_model()
                && (k < old ==> j + 1 < head)
    }
}

/// The nodes found since the queue had `old` nodes were found from `y`, among its first `i` parents.
#[logic(open)]
pub fn bfs_expanding<K, T, S>(
    nodes: Nodes<K, T, S>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    y: Id<K>,
    old: Int,
    i: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<k> old <= k && k < queue.len()
            ==> map[queue[k]].deep_model() == y
                && exists<b> 0 <= b && b < i && nodes[y]@.from[b] == queue[k]
    }
}

/// The levels which [`bfs_found`] gives the found nodes satisfy [`bfs_levels`] and
/// [`levels_down`].
#[logic]
#[ensures(result == (bfs_found(start, queue, map, level, head)
    && (forall<k> map.contains(&k) == queue.contains(k))
    && predecessors(nodes, start, map)))]
#[ensures(result ==> bfs_levels(start, map, level) && levels_down(map, level))]
pub fn bfs_levels_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    head: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = bfs_found(start, queue, map, level, head)
            && (forall<k> map.contains(&k) == queue.contains(k))
            && predecessors(nodes, start, map);
        proof_assert!(pre ==> (forall<j> 0 <= j && j < queue.len() ==> 0 <= level[queue[j]])
            && (forall<k> map.contains(&k) ==> 0 <= level[k])
            && (forall<k> map.contains(&k) && 0 < level[k] ==> k != start));
        pre
    }
}

/// The search starts with [`bfs_ordered`] and [`bfs_least`]: only `start` is found.
#[logic]
#[ensures(result == (queue == Seq::singleton(start) && level[start] == 0))]
#[ensures(result ==> bfs_ordered(nodes, queue, map, level))]
#[ensures(result ==> bfs_least(nodes, start, queue, map, level))]
pub fn bfs_init_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = queue == Seq::singleton(start) && level[start] == 0;
        proof_assert!(pre ==> tree_path(map, start, 0) == Seq::singleton(start)
            && (forall<x> queue.contains(x) ==> x == start)
            && forall<d> descending_to(nodes, d, start) && d.len() == 1
                ==> d.ext_eq(Seq::singleton(start)));
        pre
    }
}

/// The state of the search before it finds `p` from `y = queue[head - 1]`, the `i`-th parent of
/// `y`, which it hadn't found.
#[logic(open)]
#[allow(clippy::too_many_arguments, reason = "The state of the search")]
pub fn bfs_pushing<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    head: Int,
    old: Int,
    i: Int,
    p: Id<K>,
    yk: K,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        all_linked(nodes)
            && bfs_found(start, queue, map, level, head)
            && bfs_expanded(nodes, queue, map, level, head - 1)
            && (forall<k> map.contains(&k) == queue.contains(k))
            && predecessors(nodes, start, map)
            && no_dup(queue)
            && (forall<j> 0 <= j && j < queue.len() ==> nodes.contains(&queue[j]))
            && 0 < head && head <= old && old <= queue.len()
            && yk.deep_model() == queue[head - 1]
            && bfs_ordered(nodes, queue, map, level)
            && bfs_least(nodes, start, queue, map, level)
            && bfs_discovered(start, queue, map, head, old)
            && bfs_expanding(nodes, queue, map, queue[head - 1], old, i)
            && 0 <= i && i < nodes[queue[head - 1]]@.from.len()
            && nodes[queue[head - 1]]@.from[i] == p
            && !map.contains(&p)
    }
}

/// Finding `p` keeps the queue in the order of [`precedes`] within each level.
#[logic]
#[ensures(result == bfs_pushing(nodes, start, queue, map, level, head, old, i, p, yk))]
#[ensures(result ==> bfs_ordered(
    nodes,
    queue.snoc(p),
    map.add(p, yk),
    level.set(p, level[yk.deep_model()] + 1),
))]
#[allow(clippy::too_many_arguments, reason = "The state of the search")]
pub fn bfs_push_ordered_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    head: Int,
    old: Int,
    i: Int,
    p: Id<K>,
    yk: K,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = bfs_pushing(nodes, start, queue, map, level, head, old, i, p, yk);
        let y = yk.deep_model();
        let q2 = queue.snoc(p);
        let m2 = map.add(p, yk);
        let l2 = level.set(p, level[y] + 1);
        proof_assert!(pre ==> {
            proof_assert!(bfs_levels_ok(nodes, start, queue, map, level, head));
            proof_assert!(map.contains(&y) && 0 <= level[y]);
            lemma_tree_path_push(map, level, y, p, yk);
            let ty = tree_path(map, y, level[y]);
            proof_assert!(tree_path(m2, p, l2[p]) == Seq::singleton(p).concat(ty));
            proof_assert!(forall<j> 0 <= j && j < queue.len() ==> queue[j] != p && q2[j] == queue[j]
                && l2[queue[j]] == level[queue[j]]
                && {
                    lemma_tree_path_add(map, level, queue[j], p, yk);
                    true
                }
                && tree_path(m2, queue[j], l2[queue[j]]) == tree_path(map, queue[j], level[queue[j]]));
            proof_assert!(q2[queue.len()] == p);
            proof_assert!(forall<j> 0 <= j && j < queue.len() && level[queue[j]] == level[y] + 1
                ==> {
                    let x = queue[j];
                    let z = map[x].deep_model();
                    proof_assert!(x != start && map.contains(&x) && level[z] == level[y]);
                    proof_assert!(tree_path(map, x, level[x]) == Seq::singleton(x).concat(tree_path(map, z, level[z])));
                    proof_assert!(j < old ==> exists<jz> 0 <= jz && jz + 1 < head && queue[jz] == z
                        && precedes(nodes, tree_path(map, z, level[z]), ty)
                        && {
                            lemma_precedes_cons(nodes, tree_path(map, z, level[z]), ty, x, p);
                            true
                        });
                    proof_assert!(old <= j ==> z == y && exists<b> 0 <= b && b < i
                        && nodes[y]@.from[b] == x
                        && {
                            lemma_tree_path_descent(nodes, start, map, level, y);
                            lemma_precedes_head(nodes, ty, x, p, b, i);
                            true
                        });
                    true
                } && precedes(nodes, tree_path(map, queue[j], level[queue[j]]), Seq::singleton(p).concat(ty)));
            true
        } && bfs_ordered(nodes, q2, m2, l2));
        pre
    }
}

/// `d` comes no earlier than `e` in the order of [`precedes`]: it is `e`, or `e` precedes it.
#[logic(open)]
pub fn desc_le<K, T, S>(nodes: Nodes<K, T, S>, e: Seq<Id<K>>, d: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { d == e || precedes(nodes, e, d) }
}

/// [`lemma_tree_path_descent`] and [`lemma_tree_path_shape`] as a fact.
#[logic]
#[ensures(result == (predecessors(nodes, start, map) && bfs_levels(start, map, level) && map.contains(&x)))]
#[ensures(result ==> descending_to(nodes, tree_path(map, x, level[x]), start)
    && tree_path(map, x, level[x]).len() == level[x] + 1
    && tree_path(map, x, level[x])[0] == x
    && (forall<j> 0 <= j && j < tree_path(map, x, level[x]).len()
        ==> nodes.contains(&tree_path(map, x, level[x])[j]))
    && forall<j> 0 <= j && j < level[x]
        ==> tree_path(map, x, level[x])[j + 1] == map[tree_path(map, x, level[x])[j]].deep_model())]
pub fn tree_path_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    x: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = predecessors(nodes, start, map) && bfs_levels(start, map, level) && map.contains(&x);
        proof_assert!(pre ==> {
            lemma_tree_path_descent(nodes, start, map, level, x);
            lemma_tree_path_shape(map, x, level[x]);
            true
        });
        pre
    }
}

/// [`lemma_tree_path_add`] as a fact.
#[logic]
#[ensures(result == (levels_down(map, level) && map.contains(&x) && !map.contains(&p)))]
#[ensures(result ==> tree_path(map.add(p, v), x, level[x]) == tree_path(map, x, level[x]))]
pub fn tree_path_add_ok<K: DeepModel>(
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    x: Id<K>,
    p: Id<K>,
    v: K,
) -> bool {
    pearlite! {
        let pre = levels_down(map, level) && map.contains(&x) && !map.contains(&p);
        proof_assert!(pre ==> {
            lemma_tree_path_add(map, level, x, p, v);
            true
        });
        pre
    }
}

/// [`precedes`] followed by [`desc_le`] is [`precedes`].
#[logic]
#[ensures(result == (all_linked(nodes)
    && (forall<j> 0 <= j && j < d.len() ==> nodes.contains(&d[j]))
    && precedes(nodes, d, e)
    && desc_le(nodes, e, f)))]
#[ensures(result ==> precedes(nodes, d, f))]
pub fn precedes_le_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    d: Seq<Id<K>>,
    e: Seq<Id<K>>,
    f: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = all_linked(nodes)
            && (forall<j> 0 <= j && j < d.len() ==> nodes.contains(&d[j]))
            && precedes(nodes, d, e)
            && desc_le(nodes, e, f);
        proof_assert!(pre && precedes(nodes, e, f) ==> {
            lemma_precedes_trans(nodes, d, e, f);
            true
        });
        pre
    }
}

/// [`desc_le`] is kept when both descents are extended by the same node.
#[logic]
#[ensures(result == desc_le(nodes, e, d))]
#[ensures(result ==> desc_le(nodes, Seq::singleton(p).concat(e), Seq::singleton(p).concat(d)))]
pub fn desc_le_cons_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    e: Seq<Id<K>>,
    d: Seq<Id<K>>,
    p: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(precedes(nodes, e, d) ==> {
            lemma_precedes_cons(nodes, e, d, p, p);
            true
        });
        desc_le(nodes, e, d)
    }
}

/// `d` is a descent from `p` to `start`, which the search is about to find from `y`, and its next
/// node is `queue[kc]`.
#[logic(open)]
#[allow(clippy::too_many_arguments, reason = "The state of the search")]
pub fn bfs_push_case<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    head: Int,
    old: Int,
    i: Int,
    p: Id<K>,
    yk: K,
    d: Seq<Id<K>>,
    kc: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        bfs_pushing(nodes, start, queue, map, level, head, old, i, p, yk)
            && descending_to(nodes, d, start)
            && d[0] == p
            && d.len() == level[yk.deep_model()] + 2
            && 0 <= kc && kc < queue.len() && queue[kc] == d[1]
    }
}

/// One descent `d` from `p` to `start`, which continues from `d[1] = queue[kc]`, comes no earlier,
/// in the order of [`precedes`], than the descent recorded for `p` once it is found from `y`.
#[logic]
#[ensures(result == bfs_push_case(nodes, start, queue, map, level, head, old, i, p, yk, d, kc))]
#[ensures(result ==> desc_le(
    nodes,
    Seq::singleton(p).concat(tree_path(map, yk.deep_model(), level[yk.deep_model()])),
    d,
))]
#[allow(clippy::too_many_arguments, reason = "The state of the search")]
pub fn bfs_push_case_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    head: Int,
    old: Int,
    i: Int,
    p: Id<K>,
    yk: K,
    d: Seq<Id<K>>,
    kc: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = bfs_push_case(nodes, start, queue, map, level, head, old, i, p, yk, d, kc);
        let y = yk.deep_model();
        let r = d.subsequence(1, d.len());
        let c = d[1];
        let ty = tree_path(map, y, level[y]);
        let tc = tree_path(map, c, level[c]);
        proof_assert!(pre ==> {
            proof_assert!(bfs_levels_ok(nodes, start, queue, map, level, head));
            proof_assert!(queue.contains(y));
            proof_assert!(tree_path_ok(nodes, start, map, level, y));
            proof_assert!(descent_tail_ok(nodes, d, start));
            proof_assert!(r.len() == level[y] + 1);
            proof_assert!(r[0] == c);
            proof_assert!(bfs_reach_ok(nodes, start, queue, map, level, head - 1, r));
            proof_assert!(map.contains(&c));
            proof_assert!(level[c] <= level[y]);
            proof_assert!(nodes[c]@.from.contains(p));
            proof_assert!(!(kc + 1 < head));
            proof_assert!(kc + 1 == head ==> c == y);
            proof_assert!(kc + 1 == head ==> desc_le(nodes, ty, r));
            proof_assert!(head <= kc ==> level[c] == level[y]);
            proof_assert!(head <= kc ==> precedes(nodes, ty, tc));
            proof_assert!(head <= kc ==> desc_le(nodes, tc, r));
            proof_assert!(head <= kc ==> precedes_le_ok(nodes, ty, tc, r));
            proof_assert!(desc_le(nodes, ty, r));
            proof_assert!(desc_le_cons_ok(nodes, ty, r, p));
            proof_assert!(d == Seq::singleton(p).concat(r));
            desc_le(nodes, Seq::singleton(p).concat(ty), d)
        });
        pre
    }
}

/// Finding `p` keeps the recorded descent from each found node the first among its descents with
/// as many nodes: one from `p` continues from a child of `p` which the search had found, at the
/// level of `y`, and so no earlier than `y` in the queue, since `p` is new.
#[logic]
#[ensures(result == bfs_pushing(nodes, start, queue, map, level, head, old, i, p, yk))]
#[ensures(result ==> bfs_least(
    nodes,
    start,
    queue.snoc(p),
    map.add(p, yk),
    level.set(p, level[yk.deep_model()] + 1),
))]
#[allow(clippy::too_many_arguments, reason = "The state of the search")]
pub fn bfs_push_least_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    head: Int,
    old: Int,
    i: Int,
    p: Id<K>,
    yk: K,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = bfs_pushing(nodes, start, queue, map, level, head, old, i, p, yk);
        let y = yk.deep_model();
        let q2 = queue.snoc(p);
        let m2 = map.add(p, yk);
        let l2 = level.set(p, level[y] + 1);
        let ty = tree_path(map, y, level[y]);
        proof_assert!(pre ==> {
            proof_assert!(bfs_levels_ok(nodes, start, queue, map, level, head));
            proof_assert!(queue.contains(y));
            proof_assert!(map.contains(&y));
            proof_assert!(0 <= level[y]);
            proof_assert!({
                lemma_tree_path_push(map, level, y, p, yk);
                tree_path(m2, p, l2[p]) == Seq::singleton(p).concat(ty)
            });
            proof_assert!(forall<x> queue.contains(x) ==> x != p && l2[x] == level[x]
                && map.contains(&x) && tree_path_add_ok(map, level, x, p, yk)
                && tree_path(m2, x, l2[x]) == tree_path(map, x, level[x]));
            proof_assert!(forall<x> {
                crate::verification::lemma_snoc_contains(queue, p, x);
                q2.contains(x) == (queue.contains(x) || x == p)
            });
            proof_assert!(forall<d> descending_to(nodes, d, start) && d[0] == p
                && d.len() == level[y] + 2
                ==> descent_tail_ok(nodes, d, start)
                    && bfs_reach_ok(nodes, start, queue, map, level, head - 1, d.subsequence(1, d.len()))
                    && queue.contains(d[1]));
            proof_assert!(forall<d> descending_to(nodes, d, start) && d[0] == p
                && d.len() == level[y] + 2
                ==> exists<kc> 0 <= kc && kc < queue.len() && queue[kc] == d[1]
                    && bfs_push_case_ok(nodes, start, queue, map, level, head, old, i, p, yk, d, kc));
            proof_assert!(forall<d> descending_to(nodes, d, start) && d[0] == p
                && d.len() == l2[p] + 1
                ==> desc_le(nodes, tree_path(m2, p, l2[p]), d));
            bfs_least(nodes, start, q2, m2, l2)
        });
        pre
    }
}

/// [`lemma_precedes_trans`] as a fact, which applies wherever it is asserted.
#[logic]
#[ensures(result == (all_linked(nodes)
    && (forall<j> 0 <= j && j < d.len() ==> nodes.contains(&d[j]))
    && precedes(nodes, d, e)
    && precedes(nodes, e, f)))]
#[ensures(result ==> precedes(nodes, d, f))]
pub fn precedes_trans_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    d: Seq<Id<K>>,
    e: Seq<Id<K>>,
    f: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = all_linked(nodes)
            && (forall<j> 0 <= j && j < d.len() ==> nodes.contains(&d[j]))
            && precedes(nodes, d, e)
            && precedes(nodes, e, f);
        proof_assert!(pre ==> {
            lemma_precedes_trans(nodes, d, e, f);
            precedes(nodes, d, f)
        });
        pre
    }
}

/// [`lemma_precedes_cons`] as a fact, which applies wherever it is asserted.
#[logic]
#[ensures(result == precedes(nodes, d, e))]
#[ensures(result ==> precedes(nodes, Seq::singleton(a).concat(d), Seq::singleton(b).concat(e)))]
pub fn precedes_cons_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    d: Seq<Id<K>>,
    e: Seq<Id<K>>,
    a: Id<K>,
    b: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(precedes(nodes, d, e) ==> {
            lemma_precedes_cons(nodes, d, e, a, b);
            true
        });
        precedes(nodes, d, e)
    }
}

/// Without its first node, a descent of more than one node is a descent from that node's child.
#[logic]
#[ensures(result == (descending_to(nodes, d, id) && 1 < d.len()))]
#[ensures(result ==> descending_to(nodes, d.subsequence(1, d.len()), id)
    && d.subsequence(1, d.len()).len() == d.len() - 1
    && d.subsequence(1, d.len())[0] == d[1]
    && nodes[d[1]]@.from.contains(d[0])
    && d == Seq::singleton(d[0]).concat(d.subsequence(1, d.len())))]
pub fn descent_tail_ok<K, T, S>(nodes: Nodes<K, T, S>, d: Seq<Id<K>>, id: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = descending_to(nodes, d, id) && 1 < d.len();
        let r = d.subsequence(1, d.len());
        proof_assert!(pre ==> r.len() == d.len() - 1
            && (forall<j> 0 <= j && j < r.len() ==> r[j] == d[j + 1])
            && r[r.len() - 1] == id
            && (forall<j> 0 <= j && j < r.len() ==> nodes.contains(&r[j]))
            && (forall<j> 0 <= j && j + 1 < r.len() ==> nodes[r[j + 1]]@.from.contains(r[j]))
            && no_dup(r)
            && descending_to(nodes, r, id)
            && d.ext_eq(Seq::singleton(d[0]).concat(r)));
        pre
    }
}

/// [`lemma_bfs_reach`] as a fact, for the first node of `q`.
#[logic]
#[ensures(result == (bfs_found(start, queue, map, level, h + 1)
    && bfs_expanded(nodes, queue, map, level, h)
    && (forall<k> map.contains(&k) == queue.contains(k))
    && 0 <= h && h < queue.len()
    && descending_to(nodes, q, start)
    && q.len() - 1 <= level[queue[h]]))]
#[ensures(result ==> map.contains(&q[0]) && level[q[0]] <= q.len() - 1)]
#[allow(clippy::too_many_arguments, reason = "The state of the search")]
pub fn bfs_reach_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    h: Int,
    q: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = bfs_found(start, queue, map, level, h + 1)
            && bfs_expanded(nodes, queue, map, level, h)
            && (forall<k> map.contains(&k) == queue.contains(k))
            && 0 <= h && h < queue.len()
            && descending_to(nodes, q, start)
            && q.len() - 1 <= level[queue[h]];
        proof_assert!(pre ==> {
            lemma_bfs_reach(nodes, start, queue, map, level, h, q, q.len() - 1);
            map.contains(&q[0]) && level[q[0]] <= q.len() - 1
        });
        pre
    }
}

/// The state of the search when it visits `queue[h]`, the first target it visits.
#[logic(open)]
#[allow(clippy::too_many_arguments, reason = "The state of the search")]
pub fn bfs_visiting<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    h: Int,
    is_target: Mapping<IndependentNode<K, T, S>, bool>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        all_linked(nodes)
            && bfs_found(start, queue, map, level, h + 1)
            && bfs_expanded(nodes, queue, map, level, h)
            && (forall<k> map.contains(&k) == queue.contains(k))
            && predecessors(nodes, start, map)
            && (forall<j> 0 <= j && j < queue.len() ==> nodes.contains(&queue[j]))
            && 0 <= h && h < queue.len()
            && (forall<j> 0 <= j && j < h ==> !is_target[nodes[queue[j]]])
            && bfs_ordered(nodes, queue, map, level)
            && bfs_least(nodes, start, queue, map, level)
    }
}

/// `q` is a shortest descent from a target to `start`, which starts at `queue[k]`, when the search
/// visits `queue[h]`, the first target it visits.
#[logic(open)]
#[allow(clippy::too_many_arguments, reason = "The state of the search")]
pub fn bfs_first_case<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    h: Int,
    is_target: Mapping<IndependentNode<K, T, S>, bool>,
    q: Seq<Id<K>>,
    k: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        bfs_visiting(nodes, start, queue, map, level, h, is_target)
            && descending_to(nodes, q, start)
            && is_target[nodes[q[0]]]
            && q.len() == level[queue[h]] + 1
            && 0 <= k && k < queue.len() && queue[k] == q[0]
    }
}

/// Such a descent comes no earlier, in the order of [`precedes`], than the one recorded for
/// `queue[h]`.
#[logic]
#[ensures(result == bfs_first_case(nodes, start, queue, map, level, h, is_target, q, k))]
#[ensures(result ==> desc_le(nodes, tree_path(map, queue[h], level[queue[h]]), q))]
#[allow(clippy::too_many_arguments, reason = "The state of the search")]
pub fn bfs_first_case_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    h: Int,
    is_target: Mapping<IndependentNode<K, T, S>, bool>,
    q: Seq<Id<K>>,
    k: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = bfs_first_case(nodes, start, queue, map, level, h, is_target, q, k);
        let x = queue[h];
        let tx = tree_path(map, x, level[x]);
        let tq = tree_path(map, q[0], level[q[0]]);
        proof_assert!(pre ==> {
            proof_assert!(bfs_levels_ok(nodes, start, queue, map, level, h + 1));
            proof_assert!(queue.contains(x));
            proof_assert!(tree_path_ok(nodes, start, map, level, x));
            proof_assert!(bfs_reach_ok(nodes, start, queue, map, level, h, q));
            proof_assert!(level[q[0]] <= level[x]);
            proof_assert!(!(k < h));
            proof_assert!(k == h ==> q[0] == x);
            proof_assert!(k == h ==> desc_le(nodes, tx, q));
            proof_assert!(h < k ==> level[q[0]] == level[x]);
            proof_assert!(h < k ==> precedes(nodes, tx, tq));
            proof_assert!(h < k ==> queue.contains(q[0]));
            proof_assert!(h < k ==> desc_le(nodes, tq, q));
            proof_assert!(h < k ==> precedes_le_ok(nodes, tx, tq, q));
            desc_le(nodes, tx, q)
        });
        pre
    }
}

/// When the search visits `queue[h]`, the first target it visits, the recorded descent from it is
/// the first, in the order of [`precedes`], of the shortest descents from a target to `start`.
#[logic]
#[ensures(result == bfs_visiting(nodes, start, queue, map, level, h, is_target))]
#[ensures(result
    ==> first_descent(nodes, tree_path(map, queue[h], level[queue[h]]), start, is_target))]
#[ensures(result ==> descending_to(nodes, tree_path(map, queue[h], level[queue[h]]), start))]
#[ensures(result ==> tree_path(map, queue[h], level[queue[h]]).len() == level[queue[h]] + 1
    && tree_path(map, queue[h], level[queue[h]])[0] == queue[h])]
#[ensures(result ==> forall<j> 0 <= j && j < level[queue[h]]
    ==> tree_path(map, queue[h], level[queue[h]])[j + 1]
        == map[tree_path(map, queue[h], level[queue[h]])[j]].deep_model())]
#[allow(clippy::too_many_arguments, reason = "The state of the search")]
pub fn bfs_first_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    start: Id<K>,
    queue: Seq<Id<K>>,
    map: FMap<Id<K>, K>,
    level: Mapping<Id<K>, Int>,
    h: Int,
    is_target: Mapping<IndependentNode<K, T, S>, bool>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = bfs_visiting(nodes, start, queue, map, level, h, is_target);
        let x = queue[h];
        let tx = tree_path(map, x, level[x]);
        proof_assert!(pre ==> {
            proof_assert!(bfs_levels_ok(nodes, start, queue, map, level, h + 1));
            proof_assert!(queue.contains(x));
            proof_assert!(tree_path_ok(nodes, start, map, level, x));
            proof_assert!(forall<q> descending_to(nodes, q, start) && is_target[nodes[q[0]]]
                && q.len() == tx.len()
                ==> bfs_reach_ok(nodes, start, queue, map, level, h, q) && queue.contains(q[0]));
            proof_assert!(forall<q> descending_to(nodes, q, start) && is_target[nodes[q[0]]]
                && q.len() == tx.len()
                ==> exists<k> 0 <= k && k < queue.len() && queue[k] == q[0]
                    && bfs_first_case_ok(nodes, start, queue, map, level, h, is_target, q, k));
            proof_assert!(forall<q> descending_to(nodes, q, start) && is_target[nodes[q[0]]]
                && q.len() == tx.len()
                ==> desc_le(nodes, tx, q));
            first_descent(nodes, tx, start, is_target)
        });
        pre
    }
}

/// [`first_descent`] for a target given by its identifier gives [`first_descent_from`].
#[logic]
#[ensures(result == (first_descent(nodes, down, id, is_target)
    && 0 < down.len() && down[0] == target
    && all_linked(nodes)
    && forall<node: IndependentNode<K, T, S>> is_target[node] == (node@.id == target)))]
#[ensures(result ==> first_descent_from(nodes, down, id))]
pub fn first_descent_from_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    down: Seq<Id<K>>,
    id: Id<K>,
    is_target: Mapping<IndependentNode<K, T, S>, bool>,
    target: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = first_descent(nodes, down, id, is_target)
            && 0 < down.len() && down[0] == target
            && all_linked(nodes)
            && forall<node: IndependentNode<K, T, S>> is_target[node] == (node@.id == target);
        proof_assert!(pre ==> forall<q> descending_to(nodes, q, id) && q[0] == down[0]
            ==> linked(nodes, q[0]) && nodes[q[0]]@.id == target && is_target[nodes[q[0]]]);
        pre
    }
}

/// Reversing `down` without its first node, followed by a sequence which starts with that node,
/// and reversing the first `down.len()` elements again gives back `down`.
#[logic]
#[requires(0 < down.len() && 0 < up.len() && up[0] == down[0])]
#[ensures(down.reverse().subsequence(0, down.len() - 1).concat(up).subsequence(0, down.len()).reverse()
    == down)]
pub fn lemma_reverse_prefix_concat<I>(down: Seq<I>, up: Seq<I>) {
    pearlite! {
        let n = down.len();
        let pre = down.reverse().subsequence(0, n - 1);
        let o = pre.concat(up);
        let s = o.subsequence(0, n);
        proof_assert!(forall<j> 0 <= j && j < n - 1 ==> pre[j] == down[n - 1 - j]);
        proof_assert!(forall<j> 0 <= j && j < n - 1 ==> o[j] == pre[j]);
        proof_assert!(o[n - 1] == up[0]);
        proof_assert!(forall<j> 0 <= j && j < n ==> s[j] == o[j]);
        proof_assert!(forall<j> 0 <= j && j < n ==> s.reverse()[j] == s[n - 1 - j]);
        proof_assert!(s.reverse().ext_eq(down))
    }
}

/// No descent has no nodes, so an empty one is the first of those with as many nodes.
#[logic]
#[requires(d.len() == 0)]
#[ensures(first_descent_from_root(nodes, d, id))]
pub fn lemma_first_descent_from_root_empty<K, T, S>(nodes: Nodes<K, T, S>, d: Seq<Id<K>>, id: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// [`first_descent`] for the roots gives [`first_descent_from_root`].
#[logic]
#[ensures(result == (first_descent(nodes, down, id, is_target)
    && forall<node: IndependentNode<K, T, S>> is_target[node] == (node@.from.len() == 0)))]
#[ensures(result ==> first_descent_from_root(nodes, down, id))]
pub fn first_descent_from_root_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    down: Seq<Id<K>>,
    id: Id<K>,
    is_target: Mapping<IndependentNode<K, T, S>, bool>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        first_descent(nodes, down, id, is_target)
            && forall<node: IndependentNode<K, T, S>> is_target[node] == (node@.from.len() == 0)
    }
}
