//! Lemmas about recomputing the active path.

use core::hash::{BuildHasher, Hash};

use creusot_std::logic::{FMap, FSet, Mapping, such_that};

#[allow(unused_imports)]
use super::*;
use crate::{
    IndependentContents,
    verification::{Id, no_dup, set_bound},
};

/// The scores which `update_node_activity_in_place` records for each node: ((inactive nodes,
/// active nodes) along its best path, and the next node along that path).
pub type ScoreMap<K> = FMap<Id<K>, ((usize, usize), Option<K>)>;

/// The score `s` is no better than `t`: it counts fewer active nodes, or as many and at least as
/// many inactive nodes. Scores are pairs (inactive nodes, active nodes).
#[logic(open)]
pub fn score_le(s: (usize, usize), t: (usize, usize)) -> bool {
    pearlite! { s.1@ < t.1@ || (s.1@ == t.1@ && t.0@ <= s.0@) }
}

/// Changing only which nodes are active keeps a path through `set` valid.
#[logic]
#[requires(same_graph(new, old))]
#[requires(path_of(old, set, path))]
#[ensures(path_of(new, set, path))]
pub fn lemma_same_graph_path_of<K, T, S>(
    new: Nodes<K, T, S>,
    old: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    path: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// Changing only which nodes are active keeps the nodes of a path forming a path.
#[logic]
#[requires(same_graph(new, old))]
#[requires(forms_path(old, set))]
#[ensures(forms_path(new, set))]
pub fn lemma_same_graph_forms_path<K, T, S>(
    new: Nodes<K, T, S>,
    old: Nodes<K, T, S>,
    set: FSet<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let path = such_that(|path| path_of(old, set, path));
        lemma_same_graph_path_of(new, old, set, path)
    }
}

/// The entry of node `k` in `scores` names one of its parents which has an entry too, or none if
/// `k` has no parents.
#[logic(open)]
pub fn parent_linked<K, T, S>(nodes: Nodes<K, T, S>, scores: ScoreMap<K>, k: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        nodes.contains(&k)
            && match scores[k].1 {
                None => nodes[k]@.from.len() == 0,
                Some(p) => nodes[k]@.from.contains(p.deep_model()) && scores.contains(&p.deep_model()),
            }
    }
}

/// The entry of node `k` in `scores` names one of its children which has an entry too, if any.
#[logic(open)]
pub fn child_linked<K, T, S>(nodes: Nodes<K, T, S>, scores: ScoreMap<K>, k: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        nodes.contains(&k)
            && match scores[k].1 {
                None => true,
                Some(c) => nodes[k]@.to.contains(c.deep_model()) && scores.contains(&c.deep_model()),
            }
    }
}

/// The entry of `k` in `scores` is [`parent_linked`], and its score counts at most `bound` nodes.
#[logic(open)]
pub fn parent_scored<K, T, S>(
    nodes: Nodes<K, T, S>,
    scores: ScoreMap<K>,
    k: Id<K>,
    bound: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { parent_linked(nodes, scores, k) && scores[k].0.0@ + scores[k].0.1@ <= bound }
}

/// The entry of `k` in `scores` is [`child_linked`], and its score counts at most `bound` nodes.
#[logic(open)]
pub fn child_scored<K, T, S>(
    nodes: Nodes<K, T, S>,
    scores: ScoreMap<K>,
    k: Id<K>,
    bound: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { child_linked(nodes, scores, k) && scores[k].0.0@ + scores[k].0.1@ <= bound }
}

/// The walk from `id` along the parents recorded in `scores` has visited `chain` and is now at
/// `current`, or has stopped at a root: `chain` starts at `id`, lists distinct nodes, each of which
/// is followed by one of its parents, and `current` is a parent of its last element with a lower
/// rank than all of them.
#[logic(open)]
pub fn ancestor_walk<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    scores: ScoreMap<K>,
    chain: Seq<Id<K>>,
    current: Option<Id<K>>,
    id: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        no_dup(chain)
            && (forall<j> 0 <= j && j < chain.len() ==> nodes.contains(&chain[j]))
            && (forall<j> 0 <= j && j + 1 < chain.len() ==> nodes[chain[j]]@.from.contains(chain[j + 1]))
            && (0 < chain.len() ==> chain[0] == id)
            && match current {
                Some(x) => scores.contains(&x)
                    && nodes.contains(&x)
                    && (chain.len() == 0 ==> x == id)
                    && (0 < chain.len() ==> nodes[chain[chain.len() - 1]]@.from.contains(x))
                    && forall<j> 0 <= j && j < chain.len() ==> rank[x] < rank[chain[j]],
                None => 0 < chain.len() && nodes[chain[chain.len() - 1]]@.from.len() == 0,
            }
    }
}

/// One step of the walk along recorded parents.
#[logic]
#[requires(ranked(nodes, rank))]
#[requires(ancestor_walk(nodes, rank, scores, chain, Some(x), id))]
#[requires(forall<k> scores.contains(&k) ==> parent_linked(nodes, scores, k))]
#[ensures(ancestor_walk(nodes, rank, scores, chain.snoc(x), scores[x].1.deep_model(), id))]
#[ensures(forall<k> chain.snoc(x).contains(k) == (chain.contains(k) || k == x))]
pub fn lemma_ancestor_step<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    scores: ScoreMap<K>,
    chain: Seq<Id<K>>,
    x: Id<K>,
    id: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let next = chain.snoc(x);
        proof_assert!(next.len() == chain.len() + 1 && next[chain.len()] == x);
        proof_assert!(forall<j> 0 <= j && j < chain.len() ==> next[j] == chain[j]);
        proof_assert!(!chain.contains(x));
        proof_assert!(parent_linked(nodes, scores, x));
        proof_assert!(match scores[x].1 {
            None => true,
            Some(p) => rank[p.deep_model()] < rank[x],
        })
    }
}

/// The walk ending at a root has visited exactly the nodes of a path through `set`.
#[logic]
#[requires(ancestor_walk(nodes, rank, scores, chain, None, id))]
#[requires(forall<k> set.contains(&k) == chain.contains(k))]
#[ensures(path_of(nodes, set, chain))]
#[ensures(0 < chain.len() && chain[0] == id)]
pub fn lemma_ancestor_done<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    scores: ScoreMap<K>,
    chain: Seq<Id<K>>,
    id: Id<K>,
    set: FSet<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// The path which the walk along recorded children has built after visiting `x`.
#[logic(open)]
pub fn walk_prepend<I>(path: Seq<I>, x: I) -> Seq<I> {
    pearlite! { if x == path[0] { path } else { Seq::singleton(x).concat(path) } }
}

/// The walk along the children recorded in `scores` has built `path`, a path through exactly
/// `set` whose first node has the highest rank, and is now at `current`, which is that first node
/// or one of its children.
#[logic(open)]
pub fn descendant_walk<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    scores: ScoreMap<K>,
    set: FSet<Id<K>>,
    path: Seq<Id<K>>,
    current: Option<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        path_of(nodes, set, path)
            && 0 < path.len()
            && (forall<j> 0 <= j && j < path.len() ==> rank[path[j]] <= rank[path[0]])
            && match current {
                Some(x) => scores.contains(&x)
                    && nodes.contains(&x)
                    && (x == path[0] || nodes[x]@.from.contains(path[0])),
                None => true,
            }
    }
}

/// The walk along recorded parents goes from `x` to a node of lower rank, which bounds its length.
#[logic]
#[ensures(result == (all_linked(nodes) && ranked(nodes, rank) && nodes.contains(&x)
    && scores.contains(&x) && parent_linked(nodes, scores, x)))]
#[ensures(result ==> 0 <= rank[x])]
#[ensures(result ==> match scores[x].1 {
    Some(p) => nodes.contains(&p.deep_model()) && 0 <= rank[p.deep_model()] && rank[p.deep_model()] < rank[x],
    None => true,
})]
pub fn up_rank_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    scores: ScoreMap<K>,
    x: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(all_linked(nodes) && nodes.contains(&x) ==> linked(nodes, x));
        all_linked(nodes) && ranked(nodes, rank) && nodes.contains(&x) && scores.contains(&x)
            && parent_linked(nodes, scores, x)
    }
}

/// The walk along recorded children goes from `x` to a node of higher rank, below the ranks' bound,
/// which bounds its length.
#[logic]
#[ensures(result == (all_linked(nodes) && ranked(nodes, rank) && nodes.contains(&x)
    && scores.contains(&x) && child_linked(nodes, scores, x)))]
#[ensures(result ==> rank[x] < set_bound(nodes.keys(), rank))]
#[ensures(result ==> match scores[x].1 {
    Some(c) => nodes.contains(&c.deep_model()) && rank[x] < rank[c.deep_model()]
        && rank[c.deep_model()] < set_bound(nodes.keys(), rank),
    None => true,
})]
pub fn down_rank_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    scores: ScoreMap<K>,
    x: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(all_linked(nodes) && ranked(nodes, rank) && nodes.contains(&x)
            && child_linked(nodes, scores, x) ==> linked(nodes, x));
        all_linked(nodes) && ranked(nodes, rank) && nodes.contains(&x) && scores.contains(&x)
            && child_linked(nodes, scores, x)
    }
}

/// One step of the walk along recorded children.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(descendant_walk(nodes, rank, scores, set, path, Some(x)))]
#[requires(forall<k> scores.contains(&k) ==> child_linked(nodes, scores, k))]
#[ensures(descendant_walk(nodes, rank, scores, set.add(x), walk_prepend(path, x), scores[x].1.deep_model()))]
#[ensures(descendant_walk(nodes, rank, scores, set.add(x), walk_prepend(path, x), None))]
pub fn lemma_descendant_step<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    scores: ScoreMap<K>,
    set: FSet<Id<K>>,
    path: Seq<Id<K>>,
    x: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        if x == path[0] {
            proof_assert!(path.contains(x) && set.contains(&x));
            proof_assert!(set.add(x).ext_eq(set));
        } else {
            let new = Seq::singleton(x).concat(path);
            proof_assert!(new.len() == path.len() + 1 && new[0] == x);
            proof_assert!(forall<i> 0 <= i && i < path.len() ==> new[i + 1] == path[i]);
            proof_assert!(rank[path[0]] < rank[x]);
            proof_assert!(!path.contains(x));
            proof_assert!(forall<i> 0 <= i && i < new.len() ==> nodes.contains(&new[i]));
            proof_assert!(nodes[new[0]]@.from.contains(new[1]));
            proof_assert!(forall<i> 1 <= i && i + 1 < new.len() ==> nodes[new[i]]@.from.contains(new[i + 1]));
            proof_assert!(nodes[new[new.len() - 1]]@.from.len() == 0);
            proof_assert!(valid_path(nodes, new));
            proof_assert!(forall<i, j> 1 <= i && i < j && j < new.len() ==> new[i] != new[j]);
            proof_assert!(forall<j> 1 <= j && j < new.len() ==> new[0] != new[j]);
            proof_assert!(no_dup(new));
            proof_assert!(forall<k> new.contains(k) == (path.contains(k) || k == x));
            proof_assert!(forall<j> 0 <= j && j < new.len() ==> rank[new[j]] <= rank[new[0]]);
        }
        proof_assert!(child_linked(nodes, scores, x) && linked(nodes, x));
        proof_assert!(match scores[x].1 {
            None => true,
            Some(c) => nodes.contains(&c.deep_model()) && nodes[c.deep_model()]@.from.contains(x),
        })
    }
}

/// In a topological order of the nodes accepted by `admitted`, the accepted children of the node at
/// index `i` come after it.
#[logic]
#[requires(all_linked(nodes))]
#[requires(kahn_order(nodes, admitted, order, Seq::empty()))]
#[requires(0 <= i && i < order.len())]
#[requires(nodes[order[i]]@.to.contains(c))]
#[requires(order.contains(c))]
#[ensures(exists<j> i < j && j < order.len() && order[j] == c)]
pub fn lemma_child_after<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    order: Seq<Id<K>>,
    i: Int,
    c: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let j = such_that(|j: Int| 0 <= j && j < order.len() && order[j] == c);
        proof_assert!(linked(nodes, order[i]) && nodes[c]@.from.contains(order[i]));
        proof_assert!(admitted_parent(nodes, admitted, order[j], order[i]));
        proof_assert!(order.subsequence(0, j).contains(order[i]));
        let m = such_that(|m: Int| 0 <= m && m < j && order.subsequence(0, j)[m] == order[i]);
        proof_assert!(order[m] == order[i]);
        proof_assert!(m == i);
        proof_assert!(i < j)
    }
}

/// In a topological order of the nodes accepted by `admitted`, the accepted parents of the node at
/// index `i` come before it.
#[logic]
#[requires(kahn_order(nodes, admitted, order, Seq::empty()))]
#[requires(0 <= i && i < order.len())]
#[requires(admitted_parent(nodes, admitted, order[i], p))]
#[ensures(exists<j> 0 <= j && j < i && order[j] == p)]
pub fn lemma_parent_before<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    order: Seq<Id<K>>,
    i: Int,
    p: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(order.subsequence(0, i).contains(p));
        let m = such_that(|m: Int| 0 <= m && m < i && order.subsequence(0, i)[m] == p);
        proof_assert!(order[m] == p)
    }
}

/// In a path starting at `id`, no node is a child of `id`.
#[logic]
#[requires(all_linked(nodes))]
#[requires(acyclic(nodes))]
#[requires(path_of(nodes, set, path))]
#[requires(0 < path.len() && path[0] == id)]
#[ensures(!has_child_in(nodes, set, id))]
pub fn lemma_path_start_childless<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    path: Seq<Id<K>>,
    id: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let rank = rank_of(nodes);
        lemma_path_ranks_all(nodes, rank, path);
        proof_assert!(linked(nodes, id));
        proof_assert!(forall<c> nodes[id]@.to.contains(c) ==> nodes.contains(&c) && rank[id] < rank[c]);
        proof_assert!(forall<j> 0 <= j && j < path.len() ==> rank[path[j]] <= rank[id]);
        proof_assert!(forall<c> nodes[id]@.to.contains(c) ==> !path.contains(c))
    }
}

/// `same_graph` is transitive.
#[logic]
#[requires(same_graph(a, b))]
#[requires(same_graph(b, c))]
#[ensures(same_graph(a, c))]
pub fn lemma_same_graph_trans<K, T, S>(a: Nodes<K, T, S>, b: Nodes<K, T, S>, c: Nodes<K, T, S>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// A root which the topological sort hasn't been started from yet hasn't been output, as long as
/// every output node without parents is one of the roots it has been started from.
#[logic]
#[requires(no_dup(roots))]
#[requires(forall<k> output.contains(k) && nodes[k]@.from.len() == 0
    ==> exists<j> 0 <= j && j < n && roots[j] == k)]
#[requires(0 <= n && n < roots.len())]
#[requires(nodes[roots[n]]@.from.len() == 0)]
#[ensures(!output.contains(roots[n]))]
pub fn lemma_root_unsorted<K, T, S>(
    nodes: Nodes<K, T, S>,
    roots: Seq<Id<K>>,
    n: Int,
    output: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// The topological sort of a parent-closed set from all of its roots outputs all of it.
#[logic]
#[requires(all_linked(nodes) && acyclic(nodes))]
#[requires(forall<k> set.contains(&k) ==> nodes.contains(&k))]
#[requires(parent_closed(nodes, set))]
#[requires(roots_of(nodes, set, roots))]
#[requires(forall<k> admitted[k] == set.contains(&k))]
#[requires(kahn_state(nodes, admitted, order, counts))]
#[requires(forall<j> 0 <= j && j < roots.len() ==> order.contains(roots[j]))]
#[ensures(forall<k> set.contains(&k) ==> order.contains(k))]
pub fn lemma_ancestors_sorted<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    roots: Seq<Id<K>>,
    admitted: Mapping<Id<K>, bool>,
    order: Seq<Id<K>>,
    counts: FMap<Id<K>, usize>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> set.contains(&k) && nodes[k]@.from.len() == 0 ==> roots.contains(k) && order.contains(k));
        proof_assert!(forall<k> set.contains(&k) && nodes[k]@.from.len() > 0
            ==> nodes[k]@.from.contains(nodes[k]@.from[0]) && admitted_parent(nodes, admitted, k, nodes[k]@.from[0]));
        lemma_kahn_complete(nodes, admitted, order, counts)
    }
}

/// Activating a node which extends the active path leaves it without active children.
#[logic]
#[requires(all_linked(nodes) && acyclic(nodes))]
#[requires(forms_path(nodes, active))]
#[requires(nodes.contains(&id))]
#[requires(extends_path(nodes, active, id))]
#[ensures(!has_child_in(nodes, active.add(id), id))]
pub fn lemma_extends_childless<K, T, S>(nodes: Nodes<K, T, S>, active: FSet<Id<K>>, id: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let rank = rank_of(nodes);
        proof_assert!(linked(nodes, id));
        proof_assert!(!nodes[id]@.to.contains(id));
        if nodes[id]@.from.len() == 0 && active.len() == 0 {
            proof_assert!(forall<k> !active.contains(&k));
        } else {
            let p = such_that(|p| nodes[id]@.from.contains(p) && active.contains(&p)
                && !has_child_in(nodes, active, p));
            let path = such_that(|path| path_of(nodes, active, path));
            lemma_path_end(nodes, active, path, p);
            lemma_path_ranks_all(nodes, rank, path);
            proof_assert!(rank[p] < rank[id]);
            proof_assert!(forall<c> nodes[id]@.to.contains(c) ==> nodes.contains(&c) && rank[id] < rank[c]);
            proof_assert!(forall<j> 0 <= j && j < path.len() ==> rank[path[j]] <= rank[p]);
            proof_assert!(forall<c> nodes[id]@.to.contains(c) ==> !path.contains(c) && !active.contains(&c));
        }
    }
}

// The following facts are stated as boolean functions which hold exactly when their
// preconditions do: asserting `fact(args)` in a large function puts the call in the context, and
// the call triggers the conclusions (`result ==> ...`). Lemmas returning `()` are only used when
// the prover happens to instantiate them, as their calls are erased from the goals.

/// One step of the walk along recorded parents.
#[logic]
#[ensures(result == (ranked(nodes, rank)
    && ancestor_walk(nodes, rank, scores, chain, Some(x), id)
    && forall<k> scores.contains(&k) ==> parent_linked(nodes, scores, k)))]
#[ensures(result ==> ancestor_walk(nodes, rank, scores, chain.snoc(x), scores[x].1.deep_model(), id))]
#[ensures(result ==> forall<k> chain.snoc(x).contains(k) == (chain.contains(k) || k == x))]
pub fn ancestor_step_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    scores: ScoreMap<K>,
    chain: Seq<Id<K>>,
    x: Id<K>,
    id: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = ranked(nodes, rank)
            && ancestor_walk(nodes, rank, scores, chain, Some(x), id)
            && forall<k> scores.contains(&k) ==> parent_linked(nodes, scores, k);
        proof_assert!(pre ==> {
            lemma_ancestor_step(nodes, rank, scores, chain, x, id);
            true
        } && ancestor_walk(nodes, rank, scores, chain.snoc(x), scores[x].1.deep_model(), id)
            && forall<k> chain.snoc(x).contains(k) == (chain.contains(k) || k == x));
        pre
    }
}

/// One step of the walk along recorded children.
#[logic]
#[ensures(result == (all_linked(nodes) && ranked(nodes, rank)
    && descendant_walk(nodes, rank, scores, set, path, Some(x))
    && forall<k> scores.contains(&k) ==> child_linked(nodes, scores, k)))]
#[ensures(result ==> descendant_walk(nodes, rank, scores, set.add(x), walk_prepend(path, x), scores[x].1.deep_model()))]
#[ensures(result ==> descendant_walk(nodes, rank, scores, set.add(x), walk_prepend(path, x), None))]
pub fn descendant_step_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    scores: ScoreMap<K>,
    set: FSet<Id<K>>,
    path: Seq<Id<K>>,
    x: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = all_linked(nodes) && ranked(nodes, rank)
            && descendant_walk(nodes, rank, scores, set, path, Some(x))
            && forall<k> scores.contains(&k) ==> child_linked(nodes, scores, k);
        proof_assert!(pre ==> {
            lemma_descendant_step(nodes, rank, scores, set, path, x);
            true
        } && descendant_walk(nodes, rank, scores, set.add(x), walk_prepend(path, x), scores[x].1.deep_model())
            && descendant_walk(nodes, rank, scores, set.add(x), walk_prepend(path, x), None));
        pre
    }
}

/// Recording the score `entry` of `k`, whose recorded parents all have scores, keeps every
/// recorded parent scored, with a bound one higher.
#[logic]
#[ensures(result == ((forall<j> scores.contains(&j) ==> parent_scored(nodes, scores, j, bound))
    && nodes.contains(&k)
    && entry.0.0@ + entry.0.1@ <= bound + 1
    && match entry.1 {
        None => nodes[k]@.from.len() == 0,
        Some(p) => nodes[k]@.from.contains(p.deep_model()) && scores.contains(&p.deep_model()),
    }))]
#[ensures(result ==> forall<j> scores.add(k, entry).contains(&j)
    ==> parent_scored(nodes, scores.add(k, entry), j, bound + 1))]
pub fn parent_insert_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    scores: ScoreMap<K>,
    k: Id<K>,
    entry: ((usize, usize), Option<K>),
    bound: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<j> scores.add(k, entry).contains(&j) == (scores.contains(&j) || j == k));
        proof_assert!(forall<j> scores.contains(&j) && j != k ==> scores.add(k, entry)[j] == scores[j]);
        proof_assert!(scores.add(k, entry)[k] == entry);
        (forall<j> scores.contains(&j) ==> parent_scored(nodes, scores, j, bound))
            && nodes.contains(&k)
            && entry.0.0@ + entry.0.1@ <= bound + 1
            && match entry.1 {
                None => nodes[k]@.from.len() == 0,
                Some(p) => nodes[k]@.from.contains(p.deep_model()) && scores.contains(&p.deep_model()),
            }
    }
}

/// Recording the score `entry` of `k`, whose recorded children all have scores, keeps every
/// recorded child scored, with a bound one higher.
#[logic]
#[ensures(result == ((forall<j> scores.contains(&j) ==> child_scored(nodes, scores, j, bound))
    && nodes.contains(&k)
    && entry.0.0@ + entry.0.1@ <= bound + 1
    && match entry.1 {
        None => true,
        Some(c) => nodes[k]@.to.contains(c.deep_model()) && scores.contains(&c.deep_model()),
    }))]
#[ensures(result ==> forall<j> scores.add(k, entry).contains(&j)
    ==> child_scored(nodes, scores.add(k, entry), j, bound + 1))]
pub fn child_insert_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    scores: ScoreMap<K>,
    k: Id<K>,
    entry: ((usize, usize), Option<K>),
    bound: Int,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<j> scores.add(k, entry).contains(&j) == (scores.contains(&j) || j == k));
        proof_assert!(forall<j> scores.contains(&j) && j != k ==> scores.add(k, entry)[j] == scores[j]);
        proof_assert!(scores.add(k, entry)[k] == entry);
        (forall<j> scores.contains(&j) ==> child_scored(nodes, scores, j, bound))
            && nodes.contains(&k)
            && entry.0.0@ + entry.0.1@ <= bound + 1
            && match entry.1 {
                None => true,
                Some(c) => nodes[k]@.to.contains(c.deep_model()) && scores.contains(&c.deep_model()),
            }
    }
}

/// In a set whose elements other than `id` all have a parent in it, `id` has no parent in it.
#[logic]
#[ensures(result == (all_linked(nodes) && acyclic(nodes)
    && nodes.contains(&id)
    && (forall<k> set.contains(&k) ==> nodes.contains(&k))
    && (forall<k> set.contains(&k) && k != id ==> has_parent_in(nodes, set, k))
    && forall<k> admitted[k] == set.contains(&k)))]
#[ensures(result ==> forall<p> !admitted_parent(nodes, admitted, id, p))]
pub fn descendants_start_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    admitted: Mapping<Id<K>, bool>,
    id: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = all_linked(nodes) && acyclic(nodes)
            && nodes.contains(&id)
            && (forall<k> set.contains(&k) ==> nodes.contains(&k))
            && (forall<k> set.contains(&k) && k != id ==> has_parent_in(nodes, set, k))
            && forall<k> admitted[k] == set.contains(&k);
        proof_assert!(pre ==> {
            let rank = rank_of(nodes);
            proof_assert!(forall<p> admitted_parent(nodes, admitted, id, p) ==> {
                lemma_closure_rank(nodes, rank, set, id, p);
                false
            });
            true
        } && forall<p> !admitted_parent(nodes, admitted, id, p));
        pre
    }
}

/// Nodes with an admitted parent have parents.
#[logic]
#[requires(forall<i> lo < i && i < s.len()
    ==> exists<p> admitted_parent(nodes, admitted, s[i].deep_model(), p))]
#[ensures(forall<i> lo < i && i < s.len() ==> nodes[s[i].deep_model()]@.from.len() > 0)]
pub fn lemma_admitted_have_parents<K, T, S>(
    nodes: Nodes<K, T, S>,
    admitted: Mapping<Id<K>, bool>,
    s: Seq<K>,
    lo: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<i> lo < i && i < s.len() ==> {
            let p = such_that(|p| admitted_parent(nodes, admitted, s[i].deep_model(), p));
            nodes[s[i].deep_model()]@.from.contains(p)
        })
    }
}
