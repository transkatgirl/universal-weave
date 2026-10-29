//! Lemmas about the paths which activation chooses: how their nodes are counted and compared, the
//! scores from which `update_node_activity_in_place` and `set_active_dependent_semantics` pick the
//! best ones, and the longest paths which deactivation and removal keep (`paths` proves that
//! `longest_candidate_path_to_root` finds them).
//!
//! The scores record, for each node, (inactive nodes, active nodes) along the best path which the
//! recorded parents or children lead along from it. [`up_scored`] and [`down_scored`] say that a
//! score is the best one, and are only visible in this module, so that they don't weigh on the
//! goals of the functions which use the facts below.

use core::hash::{BuildHasher, Hash};

use creusot_std::logic::{FSet, Mapping, such_that};

#[allow(unused_imports)]
use super::*;
use crate::{
    IndependentContents,
    verification::{Id, increasing_steps, lemma_increasing, lemma_subset_len, no_dup},
};

// Counting.

/// Counting gives at least zero and at most the length.
#[logic]
#[ensures(0 <= count_in(set, s) && count_in(set, s) <= s.len())]
#[variant(s.len())]
pub fn lemma_count_in_bounds<I>(set: FSet<I>, s: Seq<I>) {
    if s.len() > 0 {
        lemma_count_in_bounds(set, s.tail())
    }
}

/// Counting with one more element at the end.
#[logic]
#[ensures(count_in(set, s.snoc(x)) == count_in(set, s) + if set.contains(&x) { 1 } else { 0 })]
#[variant(s.len())]
pub fn lemma_count_snoc<I>(set: FSet<I>, s: Seq<I>, x: I) {
    pearlite! {
        if s.len() == 0 {
            proof_assert!(s.snoc(x).len() == 1 && s.snoc(x)[0] == x && s.snoc(x).tail().len() == 0);
        } else {
            proof_assert!(s.snoc(x).tail().ext_eq(s.tail().snoc(x)));
            proof_assert!(s.snoc(x)[0] == s[0]);
            lemma_count_snoc(set, s.tail(), x)
        }
    }
}

/// Counting over a concatenation.
#[logic]
#[ensures(count_in(set, a.concat(b)) == count_in(set, a) + count_in(set, b))]
#[variant(a.len())]
pub fn lemma_count_concat<I>(set: FSet<I>, a: Seq<I>, b: Seq<I>) {
    pearlite! {
        if a.len() == 0 {
            proof_assert!(a.concat(b).ext_eq(b));
        } else {
            proof_assert!(a.concat(b).tail().ext_eq(a.tail().concat(b)));
            proof_assert!(a.concat(b)[0] == a[0]);
            lemma_count_concat(set, a.tail(), b)
        }
    }
}

/// Adding an element which isn't in `s` to `set` doesn't change the count.
#[logic]
#[requires(!s.contains(x))]
#[ensures(count_in(set.add(x), s) == count_in(set, s))]
#[variant(s.len())]
pub fn lemma_count_add_absent<I>(set: FSet<I>, s: Seq<I>, x: I) {
    pearlite! {
        if s.len() > 0 {
            proof_assert!(forall<i> 0 <= i && i < s.tail().len() ==> s.tail()[i] == s[i + 1]);
            proof_assert!(!s.tail().contains(x));
            lemma_count_add_absent(set, s.tail(), x)
        }
    }
}

/// Nothing is counted exactly when no element is in `set`.
#[logic]
#[ensures((count_in(set, s) == 0) == forall<i> 0 <= i && i < s.len() ==> !set.contains(&s[i]))]
#[variant(s.len())]
pub fn lemma_count_in_zero<I>(set: FSet<I>, s: Seq<I>) {
    pearlite! {
        if s.len() > 0 {
            proof_assert!(forall<i> 0 <= i && i < s.tail().len() ==> s.tail()[i] == s[i + 1]);
            lemma_count_in_bounds(set, s.tail());
            lemma_count_in_zero(set, s.tail())
        }
    }
}

/// A sequence without duplicates starting at `id` counts one more node of `set.add(id)` than of
/// `set`, when `id` isn't in `set`.
#[logic]
#[requires(!set.contains(&id))]
#[requires(0 < q.len() && q[0] == id && no_dup(q))]
#[ensures(count_in(set.add(id), q) == count_in(set, q) + 1)]
pub fn lemma_count_shift<I>(set: FSet<I>, id: I, q: Seq<I>) {
    pearlite! {
        proof_assert!(forall<i> 0 <= i && i < q.tail().len() ==> q.tail()[i] == q[i + 1]);
        proof_assert!(!q.tail().contains(id));
        lemma_count_add_absent(set, q.tail(), id)
    }
}

// Paths.

/// Without its first node, a path up of more than one node is a path up from the next node, a
/// parent of the first.
#[logic]
#[requires(path_up(nodes, id, q))]
#[requires(1 < q.len())]
#[ensures(path_up(nodes, q[1], q.tail()))]
#[ensures(nodes[id]@.from.contains(q[1]))]
#[ensures(q.tail().len() == q.len() - 1)]
#[ensures(count_in(set, q) == count_in(set, q.tail()) + if set.contains(&id) { 1 } else { 0 })]
pub fn lemma_path_up_tail<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    id: Id<K>,
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

/// Without its first node, a path down of more than one node is a path down from the next node, a
/// child of the first.
#[logic]
#[requires(path_down(nodes, id, q))]
#[requires(1 < q.len())]
#[ensures(path_down(nodes, q[1], q.tail()))]
#[ensures(nodes[id]@.to.contains(q[1]))]
#[ensures(q.tail().len() == q.len() - 1)]
#[ensures(count_in(set, q) == count_in(set, q.tail()) + if set.contains(&id) { 1 } else { 0 })]
pub fn lemma_path_down_tail<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    id: Id<K>,
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

/// A path of one node counts that node.
#[logic]
#[requires(q.len() == 1)]
#[ensures(count_in(set, q) == if set.contains(&q[0]) { 1 } else { 0 })]
pub fn lemma_count_single<I>(set: FSet<I>, q: Seq<I>) {
    proof_assert!(q.tail().len() == 0)
}

/// Ranks increase strictly along a sequence in which each node is followed by one of its
/// children.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(forall<i> 0 <= i && i < q.len() ==> nodes.contains(&q[i]))]
#[requires(forall<i> 0 <= i && i + 1 < q.len() ==> nodes[q[i]]@.to.contains(q[i + 1]))]
#[ensures(forall<i, j> 0 <= i && i < j && j < q.len() ==> rank[q[i]] < rank[q[j]])]
#[ensures(no_dup(q))]
pub fn lemma_down_ranks<K, T, S>(nodes: Nodes<K, T, S>, rank: Mapping<Id<K>, Int>, q: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<i> 0 <= i && i + 1 < q.len()
            ==> linked(nodes, q[i]) && nodes[q[i + 1]]@.from.contains(q[i]));
        proof_assert!(increasing_steps(q, rank));
        lemma_increasing(q, rank)
    }
}

/// A sequence in which each node is followed by one of its children stays in a child-closed set
/// containing its first node.
#[logic]
#[requires(child_closed(nodes, closure))]
#[requires(0 < q.len() && closure.contains(&q[0]))]
#[requires(forall<i> 0 <= i && i + 1 < q.len() ==> nodes[q[i]]@.to.contains(q[i + 1]))]
#[requires(0 <= n && n < q.len())]
#[ensures(closure.contains(&q[n]))]
#[variant(n)]
pub fn lemma_down_closed<K, T, S>(
    nodes: Nodes<K, T, S>,
    closure: FSet<Id<K>>,
    q: Seq<Id<K>>,
    n: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if n > 0 {
        lemma_down_closed(nodes, closure, q, n - 1)
    }
}

// Comparing paths.

/// A best path up from `id` for keeping the nodes of `set.add(id)` is also one for keeping those
/// of `set`, as every path up from `id` contains `id` once.
#[logic]
#[requires(best_path_up(nodes, set.add(id), id, p))]
#[ensures(best_path_up(nodes, set, id, p))]
pub fn lemma_best_up_shift<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    id: Id<K>,
    p: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        if set.contains(&id) {
            proof_assert!(set.add(id).ext_eq(set));
        } else {
            lemma_count_shift(set, id, p);
            proof_assert!(forall<q> path_up(nodes, id, q) ==> {
                lemma_count_shift(set, id, q);
                true
            } && no_better(set, q, p));
        }
    }
}

/// A best path down from `id` for keeping the nodes of `set.add(id)` is also one for keeping
/// those of `set`, as every path down from `id` contains `id` once.
#[logic]
#[requires(best_path_down(nodes, set.add(id), id, p))]
#[ensures(best_path_down(nodes, set, id, p))]
pub fn lemma_best_down_shift<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    id: Id<K>,
    p: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        if set.contains(&id) {
            proof_assert!(set.add(id).ext_eq(set));
        } else {
            lemma_count_shift(set, id, p);
            proof_assert!(forall<q> path_down(nodes, id, q) ==> {
                lemma_count_shift(set, id, q);
                true
            } && no_better(set, q, p));
        }
    }
}

/// `below` is one of the best paths from `id` down to a leaf, when the nodes of `set` were active,
/// cut after its last node in `set`. With `id` in `set`, this may be `id` alone.
#[logic(open)]
pub fn cut_best_down<K, T, S>(
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
        exists<path> best_path_down(nodes, set, id, path)
            && 0 < below.len()
            && below.len() <= path.len()
            && (forall<i> 0 <= i && i < below.len() ==> below[i] == path[i])
            && set.contains(&below[below.len() - 1])
            && forall<i> below.len() <= i && i < path.len() ==> !set.contains(&path[i])
    }
}

/// Cutting a best path down for keeping the nodes of `set` and `id` active keeps the nodes of
/// `set` below `id`, as [`kept_below`] describes.
#[logic]
#[requires(!set.contains(&id))]
#[requires(cut_best_down(nodes, set.add(id), id, below))]
#[ensures(kept_below(nodes, set, id, below))]
pub fn lemma_kept_below<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    id: Id<K>,
    below: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let all = set.add(id);
        let path = such_that(|path| best_path_down(nodes, all, id, path)
            && 0 < below.len()
            && below.len() <= path.len()
            && (forall<i> 0 <= i && i < below.len() ==> below[i] == path[i])
            && all.contains(&below[below.len() - 1])
            && forall<i> below.len() <= i && i < path.len() ==> !all.contains(&path[i]));
        lemma_best_down_shift(nodes, set, id, path);
        if below.len() == 1 {
            proof_assert!(below.ext_eq(Seq::singleton(id)));
            proof_assert!(forall<i> 0 <= i && i < path.tail().len() ==> path.tail()[i] == path[i + 1]);
            lemma_count_in_zero(all, path.tail());
            proof_assert!(count_in(all, path) == 1);
            proof_assert!(forall<q, i> path_down(nodes, id, q) && 0 < i && i < q.len() ==> {
                proof_assert!(no_better(all, q, path));
                proof_assert!(forall<j> 0 <= j && j < q.tail().len() ==> q.tail()[j] == q[j + 1]);
                lemma_count_in_bounds(all, q.tail());
                lemma_count_in_zero(all, q.tail());
                proof_assert!(q.tail()[i - 1] == q[i]);
                true
            } && !set.contains(&q[i]));
        } else {
            proof_assert!(path[0] == id && below[below.len() - 1] == path[below.len() - 1]);
            proof_assert!(below[below.len() - 1] != id);
        }
    }
}

// The best paths up.

/// The recorded score of `k`, with the nodes of `set` active, is that of the path which the
/// recorded parents lead along from `k`, and no path from `k` up to a root is better.
#[logic(open(self))]
pub fn up_scored<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    scores: ScoreMap<K>,
    k: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let s = scores[k].0;
        let a = if set.contains(&k) { 1 } else { 0 };
        (match scores[k].1 {
            None => s.0@ == 1 - a && s.1@ == a,
            Some(p) => s.0@ == scores[p.deep_model()].0.0@ + 1 - a
                && s.1@ == scores[p.deep_model()].0.1@ + a,
        })
            && forall<q> path_up(nodes, k, q)
                ==> count_in(set, q) < s.1@ || (count_in(set, q) == s.1@ && s.0@ + s.1@ <= q.len())
    }
}

/// `entry` is what `update_node_activity_in_place` records for `k`: its own score added to that
/// of the best-scored parent, which it records, or its own score alone when it has no parents.
#[logic(open)]
pub fn up_entry<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    scores: ScoreMap<K>,
    k: Id<K>,
    entry: ((usize, usize), Option<K>),
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let a = if set.contains(&k) { 1 } else { 0 };
        match entry.1 {
            None => nodes[k]@.from.len() == 0 && entry.0.0@ == 1 - a && entry.0.1@ == a,
            Some(p) => nodes[k]@.from.contains(p.deep_model())
                && (forall<c> nodes[k]@.from.contains(c)
                    ==> score_le(scores[c].0, scores[p.deep_model()].0))
                && entry.0.0@ == scores[p.deep_model()].0.0@ + 1 - a
                && entry.0.1@ == scores[p.deep_model()].0.1@ + a,
        }
    }
}

/// Recording `entry` for `k`, whose parents all have best scores, keeps every recorded score the
/// best one.
#[logic]
#[ensures(result == (nodes.contains(&k)
    && !scores.contains(&k)
    && (forall<j> scores.contains(&j)
        ==> parent_linked(nodes, scores, j) && up_scored(nodes, set, scores, j))
    && (forall<p> nodes[k]@.from.contains(p) ==> scores.contains(&p))
    && up_entry(nodes, set, scores, k, entry)))]
#[ensures(result ==> forall<j> scores.add(k, entry).contains(&j)
    ==> up_scored(nodes, set, scores.add(k, entry), j))]
pub fn up_insert_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    scores: ScoreMap<K>,
    k: Id<K>,
    entry: ((usize, usize), Option<K>),
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = nodes.contains(&k)
            && !scores.contains(&k)
            && (forall<j> scores.contains(&j)
                ==> parent_linked(nodes, scores, j) && up_scored(nodes, set, scores, j))
            && (forall<p> nodes[k]@.from.contains(p) ==> scores.contains(&p))
            && up_entry(nodes, set, scores, k, entry);
        let new = scores.add(k, entry);
        proof_assert!(pre ==> {
            proof_assert!(forall<j> scores.contains(&j) ==> new[j] == scores[j]);
            proof_assert!(forall<j> scores.contains(&j) ==> match scores[j].1 {
                None => true,
                Some(p) => scores.contains(&p.deep_model()) && p.deep_model() != k,
            });
            proof_assert!(forall<j> scores.contains(&j) ==> up_scored(nodes, set, new, j));
            proof_assert!(new[k] == entry);
            proof_assert!(forall<q> path_up(nodes, k, q) ==> {
                if q.len() == 1 {
                    lemma_count_single(set, q);
                    proof_assert!(nodes[k]@.from.len() == 0);
                    proof_assert!(entry.1 == None);
                } else {
                    lemma_path_up_tail(nodes, set, k, q);
                    proof_assert!(scores.contains(&q[1]) && up_scored(nodes, set, scores, q[1]));
                    proof_assert!(match entry.1 {
                        None => false,
                        Some(p) => score_le(scores[q[1]].0, scores[p.deep_model()].0),
                    });
                }
                true
            } && (count_in(set, q) < entry.0.1@
                || (count_in(set, q) == entry.0.1@ && entry.0.0@ + entry.0.1@ <= q.len())));
            proof_assert!(match entry.1 {
                None => true,
                Some(p) => new[p.deep_model()] == scores[p.deep_model()],
            });
            proof_assert!(up_scored(nodes, set, new, k));
            true
        } && forall<j> new.contains(&j) ==> up_scored(nodes, set, new, j));
        pre
    }
}

/// The walk from `id` along the recorded parents has visited `chain` and is now at `current`, or
/// has stopped: the scores of the nodes visited and of `current` add up to that of `id`.
#[logic(open)]
pub fn up_walk<K: DeepModel>(
    set: FSet<Id<K>>,
    scores: ScoreMap<K>,
    chain: Seq<Id<K>>,
    current: Option<Id<K>>,
    id: Id<K>,
) -> bool {
    pearlite! {
        match current {
            Some(c) => scores.contains(&c)
                && count_in(set, chain) + scores[c].0.1@ == scores[id].0.1@
                && chain.len() + scores[c].0.0@ + scores[c].0.1@
                    == scores[id].0.0@ + scores[id].0.1@,
            None => count_in(set, chain) == scores[id].0.1@
                && chain.len() == scores[id].0.0@ + scores[id].0.1@,
        }
    }
}

/// One step of the walk along recorded parents.
#[logic]
#[ensures(result == (up_walk(set, scores, chain, Some(x), id)
    && parent_linked(nodes, scores, x)
    && up_scored(nodes, set, scores, x)))]
#[ensures(result ==> up_walk(set, scores, chain.snoc(x), scores[x].1.deep_model(), id))]
pub fn up_walk_step_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
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
        let pre = up_walk(set, scores, chain, Some(x), id)
            && parent_linked(nodes, scores, x)
            && up_scored(nodes, set, scores, x);
        proof_assert!(pre ==> {
            lemma_count_snoc(set, chain, x);
            true
        } && up_walk(set, scores, chain.snoc(x), scores[x].1.deep_model(), id));
        pre
    }
}

/// `up` is one of the best paths from `id` up to a root: [`best_path_up`], hidden from the
/// functions which find it, as the facts below use it.
#[logic(open(self))]
pub fn up_found<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    id: Id<K>,
    up: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    best_path_up(nodes, set, id, up)
}

/// `below` is what is kept below `id`: [`kept_below`], hidden from the functions which find it, as
/// the facts below use it.
#[logic(open(self))]
pub fn below_found<K, T, S>(
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
    kept_below(nodes, set, id, below)
}

/// The walk along recorded parents from `id` has followed one of the best paths up.
#[logic]
#[ensures(result == (up_walk(set, scores, chain, None, id)
    && scores.contains(&id)
    && up_scored(nodes, set, scores, id)
    && path_up(nodes, id, chain)))]
#[ensures(result ==> best_path_up(nodes, set, id, chain))]
#[ensures(result ==> up_found(nodes, set, id, chain))]
pub fn up_walk_done_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    scores: ScoreMap<K>,
    chain: Seq<Id<K>>,
    id: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    up_walk(set, scores, chain, None, id)
        && scores.contains(&id)
        && up_scored(nodes, set, scores, id)
        && path_up(nodes, id, chain)
}

// The best paths down.

/// The recorded score of `k`, with the nodes of `set` active, is that of the path which the
/// recorded children lead along from `k`, and of some path from `k` down to a leaf, and no path
/// from `k` down to a leaf is better.
#[logic(open(self))]
pub fn down_scored<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    scores: ScoreMap<K>,
    k: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let s = scores[k].0;
        let a = if set.contains(&k) { 1 } else { 0 };
        (match scores[k].1 {
            None => s.0@ == 1 - a && s.1@ == a && nodes[k]@.to.len() == 0,
            Some(c) => s.0@ == scores[c.deep_model()].0.0@ + 1 - a
                && s.1@ == scores[c.deep_model()].0.1@ + a,
        })
            && (exists<q> path_down(nodes, k, q) && count_in(set, q) == s.1@ && q.len() == s.0@ + s.1@)
            && forall<q> path_down(nodes, k, q)
                ==> count_in(set, q) < s.1@ || (count_in(set, q) == s.1@ && s.0@ + s.1@ <= q.len())
    }
}

/// `entry` is what `update_node_activity_in_place` records for `k`: its own score added to that
/// of the best-scored child, which it records, or its own score alone when it has no children.
#[logic(open)]
pub fn down_entry<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    scores: ScoreMap<K>,
    k: Id<K>,
    entry: ((usize, usize), Option<K>),
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let a = if set.contains(&k) { 1 } else { 0 };
        match entry.1 {
            None => nodes[k]@.to.len() == 0 && entry.0.0@ == 1 - a && entry.0.1@ == a,
            Some(c) => nodes[k]@.to.contains(c.deep_model())
                && (forall<d> nodes[k]@.to.contains(d)
                    ==> score_le(scores[d].0, scores[c.deep_model()].0))
                && entry.0.0@ == scores[c.deep_model()].0.0@ + 1 - a
                && entry.0.1@ == scores[c.deep_model()].0.1@ + a,
        }
    }
}

/// Recording `entry` for `k`, whose children all have best scores, keeps every recorded score the
/// best one.
#[logic]
#[ensures(result == (all_linked(nodes)
    && ranked(nodes, rank)
    && nodes.contains(&k)
    && !scores.contains(&k)
    && (forall<j> scores.contains(&j)
        ==> child_linked(nodes, scores, j) && down_scored(nodes, set, scores, j))
    && (forall<c> nodes[k]@.to.contains(c) ==> scores.contains(&c))
    && down_entry(nodes, set, scores, k, entry)))]
#[ensures(result ==> forall<j> scores.add(k, entry).contains(&j)
    ==> down_scored(nodes, set, scores.add(k, entry), j))]
pub fn down_insert_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    set: FSet<Id<K>>,
    scores: ScoreMap<K>,
    k: Id<K>,
    entry: ((usize, usize), Option<K>),
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = all_linked(nodes)
            && ranked(nodes, rank)
            && nodes.contains(&k)
            && !scores.contains(&k)
            && (forall<j> scores.contains(&j)
                ==> child_linked(nodes, scores, j) && down_scored(nodes, set, scores, j))
            && (forall<c> nodes[k]@.to.contains(c) ==> scores.contains(&c))
            && down_entry(nodes, set, scores, k, entry);
        let new = scores.add(k, entry);
        proof_assert!(pre ==> {
            proof_assert!(forall<j> scores.contains(&j) ==> new[j] == scores[j]);
            proof_assert!(forall<j> scores.contains(&j) ==> match scores[j].1 {
                None => true,
                Some(c) => scores.contains(&c.deep_model()) && c.deep_model() != k,
            });
            proof_assert!(forall<j> scores.contains(&j) ==> down_scored(nodes, set, new, j));
            proof_assert!(new[k] == entry);
            proof_assert!(forall<q> path_down(nodes, k, q) ==> {
                if q.len() == 1 {
                    lemma_count_single(set, q);
                    proof_assert!(nodes[k]@.to.len() == 0);
                    proof_assert!(entry.1 == None);
                } else {
                    lemma_path_down_tail(nodes, set, k, q);
                    proof_assert!(scores.contains(&q[1]) && down_scored(nodes, set, scores, q[1]));
                    proof_assert!(match entry.1 {
                        None => false,
                        Some(c) => score_le(scores[q[1]].0, scores[c.deep_model()].0),
                    });
                }
                true
            } && (count_in(set, q) < entry.0.1@
                || (count_in(set, q) == entry.0.1@ && entry.0.0@ + entry.0.1@ <= q.len())));
            // A path down from `k` achieving its score.
            proof_assert!(match entry.1 {
                None => {
                    let w = Seq::singleton(k);
                    lemma_count_single(set, w);
                    proof_assert!(path_down(nodes, k, w));
                    proof_assert!(count_in(set, w) == entry.0.1@ && w.len() == entry.0.0@ + entry.0.1@);
                    exists<q> path_down(nodes, k, q)
                        && count_in(set, q) == entry.0.1@ && q.len() == entry.0.0@ + entry.0.1@
                },
                Some(c) => {
                    let c = c.deep_model();
                    proof_assert!(down_scored(nodes, set, scores, c));
                    let qc = such_that(|q| path_down(nodes, c, q)
                        && count_in(set, q) == scores[c].0.1@
                        && q.len() == scores[c].0.0@ + scores[c].0.1@);
                    let w = Seq::singleton(k).concat(qc);
                    proof_assert!(w.len() == qc.len() + 1 && w[0] == k);
                    proof_assert!(forall<i> 0 <= i && i < qc.len() ==> w[i + 1] == qc[i]);
                    proof_assert!(w.tail().ext_eq(qc));
                    proof_assert!(w[w.len() - 1] == qc[qc.len() - 1]);
                    proof_assert!(forall<i> 0 <= i && i < w.len() ==> nodes.contains(&w[i]));
                    proof_assert!(nodes[w[0]]@.to.contains(w[1]));
                    proof_assert!(forall<i> 1 <= i && i + 1 < w.len()
                        ==> nodes[w[i]]@.to.contains(w[i + 1]));
                    lemma_down_ranks(nodes, rank, w);
                    proof_assert!(path_down(nodes, k, w));
                    proof_assert!(count_in(set, w)
                        == count_in(set, qc) + if set.contains(&k) { 1 } else { 0 });
                    proof_assert!(count_in(set, w) == entry.0.1@ && w.len() == entry.0.0@ + entry.0.1@);
                    exists<q> path_down(nodes, k, q)
                        && count_in(set, q) == entry.0.1@ && q.len() == entry.0.0@ + entry.0.1@
                },
            });
            proof_assert!(exists<q> path_down(nodes, k, q)
                && count_in(set, q) == entry.0.1@ && q.len() == entry.0.0@ + entry.0.1@);
            proof_assert!(match entry.1 {
                None => true,
                Some(c) => new[c.deep_model()] == scores[c.deep_model()],
            });
            proof_assert!(down_scored(nodes, set, new, k));
            true
        } && forall<j> new.contains(&j) ==> down_scored(nodes, set, new, j));
        pre
    }
}

/// The walk from `id` along the recorded children of nodes with active nodes below them has
/// visited the nodes of `down`, each a child of the previous one, and is now at `current`, or has
/// stopped at the last node of `down`, which is active and has no active nodes below it along its
/// recorded children: the scores of the nodes visited and of `current` add up to that of `id`.
#[logic(open)]
pub fn down_walk<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    scores: ScoreMap<K>,
    down: Seq<Id<K>>,
    current: Option<Id<K>>,
    id: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (0 < down.len() ==> down[0] == id)
            && (forall<i> 0 <= i && i < down.len() ==> nodes.contains(&down[i]))
            && (forall<i> 0 <= i && i + 1 < down.len() ==> nodes[down[i]]@.to.contains(down[i + 1]))
            && match current {
                Some(c) => scores.contains(&c)
                    && nodes.contains(&c)
                    && (down.len() == 0 ==> c == id)
                    && (0 < down.len() ==> nodes[down[down.len() - 1]]@.to.contains(c))
                    && 0 < scores[c].0.1@
                    && count_in(set, down) + scores[c].0.1@ == scores[id].0.1@
                    && down.len() + scores[c].0.0@ + scores[c].0.1@
                        == scores[id].0.0@ + scores[id].0.1@,
                None => 0 < down.len()
                    && scores.contains(&down[down.len() - 1])
                    && set.contains(&down[down.len() - 1])
                    && scores[down[down.len() - 1]].0.1@ == 1
                    && count_in(set, down) == scores[id].0.1@
                    && down.len() - 1 + scores[down[down.len() - 1]].0.0@
                        + scores[down[down.len() - 1]].0.1@
                        == scores[id].0.0@ + scores[id].0.1@,
            }
    }
}

/// The walk along recorded children starts at `id`, which is active.
#[logic]
#[ensures(result == (scores.contains(&id)
    && nodes.contains(&id)
    && set.contains(&id)
    && down_scored(nodes, set, scores, id)))]
#[ensures(result ==> down_walk(nodes, set, scores, Seq::empty(), Some(id), id))]
pub fn down_walk_start_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    scores: ScoreMap<K>,
    id: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = scores.contains(&id)
            && nodes.contains(&id)
            && set.contains(&id)
            && down_scored(nodes, set, scores, id);
        proof_assert!(pre ==> scores[id].0.1@ >= 1 && {
            lemma_count_in_bounds(set, Seq::<Id<K>>::empty());
            true
        } && down_walk(nodes, set, scores, Seq::empty(), Some(id), id));
        pre
    }
}

/// One step of the walk along recorded children, which goes on to the recorded child of `x` when
/// there are active nodes below `x` along its recorded children, and stops otherwise.
#[logic]
#[ensures(result == (all_linked(nodes)
    && down_walk(nodes, set, scores, down, Some(x), id)
    && child_linked(nodes, scores, x)
    && down_scored(nodes, set, scores, x)))]
#[ensures(result && scores[x].0.1@ > (if set.contains(&x) { 1 } else { 0 })
    ==> down_walk(nodes, set, scores, down.snoc(x), scores[x].1.deep_model(), id))]
#[ensures(result && scores[x].0.1@ <= (if set.contains(&x) { 1 } else { 0 })
    ==> down_walk(nodes, set, scores, down.snoc(x), None, id))]
pub fn down_walk_step_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    scores: ScoreMap<K>,
    down: Seq<Id<K>>,
    x: Id<K>,
    id: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = all_linked(nodes)
            && down_walk(nodes, set, scores, down, Some(x), id)
            && child_linked(nodes, scores, x)
            && down_scored(nodes, set, scores, x);
        let next = down.snoc(x);
        proof_assert!(pre ==> {
            lemma_count_snoc(set, down, x);
            proof_assert!(next.len() == down.len() + 1 && next[down.len()] == x);
            proof_assert!(forall<i> 0 <= i && i < down.len() ==> next[i] == down[i]);
            proof_assert!(match scores[x].1 {
                None => true,
                Some(c) => nodes.contains(&c.deep_model()) && nodes[x]@.to.contains(c.deep_model()),
            });
            true
        } && (scores[x].0.1@ > (if set.contains(&x) { 1 } else { 0 })
            ==> down_walk(nodes, set, scores, next, scores[x].1.deep_model(), id))
          && (scores[x].0.1@ <= (if set.contains(&x) { 1 } else { 0 })
            ==> down_walk(nodes, set, scores, next, None, id)));
        pre
    }
}

/// The walk along recorded children from `id`, which is active, has followed one of the best paths
/// down, up to its last active node, which keeps the nodes of `before` below `id`.
#[logic]
#[ensures(result == (all_linked(nodes)
    && ranked(nodes, rank)
    && down_walk(nodes, set, scores, down, None, id)
    && down_scored(nodes, set, scores, id)
    && down_scored(nodes, set, scores, down[down.len() - 1])
    && set == before.add(id)
    && !before.contains(&id)))]
#[ensures(result ==> below_found(nodes, before, id, down))]
pub fn down_walk_done_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    before: FSet<Id<K>>,
    set: FSet<Id<K>>,
    scores: ScoreMap<K>,
    down: Seq<Id<K>>,
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
            && down_walk(nodes, set, scores, down, None, id)
            && down_scored(nodes, set, scores, id)
            && down_scored(nodes, set, scores, down[down.len() - 1])
            && set == before.add(id)
            && !before.contains(&id);
        proof_assert!(pre ==> {
            let x = down[down.len() - 1];
            let qx = such_that(|q| path_down(nodes, x, q)
                && count_in(set, q) == scores[x].0.1@
                && q.len() == scores[x].0.0@ + scores[x].0.1@);
            let rest = qx.tail();
            proof_assert!(forall<i> 0 <= i && i < rest.len() ==> rest[i] == qx[i + 1]);
            proof_assert!(count_in(set, qx) == count_in(set, rest) + 1);
            lemma_count_in_zero(set, rest);
            proof_assert!(forall<i> 0 <= i && i < rest.len() ==> !set.contains(&rest[i]));
            let m = down.concat(rest);
            proof_assert!(m.len() == down.len() + rest.len());
            proof_assert!(forall<i> 0 <= i && i < down.len() ==> m[i] == down[i]);
            proof_assert!(forall<i> 0 <= i && i < rest.len() ==> m[down.len() + i] == rest[i]);
            proof_assert!(forall<i> 0 <= i && i < m.len() ==> nodes.contains(&m[i]));
            proof_assert!(forall<i> 0 <= i && i + 1 < down.len() ==> nodes[m[i]]@.to.contains(m[i + 1]));
            proof_assert!(0 < rest.len() ==> m[down.len() - 1] == qx[0] && m[down.len()] == qx[1]
                && nodes[qx[0]]@.to.contains(qx[1]));
            proof_assert!(forall<i> down.len() <= i && i + 1 < m.len()
                ==> m[i] == qx[i - down.len() + 1] && m[i + 1] == qx[i - down.len() + 2]);
            proof_assert!(forall<i> 0 <= i && i + 1 < m.len() ==> nodes[m[i]]@.to.contains(m[i + 1]));
            proof_assert!(m[m.len() - 1] == qx[qx.len() - 1]);
            lemma_down_ranks(nodes, rank, m);
            proof_assert!(path_down(nodes, id, m));
            lemma_count_concat(set, down, rest);
            proof_assert!(count_in(set, m) == scores[id].0.1@);
            proof_assert!(m.len() == scores[id].0.0@ + scores[id].0.1@);
            proof_assert!(best_path_down(nodes, set, id, m));
            proof_assert!(forall<i> down.len() <= i && i < m.len() ==> !set.contains(&m[i]));
            proof_assert!(cut_best_down(nodes, set, id, down));
            lemma_kept_below(nodes, before, id, down);
            true
        } && below_found(nodes, before, id, down));
        pre
    }
}

/// No node of `set` other than `id` is in a child-closed set containing `id`, so no path down from
/// `id` reaches one.
#[logic]
#[ensures(result == (child_closed(nodes, closure)
    && closure.contains(&id)
    && forall<k> closure.contains(&k) && k != id ==> !set.contains(&k)))]
#[ensures(result ==> below_found(nodes, set, id, Seq::singleton(id)))]
pub fn nothing_below_closed_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    id: Id<K>,
    closure: FSet<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = child_closed(nodes, closure)
            && closure.contains(&id)
            && forall<k> closure.contains(&k) && k != id ==> !set.contains(&k);
        proof_assert!(pre ==> forall<q, i> path_down(nodes, id, q) && 0 < i && i < q.len() ==> {
            lemma_down_closed(nodes, closure, q, i);
            q[i] != q[0]
        } && !set.contains(&q[i]));
        proof_assert!(pre ==> nothing_below(nodes, set, id) && below_found(nodes, set, id, Seq::singleton(id)));
        pre
    }
}

/// No path down from a node without children reaches another node.
#[logic]
#[ensures(result == (nodes[id]@.to.len() == 0 || forall<k> !set.contains(&k)))]
#[ensures(result ==> below_found(nodes, set, id, Seq::singleton(id)))]
pub fn nothing_below_ok<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>, id: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = nodes[id]@.to.len() == 0 || forall<k> !set.contains(&k);
        proof_assert!(pre ==> forall<q, i> path_down(nodes, id, q) && 0 < i && i < q.len()
            ==> nodes[q[0]]@.to.contains(q[1]) && !set.contains(&q[i]));
        proof_assert!(pre ==> nothing_below(nodes, set, id) && below_found(nodes, set, id, Seq::singleton(id)));
        pre
    }
}

// Transferring between graphs which differ only by which nodes are active.

/// Graphs which differ only by which nodes are active have the same paths.
#[logic]
#[requires(same_graph(new, old))]
#[ensures(forall<q> valid_path(new, q) == valid_path(old, q))]
#[ensures(forall<id, q> path_up(new, id, q) == path_up(old, id, q))]
#[ensures(forall<id, q> path_down(new, id, q) == path_down(old, id, q))]
#[ensures(forall<set, id> has_child_in(new, set, id) == has_child_in(old, set, id))]
#[ensures(forall<set, id> extends_path(new, set, id) == extends_path(old, set, id))]
#[ensures(forall<set, id> nothing_below(new, set, id) == nothing_below(old, set, id))]
#[ensures(forall<set, id, p> best_path_up(new, set, id, p) == best_path_up(old, set, id, p))]
#[ensures(forall<set, id, p> best_path_down(new, set, id, p) == best_path_down(old, set, id, p))]
#[ensures(forall<set, b> path_of(new, set, b) == path_of(old, set, b))]
pub fn lemma_same_graph_routes<K, T, S>(new: Nodes<K, T, S>, old: Nodes<K, T, S>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// What is kept below a node is the same in graphs which differ only by which nodes are active.
#[logic]
#[requires(same_graph(new, old))]
#[requires(kept_below(new, set, id, below))]
#[ensures(kept_below(old, set, id, below))]
pub fn lemma_same_graph_kept<K, T, S>(
    new: Nodes<K, T, S>,
    old: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    id: Id<K>,
    below: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        lemma_same_graph_routes(new, old);
        if below == Seq::singleton(id) && nothing_below(new, set, id) {
            proof_assert!(nothing_below(old, set, id));
        } else {
            let path = such_that(|path| best_path_down(new, set, id, path)
                && 1 < below.len()
                && below.len() <= path.len()
                && (forall<i> 0 <= i && i < below.len() ==> below[i] == path[i])
                && set.contains(&below[below.len() - 1])
                && forall<i> below.len() <= i && i < path.len() ==> !set.contains(&path[i]));
            proof_assert!(best_path_down(old, set, id, path));
        }
    }
}

/// Graphs which differ only by which nodes are active have the same paths, and keep the same
/// nodes below each node.
#[logic]
#[requires(same_graph(new, old))]
#[ensures(forall<q> valid_path(new, q) == valid_path(old, q))]
#[ensures(forall<id, q> path_up(new, id, q) == path_up(old, id, q))]
#[ensures(forall<id, q> path_down(new, id, q) == path_down(old, id, q))]
#[ensures(forall<set, id> has_child_in(new, set, id) == has_child_in(old, set, id))]
#[ensures(forall<set, id> extends_path(new, set, id) == extends_path(old, set, id))]
#[ensures(forall<set, id> nothing_below(new, set, id) == nothing_below(old, set, id))]
#[ensures(forall<set, id, p> best_path_up(new, set, id, p) == best_path_up(old, set, id, p))]
#[ensures(forall<set, id, p> best_path_down(new, set, id, p) == best_path_down(old, set, id, p))]
#[ensures(forall<set, id, b> kept_below(new, set, id, b) == kept_below(old, set, id, b))]
#[ensures(forall<set, b> path_of(new, set, b) == path_of(old, set, b))]
pub fn lemma_same_graph_paths<K, T, S>(new: Nodes<K, T, S>, old: Nodes<K, T, S>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        lemma_same_graph_routes(new, old);
        proof_assert!(same_graph(old, new));
        proof_assert!(forall<set, id, b> kept_below(new, set, id, b) ==> {
            lemma_same_graph_kept(new, old, set, id, b);
            true
        } && kept_below(old, set, id, b));
        proof_assert!(forall<set, id, b> kept_below(old, set, id, b) ==> {
            lemma_same_graph_kept(old, new, set, id, b);
            true
        } && kept_below(new, set, id, b));
    }
}

/// Activation and the longest paths are the same in graphs which differ only by which nodes are
/// active.
#[logic]
#[ensures(result == same_graph(new, old))]
#[ensures(result ==> forall<before, id, after> activated(new, before, id, after)
    == activated(old, before, id, after))]
#[ensures(result ==> forall<before, id, after> activated_at_end(new, before, id, after)
    == activated_at_end(old, before, id, after))]
#[ensures(result ==> forall<within, after> longest_path_within(new, within, after)
    == longest_path_within(old, within, after))]
pub fn same_graph_activation_ok<K, T, S>(new: Nodes<K, T, S>, old: Nodes<K, T, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(same_graph(new, old) ==> {
            lemma_same_graph_paths(new, old);
            proof_assert!(forall<before, id, after> rerouted(new, before, id, after)
                == rerouted(old, before, id, after));
            true
        } && (forall<before, id, after> activated(new, before, id, after)
                == activated(old, before, id, after))
            && (forall<before, id, after> activated_at_end(new, before, id, after)
                == activated_at_end(old, before, id, after))
            && forall<within, after> longest_path_within(new, within, after)
                == longest_path_within(old, within, after));
        same_graph(new, old)
    }
}

// Assembling the results.

/// The active path recomputed through `id` from the active nodes `before`, in the graph `old`:
/// `after` holds the nodes of `up`, one of the best paths from `id` up to a root, and of `below`.
#[logic]
#[ensures(result == (same_graph(nodes, old)
    && up_found(nodes, before.add(id), id, up)
    && below_found(nodes, before, id, below)
    && forall<k> after.contains(&k) == (up.contains(k) || below.contains(k))))]
#[ensures(result ==> rerouted(old, before, id, after))]
pub fn rerouted_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
    old: Nodes<K, T, S>,
    before: FSet<Id<K>>,
    id: Id<K>,
    after: FSet<Id<K>>,
    up: Seq<Id<K>>,
    below: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let pre = same_graph(nodes, old)
            && up_found(nodes, before.add(id), id, up)
            && below_found(nodes, before, id, below)
            && forall<k> after.contains(&k) == (up.contains(k) || below.contains(k));
        proof_assert!(pre ==> {
            lemma_best_up_shift(nodes, before, id, up);
            lemma_same_graph_paths(nodes, old);
            proof_assert!(best_path_up(old, before, id, up) && kept_below(old, before, id, below));
            true
        } && rerouted(old, before, id, after));
        pre
    }
}

/// The nodes of a path to a root are the longest such path among their own nodes.
#[logic]
#[ensures(result == forms_path(nodes, set))]
#[ensures(result ==> longest_path_within(nodes, set, set))]
pub fn longest_whole_ok<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forms_path(nodes, set) ==> {
            let path = such_that(|path| path_of(nodes, set, path));
            proof_assert!(forall<q> valid_path(nodes, q) && no_dup(q)
                && (forall<i> 0 <= i && i < q.len() ==> set.contains(&q[i])) ==> {
                    lemma_subset_len(q, path);
                    true
                } && q.len() <= path.len());
            true
        } && longest_path_within(nodes, set, set));
        forms_path(nodes, set)
    }
}

/// A longest path through the nodes of `within` is one through those of `wider`, if `wider`
/// contains no more nodes.
#[logic]
#[ensures(result == (longest_path_within(nodes, within, after)
    && (forall<k> within.contains(&k) ==> wider.contains(&k))
    && forall<k> nodes.contains(&k) && wider.contains(&k) ==> within.contains(&k)))]
#[ensures(result ==> longest_path_within(nodes, wider, after))]
pub fn longest_widen_ok<K, T, S>(
    nodes: Nodes<K, T, S>,
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
        let pre = longest_path_within(nodes, within, after)
            && (forall<k> within.contains(&k) ==> wider.contains(&k))
            && forall<k> nodes.contains(&k) && wider.contains(&k) ==> within.contains(&k);
        proof_assert!(pre ==> forall<q> valid_path(nodes, q)
            && (forall<i> 0 <= i && i < q.len() ==> wider.contains(&q[i]))
            ==> forall<i> 0 <= i && i < q.len() ==> within.contains(&q[i]));
        pre
    }
}
