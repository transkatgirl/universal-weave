//! Lemmas about the operations which add, change or relink nodes.

use core::hash::{BuildHasher, Hash};

use creusot_std::logic::{Mapping, such_that};

use super::*;
use crate::{
    IndependentContents,
    verification::{Id, no_dup, replaced_in_place, shift_removed},
};

/// Ranks strictly decrease along a path.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(valid_path(nodes, path))]
#[requires(0 <= a && a < b && b < path.len())]
#[ensures(rank[path[b]] < rank[path[a]])]
#[variant(b - a)]
pub fn lemma_path_ranks<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    path: Seq<Id<K>>,
    a: Int,
    b: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if b > a + 1 {
        lemma_path_ranks(nodes, rank, path, a + 1, b)
    }
}

/// Ranks strictly decrease along every path.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ranked(nodes, rank))]
#[requires(valid_path(nodes, path))]
#[ensures(forall<a, b> 0 <= a && a < b && b < path.len() ==> rank[path[b]] < rank[path[a]])]
pub fn lemma_path_ranks_all<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    path: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = lemma_path_ranks::<K, T, S>;
}

/// `new` is `old` with `a` replaced by `b`, at the same position, as `IndexSet::replace_index`
/// does.
#[logic(open)]
pub fn replaced<I>(new: Seq<I>, old: Seq<I>, a: I, b: I) -> bool {
    pearlite! {
        (forall<k> new.contains(k) == (k == b || (k != a && old.contains(k))))
            && replaced_in_place(new, old, a, b)
    }
}

/// `new` is `old` once node `id` is split into `id`, whose only child is `new_id`, and `new_id`,
/// which takes `id`'s children. `new_id` is active if `id` and one of its children were.
#[logic(open)]
pub fn split_shape<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    new_id: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = old.nodes[id]@;
        old.nodes.contains(&id)
            && !old.nodes.contains(&new_id)
            && id != new_id
            && (forall<k> new.nodes.contains(&k) == (old.nodes.contains(&k) || k == new_id))
            && new.roots == old.roots
            && new.bookmarked == old.bookmarked
            && new.active == (if n.active && has_child_in(old.nodes, old.active, id) {
                old.active.add(new_id)
            } else {
                old.active
            })
            && new.nodes[id]@ == NodeView {
                id,
                from: n.from,
                to: Seq::singleton(new_id),
                active: n.active,
                bookmarked: n.bookmarked,
            }
            && new.nodes[new_id]@ == NodeView {
                id: new_id,
                from: Seq::singleton(id),
                to: n.to,
                active: new.active.contains(&new_id),
                bookmarked: false,
            }
            && (forall<c> n.to.contains(c) ==> new.nodes[c]@.id == old.nodes[c]@.id
                && new.nodes[c]@.to == old.nodes[c]@.to
                && new.nodes[c]@.active == old.nodes[c]@.active
                && new.nodes[c]@.bookmarked == old.nodes[c]@.bookmarked
                && replaced(new.nodes[c]@.from, old.nodes[c]@.from, id, new_id))
            && (forall<k> old.nodes.contains(&k) && k != id && !n.to.contains(k)
                ==> new.nodes[k]@ == old.nodes[k]@)
    }
}

/// Splitting a node keeps the weave's structure valid.
#[logic]
#[requires(old.valid())]
#[requires(split_shape(old, new, id, new_id))]
#[ensures(new.valid_structure())]
pub fn lemma_split_structure<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    new_id: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = old.nodes[id]@;
        let rank = rank_of(old.nodes);
        let rank2 = |k| if k == new_id { 2 * rank[id] + 1 } else { 2 * rank[k] };
        proof_assert!(linked(old.nodes, id));
        proof_assert!(forall<c> n.to.contains(c) ==> old.nodes.contains(&c) && linked(old.nodes, c)
            && old.nodes[c]@.from.contains(id) && c != id && c != new_id && rank[id] < rank[c]
            && !n.from.contains(c));
        proof_assert!(forall<p> n.from.contains(p) ==> old.nodes.contains(&p) && p != id
            && p != new_id && !n.to.contains(p) && rank[p] < rank[id]);
        proof_assert!(forall<k> old.nodes.contains(&k) && k != id && !n.to.contains(k)
            ==> linked(old.nodes, k) && !old.nodes[k]@.from.contains(id) && !old.nodes[k]@.to.contains(id)
                || n.from.contains(k));
        proof_assert!(forall<k> old.nodes.contains(&k) ==> !old.nodes[k]@.from.contains(new_id)
            && !old.nodes[k]@.to.contains(new_id) && linked(old.nodes, k));
        proof_assert!(forall<k> new.active.contains(&k) == (old.active.contains(&k)
            || k == new_id && new.active.contains(&new_id)));
        proof_assert!(ranked(new.nodes, rank2));
        proof_assert!(linked(new.nodes, id));
        proof_assert!(linked(new.nodes, new_id));
        proof_assert!(forall<c> n.to.contains(c) ==> linked(new.nodes, c));
        proof_assert!(forall<k> old.nodes.contains(&k) && k != id && !n.to.contains(k)
            ==> linked(new.nodes, k));
        proof_assert!(forall<k> new.nodes.contains(&k) ==> new.flagged(k))
    }
}

/// Splitting a node keeps the active nodes a path.
#[logic]
#[requires(old.valid())]
#[requires(split_shape(old, new, id, new_id))]
#[ensures(forms_path(new.nodes, new.active))]
pub fn lemma_split_path<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    new_id: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = old.nodes[id]@;
        let path = such_that(|path| path_of(old.nodes, old.active, path));
        proof_assert!(linked(old.nodes, id));
        proof_assert!(forall<k> old.nodes.contains(&k) ==> linked(old.nodes, k)
            && !old.nodes[k]@.from.contains(new_id));
        proof_assert!(forall<k> old.nodes.contains(&k) ==> old.flagged(k));
        proof_assert!(forall<c> n.to.contains(c) ==> old.nodes.contains(&c)
            && old.nodes[c]@.from.contains(id) && c != id);
        proof_assert!(forall<k, p> new.nodes.contains(&k) && k != new_id && p != id
            && old.nodes[k]@.from.contains(p) ==> new.nodes[k]@.from.contains(p));
        proof_assert!(new.nodes[id]@.from == old.nodes[id]@.from);
        proof_assert!(forall<k> old.nodes.contains(&k) && old.nodes[k]@.from.len() == 0
            ==> new.nodes[k]@.from.len() == 0);
        proof_assert!(forall<i> 0 <= i && i < path.len() ==> old.nodes.contains(&path[i])
            && path[i] != new_id && old.active.contains(&path[i]));
        if n.active && has_child_in(old.nodes, old.active, id) {
            let rank = rank_of(old.nodes);
            let j = such_that(|j| 0 <= j && j < path.len() && path[j] == id);
            let c = such_that(|c| n.to.contains(c) && old.active.contains(&c));
            let i = such_that(|i| 0 <= i && i < path.len() && path[i] == c);
            lemma_path_ranks_all(old.nodes, rank, path);
            proof_assert!(rank[id] < rank[c]);
            proof_assert!(i < j);
            proof_assert!(n.to.contains(path[j - 1]));
            let p2 = path.subsequence(0, j).snoc(new_id).concat(path.subsequence(j, path.len()));
            proof_assert!(p2.len() == path.len() + 1);
            proof_assert!(forall<m> 0 <= m && m < j ==> p2[m] == path[m]);
            proof_assert!(p2[j] == new_id);
            proof_assert!(forall<m> j <= m && m < path.len() ==> p2[m + 1] == path[m]);
            proof_assert!(forall<m> 0 <= m && m < p2.len() ==> new.nodes.contains(&p2[m]));
            proof_assert!(forall<m> 0 <= m && m + 1 < j
                ==> path[m + 1] != id && new.nodes[p2[m]]@.from.contains(p2[m + 1]));
            proof_assert!(new.nodes[p2[j - 1]]@.from.contains(p2[j]));
            proof_assert!(new.nodes[p2[j]]@.from.contains(p2[j + 1]));
            proof_assert!(forall<m> j < m && m + 1 < p2.len()
                ==> path[m] != id && new.nodes[p2[m]]@.from.contains(p2[m + 1]));
            proof_assert!(new.nodes[p2[p2.len() - 1]]@.from.len() == 0);
            proof_assert!(valid_path(new.nodes, p2));
            proof_assert!(forall<a, b> 0 <= a && a < b && b < p2.len() && a != j && b != j
                ==> p2[a] != p2[b]);
            proof_assert!(no_dup(p2));
            proof_assert!(forall<k> path.contains(k) ==> p2.contains(k));
            proof_assert!(forall<k> p2.contains(k) ==> path.contains(k) || k == new_id);
            proof_assert!(path_of(new.nodes, new.active, p2))
        } else {
            proof_assert!(new.active == old.active);
            proof_assert!(n.active ==> forall<c> n.to.contains(c) ==> !old.active.contains(&c));
            proof_assert!(forall<m> 0 <= m && m + 1 < path.len() && path[m + 1] == id
                ==> linked(old.nodes, path[m]) && n.to.contains(path[m]) && old.active.contains(&path[m])
                    && n.active);
            proof_assert!(forall<m> 0 <= m && m + 1 < path.len() ==> path[m + 1] != id);
            proof_assert!(valid_path(new.nodes, path));
            proof_assert!(path_of(new.nodes, new.active, path))
        }
    }
}

/// Splitting a node keeps the weave valid.
#[logic]
#[requires(old.valid())]
#[requires(split_shape(old, new, id, new_id))]
#[ensures(new.valid())]
pub fn lemma_split_valid<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    new_id: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    lemma_split_structure(old, new, id, new_id);
    lemma_split_path(old, new, id, new_id);
}

/// `new` is `old` once node `id` is merged into its parent `p`, which was `id`'s only parent and
/// had no other children: `p` takes `id`'s children and bookmark, and `id` leaves the active
/// path. If only `id` was bookmarked, `p` takes its place among the bookmarks.
#[logic(open)]
pub fn merge_shape<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    p: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = old.nodes[id]@;
        let pn = old.nodes[p]@;
        old.nodes.contains(&id)
            && n.from.len() == 1
            && n.from[0] == p
            && pn.to.len() <= 1
            && (forall<k> new.nodes.contains(&k) == (old.nodes.contains(&k) && k != id))
            && new.roots == old.roots
            && new.active == old.active.remove(id)
            && no_dup(new.bookmarked)
            && (forall<k> new.bookmarked.contains(k) == if k == p {
                old.bookmarked.contains(p) || old.bookmarked.contains(id)
            } else {
                k != id && old.bookmarked.contains(k)
            })
            && (if n.bookmarked && !pn.bookmarked {
                replaced_in_place(new.bookmarked, old.bookmarked, id, p)
            } else if n.bookmarked {
                shift_removed(new.bookmarked, old.bookmarked, id)
            } else {
                new.bookmarked == old.bookmarked
            })
            && new.nodes[p]@ == NodeView {
                id: p,
                from: pn.from,
                to: n.to,
                active: pn.active,
                bookmarked: pn.bookmarked || n.bookmarked,
            }
            && (forall<c> n.to.contains(c) ==> new.nodes[c]@.id == old.nodes[c]@.id
                && new.nodes[c]@.to == old.nodes[c]@.to
                && new.nodes[c]@.active == old.nodes[c]@.active
                && new.nodes[c]@.bookmarked == old.nodes[c]@.bookmarked
                && replaced(new.nodes[c]@.from, old.nodes[c]@.from, id, p))
            && (forall<k> old.nodes.contains(&k) && k != id && k != p && !n.to.contains(k)
                ==> new.nodes[k]@ == old.nodes[k]@)
    }
}

/// Facts about the nodes around a node `id` which is merged into its parent `p`.
#[logic]
#[requires(old.valid())]
#[requires(merge_shape(old, new, id, p))]
#[ensures(linked(old.nodes, id) && linked(old.nodes, p))]
#[ensures(old.nodes.contains(&p) && p != id && old.nodes[p]@.to.contains(id))]
#[ensures(forall<k> old.nodes[p]@.to.contains(k) == (k == id))]
#[ensures(forall<k> old.nodes[id]@.from.contains(k) == (k == p))]
#[ensures(forall<c> old.nodes[id]@.to.contains(c) ==> old.nodes.contains(&c) && linked(old.nodes, c)
    && old.nodes[c]@.from.contains(id) && !old.nodes[c]@.from.contains(p)
    && !old.nodes[c]@.to.contains(p) && c != p && c != id)]
#[ensures(forall<q> old.nodes[p]@.from.contains(q) ==> old.nodes.contains(&q) && q != id
    && !old.nodes[id]@.to.contains(q))]
#[ensures(forall<k> old.nodes.contains(&k) && k != id && k != p && !old.nodes[id]@.to.contains(k)
    ==> linked(old.nodes, k) && !old.nodes[k]@.from.contains(id) && !old.nodes[k]@.to.contains(id)
        && !old.nodes[k]@.from.contains(p))]
pub fn lemma_merge_neighbours<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    p: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = old.nodes[id]@;
        let pn = old.nodes[p]@;
        let rank = rank_of(old.nodes);
        proof_assert!(n.from.contains(p));
        proof_assert!(linked(old.nodes, id) && linked(old.nodes, p));
        proof_assert!(pn.to.contains(id) && pn.to.len() == 1 && pn.to[0] == id);
        proof_assert!(forall<k> n.from.contains(k) ==> k == n.from[0]);
        proof_assert!(rank[p] < rank[id]);
        proof_assert!(forall<c> n.to.contains(c) ==> old.nodes.contains(&c) && linked(old.nodes, c)
            && rank[id] < rank[c]);
        proof_assert!(forall<q> pn.from.contains(q) ==> old.nodes.contains(&q) && rank[q] < rank[p])
    }
}

/// Merging a node into its parent keeps the weave's structure valid.
#[logic]
#[requires(old.valid())]
#[requires(merge_shape(old, new, id, p))]
#[ensures(new.valid_structure())]
pub fn lemma_merge_structure<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    p: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = old.nodes[id]@;
        lemma_merge_neighbours(old, new, id, p);
        proof_assert!(ranked(new.nodes, rank_of(old.nodes)));
        proof_assert!(linked(new.nodes, p));
        proof_assert!(forall<c> n.to.contains(c) ==> linked(new.nodes, c));
        proof_assert!(forall<k> old.nodes.contains(&k) && k != id && k != p && !n.to.contains(k)
            ==> linked(new.nodes, k));
        proof_assert!(!old.roots.contains(id));
        proof_assert!(forall<k> new.nodes.contains(&k) ==> new.flagged(k))
    }
}

/// Merging a node into its parent keeps the active nodes a path, which contained the parent if it
/// contained the node.
#[logic]
#[requires(old.valid())]
#[requires(merge_shape(old, new, id, p))]
#[ensures(forms_path(new.nodes, new.active))]
#[ensures(old.active.contains(&id) ==> old.active.contains(&p))]
pub fn lemma_merge_path<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    p: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = old.nodes[id]@;
        let path = such_that(|path| path_of(old.nodes, old.active, path));
        lemma_merge_neighbours(old, new, id, p);
        proof_assert!(forall<k, q> new.nodes.contains(&k) && q != id
            && old.nodes[k]@.from.contains(q) ==> new.nodes[k]@.from.contains(q));
        proof_assert!(forall<k> old.nodes.contains(&k) && old.nodes[k]@.from.len() == 0
            ==> k != id && new.nodes[k]@.from.len() == 0);
        proof_assert!(forall<k> new.active.contains(&k) == (old.active.contains(&k) && k != id));
        if old.active.contains(&id) {
            let j = such_that(|j| 0 <= j && j < path.len() && path[j] == id);
            proof_assert!(j + 1 < path.len());
            proof_assert!(path[j + 1] == p);
            let p2 = path.subsequence(0, j).concat(path.subsequence(j + 1, path.len()));
            proof_assert!(p2.len() == path.len() - 1);
            proof_assert!(forall<m> 0 <= m && m < j ==> p2[m] == path[m]);
            proof_assert!(forall<m> j <= m && m < p2.len() ==> p2[m] == path[m + 1]);
            proof_assert!(forall<m> 0 <= m && m < p2.len() ==> new.nodes.contains(&p2[m]));
            proof_assert!(forall<m> 0 <= m && m + 1 < j
                ==> path[m + 1] != id && new.nodes[p2[m]]@.from.contains(p2[m + 1]));
            proof_assert!(0 < j ==> n.to.contains(path[j - 1])
                && new.nodes[p2[j - 1]]@.from.contains(p2[j]));
            proof_assert!(forall<m> j <= m && m + 1 < p2.len()
                ==> path[m + 2] != id && new.nodes[p2[m]]@.from.contains(p2[m + 1]));
            proof_assert!(new.nodes[p2[p2.len() - 1]]@.from.len() == 0);
            proof_assert!(valid_path(new.nodes, p2));
            proof_assert!(no_dup(p2));
            proof_assert!(forall<k> path.contains(k) && k != id ==> p2.contains(k));
            proof_assert!(forall<k> p2.contains(k) ==> path.contains(k) && k != id);
            proof_assert!(path_of(new.nodes, new.active, p2))
        } else {
            proof_assert!(forall<m> 0 <= m && m < path.len() ==> path[m] != id);
            proof_assert!(valid_path(new.nodes, path));
            proof_assert!(path_of(new.nodes, new.active, path))
        }
    }
}

/// Merging a node into its parent keeps the weave valid.
#[logic]
#[requires(old.valid())]
#[requires(merge_shape(old, new, id, p))]
#[ensures(new.valid())]
#[ensures(old.active.contains(&id) ==> old.active.contains(&p))]
pub fn lemma_merge_valid<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    p: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    lemma_merge_structure(old, new, id, p);
    lemma_merge_path(old, new, id, p);
}

/// Node `n` can be linked into `nodes`: it is consistent, its parents and children exist, and
/// linking it forms no cycle, because some parent-closed set `a` holds its parents but none of its
/// children.
#[logic(open)]
pub fn insertable<K, T, S>(nodes: Nodes<K, T, S>, n: NodeView<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        n.valid()
            && no_dup(n.from)
            && no_dup(n.to)
            && !nodes.contains(&n.id)
            && (forall<p> n.from.contains(p) ==> nodes.contains(&p))
            && (forall<c> n.to.contains(c) ==> nodes.contains(&c))
            && exists<a: Mapping<Id<K>, bool>> separates(nodes, n, a)
    }
}

/// `a` is closed under taking parents in `nodes`, and holds `n`'s parents but none of its
/// children.
#[logic(open)]
pub fn separates<K, T, S>(
    nodes: Nodes<K, T, S>,
    n: NodeView<Id<K>>,
    a: Mapping<Id<K>, bool>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (forall<p> n.from.contains(p) ==> a[p])
            && (forall<c> n.to.contains(c) ==> !a[c])
            && forall<k, p> a[k] && nodes.contains(&k) && nodes[k]@.from.contains(p) ==> a[p]
    }
}

/// `new` is `old` with node `n` inserted and linked to its parents and children, which are
/// otherwise unchanged. Only `n`'s activity may differ between `old` and `new`'s active nodes.
#[logic(open)]
pub fn insert_shape<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    n: NodeView<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let id = n.id;
        (forall<k> new.nodes.contains(&k) == (old.nodes.contains(&k) || k == id))
            && new.nodes[id]@ == NodeView {
                id,
                from: n.from,
                to: n.to,
                active: new.active.contains(&id),
                bookmarked: n.bookmarked,
            }
            && (forall<k> old.nodes.contains(&k) ==> new.nodes[k]@.id == old.nodes[k]@.id
                && new.nodes[k]@.active == old.nodes[k]@.active
                && new.nodes[k]@.bookmarked == old.nodes[k]@.bookmarked
                && new.nodes[k]@.from
                    == (if n.to.contains(k) { old.nodes[k]@.from.snoc(id) } else { old.nodes[k]@.from })
                && new.nodes[k]@.to
                    == (if n.from.contains(k) { old.nodes[k]@.to.snoc(id) } else { old.nodes[k]@.to }))
            && no_dup(new.roots)
            && (forall<k> new.roots.contains(k)
                == ((k == id && n.from.len() == 0) || (old.roots.contains(k) && !n.to.contains(k))))
            && no_dup(new.bookmarked)
            && (forall<k> new.bookmarked.contains(k)
                == (old.bookmarked.contains(k) || (k == id && n.bookmarked)))
            && forall<k> k != id ==> new.active.contains(&k) == old.active.contains(&k)
    }
}

/// `new` is `old` with node `n`, whose contents are `contents`, inserted and linked to its parents
/// and children as [`insert_shape`] describes, except that which nodes are active may differ. No
/// other node's contents change, the roots keep their order, a parentless `n` takes the place of
/// its first child which was a root, or becomes the last root if none was, and a bookmarked `n`
/// becomes the last bookmark.
#[logic(open)]
pub fn inserted<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    n: NodeView<Id<K>>,
    contents: T,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let id = n.id;
        (forall<k> new.nodes.contains(&k) == (old.nodes.contains(&k) || k == id))
            && new.nodes[id]@.id == id
            && new.nodes[id]@.from == n.from
            && new.nodes[id]@.to == n.to
            && new.nodes[id]@.bookmarked == n.bookmarked
            && new.nodes[id].contents == contents
            && (forall<k> old.nodes.contains(&k) ==> new.nodes[k]@.id == old.nodes[k]@.id
                && new.nodes[k]@.bookmarked == old.nodes[k]@.bookmarked
                && new.nodes[k].contents == old.nodes[k].contents
                && new.nodes[k]@.from
                    == (if n.to.contains(k) { old.nodes[k]@.from.snoc(id) } else { old.nodes[k]@.from })
                && new.nodes[k]@.to
                    == (if n.from.contains(k) { old.nodes[k]@.to.snoc(id) } else { old.nodes[k]@.to }))
            && no_dup(new.roots)
            && (forall<k> new.roots.contains(k)
                == ((k == id && n.from.len() == 0) || (old.roots.contains(k) && !n.to.contains(k))))
            && (n.from.len() != 0 ==> crate::verification::in_order(new.roots, old.roots))
            && (n.from.len() == 0 && (forall<c> n.to.contains(c) ==> !old.roots.contains(c))
                ==> new.roots == old.roots.snoc(id))
            && (n.from.len() == 0 ==> forall<i> 0 <= i && i < old.roots.len() && n.to.contains(old.roots[i])
                && (forall<j> 0 <= j && j < i ==> !n.to.contains(old.roots[j]))
                ==> crate::verification::in_order(new.roots, old.roots.set(i, id)))
            && new.bookmarked == if n.bookmarked { old.bookmarked.snoc(id) } else { old.bookmarked }
    }
}

/// When `r` keeps the order of `o`, holds only elements of it, and holds its first `i` elements,
/// these are also the first elements of `r`.
#[logic]
#[requires(no_dup(o) && no_dup(r))]
#[requires(crate::verification::in_order(r, o))]
#[requires(forall<x> r.contains(x) ==> o.contains(x))]
#[requires(0 <= m && m < i && i <= o.len())]
#[requires(forall<j> 0 <= j && j < i ==> r.contains(o[j]))]
#[ensures(forall<y> 0 <= y && y <= m ==> y < r.len() && r[y] == o[y])]
#[variant(m)]
pub fn lemma_kept_prefix<I>(r: Seq<I>, o: Seq<I>, i: Int, m: Int) {
    pearlite! {
        if m > 0 {
            lemma_kept_prefix(r, o, i, m - 1)
        }
        let z = such_that(|z: Int| 0 <= z && z < r.len() && r[z] == o[m]);
        proof_assert!(forall<y> 0 <= y && y < m ==> r[y] == o[y] && o[y] != o[m] && z != y);
        proof_assert!(m <= z && m < r.len());
        let p = such_that(|p: Int| 0 <= p && p < o.len() && o[p] == r[m]);
        proof_assert!(forall<y> 0 <= y && y < m ==> r[y] != r[m] && p != y);
        proof_assert!(m <= p);
        proof_assert!(forall<k> 0 <= k && k < o.len() && o[k] == r[m] ==> k == p);
        proof_assert!(forall<l> 0 <= l && l < o.len() && o[l] == r[z] ==> l == m);
        proof_assert!(m < z ==> exists<k, l> 0 <= k && k < l && l < o.len() && o[k] == r[m] && o[l] == r[z]);
        proof_assert!(m < z ==> p < m);
        proof_assert!(z == m)
    }
}

/// Inserting `id` at index `i` of `r`, which keeps the order of `o` and holds the first `i`
/// elements of `o` but not the next one, keeps the order of `o` with `id` in place of that one.
#[logic]
#[requires(no_dup(o) && no_dup(r))]
#[requires(crate::verification::in_order(r, o))]
#[requires(forall<x> r.contains(x) ==> o.contains(x))]
#[requires(0 <= i && i < o.len() && i <= r.len())]
#[requires(forall<j> 0 <= j && j < i ==> r.contains(o[j]))]
#[requires(!r.contains(o[i]))]
#[ensures(crate::verification::in_order(r.add_at(i, id), o.set(i, id)))]
pub fn lemma_insert_in_place<I>(r: Seq<I>, o: Seq<I>, i: Int, id: I) {
    pearlite! {
        if i > 0 {
            lemma_kept_prefix(r, o, i, i - 1)
        }
        let new = r.add_at(i, id);
        let o2 = o.set(i, id);
        proof_assert!(new.len() == r.len() + 1 && o2.len() == o.len());
        proof_assert!(forall<x> 0 <= x && x < i ==> new[x] == o[x] && o2[x] == o[x]);
        proof_assert!(new[i] == id && o2[i] == id);
        proof_assert!(forall<x> i < x && x < new.len() ==> new[x] == r[x - 1]);
        proof_assert!(forall<p> 0 <= p && p < o.len() && p != i ==> o2[p] == o[p]);
        proof_assert!(forall<x, p> i <= x && x < r.len() && 0 <= p && p < o.len() && o[p] == r[x]
            ==> i < p);
        proof_assert!(forall<x> i <= x && x < r.len()
            ==> exists<p> i < p && p < o.len() && o2[p] == r[x]);
        proof_assert!(forall<a, b> 0 <= a && a < b && b <= i
            ==> 0 <= a && a < b && b < o2.len() && o2[a] == new[a] && o2[b] == new[b]);
        proof_assert!(forall<a, b> 0 <= a && a <= i && i < b && b < new.len()
            ==> exists<l> a < l && l < o2.len() && o2[a] == new[a] && o2[l] == new[b]);
        proof_assert!(forall<a, b> i < a && a < b && b < new.len()
            ==> exists<k, l> 0 <= k && k < l && l < o.len() && o[k] == r[a - 1] && o[l] == r[b - 1]);
        proof_assert!(forall<a, b> i < a && a < b && b < new.len()
            ==> exists<k, l> i < k && k < l && l < o.len() && o[k] == r[a - 1] && o[l] == r[b - 1]
                && o2[k] == new[a] && o2[l] == new[b])
    }
}

/// A node can't be inserted when one of its children is one of its parents or one of their
/// ancestors.
#[logic]
#[requires(all_linked(nodes))]
#[requires(exists<i, k> n.from.contains(i) && n.to.contains(k) && nodes.contains(&k)
    && descends(nodes, i, k))]
#[ensures(!insertable(nodes, n))]
pub fn lemma_not_insertable<K, T, S>(nodes: Nodes<K, T, S>, n: NodeView<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let i = such_that(|i| n.from.contains(i)
            && exists<k> n.to.contains(k) && nodes.contains(&k) && descends(nodes, i, k));
        let k = such_that(|k| n.to.contains(k) && nodes.contains(&k) && descends(nodes, i, k));
        proof_assert!(forall<a: Mapping<Id<K>, bool>> separates(nodes, n, a) ==> {
            lemma_descends_closed(nodes, a, i, k);
            false
        })
    }
}

/// Facts about the links of a weave in which a node was inserted.
#[logic]
#[requires(old.valid())]
#[requires(insertable(old.nodes, n))]
#[requires(insert_shape(old, new, n))]
#[ensures(forall<k> old.nodes.contains(&k) ==> linked(old.nodes, k) && old.flagged(k)
    && !old.nodes[k]@.from.contains(n.id) && !old.nodes[k]@.to.contains(n.id))]
#[ensures(forall<k, x> old.nodes.contains(&k) && old.nodes[k]@.from.contains(x)
    ==> new.nodes[k]@.from.contains(x))]
#[ensures(forall<k, x> old.nodes.contains(&k) && old.nodes[k]@.to.contains(x)
    ==> new.nodes[k]@.to.contains(x))]
#[ensures(forall<k> old.nodes.contains(&k) ==> new.nodes[k]@.id == old.nodes[k]@.id
    && new.nodes[k]@.active == old.nodes[k]@.active
    && new.nodes[k]@.bookmarked == old.nodes[k]@.bookmarked)]
#[ensures(forall<k, x> old.nodes.contains(&k) && new.nodes[k]@.from.contains(x)
    ==> old.nodes[k]@.from.contains(x) || (x == n.id && n.to.contains(k)))]
#[ensures(forall<k, x> old.nodes.contains(&k) && new.nodes[k]@.to.contains(x)
    ==> old.nodes[k]@.to.contains(x) || (x == n.id && n.from.contains(k)))]
#[ensures(forall<k> old.nodes.contains(&k) && !n.to.contains(k)
    ==> new.nodes[k]@.from == old.nodes[k]@.from)]
pub fn lemma_insert_links<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    n: NodeView<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> old.nodes.contains(&k) ==> linked(old.nodes, k) && old.flagged(k));
        let _ = crate::verification::lemma_snoc_contains::<Id<K>>;
        proof_assert!(forall<k> n.from.contains(k) ==> !n.to.contains(k))
    }
}

/// Inserting a node keeps the weave's structure valid.
#[logic]
#[requires(old.valid())]
#[requires(insertable(old.nodes, n))]
#[requires(insert_shape(old, new, n))]
#[ensures(new.valid_structure())]
pub fn lemma_insert_structure<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    n: NodeView<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let id = n.id;
        let a = such_that(|a| separates(old.nodes, n, a));
        let rank = rank_of(old.nodes);
        let b = crate::verification::bound(n.from, rank);
        let rank2 = |k| if k == id { b } else if a[k] { rank[k] } else { rank[k] + b + 1 };
        lemma_insert_links(old, new, n);
        proof_assert!(forall<s: Seq<Id<K>>, x> no_dup(s) && !s.contains(x) ==> no_dup(s.snoc(x)));
        proof_assert!(forall<p> n.from.contains(p) ==> rank[p] < b);
        proof_assert!(forall<k> new.nodes.contains(&k) ==> 0 <= rank2[k]);
        proof_assert!(forall<k, p> old.nodes.contains(&k) && old.nodes[k]@.from.contains(p)
            ==> rank2[p] < rank2[k]);
        proof_assert!(forall<c> n.to.contains(c) ==> rank2[id] < rank2[c]);
        proof_assert!(ranked(new.nodes, rank2));
        proof_assert!(linked(new.nodes, id));
        proof_assert!(forall<k> old.nodes.contains(&k) ==> linked(new.nodes, k));
        proof_assert!(forall<k> new.nodes.contains(&k) ==> new.flagged(k))
    }
}

/// Inserting an inactive node keeps the active nodes a path, unless one of its children was an
/// active root.
#[logic]
#[requires(old.valid())]
#[requires(insertable(old.nodes, n))]
#[requires(insert_shape(old, new, n))]
#[requires(!new.active.contains(&n.id))]
#[requires(forall<c> n.to.contains(c) ==> old.nodes[c]@.from.len() != 0 || !old.active.contains(&c))]
#[ensures(forms_path(new.nodes, new.active))]
pub fn lemma_insert_inactive<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    n: NodeView<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let path = such_that(|path| path_of(old.nodes, old.active, path));
        lemma_insert_links(old, new, n);
        proof_assert!(forall<i> 0 <= i && i < path.len() ==> old.active.contains(&path[i])
            && old.nodes.contains(&path[i]));
        proof_assert!(valid_path(new.nodes, path));
        proof_assert!(path_of(new.nodes, new.active, path))
    }
}

/// A set closed under taking parents which contains a node of a path contains the rest of it.
#[logic]
#[requires(valid_path(nodes, path))]
#[requires(forall<k, p> a[k] && nodes.contains(&k) && nodes[k]@.from.contains(p) ==> a[p])]
#[requires(0 <= i && i < path.len() && a[path[i]])]
#[ensures(a[path[path.len() - 1]])]
#[variant(path.len() - i)]
pub fn lemma_closed_path<K, T, S>(
    nodes: Nodes<K, T, S>,
    a: Mapping<Id<K>, bool>,
    path: Seq<Id<K>>,
    i: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if i + 1 < path.len() {
        lemma_closed_path(nodes, a, path, i + 1)
    }
}

/// A weave in which an inactive node was inserted is ready to activate it.
#[logic]
#[requires(old.valid())]
#[requires(insertable(old.nodes, n))]
#[requires(insert_shape(old, new, n))]
#[requires(!new.active.contains(&n.id))]
#[ensures(ready_to_activate(new.nodes, new.active, n.id, true))]
pub fn lemma_insert_ready<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    n: NodeView<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let id = n.id;
        lemma_insert_links(old, new, n);
        lemma_insert_structure(old, new, n);
        if forall<c> n.to.contains(c) ==> old.nodes[c]@.from.len() != 0 || !old.active.contains(&c) {
            lemma_insert_inactive(old, new, n);
            lemma_ready_parts(new.nodes, new.roots, new.active, id, true)
        } else {
            let r = such_that(|r| n.to.contains(r) && old.nodes[r]@.from.len() == 0
                && old.active.contains(&r));
            let path = such_that(|path| path_of(old.nodes, old.active, path));
            let a = such_that(|a| separates(old.nodes, n, a));
            proof_assert!(r != id && new.active.contains(&r));
            proof_assert!(new.active.len() != 0);
            proof_assert!(forall<i> 0 <= i && i + 1 < path.len() ==> old.nodes[path[i]]@.from.len() != 0);
            proof_assert!(path.len() > 0 && path[path.len() - 1] == r);
            proof_assert!(forall<p> n.from.contains(p) && new.active.contains(&p) ==> {
                let i = such_that(|i| 0 <= i && i < path.len() && path[i] == p);
                lemma_closed_path(old.nodes, a, path, i);
                false
            });
            proof_assert!(!extends_path(new.nodes, new.active, id))
        }
    }
}

/// Inserting an active childless node below the end of the active path extends the path.
#[logic]
#[requires(old.valid())]
#[requires(insertable(old.nodes, n))]
#[requires(insert_shape(old, new, n))]
#[requires(n.to.len() == 0)]
#[requires(new.active.contains(&n.id))]
#[requires(exists<p> n.from.contains(p) && old.active.contains(&p)
    && !has_child_in(old.nodes, old.active, p))]
#[ensures(forms_path(new.nodes, new.active))]
pub fn lemma_insert_extends<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    n: NodeView<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let id = n.id;
        let path = such_that(|path| path_of(old.nodes, old.active, path));
        let p = such_that(|p| n.from.contains(p) && old.active.contains(&p)
            && !has_child_in(old.nodes, old.active, p));
        lemma_insert_links(old, new, n);
        lemma_path_end(old.nodes, old.active, path, p);
        let new_path = Seq::singleton(id).concat(path);
        proof_assert!(new_path.len() == path.len() + 1 && new_path[0] == id);
        proof_assert!(forall<i> 0 <= i && i < path.len() ==> new_path[i + 1] == path[i]
            && old.nodes.contains(&path[i]) && path[i] != id);
        proof_assert!(valid_path(new.nodes, path));
        proof_assert!(new.nodes[new_path[0]]@.from.contains(new_path[1]));
        proof_assert!(valid_path(new.nodes, new_path));
        proof_assert!(no_dup(new_path));
        proof_assert!(forall<k> new_path.contains(k) == (path.contains(k) || k == id));
        proof_assert!(path_of(new.nodes, new.active, new_path))
    }
}

/// None of `parents` descends from `id`.
#[logic(open)]
pub fn not_below<K, T, S>(nodes: Nodes<K, T, S>, id: Id<K>, parents: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { forall<q> parents.contains(q) ==> !descends(nodes, q, id) }
}

/// `new` is `old` once node `id`'s parents are replaced by `parents`, which gain `id` as a child
/// while its former parents lose it. Only `id`'s activity may differ between the active nodes.
#[logic(open)]
pub fn move_shape<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    parents: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = old.nodes[id]@;
        old.nodes.contains(&id)
            && no_dup(parents)
            && (forall<k> new.nodes.contains(&k) == old.nodes.contains(&k))
            && new.nodes[id]@ == NodeView {
                id,
                from: parents,
                to: n.to,
                active: new.active.contains(&id),
                bookmarked: n.bookmarked,
            }
            && (forall<k> old.nodes.contains(&k) && k != id ==> new.nodes[k]@.id == old.nodes[k]@.id
                && new.nodes[k]@.from == old.nodes[k]@.from
                && new.nodes[k]@.active == old.nodes[k]@.active
                && new.nodes[k]@.bookmarked == old.nodes[k]@.bookmarked
                && no_dup(new.nodes[k]@.to)
                && forall<x> new.nodes[k]@.to.contains(x)
                    == if x == id { parents.contains(k) } else { old.nodes[k]@.to.contains(x) })
            && no_dup(new.roots)
            && (forall<k> new.roots.contains(k)
                == if k == id { parents.len() == 0 } else { old.roots.contains(k) })
            && new.bookmarked == old.bookmarked
            && forall<k> k != id ==> new.active.contains(&k) == old.active.contains(&k)
    }
}

/// `new` is `old` once node `id`'s parents are replaced by `parents`, as [`move_shape`] describes,
/// except that which nodes are active may differ. No node's contents change, the nodes which gain
/// `id` as a child list it last, the others keep their children in order, and the roots keep their
/// order, with `id` last if it becomes one.
#[logic(open)]
pub fn moved<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    parents: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = old.nodes[id]@;
        old.nodes.contains(&id)
            && no_dup(parents)
            && (forall<k> new.nodes.contains(&k) == old.nodes.contains(&k))
            && new.nodes[id]@.id == id
            && new.nodes[id]@.from == parents
            && new.nodes[id]@.to == n.to
            && new.nodes[id]@.bookmarked == n.bookmarked
            && (forall<k> old.nodes.contains(&k) ==> new.nodes[k].contents == old.nodes[k].contents)
            && (forall<k> old.nodes.contains(&k) && k != id ==> new.nodes[k]@.id == old.nodes[k]@.id
                && new.nodes[k]@.from == old.nodes[k]@.from
                && new.nodes[k]@.bookmarked == old.nodes[k]@.bookmarked
                && no_dup(new.nodes[k]@.to)
                && (forall<x> new.nodes[k]@.to.contains(x)
                    == if x == id { parents.contains(k) } else { old.nodes[k]@.to.contains(x) })
                && if parents.contains(k) && !n.from.contains(k) {
                    new.nodes[k]@.to == old.nodes[k]@.to.snoc(id)
                } else {
                    crate::verification::in_order(new.nodes[k]@.to, old.nodes[k]@.to)
                })
            && no_dup(new.roots)
            && (forall<k> new.roots.contains(k)
                == if k == id { parents.len() == 0 } else { old.roots.contains(k) })
            && (if parents.len() == 0 && !old.roots.contains(id) {
                new.roots == old.roots.snoc(id)
            } else {
                crate::verification::in_order(new.roots, old.roots)
            })
            && new.bookmarked == old.bookmarked
    }
}

/// The parents of a node don't descend from it.
#[logic]
#[requires(all_linked(nodes) && acyclic(nodes))]
#[requires(nodes.contains(&id))]
#[ensures(not_below(nodes, id, nodes[id]@.from))]
pub fn lemma_parents_not_below<K, T, S>(nodes: Nodes<K, T, S>, id: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let rank = rank_of(nodes);
        proof_assert!(linked(nodes, id));
        proof_assert!(forall<q> nodes[id]@.from.contains(q) ==> nodes.contains(&q) && rank[q] < rank[id]);
        proof_assert!(forall<q> nodes[id]@.from.contains(q) && descends(nodes, q, id) ==> {
            lemma_descends_rank(nodes, rank, q, id);
            false
        })
    }
}

/// A node is below one of `parents` when they include it, or one of them descends from one of its
/// children.
#[logic]
#[requires(all_linked(nodes))]
#[requires(parents.contains(id) || exists<c, q> nodes[id]@.to.contains(c) && parents.contains(q)
    && descends(nodes, q, c))]
#[ensures(!not_below(nodes, id, parents))]
pub fn lemma_below<K, T, S>(nodes: Nodes<K, T, S>, id: Id<K>, parents: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    lemma_descends_refl(nodes, id);
    pearlite! {
        if !parents.contains(id) {
            let c = such_that(|c| nodes[id]@.to.contains(c)
                && exists<q> parents.contains(q) && descends(nodes, q, c));
            let q = such_that(|q| parents.contains(q) && descends(nodes, q, c));
            lemma_descends_child(nodes, id, c, id);
            lemma_descends_trans(nodes, q, c, id)
        }
    }
}

/// Every node of a path before the `j`th descends from it.
#[logic]
#[requires(all_linked(nodes))]
#[requires(valid_path(nodes, path))]
#[requires(0 <= i && i <= j && j < path.len())]
#[ensures(descends(nodes, path[i], path[j]))]
#[variant(j - i)]
pub fn lemma_path_descends<K, T, S>(nodes: Nodes<K, T, S>, path: Seq<Id<K>>, i: Int, j: Int)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    if i < j {
        pearlite! {
            proof_assert!(linked(nodes, path[i]) && nodes[path[i + 1]]@.to.contains(path[i]));
            lemma_path_descends(nodes, path, i + 1, j)
        }
    }
}

/// Moving a node below parents which don't descend from it keeps the weave's structure valid.
#[logic]
#[requires(old.valid())]
#[requires(move_shape(old, new, id, parents))]
#[requires(forall<q> parents.contains(q) ==> old.nodes.contains(&q))]
#[requires(!parents.contains(id))]
#[requires(not_below(old.nodes, id, parents))]
#[requires(forall<k> new.active.contains(&k) ==> new.nodes.contains(&k))]
#[ensures(new.valid_structure())]
pub fn lemma_move_structure<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    parents: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let n = old.nodes[id]@;
        let rank = rank_of(old.nodes);
        let b = crate::verification::bound(parents, rank);
        let rank2 = |k| if descends(old.nodes, k, id) { rank[k] + b } else { rank[k] };
        proof_assert!(forall<k> old.nodes.contains(&k) ==> linked(old.nodes, k) && old.flagged(k));
        proof_assert!(descends(old.nodes, id, id));
        proof_assert!(forall<k, c> descends(old.nodes, k, id) && old.nodes[k]@.to.contains(c)
            ==> descends(old.nodes, c, id));
        proof_assert!(forall<c> n.to.contains(c) ==> descends(old.nodes, c, id));
        proof_assert!(forall<q> parents.contains(q) ==> rank[q] < b && !descends(old.nodes, q, id));
        proof_assert!(forall<k> old.nodes.contains(&k) && old.nodes[k]@.from.contains(id)
            ==> n.to.contains(k) && descends(old.nodes, k, id));
        proof_assert!(forall<k> old.nodes.contains(&k) && old.nodes[k]@.to.contains(id) ==> n.from.contains(k));
        proof_assert!(forall<q> parents.contains(q) ==> !n.to.contains(q) && !old.nodes[q]@.from.contains(id));
        proof_assert!(forall<k> new.nodes.contains(&k) ==> 0 <= rank2[k]);
        proof_assert!(forall<p> parents.contains(p) ==> rank2[p] < rank2[id]);
        proof_assert!(forall<k, p> old.nodes.contains(&k) && k != id && old.nodes[k]@.from.contains(p)
            ==> rank2[p] < rank2[k]);
        proof_assert!(ranked(new.nodes, rank2));
        proof_assert!(n.valid() && new.nodes[id]@.valid());
        proof_assert!(linked(new.nodes, id));
        proof_assert!(forall<k> old.nodes.contains(&k) && k != id ==> linked(new.nodes, k));
        proof_assert!(forall<k> new.nodes.contains(&k) ==> new.flagged(k))
    }
}

/// Moving an inactive node keeps the active nodes a path.
#[logic]
#[requires(old.valid())]
#[requires(move_shape(old, new, id, parents))]
#[requires(!old.active.contains(&id))]
#[requires(new.active == old.active)]
#[ensures(forms_path(new.nodes, new.active))]
pub fn lemma_move_inactive<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    parents: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let path = such_that(|path| path_of(old.nodes, old.active, path));
        proof_assert!(forall<i> 0 <= i && i < path.len() ==> path[i] != id && old.nodes.contains(&path[i]));
        proof_assert!(valid_path(new.nodes, path));
        proof_assert!(path_of(new.nodes, new.active, path))
    }
}

/// Moving an active node and then deactivating it leaves the weave ready to activate it again.
#[logic]
#[requires(old.valid())]
#[requires(move_shape(old, new, id, parents))]
#[requires(!parents.contains(id))]
#[requires(not_below(old.nodes, id, parents))]
#[requires(old.active.contains(&id))]
#[requires(new.active == old.active.remove(id))]
#[ensures(ready_to_activate(new.nodes, new.active, id, true))]
pub fn lemma_move_ready<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    parents: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let path = such_that(|path| path_of(old.nodes, old.active, path));
        proof_assert!(old.active.ext_eq(new.active.add(id)));
        proof_assert!(forall<k> old.nodes.contains(&k) ==> linked(old.nodes, k));
        proof_assert!(!new.active.contains(&id));
        if parents.len() == 0 && new.active.len() == 0 {
            proof_assert!(forall<k> old.active.contains(&k) == (k == id));
            proof_assert!(path_of(new.nodes, new.active.add(id), Seq::singleton(id)))
        } else if exists<p> parents.contains(p) && new.active.contains(&p)
            && !has_child_in(new.nodes, new.active, p)
        {
            let p = such_that(|p| parents.contains(p) && new.active.contains(&p)
                && !has_child_in(new.nodes, new.active, p));
            let j = such_that(|j| 0 <= j && j < path.len() && path[j] == id);
            let i = such_that(|i| 0 <= i && i < path.len() && path[i] == p);
            proof_assert!(i != j);
            proof_assert!(i < j ==> {
                lemma_path_descends(old.nodes, path, i, j);
                false
            });
            proof_assert!(j < i);
            proof_assert!(old.nodes[path[i - 1]]@.from.contains(path[i]) && linked(old.nodes, path[i - 1]));
            proof_assert!(old.nodes[p]@.to.contains(path[i - 1]));
            proof_assert!(path[i - 1] != id ==> new.nodes[p]@.to.contains(path[i - 1])
                && new.active.contains(&path[i - 1]));
            proof_assert!(i == j + 1);
            proof_assert!(forall<m> 0 <= m && m + 1 < path.len() && m != j
                ==> new.nodes[path[m]]@.from.contains(path[m + 1]));
            proof_assert!(new.nodes[path[j]]@.from.contains(path[j + 1]));
            proof_assert!(path[path.len() - 1] != id);
            proof_assert!(valid_path(new.nodes, path));
            proof_assert!(path_of(new.nodes, new.active.add(id), path))
        }
    }
}
