//! Smaller operations which [`IndependentWeave`](super::IndependentWeave) is built on.
//!
//! The functions marked `#[trusted]` iterate over hash sets or maps, whose iterators can't be
//! specified from this crate; their bodies are the iterator code the weave's methods used to
//! contain inline.

use alloc::vec::Vec;
use core::{
    cmp::Ordering,
    hash::{BuildHasher, Hash},
};

use creusot_std::macros::*;
use hashbrown::{HashMap, HashSet, hash_map::Entry};
use indexmap::IndexSet;
use scratchpads::{Scratchpad, ScratchpadMap, ScratchpadSet, ScratchpadVec};

#[cfg(creusot)]
use creusot_std::{invariant::inv, prelude::*};

use super::{IndependentNode, graph::longest_candidate_path_to_root};
use crate::{IndependentContents, Step, Verifiable, default_hasher, same_id};

pub(super) use crate::dependent::{all_entries, contains_all};

#[cfg(creusot)]
use super::verification::*;

#[cfg(creusot)]
use crate::verification::{
    Id, Model, compares_by, dedup, identity, ids, lemma_dedup_prefix, lemma_ids_snoc,
    lemma_keys_subset_len, member_of, no_dup, order_by, sorted_stably, total_preorder, value_of,
};
#[cfg(creusot)]
use creusot_std::logic::{FSet, Mapping};

/// The score of each node considered for the active path ((inactive nodes, active nodes) along
/// its best path), and the next node along that path.
pub(super) type Scores<'a, K, S> = ScratchpadMap<'a, K, ((usize, usize), Option<K>), S>;

/// Checks that the given parts form a valid [`IndependentWeave`](super::IndependentWeave).
#[check(terminates)]
#[ensures(result == IndependentWeaveView {
    nodes: nodes.model(),
    roots: roots.model(),
    active: active.model(),
    bookmarked: bookmarked.model(),
}.valid())]
pub(super) fn validate<K, T, S>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    roots: &IndexSet<K, S>,
    active: &HashSet<K, S>,
    bookmarked: &IndexSet<K, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    contains_all(roots, nodes)
        && all_present(active, nodes)
        && contains_all(bookmarked, nodes)
        && all_entries(nodes, |key, value| {
            validate_node(nodes, roots, active, bookmarked, key, value)
        })
        && valid_topology(nodes, roots, active)
}

/// Checks that the node `value`, stored at `key`, is consistent with the given parts of a weave.
#[check(terminates)]
#[requires(nodes.model().lookup(key.deep_model()) == Some(*value))]
#[ensures(result == (linked(nodes.model(), key.deep_model())
    && (value@.from.len() == 0) == roots.model().contains(key.deep_model())
    && value@.active == active.model().contains(&key.deep_model())
    && value@.bookmarked == bookmarked.model().contains(key.deep_model())))]
fn validate_node<K, T, S>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    roots: &IndexSet<K, S>,
    active: &HashSet<K, S>,
    bookmarked: &IndexSet<K, S>,
    key: &K,
    value: &IndependentNode<K, T, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    value.validate()
        && same_id(&value.id, key)
        && parents_link_back(nodes, &value.from, key)
        && children_link_back(nodes, &value.to, key)
        && value.from.is_empty() == roots.contains(key)
        && value.active == active.contains(key)
        && value.bookmarked == bookmarked.contains(key)
}

/// Checks that every identifier in `parents` is a node with `key` as a child.
#[check(terminates)]
#[ensures(result == forall<p> parents.model().contains(p)
    ==> nodes.model().contains(&p) && nodes.model()[p]@.to.contains(key.deep_model()))]
#[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
fn parents_link_back<K, T, S>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    parents: &IndexSet<K, S>,
    key: &K,
) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let mut i = 0;
    #[invariant(i@ <= parents.model().len())]
    #[invariant(forall<j> 0 <= j && j < i@ ==> nodes.model().contains(&parents.model()[j])
        && nodes.model()[parents.model()[j]]@.to.contains(key.deep_model()))]
    #[variant(parents.model().len() - i@)]
    while i < parents.len() {
        let linked = match nodes.get(&parents[i]) {
            Some(parent) => parent.to.contains(key),
            None => false,
        };
        if !linked {
            return false;
        }
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            i += 1;
        }
    }

    true
}

/// Checks that every identifier in `children` is a node with `key` as a parent.
#[check(terminates)]
#[ensures(result == forall<c> children.model().contains(c)
    ==> nodes.model().contains(&c) && nodes.model()[c]@.from.contains(key.deep_model()))]
#[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
fn children_link_back<K, T, S>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    children: &IndexSet<K, S>,
    key: &K,
) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let mut i = 0;
    #[invariant(i@ <= children.model().len())]
    #[invariant(forall<j> 0 <= j && j < i@ ==> nodes.model().contains(&children.model()[j])
        && nodes.model()[children.model()[j]]@.from.contains(key.deep_model()))]
    #[variant(children.model().len() - i@)]
    while i < children.len() {
        let linked = match nodes.get(&children[i]) {
            Some(child) => child.from.contains(key),
            None => false,
        };
        if !linked {
            return false;
        }
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            i += 1;
        }
    }

    true
}

/// Checks that the links form no cycles and that the active nodes form a path from some node to a
/// root, given that every other part of the weave is consistent.
#[check(terminates)]
#[requires(all_linked(nodes.model()))]
#[requires(forall<k> roots.model().contains(k)
    == (nodes.model().contains(&k) && nodes.model()[k]@.from.len() == 0))]
#[requires(forall<k> active.model().contains(&k) ==> nodes.model().contains(&k))]
#[ensures(result == (acyclic(nodes.model()) && forms_path(nodes.model(), active.model())))]
pub(super) fn valid_topology<K, T, S>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    roots: &IndexSet<K, S>,
    active: &HashSet<K, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    snapshot! { lemma_ids_snoc::<K> };
    snapshot! { lemma_keys_subset_len::<Id<K>, bool, IndependentNode<K, T, S>> };
    let mut topological = Vec::with_capacity(nodes.len());
    let mut scratchpad = Scratchpad::new();

    {
        let guard = scratchpad.guard();

        let mut stack = guard.vec_with_capacity(roots.len());
        let mut scratchpad_map = guard.map_with_capacity(nodes.len(), default_hasher::<S>());

        proof_assert! { ids(topological@).len() == 0 };
        proof_assert! {
            vt_dfs(nodes.model(), scratchpad_map.model(), stack.model(), ids(topological@),
                Seq::empty(), Seq::empty())
        };

        let mut i = 0;

        #[invariant(i@ <= roots.model().len())]
        #[invariant(vt_dfs(nodes.model(), scratchpad_map.model(), stack.model(), ids(topological@),
            Seq::empty(), Seq::empty()))]
        #[invariant(stack.model().len() == 0)]
        #[invariant(forall<j> 0 <= j && j < i@
            ==> scratchpad_map.model().contains(&roots.model()[j]))]
        #[variant(roots.model().len() - i@)]
        while i < roots.len() {
            let root = roots[i];
            if !scratchpad_map.contains_key(&root) {
                proof_assert! { roots.model().contains(root.deep_model()) };
                proof! {
                    let map0 = snapshot!(scratchpad_map.model());
                    let empty = snapshot!(stack.model());
                    let mut gray = snapshot!(Seq::<Id<K>>::empty());
                    let mut exits = snapshot!(Seq::<Int>::empty());
                }
                stack.push(Step::Enter(root));
                proof_assert! {
                    lemma_vt_start(nodes.model(), scratchpad_map.model(), *empty, ids(topological@), root);
                    vt_dfs(nodes.model(), scratchpad_map.model(), stack.model(), ids(topological@),
                        *gray, *exits)
                };

                #[invariant(vt_dfs(nodes.model(), scratchpad_map.model(), stack.model(),
                    ids(topological@), *gray, *exits))]
                #[invariant(forall<k> map0.contains(&k) ==> scratchpad_map.model().contains(&k))]
                #[invariant(scratchpad_map.model().contains(&root.deep_model())
                    || (stack.model().len() == 1 && vt_enters(stack.model()[0], root.deep_model())))]
                #[variant((nodes.model().len() - scratchpad_map.model().len(), stack.model().len()))]
                while let Some(step) = stack.pop() {
                    proof! {
                        let rest = snapshot!(stack.model());
                        let map1 = snapshot!(scratchpad_map.model());
                        let finished = snapshot!(ids(topological@));
                    }
                    match step {
                        Step::Enter(id) => match scratchpad_map.entry(id) {
                            Entry::Occupied(entry) => {
                                // `!*` rather than `!`: Creusot has no specification for `Not` on `&bool`.
                                if !entry.get() {
                                    proof_assert! {
                                        lemma_vt_back_edge(
                                            nodes.model(), *map1, *rest, id, *finished, *gray, *exits);
                                        !acyclic(nodes.model())
                                    };
                                    return false;
                                }
                                proof_assert! {
                                    lemma_vt_skip(nodes.model(), *map1, *rest, id, *finished, *gray, *exits);
                                    vt_dfs(nodes.model(), *map1, *rest, *finished, *gray, *exits)
                                };
                            }
                            Entry::Vacant(entry) => {
                                entry.insert_entry(false);

                                proof_assert! { nodes.model().contains(&id.deep_model()) };
                                stack.push(Step::Exit(id));
                                extend_steps_rev(&mut stack, &nodes[&id].to);
                                proof_assert! {
                                    lemma_vt_enter(nodes.model(), *map1, *rest, id, stack.model(),
                                        *finished, *gray, *exits);
                                    vt_dfs(nodes.model(), map1.add(id.deep_model(), false), stack.model(),
                                        *finished, gray.snoc(id.deep_model()), exits.snoc(rest.len()))
                                };
                                proof! {
                                    gray = snapshot!(gray.snoc(id.deep_model()));
                                    exits = snapshot!(exits.snoc(rest.len()));
                                }
                            }
                        },
                        Step::Exit(id) => {
                            proof_assert!(map1.contains(&id.deep_model()));
                            scratchpad_map.insert(id, true);
                            proof_assert!(scratchpad_map.model().len() == map1.len());
                            topological.push(id);
                            proof_assert! {
                                lemma_vt_exit(nodes.model(), *map1, *rest, id, *finished, *gray, *exits);
                                vt_dfs(nodes.model(), scratchpad_map.model(), stack.model(),
                                    ids(topological@), gray.subsequence(0, gray.len() - 1),
                                    exits.subsequence(0, exits.len() - 1))
                            };
                            proof! {
                                gray = snapshot!(gray.subsequence(0, gray.len() - 1));
                                exits = snapshot!(exits.subsequence(0, exits.len() - 1));
                            }
                        }
                    }
                }
                proof_assert! {
                    lemma_vt_stop(nodes.model(), scratchpad_map.model(), stack.model(),
                        ids(topological@), *gray, *exits);
                    vt_dfs(nodes.model(), scratchpad_map.model(), stack.model(), ids(topological@),
                        Seq::empty(), Seq::empty())
                };
            }
            #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
            {
                i += 1;
            }
        }

        proof_assert! { forall<k> roots.model().contains(k) ==> scratchpad_map.model().contains(&k) };
        proof_assert! {
            lemma_vt_check(nodes.model(), roots.model(), scratchpad_map.model(), ids(topological@));
            (scratchpad_map.model().len() == nodes.model().len()) == acyclic(nodes.model())
                && (scratchpad_map.model().len() == nodes.model().len()
                    ==> forall<k> nodes.model().contains(&k) ==> ids(topological@).contains(k))
        };
        if scratchpad_map.len() != nodes.len() {
            return false;
        }
    }

    proof! {
        let finished = snapshot!(topological@);
    }
    proof_assert! { forall<k> ids(*finished).contains(k) == nodes.model().contains(&k) };
    topological.reverse();
    proof_assert! {
        crate::verification::lemma_ids_reverse(*finished);
        ids(topological@) == ids(*finished).reverse()
    };
    proof_assert! {
        lemma_vt_order(nodes.model(), ids(*finished));
        no_dup(ids(topological@))
            && (forall<k> ids(topological@).contains(k) == nodes.model().contains(&k))
            && forall<i, j> 0 <= i && i < topological@.len() && 0 <= j && j < topological@.len()
                && nodes.model()[ids(topological@)[j]]@.from.contains(ids(topological@)[i]) ==> i < j
    };
    proof_assert! {
        forall<i> 0 <= i && i < topological@.len()
            ==> nodes.model().contains(&topological@[i].deep_model())
    };

    let guard = scratchpad.guard();

    let mut path = guard.vec_with_capacity(active.len());
    let mut scratchpad_map = guard.map_with_capacity(nodes.len(), default_hasher::<S>());
    proof_assert!(candidates_ordered(nodes.model(), ids(topological@), member_of(active.model())));

    longest_candidate_path_to_root(
        nodes,
        &topological,
        active,
        None,
        snapshot!(member_of(active.model())),
        &mut scratchpad_map,
        &mut path,
    );

    proof_assert! { forall<k> member_of(active.model())[k] == active.model().contains(&k) };
    proof_assert! { valid_path(nodes.model(), ids(path.model())) && no_dup(ids(path.model())) };
    proof_assert! {
        forall<i> 0 <= i && i < ids(path.model()).len()
            ==> member_of(active.model())[ids(path.model())[i]]
                && ids(topological@).contains(ids(path.model())[i])
    };
    proof_assert! {
        forall<chain> candidate_chain(
            nodes.model(), ids(topological@), member_of(active.model()), chain)
            ==> ids(path.model()) == chain
    };
    proof_assert! {
        lemma_vt_path(nodes.model(), ids(topological@), active.model(), member_of(active.model()),
            ids(path.model()));
        vt_path_matches(nodes.model(), ids(topological@), active.model(), member_of(active.model()),
            ids(path.model()))
    };
    proof_assert! {
        (ids(path.model()).len() == active.model().len()) == forms_path(nodes.model(), active.model())
    };
    path.len() == active.len() && all_in(&path, active)
}

/// Checks that every identifier in `path` is in `set`.
#[check(terminates)]
#[ensures(result == forall<i> 0 <= i && i < path.model().len()
    ==> set.model().contains(&path.model()[i].deep_model()))]
fn all_in<K, S>(path: &ScratchpadVec<'_, K>, set: &HashSet<K, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    let mut i = 0;

    #[invariant(i@ <= path.model().len())]
    #[invariant(forall<j> 0 <= j && j < i@ ==> set.model().contains(&path.model()[j].deep_model()))]
    #[variant(path.model().len() - i@)]
    while i < path.len() {
        if !set.contains(&path[i]) {
            return false;
        }
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            i += 1;
        }
    }

    true
}

/// Checks that one of `from` is active and has no active child: the end of the active path.
#[requires(forall<p> from.model().contains(p) ==> nodes.model().contains(&p))]
#[ensures(result == exists<p> from.model().contains(p) && active.model().contains(&p)
    && !has_child_in(nodes.model(), active.model(), p))]
pub(super) fn extends_active_end<K, T, S>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    active: &HashSet<K, S>,
    from: &IndexSet<K, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let mut i = 0;
    #[invariant(i@ <= from.model().len())]
    #[invariant(forall<j> 0 <= j && j < i@ ==> !(active.model().contains(&from.model()[j])
        && !has_child_in(nodes.model(), active.model(), from.model()[j])))]
    while i < from.len() {
        let parent = from[i];
        if active.contains(&parent) && !any_active(active, &nodes[&parent].to) {
            return true;
        }
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            i += 1;
        }
    }

    false
}

/// Checks whether one of `ids` is active.
#[ensures(result == exists<c> ids.model().contains(c) && active.model().contains(&c))]
pub(super) fn any_active<K, S>(active: &HashSet<K, S>, ids: &IndexSet<K, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    let mut i = 0;

    #[invariant(i@ <= ids.model().len())]
    #[invariant(forall<j> 0 <= j && j < i@ ==> !active.model().contains(&ids.model()[j]))]
    while i < ids.len() {
        if active.contains(&ids[i]) {
            return true;
        }
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            i += 1;
        }
    }

    false
}

/// The first of `ids` with the best score (most active nodes, then fewest inactive ones), and its
/// score.
#[requires(forall<k> ids.model().contains(k) ==> scores.model().contains(&k))]
#[ensures(match result {
    Some((id, score)) => ids.model().contains(id.deep_model())
        && scores.model()[id.deep_model()].0 == score
        && forall<c> ids.model().contains(c) ==> score_le(scores.model()[c].0, score),
    None => ids.model().len() == 0,
})]
pub(super) fn best_scored<K, S>(
    ids: &IndexSet<K, S>,
    scores: &Scores<'_, K, S>,
) -> Option<(K, (usize, usize))>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    let mut best: Option<(K, (usize, usize))> = None;
    let mut i = 0;

    #[invariant(i@ <= ids.model().len())]
    #[invariant(match best {
        Some((id, score)) => ids.model().contains(id.deep_model())
            && scores.model()[id.deep_model()].0 == score
            && forall<j> 0 <= j && j < i@ ==> score_le(scores.model()[ids.model()[j]].0, score),
        None => i@ == 0,
    })]
    while i < ids.len() {
        let id = ids[i];
        let score = scores[&id].0;
        // score: (connectors, active)
        // `Ordering::then` is spelled out, as Creusot has no specification for it.
        if best.is_none_or(|(_, best)| {
            best.1.cmp(&score.1).then(score.0.cmp(&best.0)) == Ordering::Less
        }) {
            best = Some((id, score));
        }
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            i += 1;
        }
    }

    best
}

/// The index of the first root in `roots` which is one of `to`.
#[ensures(match result {
    Some(i) => i@ < roots.model().len() && to.model().contains(roots.model()[i@])
        && forall<j> 0 <= j && j < i@ ==> !to.model().contains(roots.model()[j]),
    None => forall<c> to.model().contains(c) ==> !roots.model().contains(c),
})]
pub(super) fn first_root_index<K, S>(roots: &IndexSet<K, S>, to: &IndexSet<K, S>) -> Option<usize>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    let mut min: Option<usize> = None;
    let mut i = 0;

    #[invariant(i@ <= to.model().len())]
    #[invariant(match min {
        Some(m) => m@ < roots.model().len()
            && (exists<j> 0 <= j && j < i@ && to.model()[j] == roots.model()[m@])
            && forall<r, j> 0 <= r && r < m@ && 0 <= j && j < i@ ==> to.model()[j] != roots.model()[r],
        None => forall<j> 0 <= j && j < i@ ==> !roots.model().contains(to.model()[j]),
    })]
    while i < to.len() {
        if let Some(index) = roots.get_index_of(&to[i])
            && min.is_none_or(|min| index < min)
        {
            min = Some(index);
        }
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            i += 1;
        }
    }

    min
}

/// Checks that every identifier in `slice` is a key of `nodes`.
#[ensures(result == forall<i> 0 <= i && i < slice@.len() ==> nodes.model().contains(&slice@[i].deep_model()))]
pub(super) fn all_present_in_slice<K, V, S>(slice: &[K], nodes: &HashMap<K, V, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    let mut i = 0;

    #[invariant(i@ <= slice@.len())]
    #[invariant(forall<j> 0 <= j && j < i@ ==> nodes.model().contains(&slice@[j].deep_model()))]
    while i < slice.len() {
        if !nodes.contains_key(&slice[i]) {
            return false;
        }
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            i += 1;
        }
    }

    true
}

/// Checks that `set` lists exactly `slice`, in order.
#[ensures(result == (set.model() == ids(slice@)))]
pub(super) fn same_ids<K, S>(set: &IndexSet<K, S>, slice: &[K]) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    if set.len() != slice.len() {
        return false;
    }

    let mut i = 0;

    #[invariant(i@ <= slice@.len())]
    #[invariant(forall<j> 0 <= j && j < i@ ==> set.model()[j] == slice@[j].deep_model())]
    while i < set.len() {
        if set[i] != slice[i] {
            return false;
        }
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            i += 1;
        }
    }

    proof_assert!(set.model().ext_eq(ids(slice@)));
    true
}

/// Checks that every identifier in `set` is a key of `nodes`.
#[trusted]
#[check(terminates)]
#[ensures(result == forall<k> set.model().contains(&k) ==> nodes.model().contains(&k))]
fn all_present<K, V, S>(set: &HashSet<K, S>, nodes: &HashMap<K, V, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    set.iter().all(|value| nodes.contains_key(value))
}

/// Checks whether one of `a` other than `id` is in `b`.
#[trusted]
#[ensures(result == exists<k> a.model().contains(&k) && b.model().contains(&k) && k != id.deep_model())]
pub(super) fn contains_other<K, S>(a: &ScratchpadSet<'_, K, S>, b: &HashSet<K, S>, id: &K) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    if a.len() >= b.len() {
        b.iter().any(|k| k != id && a.contains(k))
    } else {
        a.iter().any(|k| k != id && b.contains(k))
    }
}

/// Makes exactly the nodes in `set` active.
#[trusted]
#[requires(forall<k> set.model().contains(&k) ==> nodes.model().contains(&k))]
#[requires(forall<k> active.model().contains(&k) ==> nodes.model().contains(&k))]
#[requires(forall<k> nodes.model().contains(&k)
    ==> nodes.model()[k]@.active == active.model().contains(&k))]
#[requires(disjoint.model().len() == 0)]
#[ensures((^active).model() == set.model())]
#[ensures(forall<k> (^nodes).model().contains(&k) == nodes.model().contains(&k))]
#[ensures(forall<k> nodes.model().contains(&k) ==> (^nodes).model()[k]@.id == nodes.model()[k]@.id
    && (^nodes).model()[k]@.from == nodes.model()[k]@.from
    && (^nodes).model()[k]@.to == nodes.model()[k]@.to
    && (^nodes).model()[k]@.bookmarked == nodes.model()[k]@.bookmarked
    && (^nodes).model()[k].contents == nodes.model()[k].contents
    && (^nodes).model()[k]@.active == set.model().contains(&k))]
#[ensures((^disjoint).model().len() == 0)]
pub(super) fn activate_exactly<K, T, S>(
    nodes: &mut HashMap<K, IndependentNode<K, T, S>, S>,
    active: &mut HashSet<K, S>,
    set: &ScratchpadSet<'_, K, S>,
    disjoint: &mut ScratchpadVec<'_, K>,
) where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    disjoint.extend(active.iter().filter(|id| !set.contains(*id)).copied());

    for id in disjoint.drain(..) {
        nodes.get_mut(&id).unwrap().active = false;
        active.remove(&id);
    }

    disjoint.extend(set.iter().filter(|id| !active.contains(*id)).copied());

    for id in disjoint.drain(..) {
        nodes.get_mut(&id).unwrap().active = true;
        active.insert(id);
    }
}

/// Makes every node of `active` inactive, and empties it.
#[trusted]
#[requires(forall<k> active.model().contains(&k) ==> nodes.model().contains(&k))]
#[ensures((^active).model() == FSet::empty())]
#[ensures(forall<k> (^nodes).model().contains(&k) == nodes.model().contains(&k))]
#[ensures(forall<k> nodes.model().contains(&k) ==> (^nodes).model()[k]@.id == nodes.model()[k]@.id
    && (^nodes).model()[k]@.from == nodes.model()[k]@.from
    && (^nodes).model()[k]@.to == nodes.model()[k]@.to
    && (^nodes).model()[k]@.bookmarked == nodes.model()[k]@.bookmarked
    && (^nodes).model()[k].contents == nodes.model()[k].contents
    && (^nodes).model()[k]@.active == (nodes.model()[k]@.active && !active.model().contains(&k)))]
pub(super) fn deactivate_all<K, T, S>(
    nodes: &mut HashMap<K, IndependentNode<K, T, S>, S>,
    active: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    active.drain().for_each(|active| {
        nodes.get_mut(&active).unwrap().active = false;
    });
}

/// Pushes the elements of `set` onto `output`, which is empty.
#[trusted]
#[requires(output.model().len() == 0)]
#[ensures(no_dup(ids((^output).model())))]
#[ensures(forall<k> ids((^output).model()).contains(k) == set.model().contains(&k))]
pub(super) fn extend_from_hash_set<K, S>(output: &mut ScratchpadVec<'_, K>, set: &HashSet<K, S>)
where
    K: Copy + Verifiable,
{
    output.extend(set.iter().copied());
}

/// Pushes the elements of `a` which aren't in `b` onto `output`, which is empty.
#[trusted]
#[requires(output.model().len() == 0)]
#[ensures(no_dup(ids((^output).model())))]
#[ensures(forall<k> ids((^output).model()).contains(k)
    == (a.model().contains(&k) && !b.model().contains(&k)))]
pub(super) fn extend_difference<K, S>(
    output: &mut ScratchpadVec<'_, K>,
    a: &HashSet<K, S>,
    b: &ScratchpadSet<'_, K, S>,
) where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    output.extend(a.iter().filter(|id| !b.contains(*id)).copied());
}

/// Appends `items` to `output`.
#[trusted]
#[ensures(ids((^output)@) == ids(output@).concat(ids(items.model())))]
pub(super) fn extend_from_scratch<K>(output: &mut Vec<K>, items: &ScratchpadVec<'_, K>)
where
    K: Copy + Verifiable,
{
    output.extend_from_slice(items);
}

/// Pushes the keys of `map` onto `output`, which is empty.
#[trusted]
#[requires(output.model().len() == 0)]
#[ensures(no_dup(ids((^output).model())))]
#[ensures(forall<k> ids((^output).model()).contains(k) == map.model().contains(&k))]
pub(super) fn extend_keys<K, S>(
    output: &mut ScratchpadVec<'_, K>,
    map: &ScratchpadMap<'_, K, usize, S>,
) where
    K: Copy + Verifiable,
{
    output.extend(map.keys().copied());
}

/// Pushes the keys of `map` with a non-zero value onto `output`, which is empty.
#[trusted]
#[requires(output.model().len() == 0)]
#[ensures(no_dup(ids((^output).model())))]
#[ensures(forall<k> ids((^output).model()).contains(k)
    == (map.model().contains(&k) && map.model()[k]@ > 0))]
pub(super) fn extend_surviving_keys<K, S>(
    output: &mut ScratchpadVec<'_, K>,
    map: &ScratchpadMap<'_, K, usize, S>,
) where
    K: Copy + Verifiable,
{
    output.extend(
        map.iter()
            .filter(|(_, remaining)| **remaining > 0)
            .map(|(child, _)| *child),
    );
}

/// Checks whether `map` has an active key with a non-zero value.
#[trusted]
#[ensures(result == exists<k> map.model().contains(&k) && map.model()[k]@ > 0
    && active.model().contains(&k))]
pub(super) fn any_active_survivor<K, S>(
    map: &ScratchpadMap<'_, K, usize, S>,
    active: &HashSet<K, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    map.iter()
        .any(|(child, remaining)| *remaining > 0 && active.contains(child))
}

/// Collects `slice` into a set, which keeps the first occurrence of each identifier, in order.
#[ensures(result.model() == dedup(ids(slice@)))]
pub(super) fn index_set_from_slice<K, S>(slice: &[K]) -> IndexSet<K, S>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    let mut set = IndexSet::with_capacity_and_hasher(slice.len(), S::default());
    let mut i = 0;

    #[invariant(i@ <= slice@.len())]
    #[invariant(set.model() == dedup(ids(slice@).subsequence(0, i@)))]
    #[variant(slice@.len() - i@)]
    while i < slice.len() {
        proof_assert! { lemma_dedup_prefix(ids(slice@), i@); true };
        set.insert(slice[i]);
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            i += 1;
        }
    }

    proof_assert!(ids(slice@).subsequence(0, slice@.len()).ext_eq(ids(slice@)));
    set
}

/// Pushes `Enter` steps for the identifiers in `set` onto `stack`, in reverse order.
#[trusted]
#[check(terminates)]
#[ensures(forall<i> 0 <= i && i < stack.model().len() ==> (^stack).model()[i] == stack.model()[i])]
#[ensures((^stack).model().len() == stack.model().len() + set.model().len())]
#[ensures(forall<i> 0 <= i && i < set.model().len()
    ==> match (^stack).model()[stack.model().len() + i] {
        Step::Enter(k) => k.deep_model() == set.model()[set.model().len() - 1 - i],
        Step::Exit(_) => false,
    })]
fn extend_steps_rev<K, S>(stack: &mut ScratchpadVec<'_, Step<K, K>>, set: &IndexSet<K, S>)
where
    K: Copy + Verifiable,
{
    stack.extend(set.iter().copied().rev().map(Step::Enter));
}

/// Sorts the identifiers in `set` by comparing their nodes with `cmp`.
#[trusted]
#[requires(forall<i> 0 <= i && i < set.model().len() ==> nodes.model().contains(&set.model()[i]))]
#[requires(forall<f> (*cmp).hist_inv(f)
    ==> forall<a: &IndependentNode<K, T, S>, b: &IndependentNode<K, T, S>>
        inv(a) && inv(b) ==> f.precondition((a, b)))]
#[ensures((^set).model().len() == set.model().len())]
#[ensures(crate::verification::same_elements((^set).model(), set.model()))]
#[ensures(forall<le: Mapping<IndependentNode<K, T, S>, Mapping<IndependentNode<K, T, S>, bool>>>
    total_preorder(le) && compares_by(*cmp, identity(), le)
    ==> sorted_stably((^set).model(), set.model(), order_by(le, value_of(nodes.model()))))]
pub(super) fn sort_by_nodes<K, T, S, F>(
    set: &mut IndexSet<K, S>,
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    scratchpad: &mut Scratchpad,
    cmp: &mut F,
) where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
    F: FnMut(&IndependentNode<K, T, S>, &IndependentNode<K, T, S>) -> Ordering,
{
    if set.len() > 20 {
        let guard = scratchpad.guard();
        let mut sorted = guard
            .arena()
            .alloc_iter_exact(set.drain(..).map(|id| &nodes[&id]));
        sorted.sort_by(|a, b| cmp(*a, *b));

        set.extend(sorted.into_iter().map(|node| node.id));
    } else {
        set.sort_by(|a, b| cmp(&nodes[a], &nodes[b]));
    }
}
