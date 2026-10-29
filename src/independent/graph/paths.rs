//! The longest path through some nodes to a root, and the shortest path to an ancestor.

use alloc::vec::Vec;
use core::hash::{BuildHasher, Hash};

use creusot_std::{logic::Mapping, macros::*, snapshot::Snapshot};
use hashbrown::{HashMap, HashSet, hash_map::Entry};
use indexmap::IndexSet;
use scratchpads::{ScratchpadMap, ScratchpadSet, ScratchpadVec};

#[cfg(creusot)]
use creusot_std::{invariant::inv, prelude::*};

use super::Predicate;
use crate::{IndependentContents, Verifiable, independent::IndependentNode, same_id};

#[cfg(creusot)]
use crate::independent::verification::*;

#[cfg(creusot)]
use crate::verification::{Id, Model, ids, no_dup};

/// Outputs, from its first node to a root, the longest path through the nodes of
/// `topological_order` which are in `candidates` and, if given, in `within`.
#[check(terminates)]
#[requires(all_linked(nodes.model()) && acyclic(nodes.model()))]
#[requires(nodes.model().len() <= usize::MAX@)]
#[requires(forall<i> 0 <= i && i < topological_order@.len()
    ==> nodes.model().contains(&topological_order@[i].deep_model()))]
#[requires(forall<k> candidate[k] == (candidates.model().contains(&k)
    && match within { Some(within) => within.model().contains(&k), None => true }))]
#[requires(no_dup(ids(topological_order@)))]
#[requires(candidates_ordered(nodes.model(), ids(topological_order@), *candidate))]
#[requires(scratchpad_map.model().len() == 0)]
#[requires(reversed_path.model().len() == 0)]
#[ensures(valid_path(nodes.model(), ids((^reversed_path).model())))]
#[ensures(no_dup(ids((^reversed_path).model())))]
#[ensures(forall<i> 0 <= i && i < (^reversed_path).model().len()
    ==> candidate[ids((^reversed_path).model())[i]]
        && ids(topological_order@).contains(ids((^reversed_path).model())[i]))]
#[ensures(forall<chain> candidate_chain(nodes.model(), ids(topological_order@), *candidate, chain)
    ==> ids((^reversed_path).model()) == chain)]
#[ensures(forall<q> candidate_root_path(nodes.model(), ids(topological_order@), *candidate, q)
    ==> q.len() <= (^reversed_path).model().len())]
#[cfg_attr(
    not(creusot),
    allow(unused_variables, reason = "Used by the specification")
)]
#[allow(clippy::too_many_lines, reason = "Most of its length is proof code")]
#[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
pub(in crate::independent) fn longest_candidate_path_to_root<K, T, S>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    topological_order: &[K],
    candidates: &HashSet<K, S>,
    within: Option<&ScratchpadSet<'_, K, S>>,
    candidate: Predicate<K>,
    scratchpad_map: &mut ScratchpadMap<'_, K, (usize, K), S>,
    reversed_path: &mut ScratchpadVec<'_, K>,
) where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    snapshot! { crate::verification::lemma_ids_snoc::<K> };
    proof! {
        let order = snapshot!(ids(topological_order@));
        let rank = snapshot!(rank_of(nodes.model()));
    }
    proof_assert!(scratchpad_map.model().is_empty_logic());
    proof_assert!(forall<k> !scratchpad_map.model().contains(&k));
    proof_assert! {
        lemma_chain_init(nodes.model(), *order, *candidate, scratchpad_map.model());
        chain_invariant(
            nodes.model(),
            *order,
            *candidate,
            scratchpad_map.model(),
            None::<(usize, K)>,
            0,
        )
    };
    let mut longest_distance = None;
    proof! {
        // Fixes the type of `longest_distance` for the invariants below.
        let _: Snapshot<Option<(usize, K)>> = snapshot!(longest_distance);
    }
    proof_assert! {
        lemma_distance_init(nodes.model(), *order, *candidate, scratchpad_map.model());
        distance_bound(nodes.model(), *order, *candidate, scratchpad_map.model(), 0)
    };

    let mut i = 0;

    #[invariant(i@ <= topological_order@.len())]
    #[invariant(reversed_path.model().len() == 0)]
    #[invariant(parent_pointers(nodes.model(), *order, *candidate, scratchpad_map.model()))]
    #[invariant(forall<k> scratchpad_map.model().contains(&k)
        ==> scratchpad_map.model()[k].0@ < i@)]
    #[invariant(match longest_distance {
        None => true,
        Some((_, k)) => scratchpad_map.model().contains(&k.deep_model()),
    })]
    #[invariant(chain_invariant(
        nodes.model(),
        *order,
        *candidate,
        scratchpad_map.model(),
        longest_distance,
        i@,
    ))]
    #[invariant(forall<k> scratchpad_map.model().contains(&k)
        ==> order.subsequence(0, i@).contains(k))]
    #[invariant(exact_distances(scratchpad_map.model()))]
    #[invariant(longest_recorded(scratchpad_map.model(), longest_distance))]
    #[invariant(distance_bound(nodes.model(), *order, *candidate, scratchpad_map.model(), i@))]
    #[variant(topological_order@.len() - i@)]
    while i < topological_order.len() {
        let id = &topological_order[i];
        proof! {
            let n = snapshot!(i@);
            let old_map = snapshot!(scratchpad_map.model());
            let old_longest = snapshot!(longest_distance);
        }
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            i += 1;
        }
        proof_assert!(0 <= *n && *n < order.len() && i@ <= topological_order@.len());
        proof_assert!(*id == topological_order@[*n] && id.deep_model() == order[*n]);
        proof_assert!(forall<x> order.subsequence(0, *n + 1).contains(x)
            == (order.subsequence(0, *n).contains(x) || x == order[*n]) && {
                crate::verification::lemma_prefix_contains(*order, *n, x);
                true
            });
        proof_assert!(!old_map.contains(&order[*n]));
        let accepted = candidates.contains(id)
            && match within {
                Some(within) => within.contains(id),
                None => true,
            };
        if !accepted {
            proof_assert! {
                lemma_chain_skip(nodes.model(), *order, *candidate, *old_map, *old_longest, *n);
                chain_invariant(nodes.model(), *order, *candidate, *old_map, *old_longest, *n + 1)
            };
            proof_assert! {
                lemma_distance_skip(nodes.model(), *order, *candidate, *old_map, *n);
                distance_bound(nodes.model(), *order, *candidate, *old_map, *n + 1)
            };
            continue;
        }

        let from = &nodes[id].from;
        proof_assert!(linked(nodes.model(), id.deep_model()));

        let has_parents = !from.is_empty();
        let best_parent = best_parent(from, scratchpad_map);

        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        let distance = match best_parent {
            Some((parent_distance, parent)) => Some((parent_distance + 1, parent)),
            None => {
                if has_parents {
                    None
                } else {
                    Some((0, *id))
                }
            }
        };

        if let Some((distance, parent)) = distance {
            proof_assert!(recorded_step(
                nodes.model(),
                *old_map,
                order[*n],
                distance,
                parent
            ));
            proof_assert!(parent.deep_model() != order[*n] ==> old_map.contains(&parent.deep_model())
                && distance@ == old_map[parent.deep_model()].0@ + 1);
            proof_assert!(parent.deep_model() == order[*n] ==> distance@ == 0);
            proof_assert! {
                lemma_distance_record(nodes.model(), *order, *candidate, *old_map, *n, distance, parent);
                distance_bound(nodes.model(), *order, *candidate, old_map.add(order[*n], (distance, parent)), *n + 1)
            };
            scratchpad_map.insert(*id, (distance, parent));

            let longer = match longest_distance {
                Some((value, _)) => distance > value,
                None => true,
            };
            if longer {
                longest_distance = Some((distance, *id));
            }
            proof_assert!(longest_distance == longest_after(*old_longest, distance, *id));
            proof_assert! {
                lemma_longest_record(*old_map, *old_longest, *id, distance, parent);
                longest_recorded(scratchpad_map.model(), longest_distance)
            };
            proof_assert! {
                lemma_exact_record(*old_map, order[*n], distance, parent);
                exact_distances(scratchpad_map.model())
            };
            proof_assert! {
                lemma_chain_record(
                    nodes.model(),
                    *order,
                    *candidate,
                    *old_map,
                    *old_longest,
                    *n,
                    *id,
                    distance,
                    parent,
                );
                chain_invariant(
                    nodes.model(),
                    *order,
                    *candidate,
                    scratchpad_map.model(),
                    longest_distance,
                    *n + 1,
                )
            };
        }
        proof_assert! {
            lemma_chain_unrecorded(nodes.model(), *order, *candidate, *old_map, *old_longest, *n);
            chain_invariant(
                nodes.model(),
                *order,
                *candidate,
                scratchpad_map.model(),
                longest_distance,
                *n + 1,
            )
        };
        proof_assert! {
            lemma_distance_unrecorded(nodes.model(), *order, *candidate, *old_map, *n);
            distance_bound(nodes.model(), *order, *candidate, scratchpad_map.model(), *n + 1)
        };
    }

    proof! {
        let final_map = snapshot!(scratchpad_map.model());
        let longest = snapshot!(longest_distance);
    }
    proof_assert!(order.len() == topological_order@.len());
    proof_assert!(distance_bound(nodes.model(), *order, *candidate, *final_map, order.len()));
    proof_assert!(longest_recorded(*final_map, *longest));

    proof_assert!(parent_pointers(
        nodes.model(),
        *order,
        *candidate,
        *final_map
    ));
    proof_assert!(chain_invariant(
        nodes.model(),
        *order,
        *candidate,
        *final_map,
        longest_distance,
        order.len(),
    ));
    proof_assert!(forall<chain> candidate_chain(nodes.model(), *order, *candidate, chain) ==> {
        lemma_chain_done(nodes.model(), *order, *candidate, chain, *final_map, longest_distance);
        (forall<j> 0 <= j && j < chain.len() ==> final_map.contains(&chain[j])
            && final_map[chain[j]].1.deep_model()
                == if j + 1 == chain.len() { chain[j] } else { chain[j + 1] })
            && match longest_distance {
                None => chain.len() == 0,
                Some((_, k)) => 0 < chain.len() && k.deep_model() == chain[0],
            }
    });

    if let Some((_, mut id)) = longest_distance {
        #[invariant(scratchpad_map.model() == *final_map)]
        #[invariant(final_map.contains(&id.deep_model()))]
        #[invariant(forall<i> 0 <= i && i < reversed_path.model().len()
            ==> final_map.contains(&ids(reversed_path.model())[i]))]
        #[invariant(forall<i> 0 <= i && i + 1 < reversed_path.model().len()
            ==> nodes.model()[ids(reversed_path.model())[i]]@.from
                .contains(ids(reversed_path.model())[i + 1]))]
        #[invariant(0 < reversed_path.model().len()
            ==> nodes.model()[ids(reversed_path.model())[reversed_path.model().len() - 1]]@.from
                .contains(id.deep_model()))]
        #[invariant(forall<i, j> 0 <= i && i < j && j < reversed_path.model().len()
            ==> rank[ids(reversed_path.model())[j]] < rank[ids(reversed_path.model())[i]])]
        #[invariant(forall<i> 0 <= i && i < reversed_path.model().len()
            ==> rank[id.deep_model()] < rank[ids(reversed_path.model())[i]])]
        #[invariant(forall<chain> candidate_chain(nodes.model(), *order, *candidate, chain)
            ==> reversed_path.model().len() < chain.len()
                && (forall<i> 0 <= i && i < reversed_path.model().len()
                    ==> ids(reversed_path.model())[i] == chain[i])
                && id.deep_model() == chain[reversed_path.model().len()])]
        #[invariant(match *longest {
            None => false,
            Some((d, _)) => reversed_path.model().len() + final_map[id.deep_model()].0@ == d@,
        })]
        #[variant(final_map[id.deep_model()].0@)]
        loop {
            reversed_path.push(id);

            let parent = scratchpad_map[&id].1;
            if same_id(&parent, &id) {
                break;
            }
            id = parent;
        }
    }
    proof_assert!(forall<i> 0 <= i && i < reversed_path.model().len()
        ==> final_map.contains(&ids(reversed_path.model())[i]));
    proof_assert!(0 < reversed_path.model().len()
        ==> nodes.model()[ids(reversed_path.model())[reversed_path.model().len() - 1]]@.from.len()
            == 0);
    proof_assert!(valid_path(nodes.model(), ids(reversed_path.model())));
    proof_assert!(forall<chain> candidate_chain(nodes.model(), *order, *candidate, chain)
        ==> ids(reversed_path.model()).ext_eq(chain));
    proof_assert!(match *longest {
        None => reversed_path.model().len() == 0,
        Some((d, _)) => reversed_path.model().len() == d@ + 1,
    });
    proof_assert!(longest_done_ok(
        nodes.model(),
        *order,
        *candidate,
        *final_map,
        *longest,
        reversed_path.model().len()
    ));
}

/// The first parent in `from` with the greatest distance in `distances`, and its distance.
#[check(terminates)]
#[ensures(match result {
    Some((d, p)) => from.model().contains(p.deep_model())
        && distances.model().lookup(p.deep_model()) != None
        && distances.model()[p.deep_model()].0 == d
        && forall<q> from.model().contains(q) && distances.model().contains(&q)
            ==> distances.model()[q].0@ <= d@,
    None => forall<q> from.model().contains(q) ==> !distances.model().contains(&q),
})]
fn best_parent<K, S>(
    from: &IndexSet<K, S>,
    distances: &ScratchpadMap<'_, K, (usize, K), S>,
) -> Option<(usize, K)>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
{
    let mut best: Option<(usize, K)> = None;
    let mut i = 0;

    #[invariant(i@ <= from.model().len())]
    #[invariant(match best {
        Some((d, p)) => from.model().contains(p.deep_model())
            && distances.model().lookup(p.deep_model()) != None
            && distances.model()[p.deep_model()].0 == d
            && forall<j> 0 <= j && j < i@ && distances.model().contains(&from.model()[j])
                ==> distances.model()[from.model()[j]].0@ <= d@,
        None => forall<j> 0 <= j && j < i@ ==> !distances.model().contains(&from.model()[j]),
    })]
    #[variant(from.model().len() - i@)]
    while i < from.len() {
        let parent = from[i];
        if let Some(&(distance, _)) = distances.get(&parent)
            && match best {
                Some((value, _)) => distance > value,
                None => true,
            }
        {
            best = Some((distance, parent));
        }
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            i += 1;
        }
    }

    best
}

/// Finds a shortest path from `id` up to an ancestor accepted by `target`, listing the ancestor
/// first.
#[requires(all_linked(nodes.model()) && acyclic(nodes.model()))]
#[requires(nodes.model().contains(&id.deep_model()))]
#[requires(forall<node: &IndependentNode<K, T, S>> inv(node) ==> target.precondition((node,)))]
#[requires(forall<node: &IndependentNode<K, T, S>, accept: bool> target.postcondition((node,), accept)
    ==> accept == is_target[*node])]
#[requires(scratchpad.model().len() == 0)]
#[requires(scratchpad_map.model().len() == 0)]
#[requires(path@.len() == 0)]
#[ensures(0 < (^path)@.len() ==> ids((^path)@)[(^path)@.len() - 1] == id.deep_model()
    && is_target[nodes.model()[ids((^path)@)[0]]]
    && no_dup(ids((^path)@))
    && (forall<i> 0 <= i && i < (^path)@.len() ==> nodes.model().contains(&ids((^path)@)[i]))
    && forall<i> 0 <= i && i + 1 < (^path)@.len()
        ==> nodes.model()[ids((^path)@)[i + 1]]@.from.contains(ids((^path)@)[i]))]
#[ensures((^path)@.len() == 0 ==> ids((^scratchpad).model()).contains(id.deep_model())
    && (forall<k> ids((^scratchpad).model()).contains(k) ==> nodes.model().contains(&k)
        && !is_target[nodes.model()[k]])
    && forall<k, p> ids((^scratchpad).model()).contains(k) && nodes.model()[k]@.from.contains(p)
        ==> ids((^scratchpad).model()).contains(p))]
#[ensures(0 < (^path)@.len() ==> forall<q> descending_to(nodes.model(), q, id.deep_model())
    && is_target[nodes.model()[q[0]]] ==> (^path)@.len() <= q.len())]
#[cfg_attr(
    not(creusot),
    allow(unused_variables, reason = "Used by the specification")
)]
pub(in crate::independent) fn shortest_path_to_ancestor<K, T, S>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    id: &K,
    target: impl Fn(&IndependentNode<K, T, S>) -> bool,
    is_target: Snapshot<Mapping<IndependentNode<K, T, S>, bool>>,
    scratchpad: &mut ScratchpadVec<'_, K>,
    scratchpad_map: &mut ScratchpadMap<'_, K, K, S>,
    path: &mut Vec<K>,
) where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    snapshot! { crate::verification::lemma_ids_snoc::<K> };
    proof! {
        let rank = snapshot!(rank_of(nodes.model()));
        let start = snapshot!(id.deep_model());
        let mut level: Snapshot<Mapping<Id<K>, Int>> = snapshot!(|_| 0);
        let mut found = snapshot!(0);
    }
    proof_assert!(scratchpad_map.model().is_empty_logic());
    proof_assert!(forall<k> !scratchpad_map.model().contains(&k));
    scratchpad.push(*id);
    scratchpad_map.insert(*id, *id);

    let mut head = 0;

    #[invariant(head@ <= scratchpad.model().len())]
    #[invariant(path@.len() == 0)]
    #[invariant(no_dup(ids(scratchpad.model())))]
    #[invariant(forall<k> scratchpad_map.model().contains(&k)
        == ids(scratchpad.model()).contains(k))]
    #[invariant(scratchpad_map.model().contains(&*start))]
    #[invariant(predecessors(nodes.model(), *start, scratchpad_map.model()))]
    #[invariant(forall<j> 0 <= j && j < head@
        ==> !is_target[nodes.model()[ids(scratchpad.model())[j]]]
            && forall<p> nodes.model()[ids(scratchpad.model())[j]]@.from.contains(p)
                ==> scratchpad_map.model().contains(&p))]
    #[invariant(bfs_found(*start, ids(scratchpad.model()), scratchpad_map.model(), *level, head@))]
    #[invariant(bfs_expanded(nodes.model(), ids(scratchpad.model()), scratchpad_map.model(), *level, head@))]
    while head < scratchpad.len() {
        let id = scratchpad[head];
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            head += 1;
        }
        proof_assert!(ids(scratchpad.model())[head@ - 1] == id.deep_model());
        proof_assert!(bfs_found(*start, ids(scratchpad.model()), scratchpad_map.model(), *level, head@));

        let node = &nodes[&id];

        if target(node) {
            proof_assert!(forall<q> descending_to(nodes.model(), q, *start)
                && is_target[nodes.model()[q[0]]] ==> {
                    lemma_bfs_shortest(
                        nodes.model(),
                        *start,
                        ids(scratchpad.model()),
                        scratchpad_map.model(),
                        *level,
                        head@ - 1,
                        *is_target,
                        q,
                    );
                    level[id.deep_model()] + 1 <= q.len()
                });
            proof! {
                found = snapshot!(level[id.deep_model()]);
            }
            path.push(id);
            break;
        }

        proof! {
            let old_queue = snapshot!(scratchpad.model());
            let old_map = snapshot!(scratchpad_map.model());
        }
        let mut i = 0;
        #[invariant(i@ <= node.from.model().len())]
        #[invariant(old_queue.len() <= scratchpad.model().len())]
        #[invariant(forall<j> 0 <= j && j < old_queue.len()
            ==> scratchpad.model()[j] == old_queue[j])]
        #[invariant(forall<k> old_map.contains(&k) ==> scratchpad_map.model().contains(&k))]
        #[invariant(no_dup(ids(scratchpad.model())))]
        #[invariant(forall<k> scratchpad_map.model().contains(&k)
            == ids(scratchpad.model()).contains(k))]
        #[invariant(predecessors(nodes.model(), *start, scratchpad_map.model()))]
        #[invariant(forall<j> 0 <= j && j < i@
            ==> scratchpad_map.model().contains(&node.from.model()[j]))]
        #[invariant(ids(scratchpad.model())[head@ - 1] == id.deep_model())]
        #[invariant(bfs_found(*start, ids(scratchpad.model()), scratchpad_map.model(), *level, head@))]
        #[invariant(bfs_expanded(nodes.model(), ids(scratchpad.model()), scratchpad_map.model(), *level,
            head@ - 1))]
        #[invariant(forall<j> 0 <= j && j < i@
            ==> level[node.from.model()[j]] <= level[id.deep_model()] + 1)]
        while i < node.from.len() {
            let parent = node.from[i];
            if let Entry::Vacant(entry) = scratchpad_map.entry(parent) {
                entry.insert(id);
                scratchpad.push(parent);
                proof! {
                    level = snapshot!(level.set(parent.deep_model(), level[id.deep_model()] + 1));
                }
            }
            proof_assert! {
                lemma_bfs_found_level(
                    *start,
                    ids(scratchpad.model()),
                    scratchpad_map.model(),
                    *level,
                    head@,
                    parent.deep_model(),
                );
                level[parent.deep_model()] <= level[id.deep_model()] + 1
            };
            #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
            {
                i += 1;
            }
        }
    }

    #[invariant(path@.len() == 0 ==> ids(scratchpad.model()).contains(*start)
        && (forall<k> ids(scratchpad.model()).contains(k) ==> nodes.model().contains(&k)
            && !is_target[nodes.model()[k]])
        && forall<k, p> ids(scratchpad.model()).contains(k) && nodes.model()[k]@.from.contains(p)
            ==> ids(scratchpad.model()).contains(p))]
    #[invariant(forall<k> scratchpad_map.model().contains(&k) && k != *start
        ==> scratchpad_map.model().contains(&scratchpad_map.model()[k].deep_model())
            && nodes.model()[scratchpad_map.model()[k].deep_model()]@.from.contains(k))]
    #[invariant(0 < path@.len() ==> is_target[nodes.model()[ids(path@)[0]]])]
    #[invariant(forall<i> 0 <= i && i < path@.len()
        ==> scratchpad_map.model().contains(&ids(path@)[i]))]
    #[invariant(forall<i> 0 <= i && i + 1 < path@.len()
        ==> nodes.model()[ids(path@)[i + 1]]@.from.contains(ids(path@)[i]))]
    #[invariant(forall<i, j> 0 <= i && i < j && j < path@.len()
        ==> rank[ids(path@)[i]] < rank[ids(path@)[j]])]
    #[invariant(level[*start] == 0)]
    #[invariant(forall<k> scratchpad_map.model().contains(&k) && k != *start
        ==> level[scratchpad_map.model()[k].deep_model()] + 1 == level[k])]
    #[invariant(0 < path@.len() ==> level[ids(path@)[path@.len() - 1]] + path@.len() == *found + 1)]
    #[invariant(0 < path@.len() ==> forall<q> descending_to(nodes.model(), q, *start)
        && is_target[nodes.model()[q[0]]] ==> *found + 1 <= q.len())]
    while let Some(last) = path.last()
        && last != id
    {
        proof_assert!(
            rank[last.deep_model()] < rank[scratchpad_map.model()[last.deep_model()].deep_model()]
        );
        path.push(scratchpad_map[last]);
    }
}
