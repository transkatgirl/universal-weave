//! Topological sorts (Kahn's algorithm).

use alloc::vec::Vec;
use core::hash::{BuildHasher, Hash};

use creusot_std::macros::*;
use hashbrown::{HashMap, HashSet, hash_map::Entry};
use indexmap::IndexSet;
use scratchpads::{ScratchpadMap, ScratchpadSet, ScratchpadVec, allocator_api2::alloc::Allocator};

#[cfg(creusot)]
use creusot_std::prelude::*;

use super::{Predicate, count_parents, extend_from_set_rev};
use crate::{IndependentContents, Verifiable, independent::IndependentNode};

#[cfg(creusot)]
use crate::independent::verification::*;

#[cfg(creusot)]
use crate::{
    dependent::verification::lemma_no_dup_len,
    verification::{
        Id, Model, ids, lemma_concat_snoc, lemma_full_prefix, lemma_ids_empty, lemma_ids_singleton,
        lemma_ids_snoc, lemma_reverse, lemma_snoc_contains,
    },
};

/// Outputs every node, each after its parents (Kahn's algorithm), starting from `roots`.
#[check(terminates)]
#[requires(all_linked(nodes.model()) && acyclic(nodes.model()))]
#[requires(forall<k> roots.model().contains(k)
    == (nodes.model().contains(&k) && nodes.model()[k]@.from.len() == 0))]
#[requires(stack.model().len() == 0)]
#[requires(output@.len() == 0)]
#[requires(identifier_map.model().len() == 0)]
#[ensures((^output)@.len() == nodes.model().len())]
#[ensures(topological(nodes.model(), ids((^output)@)))]
#[ensures(ids((^output)@) == kahn_sorted(nodes.model(), roots.model()))]
#[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
pub(in crate::independent) fn topological_sort<K, T, S>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    roots: &IndexSet<K, S>,
    stack: &mut ScratchpadVec<'_, K>,
    output: &mut Vec<K>,
    identifier_map: &mut ScratchpadMap<'_, K, usize, S>,
) where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    snapshot! { lemma_ids_snoc::<K> };
    count_parents(identifier_map, nodes);

    proof! {
        let empty = snapshot!(stack.model());
    }
    extend_from_set_rev(stack, roots);
    proof_assert! {
        lemma_ids_empty(*empty);
        lemma_ids_empty(output@);
        lemma_reverse(roots.model());
        lemma_kahn_start(nodes.model(), roots.model().reverse(), identifier_map.model());
        kahn_outer(nodes.model(), admit_all(), ids(output@), ids(stack.model()), identifier_map.model())
    };

    proof_assert! { lemma_no_dup_len(ids(output@), nodes.model()); output@.len() <= nodes.model().len() };

    #[invariant(kahn_outer(nodes.model(), admit_all(), ids(output@), ids(stack.model()), identifier_map.model()))]
    #[invariant(forall<k> nodes.model().contains(&k) ==> identifier_map.model().contains(&k))]
    #[invariant(output@.len() <= nodes.model().len())]
    #[variant(nodes.model().len() - output@.len())]
    #[invariant(kahn_run(nodes.model(), admit_all(), nodes.model().len() + 1 - output@.len(), ids(output@), ids(stack.model()))
        == kahn_sorted(nodes.model(), roots.model()))]
    while let Some(id) = stack.pop() {
        proof! {
            let prev = snapshot!(ids(output@));
            let rest = snapshot!(ids(stack.model()));
        }
        proof_assert! {
            lemma_kahn_run_step(nodes.model(), admit_all(), nodes.model().len() + 1 - output@.len(), *prev, *rest, id.deep_model());
            kahn_run(
                nodes.model(),
                admit_all(),
                nodes.model().len() + 1 - output@.len() - 1,
                prev.snoc(id.deep_model()),
                rest.concat(ready(
                    nodes.model(),
                    admit_all(),
                    prev.snoc(id.deep_model()),
                    nodes.model()[id.deep_model()]@.to.reverse(),
                )),
            ) == kahn_sorted(nodes.model(), roots.model())
        };
        output.push(id);
        proof_assert! {
            lemma_kahn_pop(nodes.model(), admit_all(), *prev, *rest, id.deep_model(), identifier_map.model());
            kahn_inner(
                nodes.model(),
                admit_all(),
                *prev,
                id.deep_model(),
                nodes.model()[id.deep_model()]@.to,
                *rest,
                identifier_map.model(),
            )
        };

        let to = &nodes[&id].to;
        let mut i = to.len();
        proof_assert! { lemma_full_prefix(to.model()); to.model().subsequence(0, i@) == to.model() };
        proof_assert! {
            lemma_ready_ends(nodes.model(), admit_all(), prev.snoc(id.deep_model()), to.model());
            rest.concat_empty();
            ids(stack.model()) == rest.concat(ready(
                nodes.model(),
                admit_all(),
                prev.snoc(id.deep_model()),
                to.model().subsequence(i@, to.model().len()).reverse(),
            ))
        };
        #[invariant(i@ <= to.model().len())]
        #[invariant(to.model() == nodes.model()[id.deep_model()]@.to)]
        #[invariant(ids(output@) == prev.snoc(id.deep_model()))]
        #[invariant(kahn_inner(
            nodes.model(),
            admit_all(),
            *prev,
            id.deep_model(),
            to.model().subsequence(0, i@),
            ids(stack.model()),
            identifier_map.model(),
        ))]
        #[invariant(forall<k> nodes.model().contains(&k) ==> identifier_map.model().contains(&k))]
        #[invariant(ids(stack.model()) == rest.concat(ready(
            nodes.model(),
            admit_all(),
            prev.snoc(id.deep_model()),
            to.model().subsequence(i@, to.model().len()).reverse(),
        )))]
        #[variant(i@)]
        while i > 0 {
            i -= 1;
            let child = to[i];
            proof! {
                let counts = snapshot!(identifier_map.model());
                let st = snapshot!(ids(stack.model()));
            }
            proof_assert! {
                lemma_kahn_count(nodes.model(), admit_all(), *prev, id.deep_model(), i@, *st, *counts);
                counts.contains(&child.deep_model())
                    && 1 <= counts[child.deep_model()]@
                    && counts[child.deep_model()]@ == pending_parents(
                        nodes.model(),
                        admit_all(),
                        prev.snoc(id.deep_model()),
                        child.deep_model(),
                    ) + 1
            };

            let remaining = identifier_map.get_mut(&child).unwrap();
            *remaining -= 1;

            if *remaining == 0 {
                stack.push(child);
            }
            proof_assert! {
                lemma_ready_step(nodes.model(), admit_all(), prev.snoc(id.deep_model()), to.model(), i@ + 1);
                lemma_concat_snoc(
                    *rest,
                    ready(
                        nodes.model(),
                        admit_all(),
                        prev.snoc(id.deep_model()),
                        to.model().subsequence(i@ + 1, to.model().len()).reverse(),
                    ),
                    child.deep_model(),
                );
                ids(stack.model()) == rest.concat(ready(
                    nodes.model(),
                    admit_all(),
                    prev.snoc(id.deep_model()),
                    to.model().subsequence(i@, to.model().len()).reverse(),
                ))
            };
            proof_assert! {
                lemma_kahn_visit(
                    nodes.model(),
                    admit_all(),
                    *prev,
                    id.deep_model(),
                    i@,
                    *st,
                    *counts,
                    identifier_map.model()[child.deep_model()],
                );
                kahn_inner(
                    nodes.model(),
                    admit_all(),
                    *prev,
                    id.deep_model(),
                    to.model().subsequence(0, i@),
                    ids(stack.model()),
                    identifier_map.model(),
                )
            };
        }
        proof_assert! {
            lemma_ready_ends(nodes.model(), admit_all(), prev.snoc(id.deep_model()), to.model());
            ids(stack.model()) == rest.concat(ready(
                nodes.model(),
                admit_all(),
                prev.snoc(id.deep_model()),
                nodes.model()[id.deep_model()]@.to.reverse(),
            ))
        };
        proof_assert! {
            lemma_kahn_next(nodes.model(), admit_all(), *prev, id.deep_model(), ids(stack.model()), identifier_map.model());
            kahn_outer(nodes.model(), admit_all(), ids(output@), ids(stack.model()), identifier_map.model())
        };
        proof_assert! { lemma_no_dup_len(ids(output@), nodes.model()); output@.len() <= nodes.model().len() };
    }
    proof_assert! {
        lemma_ids_empty(stack.model());
        lemma_kahn_sorted_all(nodes.model(), ids(output@), identifier_map.model());
        ids(output@).len() == nodes.model().len() && topological(nodes.model(), ids(output@))
    };
    proof_assert! {
        lemma_kahn_run_done(nodes.model(), admit_all(), nodes.model().len() + 1 - output@.len(), ids(output@));
        ids(output@) == kahn_sorted(nodes.model(), roots.model())
    };
}

/// Outputs the nodes in `filter` (and, if given, in `within`) which can be reached from
/// `subgraph_root` through such nodes, each after its accepted parents, continuing a topological
/// sort of the accepted nodes (Kahn's algorithm) whose state is `output` and `identifier_map`.
#[check(terminates)]
#[requires(all_linked(nodes.model()) && acyclic(nodes.model()))]
#[requires(forall<k> admitted[k] == (filter.model().contains(&k)
    && match within { Some(within) => within.model().contains(&k), None => true }))]
#[requires(nodes.model().contains(&subgraph_root.deep_model()))]
#[requires(admitted[subgraph_root.deep_model()])]
#[requires(forall<p> !admitted_parent(nodes.model(), *admitted, subgraph_root.deep_model(), p))]
#[requires(!ids(output.model()).contains(subgraph_root.deep_model()))]
#[requires(stack.model().len() == 0)]
#[requires(kahn_state(nodes.model(), *admitted, ids(output.model()), identifier_map.model()))]
#[ensures((^stack).model().len() == 0)]
#[ensures(kahn_state(nodes.model(), *admitted, ids((^output).model()), (^identifier_map).model()))]
#[ensures(output.model().len() < (^output).model().len())]
#[ensures(forall<i> 0 <= i && i < output.model().len() ==> (^output).model()[i] == output.model()[i])]
#[ensures((^output).model()[output.model().len()].deep_model() == subgraph_root.deep_model())]
#[ensures(ids((^output).model()) == kahn_run(
    nodes.model(),
    *admitted,
    nodes.model().len() + 1 - output.model().len(),
    ids(output.model()),
    Seq::singleton(subgraph_root.deep_model()),
))]
#[ensures(forall<i> output.model().len() < i && i < (^output).model().len()
    ==> exists<p> admitted_parent(nodes.model(), *admitted, (^output).model()[i].deep_model(), p))]
#[allow(
    clippy::arithmetic_side_effects,
    reason = "Can never overflow or underflow"
)]
#[allow(clippy::too_many_lines, reason = "Most of its length is proof code")]
#[allow(
    clippy::too_many_arguments,
    reason = "The filter is a set and an optional set it must also be in, as closures can't be proven to end"
)]
#[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
pub(in crate::independent) fn topological_sort_subgraph<K, T, S, A>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    filter: &HashSet<K, S, A>,
    within: Option<&ScratchpadSet<'_, K, S>>,
    admitted: Predicate<K>,
    subgraph_root: K,
    stack: &mut ScratchpadVec<'_, K>,
    output: &mut ScratchpadVec<'_, K>,
    identifier_map: &mut ScratchpadMap<'_, K, usize, S>,
) where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
    A: Allocator,
{
    snapshot! { lemma_ids_snoc::<K> };
    proof! {
        let out0 = snapshot!(output.model());
        let empty = snapshot!(stack.model());
    }
    stack.push(subgraph_root);
    proof_assert! {
        lemma_ids_singleton(*empty, subgraph_root);
        lemma_kahn_resume(
            nodes.model(),
            *admitted,
            ids(output.model()),
            identifier_map.model(),
            subgraph_root.deep_model(),
        );
        kahn_outer(nodes.model(), *admitted, ids(output.model()), ids(stack.model()), identifier_map.model())
            && kahn_counted(nodes.model(), *admitted, ids(output.model()), identifier_map.model())
    };

    proof_assert! {
        lemma_no_dup_len(ids(output.model()), nodes.model());
        output.model().len() <= nodes.model().len()
    };

    #[invariant(kahn_outer(nodes.model(), *admitted, ids(output.model()), ids(stack.model()), identifier_map.model()))]
    #[invariant(kahn_counted(nodes.model(), *admitted, ids(output.model()), identifier_map.model()))]
    #[invariant(out0.len() <= output.model().len())]
    #[invariant(forall<i> 0 <= i && i < out0.len() ==> output.model()[i] == out0[i])]
    #[invariant((output.model().len() == out0.len()
            && ids(stack.model()) == Seq::singleton(subgraph_root.deep_model()))
        || (out0.len() < output.model().len()
            && output.model()[out0.len()].deep_model() == subgraph_root.deep_model()
            && forall<k> ids(stack.model()).contains(k)
                ==> exists<p> admitted_parent(nodes.model(), *admitted, k, p)))]
    #[invariant(forall<i> #[trigger(output.model()[i])] out0.len() < i && i < output.model().len()
        ==> exists<p> admitted_parent(nodes.model(), *admitted, output.model()[i].deep_model(), p))]
    #[invariant(output.model().len() <= nodes.model().len())]
    #[variant(nodes.model().len() - output.model().len())]
    #[invariant(kahn_run(nodes.model(), *admitted, nodes.model().len() + 1 - output.model().len(), ids(output.model()), ids(stack.model()))
        == kahn_run(nodes.model(), *admitted, nodes.model().len() + 1 - out0.len(), ids(*out0), Seq::singleton(subgraph_root.deep_model())))]
    while let Some(id) = stack.pop() {
        proof! {
            let prev = snapshot!(ids(output.model()));
            let rest = snapshot!(ids(stack.model()));
        }
        proof_assert! {
            lemma_kahn_run_step(nodes.model(), *admitted, nodes.model().len() + 1 - output.model().len(), *prev, *rest, id.deep_model());
            kahn_run(
                nodes.model(),
                *admitted,
                nodes.model().len() + 1 - output.model().len() - 1,
                prev.snoc(id.deep_model()),
                rest.concat(ready(
                    nodes.model(),
                    *admitted,
                    prev.snoc(id.deep_model()),
                    nodes.model()[id.deep_model()]@.to.reverse(),
                )),
            ) == kahn_run(nodes.model(), *admitted, nodes.model().len() + 1 - out0.len(), ids(*out0), Seq::singleton(subgraph_root.deep_model()))
        };
        proof_assert! {
            lemma_snoc_contains(*rest, id.deep_model(), id.deep_model());
            rest.snoc(id.deep_model()).contains(id.deep_model())
        };
        proof_assert! {
            forall<k> rest.contains(k) ==> exists<p> admitted_parent(nodes.model(), *admitted, k, p)
        };
        output.push(id);
        proof_assert! {
            lemma_kahn_pop(nodes.model(), *admitted, *prev, *rest, id.deep_model(), identifier_map.model());
            kahn_inner(
                nodes.model(),
                *admitted,
                *prev,
                id.deep_model(),
                nodes.model()[id.deep_model()]@.to,
                *rest,
                identifier_map.model(),
            )
        };

        let to = &nodes[&id].to;
        let mut i = to.len();
        proof_assert! { lemma_full_prefix(to.model()); to.model().subsequence(0, i@) == to.model() };
        proof_assert! {
            lemma_ready_ends(nodes.model(), *admitted, prev.snoc(id.deep_model()), to.model());
            rest.concat_empty();
            ids(stack.model()) == rest.concat(ready(
                nodes.model(),
                *admitted,
                prev.snoc(id.deep_model()),
                to.model().subsequence(i@, to.model().len()).reverse(),
            ))
        };
        #[invariant(i@ <= to.model().len())]
        #[invariant(to.model() == nodes.model()[id.deep_model()]@.to)]
        #[invariant(ids(output.model()) == prev.snoc(id.deep_model()))]
        #[invariant(kahn_inner(
            nodes.model(),
            *admitted,
            *prev,
            id.deep_model(),
            to.model().subsequence(0, i@),
            ids(stack.model()),
            identifier_map.model(),
        ))]
        #[invariant(forall<k> ids(stack.model()).contains(k)
            ==> exists<p> admitted_parent(nodes.model(), *admitted, k, p))]
        #[invariant(ids(stack.model()) == rest.concat(ready(
            nodes.model(),
            *admitted,
            prev.snoc(id.deep_model()),
            to.model().subsequence(i@, to.model().len()).reverse(),
        )))]
        #[variant(i@)]
        while i > 0 {
            i -= 1;
            let child = to[i];
            proof! {
                let counts = snapshot!(identifier_map.model());
                let st = snapshot!(ids(stack.model()));
            }
            proof_assert! {
                !admitted[child.deep_model()] ==> {
                    lemma_kahn_skip(nodes.model(), *admitted, *prev, id.deep_model(), i@, *st, *counts);
                    kahn_inner(
                        nodes.model(),
                        *admitted,
                        *prev,
                        id.deep_model(),
                        to.model().subsequence(0, i@),
                        *st,
                        *counts,
                    )
                }
            };

            let accepted = filter.contains(&child)
                && match within {
                    Some(within) => within.contains(&child),
                    None => true,
                };
            if accepted {
                proof_assert! {
                    lemma_kahn_count(nodes.model(), *admitted, *prev, id.deep_model(), i@, *st, *counts);
                    nodes.model().contains(&child.deep_model())
                        && 0 <= pending_parents(
                            nodes.model(),
                            *admitted,
                            prev.snoc(id.deep_model()),
                            child.deep_model(),
                        )
                        && (counts.contains(&child.deep_model()) ==> counts[child.deep_model()]@
                            == pending_parents(
                                nodes.model(),
                                *admitted,
                                prev.snoc(id.deep_model()),
                                child.deep_model(),
                            ) + 1)
                        && (!counts.contains(&child.deep_model())
                            ==> count_where(nodes.model()[child.deep_model()]@.from, *admitted)
                                == pending_parents(
                                    nodes.model(),
                                    *admitted,
                                    prev.snoc(id.deep_model()),
                                    child.deep_model(),
                                ) + 1)
                };
                let remaining = match identifier_map.entry(child) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => {
                        entry.insert(count_admitted(&nodes[&child].from, filter, within, admitted))
                    }
                };
                *remaining -= 1;
                proof! {
                    let n = snapshot!(*remaining);
                }
                proof_assert! {
                    n@ == pending_parents(
                        nodes.model(),
                        *admitted,
                        prev.snoc(id.deep_model()),
                        child.deep_model(),
                    )
                };

                if *remaining == 0 {
                    stack.push(child);
                }
                proof_assert! { identifier_map.model() == counts.add(child.deep_model(), *n) };
                proof_assert! {
                    lemma_kahn_visit(
                        nodes.model(),
                        *admitted,
                        *prev,
                        id.deep_model(),
                        i@,
                        *st,
                        *counts,
                        *n,
                    );
                    kahn_inner(
                        nodes.model(),
                        *admitted,
                        *prev,
                        id.deep_model(),
                        to.model().subsequence(0, i@),
                        ids(stack.model()),
                        identifier_map.model(),
                    )
                };
            }
            proof_assert! {
                lemma_ready_step(nodes.model(), *admitted, prev.snoc(id.deep_model()), to.model(), i@ + 1);
                lemma_concat_snoc(
                    *rest,
                    ready(
                        nodes.model(),
                        *admitted,
                        prev.snoc(id.deep_model()),
                        to.model().subsequence(i@ + 1, to.model().len()).reverse(),
                    ),
                    child.deep_model(),
                );
                ids(stack.model()) == rest.concat(ready(
                    nodes.model(),
                    *admitted,
                    prev.snoc(id.deep_model()),
                    to.model().subsequence(i@, to.model().len()).reverse(),
                ))
            };
        }
        proof_assert! {
            lemma_ready_ends(nodes.model(), *admitted, prev.snoc(id.deep_model()), to.model());
            ids(stack.model()) == rest.concat(ready(
                nodes.model(),
                *admitted,
                prev.snoc(id.deep_model()),
                nodes.model()[id.deep_model()]@.to.reverse(),
            ))
        };
        proof_assert! {
            lemma_kahn_next(nodes.model(), *admitted, *prev, id.deep_model(), ids(stack.model()), identifier_map.model());
            kahn_outer(nodes.model(), *admitted, ids(output.model()), ids(stack.model()), identifier_map.model())
                && kahn_counted(nodes.model(), *admitted, ids(output.model()), identifier_map.model())
        };
        proof_assert! {
            lemma_no_dup_len(ids(output.model()), nodes.model());
            output.model().len() <= nodes.model().len()
        };
    }
    proof_assert! {
        lemma_ids_empty(stack.model());
        lemma_kahn_end(nodes.model(), *admitted, ids(output.model()), identifier_map.model());
        kahn_state(nodes.model(), *admitted, ids(output.model()), identifier_map.model())
    };
    proof_assert! {
        lemma_kahn_run_done(
            nodes.model(),
            *admitted,
            nodes.model().len() + 1 - output.model().len(),
            ids(output.model()),
        );
        ids(output.model()) == kahn_run(nodes.model(), *admitted, nodes.model().len() + 1 - out0.len(), ids(*out0), Seq::singleton(subgraph_root.deep_model()))
    };
}

/// Counts the identifiers in `from` which are in `filter` and, if given, in `within`.
#[check(terminates)]
#[requires(forall<k> admitted[k] == (filter.model().contains(&k)
    && match within { Some(within) => within.model().contains(&k), None => true }))]
#[ensures(result@ == count_where(from.model(), *admitted))]
#[cfg_attr(
    not(creusot),
    allow(unused_variables, reason = "Used by the specification")
)]
#[allow(clippy::option_if_let_else, reason = "Closures can't be proven to end")]
fn count_admitted<K, S, A>(
    from: &IndexSet<K, S>,
    filter: &HashSet<K, S, A>,
    within: Option<&ScratchpadSet<'_, K, S>>,
    admitted: Predicate<K>,
) -> usize
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    S: BuildHasher + Default + Clone,
    A: Allocator,
{
    snapshot! { lemma_count_prefix::<Id<K>> };
    snapshot! { crate::verification::lemma_full_prefix::<Id<K>> };
    let mut count = 0;
    let mut i = 0;

    #[invariant(i@ <= from.model().len())]
    #[invariant(count@ == count_where(from.model().subsequence(0, i@), *admitted))]
    #[invariant(count@ <= i@)]
    #[variant(from.model().len() - i@)]
    while i < from.len() {
        let accepted = filter.contains(&from[i])
            && match within {
                Some(within) => within.contains(&from[i]),
                None => true,
            };
        if accepted {
            #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
            {
                count += 1;
            }
        }
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            i += 1;
        }
    }

    count
}
