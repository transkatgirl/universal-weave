//! Depth-first searches of a node's ancestors or descendants.

use core::hash::{BuildHasher, Hash};

use creusot_std::macros::*;
use hashbrown::HashMap;
use indexmap::IndexSet;
use scratchpads::{ScratchpadSet, ScratchpadVec};


use super::Predicate;
use crate::{IndependentContents, Verifiable, independent::IndependentNode};

#[cfg(creusot)]
use crate::independent::verification::*;

#[cfg(creusot)]
use crate::verification::{
    Id, Model, ids, lemma_ids_snoc, lemma_no_dup_snoc, lemma_set_in_keys_len, lemma_snoc_contains,
};

/// Adds `id` and its ancestors to `identifiers`, and those of them without parents to `roots`.
///
/// Elements already in `identifiers` are assumed to have been added along with their ancestors.
#[check(terminates)]
#[requires(all_linked(nodes.model()))]
#[requires(nodes.model().contains(&id.deep_model()))]
#[requires(stack.model().len() == 0)]
#[requires(forall<k> identifiers.model().contains(&k) ==> nodes.model().contains(&k))]
#[requires(parent_closed(nodes.model(), identifiers.model()))]
#[requires(roots_of(nodes.model(), identifiers.model(), ids(roots.model())))]
#[ensures((^stack).model().len() == 0)]
#[ensures((^identifiers).model().contains(&id.deep_model()))]
#[ensures(forall<k> identifiers.model().contains(&k) ==> (^identifiers).model().contains(&k))]
#[ensures(forall<k> (^identifiers).model().contains(&k) ==> nodes.model().contains(&k))]
#[ensures(parent_closed(nodes.model(), (^identifiers).model()))]
#[ensures(roots_of(nodes.model(), (^identifiers).model(), ids((^roots).model())))]
#[ensures(forall<k> (^identifiers).model().contains(&k) && !identifiers.model().contains(&k)
    && k != id.deep_model()
    ==> has_child_in(nodes.model(), (^identifiers).model().difference(identifiers.model()), k))]
pub(in crate::independent) fn ancestor_subgraph<K, T, S>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    id: K,
    stack: &mut ScratchpadVec<'_, K>,
    identifiers: &mut ScratchpadSet<'_, K, S>,
    roots: &mut ScratchpadVec<'_, K>,
) where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    snapshot! { lemma_ids_snoc::<K> };
    snapshot! { lemma_snoc_contains::<Id<K>> };
    snapshot! { lemma_no_dup_snoc::<Id<K>> };
    proof! {
        let id0 = snapshot!(id.deep_model());
        let old_set = snapshot!(identifiers.model());
    }

    if identifiers.insert(id) {
        stack.push(id);
    }
    proof_assert! {
        lemma_set_in_keys_len(identifiers.model(), nodes.model());
        identifiers.model().len() <= nodes.model().len()
    };

    #[invariant(ancestor_search(nodes.model(), *old_set, identifiers.model(), ids(stack.model()), *id0))]
    #[invariant(identifiers.model().contains(&*id0))]
    #[invariant(parent_closed_except(nodes.model(), identifiers.model(), ids(stack.model())))]
    #[invariant(roots_except(nodes.model(), identifiers.model(), ids(stack.model()), ids(roots.model())))]
    #[invariant(identifiers.model().len() <= nodes.model().len())]
    #[variant((nodes.model().len() - identifiers.model().len(), stack.model().len()))]
    while let Some(id) = stack.pop() {
        proof! {
            let x = snapshot!(id.deep_model());
            let size = snapshot!(identifiers.model().len());
            let depth = snapshot!(stack.model().len());
        }
        let from = &nodes[&id].from;

        if from.is_empty() {
            roots.push(id);
        } else {
            let mut i = from.len();
            #[invariant(i@ <= from.model().len())]
            #[invariant(*size <= identifiers.model().len())]
            #[invariant(identifiers.model().len() <= nodes.model().len())]
            #[invariant(stack.model().len() <= *depth + identifiers.model().len() - *size)]
            #[variant(i@)]
            #[invariant(ancestor_search(
                nodes.model(), *old_set, identifiers.model(), ids(stack.model()).snoc(*x), *id0))]
            #[invariant(identifiers.model().contains(&*id0))]
            #[invariant(parent_closed_except(
                nodes.model(), identifiers.model(), ids(stack.model()).snoc(*x)))]
            #[invariant(roots_except(
                nodes.model(), identifiers.model(), ids(stack.model()).snoc(*x), ids(roots.model())))]
            #[invariant(forall<j> i@ <= j && j < from.model().len()
                ==> identifiers.model().contains(&from.model()[j]))]
            while i > 0 {
                #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                {
                    i -= 1;
                }
                let parent = from[i];
                proof! {
                    let set = snapshot!(identifiers.model());
                    let rest = snapshot!(ids(stack.model()));
                }
                if identifiers.insert(parent) {
                    stack.push(parent);
                    proof_assert! { nodes.model()[*x]@.from.contains(parent.deep_model()) };
                    proof_assert! {
                        lemma_ancestor_search_push(
                            nodes.model(), *old_set, *set, *rest, *x, parent.deep_model(), *id0);
                        ancestor_search(
                            nodes.model(), *old_set, identifiers.model(), ids(stack.model()).snoc(*x), *id0)
                    };
                    proof_assert! {
                        lemma_set_in_keys_len(identifiers.model(), nodes.model());
                        identifiers.model().len() <= nodes.model().len()
                    };
                }
            }
        }
    }
}

/// Returns `true` if `target` accepts one of `ids` or their ancestors; otherwise, adds all of them
/// to `identifiers`.
#[check(terminates)]
#[requires(all_linked(nodes.model()))]
#[requires(forall<k> ids.model().contains(k) ==> nodes.model().contains(&k))]
#[requires(stack.model().len() == 0)]
#[requires(identifiers.model().len() == 0)]
#[requires(forall<k> is_target[k] == target.model().contains(k))]
#[ensures(!result ==> (forall<k> ids.model().contains(k) ==> (^identifiers).model().contains(&k))
    && (forall<k> (^identifiers).model().contains(&k) ==> nodes.model().contains(&k))
    && parent_closed(nodes.model(), (^identifiers).model())
    && forall<k> (^identifiers).model().contains(&k) ==> !is_target[k])]
#[ensures(result ==> exists<i, k> ids.model().contains(i) && nodes.model().contains(&k)
    && is_target[k] && descends(nodes.model(), i, k))]
#[cfg_attr(
    not(creusot),
    allow(unused_variables, reason = "Used by the specification")
)]
pub(in crate::independent) fn ancestor_subgraph_reaches<K, T, S>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    ids: &IndexSet<K, S>,
    target: &IndexSet<K, S>,
    is_target: Predicate<K>,
    stack: &mut ScratchpadVec<'_, K>,
    identifiers: &mut ScratchpadSet<'_, K, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    snapshot! { lemma_ids_snoc::<K> };
    snapshot! { lemma_snoc_contains::<Id<K>> };
    snapshot! { lemma_no_dup_snoc::<Id<K>> };
    snapshot! { lemma_descends_refl::<K, T, S> };
    snapshot! { lemma_descends_parent::<K, T, S> };

    let mut i = ids.len();
    #[invariant(i@ <= ids.model().len())]
    #[invariant(reach_search(
        nodes.model(), identifiers.model(), crate::verification::ids(stack.model()), *is_target))]
    #[invariant(parent_closed_except(
        nodes.model(), identifiers.model(), crate::verification::ids(stack.model())))]
    #[invariant(above_some(nodes.model(), identifiers.model(), ids.model()))]
    #[invariant(forall<j> i@ <= j && j < ids.model().len()
        ==> identifiers.model().contains(&ids.model()[j]))]
    #[invariant(identifiers.model().len() <= nodes.model().len())]
    #[variant(i@)]
    while i > 0 {
        #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
        {
            i -= 1;
        }
        let id = ids[i];
        proof_assert!(
            ids.model().contains(id.deep_model()) && nodes.model().contains(&id.deep_model())
        );
        if identifiers.insert(id) {
            if target.contains(&id) {
                return true;
            }

            stack.push(id);
        }
        proof_assert! {
            lemma_set_in_keys_len(identifiers.model(), nodes.model());
            identifiers.model().len() <= nodes.model().len()
        };
    }

    #[invariant(reach_search(
        nodes.model(), identifiers.model(), crate::verification::ids(stack.model()), *is_target))]
    #[invariant(parent_closed_except(
        nodes.model(), identifiers.model(), crate::verification::ids(stack.model())))]
    #[invariant(above_some(nodes.model(), identifiers.model(), ids.model()))]
    #[invariant(forall<k> ids.model().contains(k) ==> identifiers.model().contains(&k))]
    #[invariant(identifiers.model().len() <= nodes.model().len())]
    #[variant((nodes.model().len() - identifiers.model().len(), stack.model().len()))]
    while let Some(id) = stack.pop() {
        proof! {
            let x = snapshot!(id.deep_model());
            let size = snapshot!(identifiers.model().len());
            let depth = snapshot!(stack.model().len());
        }
        proof_assert!(
            identifiers.model().contains(&*x)
                && nodes.model().contains(&*x)
                && linked(nodes.model(), *x)
        );
        let from = &nodes[&id].from;
        let mut i = from.len();
        #[invariant(i@ <= from.model().len())]
        #[invariant(reach_search(nodes.model(), identifiers.model(),
            crate::verification::ids(stack.model()).snoc(*x), *is_target))]
        #[invariant(parent_closed_except(
            nodes.model(), identifiers.model(), crate::verification::ids(stack.model()).snoc(*x)))]
        #[invariant(above_some(nodes.model(), identifiers.model(), ids.model()))]
        #[invariant(forall<k> ids.model().contains(k) ==> identifiers.model().contains(&k))]
        #[invariant(*size <= identifiers.model().len())]
        #[invariant(identifiers.model().len() <= nodes.model().len())]
        #[invariant(stack.model().len() <= *depth + identifiers.model().len() - *size)]
        #[invariant(forall<j> i@ <= j && j < from.model().len()
            ==> identifiers.model().contains(&from.model()[j]))]
        #[variant(i@)]
        while i > 0 {
            #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
            {
                i -= 1;
            }
            let parent = from[i];
            proof_assert!(nodes.model()[parent.deep_model()]@.to.contains(*x)
                && nodes.model().contains(&parent.deep_model()));
            proof_assert!(exists<j> ids.model().contains(j) && descends(nodes.model(), j, parent.deep_model()));
            if identifiers.insert(parent) {
                if target.contains(&parent) {
                    return true;
                }

                stack.push(parent);
                proof_assert! {
                    lemma_set_in_keys_len(identifiers.model(), nodes.model());
                    identifiers.model().len() <= nodes.model().len()
                };
            }
        }
    }

    false
}

/// Adds `id` and its descendants to `identifiers`.
#[check(terminates)]
#[requires(all_linked(nodes.model()))]
#[requires(nodes.model().contains(&id.deep_model()))]
#[requires(stack.model().len() == 0)]
#[requires(identifiers.model().len() == 0)]
#[ensures((^stack).model().len() == 0)]
#[ensures((^identifiers).model().contains(&id.deep_model()))]
#[ensures(forall<k> (^identifiers).model().contains(&k) ==> nodes.model().contains(&k))]
#[ensures(child_closed(nodes.model(), (^identifiers).model()))]
#[ensures(forall<k> (^identifiers).model().contains(&k) && k != id.deep_model()
    ==> has_parent_in(nodes.model(), (^identifiers).model(), k))]
pub(in crate::independent) fn descendant_subgraph<K, T, S>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    id: K,
    stack: &mut ScratchpadVec<'_, K>,
    identifiers: &mut ScratchpadSet<'_, K, S>,
) where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    snapshot! { lemma_ids_snoc::<K> };
    snapshot! { lemma_snoc_contains::<Id<K>> };
    snapshot! { lemma_no_dup_snoc::<Id<K>> };
    proof! {
        let id0 = snapshot!(id.deep_model());
    }

    if identifiers.insert(id) {
        stack.push(id);
    }
    proof_assert! {
        lemma_set_in_keys_len(identifiers.model(), nodes.model());
        identifiers.model().len() <= nodes.model().len()
    };

    #[invariant(descendant_search(nodes.model(), identifiers.model(), ids(stack.model()), *id0))]
    #[invariant(identifiers.model().contains(&*id0))]
    #[invariant(child_closed_except(nodes.model(), identifiers.model(), ids(stack.model())))]
    #[invariant(identifiers.model().len() <= nodes.model().len())]
    #[variant((nodes.model().len() - identifiers.model().len(), stack.model().len()))]
    while let Some(id) = stack.pop() {
        proof! {
            let x = snapshot!(id.deep_model());
            let size = snapshot!(identifiers.model().len());
            let depth = snapshot!(stack.model().len());
        }
        proof_assert!(nodes.model().contains(&*x) && linked(nodes.model(), *x));
        let to = &nodes[&id].to;
        let mut i = to.len();
        #[invariant(i@ <= to.model().len())]
        #[invariant(*size <= identifiers.model().len())]
        #[invariant(identifiers.model().len() <= nodes.model().len())]
        #[invariant(stack.model().len() <= *depth + identifiers.model().len() - *size)]
        #[variant(i@)]
        #[invariant(descendant_search(
            nodes.model(), identifiers.model(), ids(stack.model()).snoc(*x), *id0))]
        #[invariant(identifiers.model().contains(&*id0))]
        #[invariant(child_closed_except(
            nodes.model(), identifiers.model(), ids(stack.model()).snoc(*x)))]
        #[invariant(forall<j> i@ <= j && j < to.model().len()
            ==> identifiers.model().contains(&to.model()[j]))]
        while i > 0 {
            #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
            {
                i -= 1;
            }
            let child = to[i];
            proof_assert!(nodes.model()[*x]@.to.contains(child.deep_model())
                && nodes.model().contains(&child.deep_model()));
            if identifiers.insert(child) {
                stack.push(child);
                proof_assert! {
                    lemma_set_in_keys_len(identifiers.model(), nodes.model());
                    identifiers.model().len() <= nodes.model().len()
                };
            }
        }
    }
}

/// Returns `true` if `target` accepts one of `ids` or their descendants; otherwise, adds all of
/// them to `identifiers`.
#[check(terminates)]
#[requires(all_linked(nodes.model()))]
#[requires(forall<k> ids.model().contains(k) ==> nodes.model().contains(&k))]
#[requires(stack.model().len() == 0)]
#[requires(identifiers.model().len() == 0)]
#[requires(forall<k> is_target[k] == target.model().contains(k))]
#[ensures(!result ==> (forall<k> ids.model().contains(k) ==> (^identifiers).model().contains(&k))
    && (forall<k> (^identifiers).model().contains(&k) ==> nodes.model().contains(&k))
    && child_closed(nodes.model(), (^identifiers).model())
    && forall<k> (^identifiers).model().contains(&k) ==> !is_target[k])]
#[ensures(result ==> exists<i, k> ids.model().contains(i) && nodes.model().contains(&k)
    && is_target[k] && descends(nodes.model(), k, i))]
#[cfg_attr(
    not(creusot),
    allow(unused_variables, reason = "Used by the specification")
)]
pub(in crate::independent) fn descendant_subgraph_reaches<K, T, S>(
    nodes: &HashMap<K, IndependentNode<K, T, S>, S>,
    ids: &IndexSet<K, S>,
    target: &IndexSet<K, S>,
    is_target: Predicate<K>,
    stack: &mut ScratchpadVec<'_, K>,
    identifiers: &mut ScratchpadSet<'_, K, S>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    snapshot! { lemma_ids_snoc::<K> };
    snapshot! { lemma_snoc_contains::<Id<K>> };
    snapshot! { lemma_no_dup_snoc::<Id<K>> };
    snapshot! { lemma_descends_refl::<K, T, S> };
    snapshot! { lemma_descends_child::<K, T, S> };

    let mut i = ids.len();
    #[invariant(i@ <= ids.model().len())]
    #[invariant(reach_search(
        nodes.model(), identifiers.model(), crate::verification::ids(stack.model()), *is_target))]
    #[invariant(child_closed_except(
        nodes.model(), identifiers.model(), crate::verification::ids(stack.model())))]
    #[invariant(below_some(nodes.model(), identifiers.model(), ids.model()))]
    #[invariant(forall<j> i@ <= j && j < ids.model().len()
        ==> identifiers.model().contains(&ids.model()[j]))]
    #[invariant(identifiers.model().len() <= nodes.model().len())]
    #[variant(i@)]
    while i > 0 {
        #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
        {
            i -= 1;
        }
        let id = ids[i];
        proof_assert!(
            ids.model().contains(id.deep_model()) && nodes.model().contains(&id.deep_model())
        );
        if identifiers.insert(id) {
            if target.contains(&id) {
                return true;
            }

            stack.push(id);
        }
        proof_assert! {
            lemma_set_in_keys_len(identifiers.model(), nodes.model());
            identifiers.model().len() <= nodes.model().len()
        };
    }

    #[invariant(reach_search(
        nodes.model(), identifiers.model(), crate::verification::ids(stack.model()), *is_target))]
    #[invariant(child_closed_except(
        nodes.model(), identifiers.model(), crate::verification::ids(stack.model())))]
    #[invariant(below_some(nodes.model(), identifiers.model(), ids.model()))]
    #[invariant(forall<k> ids.model().contains(k) ==> identifiers.model().contains(&k))]
    #[invariant(identifiers.model().len() <= nodes.model().len())]
    #[variant((nodes.model().len() - identifiers.model().len(), stack.model().len()))]
    while let Some(id) = stack.pop() {
        proof! {
            let x = snapshot!(id.deep_model());
            let size = snapshot!(identifiers.model().len());
            let depth = snapshot!(stack.model().len());
        }
        proof_assert!(
            identifiers.model().contains(&*x)
                && nodes.model().contains(&*x)
                && linked(nodes.model(), *x)
        );
        let to = &nodes[&id].to;
        let mut i = to.len();
        #[invariant(i@ <= to.model().len())]
        #[invariant(reach_search(nodes.model(), identifiers.model(),
            crate::verification::ids(stack.model()).snoc(*x), *is_target))]
        #[invariant(child_closed_except(
            nodes.model(), identifiers.model(), crate::verification::ids(stack.model()).snoc(*x)))]
        #[invariant(below_some(nodes.model(), identifiers.model(), ids.model()))]
        #[invariant(forall<k> ids.model().contains(k) ==> identifiers.model().contains(&k))]
        #[invariant(*size <= identifiers.model().len())]
        #[invariant(identifiers.model().len() <= nodes.model().len())]
        #[invariant(stack.model().len() <= *depth + identifiers.model().len() - *size)]
        #[invariant(forall<j> i@ <= j && j < to.model().len()
            ==> identifiers.model().contains(&to.model()[j]))]
        #[variant(i@)]
        while i > 0 {
            #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
            {
                i -= 1;
            }
            let child = to[i];
            proof_assert!(nodes.model()[*x]@.to.contains(child.deep_model())
                && nodes.model().contains(&child.deep_model()));
            proof_assert!(exists<j> ids.model().contains(j) && descends(nodes.model(), child.deep_model(), j));
            if identifiers.insert(child) {
                if target.contains(&child) {
                    return true;
                }

                stack.push(child);
                proof_assert! {
                    lemma_set_in_keys_len(identifiers.model(), nodes.model());
                    identifiers.model().len() <= nodes.model().len()
                };
            }
        }
    }

    false
}
