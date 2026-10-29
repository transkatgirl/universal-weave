//! Lemmas used to verify `crate::independent::graph::search`.

use core::hash::{BuildHasher, Hash};

use creusot_std::logic::{FSet, Mapping};

#[allow(unused_imports)]
use super::*;
#[allow(unused_imports)]
use crate::{
    IndependentContents,
    independent::IndependentNode,
    verification::{Id, Model, no_dup},
};

/// Every parent of an element of `set` which isn't in `stack` is in `set`.
#[logic(open)]
pub fn parent_closed_except<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    stack: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<k, p> set.contains(&k) && !stack.contains(k) && nodes[k]@.from.contains(p)
            ==> set.contains(&p)
    }
}

/// Every child of an element of `set` which isn't in `stack` is in `set`.
#[logic(open)]
pub fn child_closed_except<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    stack: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<k, c> set.contains(&k) && !stack.contains(k) && nodes[k]@.to.contains(c)
            ==> set.contains(&c)
    }
}

/// `roots` lists, without duplicates, exactly the elements of `set` which aren't in `stack` and
/// have no parents.
#[logic(open)]
pub fn roots_except<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    stack: Seq<Id<K>>,
    roots: Seq<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        no_dup(roots)
            && forall<k> roots.contains(k)
                == (set.contains(&k) && !stack.contains(k) && nodes[k]@.from.len() == 0)
    }
}

/// The state of `ancestor_subgraph`, which adds `id` and its ancestors to the parent-closed set
/// `old`: `set` holds the nodes found so far, and `stack` new nodes whose parents may not have
/// been found yet. Every new node but `id` has a new child.
#[logic(open)]
pub fn ancestor_search<K, T, S>(
    nodes: Nodes<K, T, S>,
    old: FSet<Id<K>>,
    set: FSet<Id<K>>,
    stack: Seq<Id<K>>,
    id: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        no_dup(stack)
            && (forall<k> old.contains(&k) ==> set.contains(&k))
            && (forall<k> set.contains(&k) ==> nodes.contains(&k))
            && (forall<k> stack.contains(k) ==> set.contains(&k) && !old.contains(&k))
            && forall<k> set.contains(&k) && !old.contains(&k) && k != id
                ==> has_child_in(nodes, set.difference(old), k)
    }
}

/// The state of `descendant_subgraph`, which adds `id` and its descendants to an empty set: `set`
/// holds the nodes found so far, and `stack` nodes whose children may not have been found yet.
/// Every node but `id` has a parent in `set`.
#[logic(open)]
pub fn descendant_search<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    stack: Seq<Id<K>>,
    id: Id<K>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        no_dup(stack)
            && (forall<k> set.contains(&k) ==> nodes.contains(&k))
            && (forall<k> stack.contains(k) ==> set.contains(&k))
            && forall<k> set.contains(&k) && k != id ==> has_parent_in(nodes, set, k)
    }
}

/// The state of `ancestor_subgraph_reaches` and `descendant_subgraph_reaches`: `set` holds the
/// nodes found so far, none of which is a target, and `stack` nodes whose parents (or children)
/// may not have been found yet.
#[logic(open)]
pub fn reach_search<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    stack: Seq<Id<K>>,
    is_target: Mapping<Id<K>, bool>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        no_dup(stack)
            && (forall<k> set.contains(&k) ==> nodes.contains(&k) && !is_target[k])
            && forall<k> stack.contains(k) ==> set.contains(&k)
    }
}

/// Every element of `set` is one of `ids` or an ancestor of one of them.
#[logic(open)]
pub fn above_some<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>, ids: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { forall<k> set.contains(&k) ==> exists<i> ids.contains(i) && descends(nodes, i, k) }
}

/// Every element of `set` is one of `ids` or a descendant of one of them.
#[logic(open)]
pub fn below_some<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>, ids: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { forall<k> set.contains(&k) ==> exists<i> ids.contains(i) && descends(nodes, k, i) }
}

/// Finding a new parent `p` of the node `x` being visited by `ancestor_subgraph`, and pushing it
/// onto the stack, keeps the state of the search.
#[logic]
#[requires(all_linked(nodes))]
#[requires(ancestor_search(nodes, old, set, stack.snoc(x), id))]
#[requires(nodes[x]@.from.contains(p))]
#[requires(!set.contains(&p))]
#[ensures(ancestor_search(nodes, old, set.add(p), stack.snoc(p).snoc(x), id))]
pub fn lemma_ancestor_search_push<K, T, S>(
    nodes: Nodes<K, T, S>,
    old: FSet<Id<K>>,
    set: FSet<Id<K>>,
    stack: Seq<Id<K>>,
    x: Id<K>,
    p: Id<K>,
    id: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    let _ = crate::verification::lemma_snoc_contains::<Id<K>>;
    crate::verification::lemma_no_dup_snoc(stack, x);
    crate::verification::lemma_no_dup_snoc(stack, p);
    crate::verification::lemma_no_dup_snoc(stack.snoc(p), x);
    pearlite! {
        let set2 = set.add(p);
        proof_assert!(stack.snoc(x).contains(x));
        proof_assert!(set.contains(&x) && !old.contains(&x) && nodes.contains(&x));
        proof_assert!(linked(nodes, x));
        proof_assert!(nodes[p]@.to.contains(x));
        proof_assert!(set2.difference(old).contains(&x));
        proof_assert!(forall<c> set.difference(old).contains(&c) ==> set2.difference(old).contains(&c));
        proof_assert!(forall<k> set.contains(&k) && !old.contains(&k) && k != id
            ==> has_child_in(nodes, set2.difference(old), k));
        proof_assert!(has_child_in(nodes, set2.difference(old), p))
    }
}
