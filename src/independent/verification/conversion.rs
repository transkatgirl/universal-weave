//! Lemmas used to verify the conversions between [`DependentWeave`] and [`IndependentWeave`].
//!
//! [`DependentWeave`]: crate::dependent::DependentWeave
//! [`IndependentWeave`]: crate::independent::IndependentWeave

use core::hash::{BuildHasher, Hash};

use creusot_std::logic::{FMap, FSet, such_that};

#[allow(unused_imports)]
use super::*;
use crate::{
    IndependentContents,
    dependent::{DependentNode, verification as dep},
    independent::IndependentNode,
    verification::{Id, no_dup},
};

/// The nodes of a [`DependentWeave`](crate::dependent::DependentWeave), by identifier.
pub type DependentNodes<K, T, S> = FMap<Id<K>, DependentNode<K, T, S>>;

/// The parents of a node whose parent is `from`.
#[logic(open)]
pub fn parent_seq<I>(from: Option<I>) -> Seq<I> {
    match from {
        Some(p) => Seq::singleton(p),
        None => Seq::empty(),
    }
}

/// `n` is the [`DependentNode`] `d`, with the same links, flags and contents, except that it is
/// active exactly when `active` says.
#[logic(open)]
pub fn converted_node<K, T, S>(
    n: IndependentNode<K, T, S>,
    d: DependentNode<K, T, S>,
    active: bool,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        n@.id == d@.id
            && n@.from == parent_seq(d@.from)
            && n@.to == d@.to
            && n@.active == active
            && n@.bookmarked == d@.bookmarked
            && n.contents == d.contents
    }
}

/// `nodes` are the nodes of `old`, all inactive.
#[logic(open)]
pub fn converted_from<K, T, S>(nodes: Nodes<K, T, S>, old: DependentNodes<K, T, S>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (forall<k> nodes.contains(&k) == old.contains(&k))
            && forall<k> old.contains(&k) ==> converted_node(nodes[k], old[k], false)
    }
}

/// `nodes` are the nodes of `old`, whose parents are their only one, active exactly when they end
/// the active path `active`.
#[logic(open)]
pub fn converted_to<K, T, S>(
    nodes: DependentNodes<K, T, S>,
    old: Nodes<K, T, S>,
    active: FSet<Id<K>>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (forall<k> nodes.contains(&k) == old.contains(&k))
            && forall<k> old.contains(&k) ==> converted_node(
                old[k],
                nodes[k],
                old[k]@.active,
            ) && nodes[k]@.active == (old[k]@.active && !has_child_in(old, active, k))
    }
}

/// The nodes of a valid [`DependentWeave`](crate::dependent::DependentWeave), all made inactive,
/// form a valid [`IndependentWeave`](crate::independent::IndependentWeave) without active nodes.
#[logic]
#[requires(old.valid())]
#[requires(converted_from(nodes, old.nodes))]
#[ensures(IndependentWeaveView { nodes, roots: old.roots, active: FSet::empty(), bookmarked: old.bookmarked }
    .valid())]
pub fn lemma_from_dependent_valid<K, T, S>(
    old: dep::DependentWeaveView<K, T, S>,
    nodes: Nodes<K, T, S>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let rank = dep::rank_of(old.nodes);
        proof_assert!(forall<k> old.nodes.contains(&k) ==> dep::linked(old.nodes, k));
        proof_assert!(forall<k, p> nodes.contains(&k) && nodes[k]@.from.contains(p)
            ==> old.nodes[k]@.from == Some(p));
        proof_assert!(forall<k> nodes.contains(&k) ==> nodes[k]@.valid());
        proof_assert!(forall<k> nodes.contains(&k) ==> no_dup(nodes[k]@.from));
        proof_assert!(forall<k> nodes.contains(&k) ==> linked(nodes, k));
        proof_assert!(ranked(nodes, rank));
        proof_assert!(forall<k> nodes.contains(&k)
            ==> (nodes[k]@.from.len() == 0) == (old.nodes[k]@.from == None));
        proof_assert!(structure(nodes, old.roots, FSet::empty()));
        proof_assert!(path_of(nodes, FSet::empty(), Seq::empty()))
    }
}

/// A nonempty active path ends at an active node without active children.
#[logic]
#[requires(all_linked(nodes) && acyclic(nodes))]
#[requires(path_of(nodes, set, path))]
#[requires(0 < path.len())]
#[ensures(set.contains(&path[0]) && !has_child_in(nodes, set, path[0]))]
pub fn lemma_path_has_end<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>, path: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let rank = rank_of(nodes);
        lemma_path_ranks_all(nodes, rank, path);
        proof_assert!(linked(nodes, path[0]));
        proof_assert!(forall<c> nodes[path[0]]@.to.contains(c) && set.contains(&c)
            ==> path.contains(c) && rank[path[0]] < rank[c]);
        proof_assert!(forall<j> 0 <= j && j < path.len() ==> rank[path[j]] <= rank[path[0]]);
        proof_assert!(path.contains(path[0]))
    }
}

/// The nodes of a valid [`IndependentWeave`](crate::independent::IndependentWeave) in which every
/// node has at most one parent, each active when it ends the active path, form a valid
/// [`DependentWeave`](crate::dependent::DependentWeave) whose active node is the end of the
/// active path.
#[logic]
#[requires(old.valid())]
#[requires(forall<k> old.nodes.contains(&k) ==> old.nodes[k]@.from.len() < 2)]
#[requires(converted_to(nodes, old.nodes, old.active))]
#[requires(match active {
    Some(a) => nodes.contains(&a) && nodes[a]@.active,
    None => forall<k> nodes.contains(&k) ==> !nodes[k]@.active,
})]
#[ensures(dep::DependentWeaveView { nodes, roots: old.roots, active, bookmarked: old.bookmarked }.valid())]
#[ensures(match active {
    Some(a) => old.active.contains(&a) && !has_child_in(old.nodes, old.active, a),
    None => old.active.len() == 0,
})]
pub fn lemma_to_dependent_valid<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    nodes: DependentNodes<K, T, S>,
    active: Option<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let rank = rank_of(old.nodes);
        let path = such_that(|path| path_of(old.nodes, old.active, path));
        proof_assert!(forall<k> old.nodes.contains(&k) ==> linked(old.nodes, k));
        proof_assert!(forall<k> old.nodes.contains(&k) ==> match nodes[k]@.from {
            Some(p) => old.nodes[k]@.from == Seq::singleton(p),
            None => old.nodes[k]@.from.len() == 0,
        });
        proof_assert!(forall<k, p> old.nodes.contains(&k) && old.nodes[k]@.from.contains(p)
            ==> nodes[k]@.from == Some(p));
        proof_assert!(forall<k> nodes.contains(&k) ==> nodes[k]@.valid());
        proof_assert!(forall<k> nodes.contains(&k) ==> dep::linked(nodes, k));
        proof_assert!(dep::ranked(nodes, rank));
        proof_assert!(dep::valid_tree(nodes));
        proof_assert!(forall<x, y> old.active.contains(&x) && !has_child_in(old.nodes, old.active, x)
            && old.active.contains(&y) && !has_child_in(old.nodes, old.active, y)
            ==> {
                lemma_path_end(old.nodes, old.active, path, x);
                lemma_path_end(old.nodes, old.active, path, y);
                x == y
            });
        proof_assert!(forall<k> nodes.contains(&k)
            ==> nodes[k]@.active == (old.active.contains(&k) && !has_child_in(old.nodes, old.active, k)));
        proof_assert!(match active {
            Some(a) => forall<k> nodes.contains(&k) ==> nodes[k]@.active == (k == a),
            None => true,
        });
        proof_assert!(active == None && 0 < path.len() ==> {
            lemma_path_has_end(old.nodes, old.active, path);
            false
        });
        proof_assert!(active == None ==> path.len() == 0 && forall<k> !old.active.contains(&k));
        proof_assert!(active == None ==> old.active.ext_eq(FSet::empty()));
        proof_assert!(forall<k> nodes.contains(&k)
            ==> dep::DependentWeaveView { nodes, roots: old.roots, active, bookmarked: old.bookmarked }
                .flagged(k))
    }
}
