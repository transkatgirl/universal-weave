//! Specifications and lemmas for [`PatchablePathWeave`](super::PatchablePathWeave) over
//! [`IndependentWeave`](crate::independent::IndependentWeave).
//!
//! The wrapper reads the active path from its root down, as a *line*, and its patch operations
//! edit the *text* of that line: the units of its nodes' contents, in order. This module only
//! exists while Creusot is verifying the crate. See `VERIFICATION.md`.

use core::{
    hash::{BuildHasher, Hash},
    iter::{Chain, Copied, Empty, Once},
    option::IntoIter as OptionIntoIter,
    slice::Iter as SliceIter,
};

use creusot_std::{
    logic::{FMap, FSet, Mapping, such_that},
    prelude::*,
    std::iter::{ChainExt as _, CopiedExt as _, IteratorSpec},
};

use crate::{
    DiscreteContentResult, DiscreteContents, IndependentContents,
    independent::verification::{
        IndependentWeaveView, NodeView, Nodes, acyclic, activated, all_linked, best_path_down,
        best_path_up, count_in, extends_path, has_child_in, kept_below, lemma_count_add_absent,
        lemma_count_concat, lemma_count_in_bounds, lemma_count_in_zero, lemma_count_single,
        lemma_down_ranks, lemma_lists_set_len, lemma_path_ranks_all, lemma_vt_set_cover, linked,
        longest_path_within, no_better, nothing_below, path_down, path_of, path_up, rank_of,
        descends, insertable, inserted, lemma_descends_rank, moved, not_below, ranked, same_graph,
        separates, split_shape, valid_path,
    },
    verification::{
        Id, dedup, ids, increasing_steps, iterates, lemma_concat_contains, lemma_full_prefix,
        lemma_increasing, lemma_snoc_contains, lemma_subset_len, no_dup, set_bound,
    },
};

// Lines and their text.

/// `line` leads down from a root: each of its identifiers is a node, the first has no parents,
/// and each other one has the previous one as a parent.
#[logic(open)]
pub fn downward<K, T, S>(nodes: Nodes<K, T, S>, line: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        (forall<i> 0 <= i && i < line.len() ==> nodes.contains(&line[i]))
            && (forall<i> 0 <= i && i + 1 < line.len() ==> nodes[line[i + 1]]@.from.contains(line[i]))
            && (0 < line.len() ==> nodes[line[0]]@.from.len() == 0)
    }
}

/// `line` leads down from a root through exactly the nodes of `set`, once each: it is the path of
/// `set` read from its root.
#[logic(open)]
pub fn line_of<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>, line: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        downward(nodes, line) && no_dup(line) && forall<k> line.contains(k) == set.contains(&k)
    }
}

/// The active path of `view`, read from its root.
#[logic(open)]
pub fn active_line<K, T, S>(view: IndependentWeaveView<K, T, S>) -> Seq<Id<K>>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    such_that(|line| line_of(view.nodes, view.active, line))
}

/// The units of the contents of the nodes of `line`, in order.
#[logic(open)]
#[variant(line.len())]
pub fn text<K, T, S>(nodes: Nodes<K, T, S>, line: Seq<Id<K>>) -> Seq<T::Unit>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        if line.len() == 0 {
            Seq::empty()
        } else {
            text(nodes, line.subsequence(0, line.len() - 1))
                .concat(nodes[line[line.len() - 1]].contents.units())
        }
    }
}

/// The text of the active path of `view`, read from its root.
#[logic(open)]
pub fn active_text<K, T, S>(view: IndependentWeaveView<K, T, S>) -> Seq<T::Unit>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    text(view.nodes, active_line(view))
}

/// Some line of `nodes`, leading down from a root, has the text `t`. It is opaque outside of this
/// module, so that the provers don't search for the line where callers pass the fact on:
/// [`lemma_line_has_text`] gives it from a line, and [`lemma_has_text`] says what it is.
#[logic(open(self))]
pub fn has_text<K, T, S>(nodes: Nodes<K, T, S>, t: Seq<T::Unit>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { exists<line: Seq<Id<K>>> downward(nodes, line) && text(nodes, line) == t }
}

/// What [`has_text`] is.
#[logic]
#[ensures(has_text(nodes, t) == exists<line: Seq<Id<K>>> downward(nodes, line) && text(nodes, line) == t)]
pub fn lemma_has_text<K, T, S>(nodes: Nodes<K, T, S>, t: Seq<T::Unit>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
}

/// A line of `nodes` has its text.
#[logic]
#[requires(downward(nodes, line))]
#[ensures(has_text(nodes, text(nodes, line)))]
pub fn lemma_line_has_text<K, T, S>(nodes: Nodes<K, T, S>, line: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
}

/// `t` with the units from `start` to `end` replaced by `x`, as [`Vec::splice`] does, except that
/// `start` and `end` are clamped to the length of `t`, and that a range which ends before it
/// starts is empty.
#[logic(open)]
pub fn spliced<U>(t: Seq<U>, start: Int, end: Int, x: Seq<U>) -> Seq<U> {
    pearlite! {
        let s = if start < t.len() { start } else { t.len() };
        let e = if end < s { s } else if end < t.len() { end } else { t.len() };
        t.subsequence(0, s).concat(x).concat(t.subsequence(e, t.len()))
    }
}

// The requirements on the contents and on the identifiers.

/// When contents split strictly inside themselves, they split at that index: half of the
/// wrapper's requirement 3 on `T::split()`.
#[logic(open)]
pub fn exact_splits<T: DiscreteContents>() -> bool {
    pearlite! {
        forall<c: T, at: usize, left: T, right: T> 0 < at@ && at@ < c.units().len()
            && c.splits(at, DiscreteContentResult::Two(left, right))
            ==> left.units() == c.units().subsequence(0, at@)
                && right.units() == c.units().subsequence(at@, c.units().len())
    }
}

/// Contents never refuse to split strictly inside themselves: the other half of the wrapper's
/// requirement 3 on `T::split()`.
#[logic(open)]
pub fn total_splits<T: DiscreteContents>() -> bool {
    pearlite! {
        forall<c: T, at: usize, outcome: DiscreteContentResult<T>> 0 < at@ && at@ < c.units().len()
            && c.splits(at, outcome)
            ==> match outcome {
                DiscreteContentResult::One(_) => false,
                DiscreteContentResult::Two(_, _) => true,
            }
    }
}

/// `generate_id` can always be called.
#[logic(open, prophetic)]
pub fn callable<K, F: FnMut() -> K>(generate_id: F) -> bool {
    pearlite! { forall<f: F> generate_id.hist_inv(f) ==> f.precondition(()) }
}

/// `generate_id` can always be called, and only returns new identifiers: none of them is a key of
/// `taken`, and no two calls return the same one.
#[logic(open, prophetic)]
pub fn generates<K: DeepModel, F: FnMut() -> K, V>(generate_id: F, taken: FMap<Id<K>, V>) -> bool {
    pearlite! {
        callable(generate_id)
            && forall<f: F, g: F, k: K> generate_id.hist_inv(f) && f.postcondition_mut((), g, k)
                ==> !taken.contains(&k.deep_model())
                    && forall<f2: F, g2: F, k2: K> g.hist_inv(f2) && f2.postcondition_mut((), g2, k2)
                        ==> k2.deep_model() != k.deep_model()
    }
}

/// A callable generator can still be called after any call.
#[logic(prophetic)]
#[requires(callable(f))]
#[requires(f.postcondition_mut((), g, k))]
#[ensures(callable(g))]
#[ensures(f.precondition(()))]
pub fn lemma_callable_call<K, F: FnMut() -> K>(f: F, g: F, k: K) {}

/// A call to a generator of new identifiers returns a new one, and it keeps generating new ones
/// once that one is taken.
#[logic(prophetic)]
#[requires(generates(f, taken))]
#[requires(f.postcondition_mut((), g, k))]
#[ensures(!taken.contains(&k.deep_model()))]
#[ensures(callable(g))]
#[ensures(forall<new: FMap<Id<K>, V>> #[trigger(generates(g, new))]
    (forall<x> new.contains(&x) ==> taken.contains(&x) || x == k.deep_model())
    ==> generates(g, new))]
pub fn lemma_generates_call<K: DeepModel, F: FnMut() -> K, V>(
    f: F,
    g: F,
    k: K,
    taken: FMap<Id<K>, V>,
) {
}

// Lines.

/// A line is the reversed path of its nodes.
#[logic]
#[ensures(line_of(nodes, set, line) == path_of(nodes, set, line.reverse()))]
pub fn lemma_line_path<K, T, S>(nodes: Nodes<K, T, S>, set: FSet<Id<K>>, line: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let r = line.reverse();
        proof_assert!(r.len() == line.len());
        proof_assert!(forall<i> 0 <= i && i < line.len() ==> r[i] == line[line.len() - 1 - i]);
        proof_assert!(forall<i> 0 <= i && i < line.len() ==> line[i] == r[line.len() - 1 - i]);
        proof_assert!(forall<k> r.contains(k) == line.contains(k));
        proof_assert!(no_dup(r) == no_dup(line));
        proof_assert!(valid_path(nodes, r) == downward(nodes, line))
    }
}

/// Sequences along which `f` increases are determined by their elements.
#[logic]
#[requires(increasing_steps(a, f) && increasing_steps(b, f))]
#[requires(forall<x> a.contains(x) == b.contains(x))]
#[ensures(a == b)]
#[variant(a.len())]
pub fn lemma_increasing_unique<I>(a: Seq<I>, b: Seq<I>, f: Mapping<I, Int>) {
    pearlite! {
        lemma_increasing(a, f);
        lemma_increasing(b, f);
        if a.len() == 0 {
            proof_assert!(b.len() == 0 ==> a.ext_eq(b));
            proof_assert!(b.len() > 0 ==> b.contains(b[0]));
        } else {
            proof_assert!(a.contains(a[a.len() - 1]) && b.len() > 0);
            let x = a[a.len() - 1];
            let y = b[b.len() - 1];
            proof_assert!(forall<i> 0 <= i && i + 1 < a.len() ==> f[a[i]] < f[x]);
            proof_assert!(forall<i> 0 <= i && i + 1 < b.len() ==> f[b[i]] < f[y]);
            proof_assert!(b.contains(x) && a.contains(y));
            proof_assert!(x == y);
            let a2 = a.subsequence(0, a.len() - 1);
            let b2 = b.subsequence(0, b.len() - 1);
            proof_assert!(forall<i> 0 <= i && i < a2.len() ==> a2[i] == a[i] && a[i] != x);
            proof_assert!(forall<i> 0 <= i && i < b2.len() ==> b2[i] == b[i] && b[i] != y);
            proof_assert!(forall<z> a2.contains(z) == (a.contains(z) && z != x));
            proof_assert!(forall<z> b2.contains(z) == (b.contains(z) && z != y));
            lemma_increasing_unique(a2, b2, f);
            proof_assert!(a.ext_eq(b));
        }
    }
}

/// Ranks increase along a line.
#[logic]
#[requires(ranked(nodes, rank))]
#[requires(downward(nodes, line))]
#[ensures(increasing_steps(line, rank))]
pub fn lemma_line_ranks<K, T, S>(nodes: Nodes<K, T, S>, rank: Mapping<Id<K>, Int>, line: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
}

/// In an acyclic graph, a set of nodes has at most one line.
#[logic]
#[requires(acyclic(nodes))]
#[requires(line_of(nodes, set, a) && line_of(nodes, set, b))]
#[ensures(a == b)]
pub fn lemma_line_unique<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    a: Seq<Id<K>>,
    b: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let rank = rank_of(nodes);
        lemma_line_ranks(nodes, rank, a);
        lemma_line_ranks(nodes, rank, b);
        lemma_increasing_unique(a, b, rank)
    }
}

/// The active line of a valid weave is the line of its active nodes, and the only one.
#[logic]
#[requires(view.valid())]
#[ensures(line_of(view.nodes, view.active, active_line(view)))]
#[ensures(forall<line> line_of(view.nodes, view.active, line) ==> line == active_line(view))]
pub fn lemma_active_line<K, T, S>(view: IndependentWeaveView<K, T, S>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let path = such_that(|path| path_of(view.nodes, view.active, path));
        proof_assert!(path.reverse().reverse().ext_eq(path));
        lemma_line_path(view.nodes, view.active, path.reverse());
        proof_assert!(line_of(view.nodes, view.active, path.reverse()));
        proof_assert!(forall<line> line_of(view.nodes, view.active, line) ==> {
            lemma_line_unique(view.nodes, view.active, line, active_line(view));
            true
        })
    }
}

// Text.

/// The text of a concatenation is the concatenation of the texts.
#[logic]
#[ensures(text(nodes, a.concat(b)) == text(nodes, a).concat(text(nodes, b)))]
#[variant(b.len())]
pub fn lemma_text_concat<K, T, S>(nodes: Nodes<K, T, S>, a: Seq<Id<K>>, b: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        if b.len() == 0 {
            proof_assert!(a.concat(b).ext_eq(a));
            proof_assert!(text(nodes, a).concat(Seq::empty()).ext_eq(text(nodes, a)));
        } else {
            let b2 = b.subsequence(0, b.len() - 1);
            let ab = a.concat(b);
            proof_assert!(ab.subsequence(0, ab.len() - 1).ext_eq(a.concat(b2)));
            proof_assert!(ab[ab.len() - 1] == b[b.len() - 1]);
            lemma_text_concat(nodes, a, b2);
            let u = nodes[b[b.len() - 1]].contents.units();
            proof_assert!(text(nodes, a).concat(text(nodes, b2)).concat(u)
                .ext_eq(text(nodes, a).concat(text(nodes, b2).concat(u))));
        }
    }
}

/// The text of one node is its contents' units.
#[logic]
#[ensures(text(nodes, Seq::singleton(k)) == nodes[k].contents.units())]
pub fn lemma_text_single<K, T, S>(nodes: Nodes<K, T, S>, k: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(Seq::singleton(k).subsequence(0, 0).ext_eq(Seq::empty()));
        proof_assert!(Seq::<T::Unit>::empty().concat(nodes[k].contents.units())
            .ext_eq(nodes[k].contents.units()));
    }
}

/// The text of a line only depends on the contents of its nodes.
#[logic]
#[requires(forall<i> 0 <= i && i < line.len() ==> a[line[i]].contents == b[line[i]].contents)]
#[ensures(text(a, line) == text(b, line))]
#[variant(line.len())]
pub fn lemma_text_same<K, T, S>(a: Nodes<K, T, S>, b: Nodes<K, T, S>, line: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    if line.len() > 0 {
        lemma_text_same(a, b, line.subsequence(0, line.len() - 1))
    }
}

/// The text of a prefix of a line is the prefix of its text up to where that prefix's text ends,
/// and the text of the rest of the line is the rest of the text.
#[logic]
#[requires(0 <= j && j <= line.len())]
#[ensures(text(nodes, line.subsequence(0, j)).len() <= text(nodes, line).len())]
#[ensures(text(nodes, line.subsequence(0, j))
    == text(nodes, line).subsequence(0, text(nodes, line.subsequence(0, j)).len()))]
#[ensures(text(nodes, line.subsequence(j, line.len()))
    == text(nodes, line).subsequence(text(nodes, line.subsequence(0, j)).len(), text(nodes, line).len()))]
pub fn lemma_text_split<K, T, S>(nodes: Nodes<K, T, S>, line: Seq<Id<K>>, j: Int)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let a = line.subsequence(0, j);
        let b = line.subsequence(j, line.len());
        proof_assert!(a.concat(b).ext_eq(line));
        lemma_text_concat(nodes, a, b);
        let ta = text(nodes, a);
        let tb = text(nodes, b);
        proof_assert!(ta.concat(tb).subsequence(0, ta.len()).ext_eq(ta));
        proof_assert!(ta.concat(tb).subsequence(ta.len(), ta.len() + tb.len()).ext_eq(tb));
    }
}

/// The text up to one more node of a line.
#[logic]
#[requires(0 <= j && j < line.len())]
#[ensures(text(nodes, line.subsequence(0, j + 1))
    == text(nodes, line.subsequence(0, j)).concat(nodes[line[j]].contents.units()))]
pub fn lemma_text_step<K, T, S>(nodes: Nodes<K, T, S>, line: Seq<Id<K>>, j: Int)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let p = line.subsequence(0, j + 1);
        proof_assert!(p.subsequence(0, j).ext_eq(line.subsequence(0, j)));
        proof_assert!(p[j] == line[j]);
    }
}

// Reading and setting the active path.

/// `get_active_path` outputs the active line, reversed.
#[logic]
#[requires(view.valid())]
#[requires(out.len() == view.active.len())]
#[requires(forall<i> 0 <= i && i < out.len() ==> view.active.contains(&out[i]))]
#[requires(no_dup(out))]
#[requires(valid_path(view.nodes, out))]
#[ensures(out.reverse() == active_line(view))]
pub fn lemma_output_line<K, T, S>(view: IndependentWeaveView<K, T, S>, out: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        lemma_vt_set_cover(out, view.active);
        proof_assert!(path_of(view.nodes, view.active, out));
        proof_assert!(out.reverse().reverse().ext_eq(out));
        lemma_line_path(view.nodes, view.active, out.reverse());
        lemma_active_line(view)
    }
}

/// When the requested nodes form a line, the longest path through them is all of them.
#[logic]
#[requires(downward(nodes, line) && no_dup(line))]
#[requires(forall<k> requested.contains(&k) == line.contains(k))]
#[requires(longest_path_within(nodes, requested, after))]
#[ensures(line_of(nodes, after, line))]
pub fn lemma_longest_line<K, T, S>(
    nodes: Nodes<K, T, S>,
    requested: FSet<Id<K>>,
    after: FSet<Id<K>>,
    line: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let path = such_that(|path| path_of(nodes, after, path)
            && (forall<i> 0 <= i && i < path.len() ==> requested.contains(&path[i]))
            && forall<q> valid_path(nodes, q) && no_dup(q)
                && (forall<i> 0 <= i && i < q.len() ==> requested.contains(&q[i]))
                ==> q.len() <= path.len());
        lemma_line_path(nodes, requested, line);
        proof_assert!(path_of(nodes, requested, line.reverse()));
        let q = line.reverse();
        proof_assert!(forall<i> 0 <= i && i < q.len() ==> q.contains(q[i]) && requested.contains(&q[i]));
        proof_assert!(q.len() <= path.len());
        proof_assert!(forall<x> path.contains(x) ==> line.contains(x));
        lemma_subset_len(path, line);
        lemma_lists_set_len(line, requested);
        lemma_vt_set_cover(path, requested);
        proof_assert!(forall<k> after.contains(&k) == requested.contains(&k))
    }
}

// Iterating over the paths passed to `set_active_path`.

/// Iterating over copies of the elements of a slice produces them.
#[logic(prophetic)]
#[requires(iterates(iter, items))]
#[ensures(items == iter.iter()@@)]
pub fn lemma_iterates_slice<'a, K: Copy>(iter: Copied<SliceIter<'a, K>>, items: Seq<K>) {
    pearlite! {
        let done = such_that(|done: &mut Copied<SliceIter<'a, K>>| iter.produces(items, *done)
            && done.completed());
        let refs = such_that(|refs: Seq<&K>| iter.iter().produces(refs, done.iter())
            && items.len() == refs.len()
            && forall<i> 0 <= i && i < refs.len() ==> items[i] == *refs[i]);
        let inner = such_that(|inner: &mut SliceIter<'a, K>| *inner == done.iter()
            && ^inner == (^done).iter() && inner.completed());
        proof_assert!((*inner@)@ == Seq::empty());
        proof_assert!(inner@.to_ref_seq().ext_eq(Seq::empty()));
        proof_assert!(iter.iter()@.to_ref_seq().ext_eq(refs));
        proof_assert!(items.ext_eq(iter.iter()@@))
    }
}

/// Iterating over `once(x)` produces `x`.
#[logic(prophetic)]
#[requires(iterates(iter, items))]
#[requires(iter@ == Some(x))]
#[ensures(items == Seq::singleton(x))]
pub fn lemma_iterates_once<K>(iter: Once<K>, x: K, items: Seq<K>) {
    pearlite! {
        let done = such_that(|done: &mut Once<K>| iter.produces(items, *done) && done.completed());
        proof_assert!((*done)@ == None);
    }
}

/// Iterating over a chain produces what iterating over its first part and then its second part
/// does.
#[logic(prophetic)]
#[requires(iterates(iter, items))]
#[requires(iter.iter_a() == Some(a) && iter.iter_b() == Some(b))]
#[ensures(exists<x: Seq<A::Item>, y: Seq<A::Item>> items == x.concat(y) && iterates(a, x) && iterates(b, y))]
pub fn lemma_iterates_chain<A, B>(iter: Chain<A, B>, a: A, b: B, items: Seq<A::Item>)
where
    A: IteratorSpec,
    B: IteratorSpec<Item = A::Item>,
{
    pearlite! {
        let done = such_that(|done: &mut Chain<A, B>| iter.produces(items, *done) && done.completed());
        proof_assert!(iter.produces(items, *done) && done.completed());
        match done.iter_a() {
            Some(ya) => {
                proof_assert!(done.iter_b() == Some(b));
                proof_assert!(b.produces(Seq::empty(), b));
                proof_assert!(exists<x: Seq<A::Item>, y: Seq<A::Item>> items == x.concat(y) && a.produces(x, ya) && y == Seq::empty());
            }
            None => {
                proof_assert!(exists<yb: B> done.iter_b() == Some(yb));
            }
        }
    }
}

/// Iterating over `once(a).chain(once(b)).chain(rest.iter().copied())` produces `a`, `b` and the
/// elements of `rest`.
#[logic(prophetic)]
#[ensures(forall<iter: Chain<Chain<Once<K>, Once<K>>, Copied<SliceIter<'a, K>>>, items: Seq<K>,
    head: Chain<Once<K>, Once<K>>, oa: Once<K>, ob: Once<K>, rest: Copied<SliceIter<'a, K>>, a: K, b: K>
    iterates(iter, items)
    && iter.iter_a() == Some(head) && iter.iter_b() == Some(rest)
    && head.iter_a() == Some(oa) && head.iter_b() == Some(ob)
    && oa@ == Some(a) && ob@ == Some(b)
    ==> items == Seq::singleton(a).concat(Seq::singleton(b)).concat(rest.iter()@@))]
pub fn lemma_iterates_two_then<'a, K: Copy>() {
    pearlite! {
        proof_assert!(forall<iter: Chain<Chain<Once<K>, Once<K>>, Copied<SliceIter<'a, K>>>, items: Seq<K>,
            head: Chain<Once<K>, Once<K>>, oa: Once<K>, ob: Once<K>, rest: Copied<SliceIter<'a, K>>, a: K, b: K>
            iterates(iter, items)
            && iter.iter_a() == Some(head) && iter.iter_b() == Some(rest)
            && head.iter_a() == Some(oa) && head.iter_b() == Some(ob)
            && oa@ == Some(a) && ob@ == Some(b)
            ==> {
                lemma_iterates_chain(iter, head, rest, items);
                let x = such_that(|x: Seq<K>| exists<y: Seq<K>> items == x.concat(y)
                    && iterates(head, x) && iterates(rest, y));
                let y = such_that(|y: Seq<K>| items == x.concat(y)
                    && iterates(head, x) && iterates(rest, y));
                lemma_iterates_chain(head, oa, ob, x);
                let xa = such_that(|xa: Seq<K>| exists<xb: Seq<K>> x == xa.concat(xb)
                    && iterates(oa, xa) && iterates(ob, xb));
                let xb = such_that(|xb: Seq<K>| x == xa.concat(xb)
                    && iterates(oa, xa) && iterates(ob, xb));
                lemma_iterates_once(oa, a, xa);
                lemma_iterates_once(ob, b, xb);
                lemma_iterates_slice(rest, y);
                true
            })
    }
}

/// Iterating over `before.iter().copied().chain(once(a)).chain(rest.iter().copied())` produces
/// the elements of `before`, `a` and the elements of `rest`.
#[logic(prophetic)]
#[ensures(forall<iter: Chain<Chain<Copied<SliceIter<'a, K>>, Once<K>>, Copied<SliceIter<'a, K>>>, items: Seq<K>,
    head: Chain<Copied<SliceIter<'a, K>>, Once<K>>, before: Copied<SliceIter<'a, K>>, oa: Once<K>,
    rest: Copied<SliceIter<'a, K>>, a: K>
    iterates(iter, items)
    && iter.iter_a() == Some(head) && iter.iter_b() == Some(rest)
    && head.iter_a() == Some(before) && head.iter_b() == Some(oa)
    && oa@ == Some(a)
    ==> items == before.iter()@@.concat(Seq::singleton(a)).concat(rest.iter()@@))]
pub fn lemma_iterates_one_between<'a, K: Copy>() {
    pearlite! {
        proof_assert!(forall<iter: Chain<Chain<Copied<SliceIter<'a, K>>, Once<K>>, Copied<SliceIter<'a, K>>>, items: Seq<K>,
            head: Chain<Copied<SliceIter<'a, K>>, Once<K>>, before: Copied<SliceIter<'a, K>>, oa: Once<K>,
            rest: Copied<SliceIter<'a, K>>, a: K>
            iterates(iter, items)
            && iter.iter_a() == Some(head) && iter.iter_b() == Some(rest)
            && head.iter_a() == Some(before) && head.iter_b() == Some(oa)
            && oa@ == Some(a)
            ==> {
                lemma_iterates_chain(iter, head, rest, items);
                let x = such_that(|x: Seq<K>| exists<y: Seq<K>> items == x.concat(y)
                    && iterates(head, x) && iterates(rest, y));
                let y = such_that(|y: Seq<K>| items == x.concat(y)
                    && iterates(head, x) && iterates(rest, y));
                lemma_iterates_chain(head, before, oa, x);
                let xa = such_that(|xa: Seq<K>| exists<xb: Seq<K>> x == xa.concat(xb)
                    && iterates(before, xa) && iterates(oa, xb));
                let xb = such_that(|xb: Seq<K>| x == xa.concat(xb)
                    && iterates(before, xa) && iterates(oa, xb));
                lemma_iterates_slice(before, xa);
                lemma_iterates_once(oa, a, xb);
                lemma_iterates_slice(rest, y);
                true
            })
    }
}

/// Iterating over
/// `before.iter().copied().chain(once(a)).chain(once(b)).chain(rest.iter().copied())` produces
/// the elements of `before`, `a`, `b` and the elements of `rest`.
#[logic(prophetic)]
#[ensures(forall<iter: Chain<Chain<Chain<Copied<SliceIter<'a, K>>, Once<K>>, Once<K>>, Copied<SliceIter<'a, K>>>,
    items: Seq<K>, head: Chain<Chain<Copied<SliceIter<'a, K>>, Once<K>>, Once<K>>,
    first: Chain<Copied<SliceIter<'a, K>>, Once<K>>, before: Copied<SliceIter<'a, K>>, oa: Once<K>,
    ob: Once<K>, rest: Copied<SliceIter<'a, K>>, a: K, b: K>
    iterates(iter, items)
    && iter.iter_a() == Some(head) && iter.iter_b() == Some(rest)
    && head.iter_a() == Some(first) && head.iter_b() == Some(ob)
    && first.iter_a() == Some(before) && first.iter_b() == Some(oa)
    && oa@ == Some(a) && ob@ == Some(b)
    ==> items == before.iter()@@.concat(Seq::singleton(a)).concat(Seq::singleton(b))
        .concat(rest.iter()@@))]
pub fn lemma_iterates_two_between<'a, K: Copy>() {
    pearlite! {
        proof_assert!(forall<iter: Chain<Chain<Chain<Copied<SliceIter<'a, K>>, Once<K>>, Once<K>>, Copied<SliceIter<'a, K>>>,
            items: Seq<K>, head: Chain<Chain<Copied<SliceIter<'a, K>>, Once<K>>, Once<K>>,
            first: Chain<Copied<SliceIter<'a, K>>, Once<K>>, before: Copied<SliceIter<'a, K>>, oa: Once<K>,
            ob: Once<K>, rest: Copied<SliceIter<'a, K>>, a: K, b: K>
            iterates(iter, items)
            && iter.iter_a() == Some(head) && iter.iter_b() == Some(rest)
            && head.iter_a() == Some(first) && head.iter_b() == Some(ob)
            && first.iter_a() == Some(before) && first.iter_b() == Some(oa)
            && oa@ == Some(a) && ob@ == Some(b)
            ==> {
                lemma_iterates_chain(iter, head, rest, items);
                let x = such_that(|x: Seq<K>| exists<y: Seq<K>> items == x.concat(y)
                    && iterates(head, x) && iterates(rest, y));
                let y = such_that(|y: Seq<K>| items == x.concat(y)
                    && iterates(head, x) && iterates(rest, y));
                lemma_iterates_chain(head, first, ob, x);
                let xa = such_that(|xa: Seq<K>| exists<xb: Seq<K>> x == xa.concat(xb)
                    && iterates(first, xa) && iterates(ob, xb));
                let xb = such_that(|xb: Seq<K>| x == xa.concat(xb)
                    && iterates(first, xa) && iterates(ob, xb));
                lemma_iterates_chain(first, before, oa, xa);
                let xc = such_that(|xc: Seq<K>| exists<xd: Seq<K>> xa == xc.concat(xd)
                    && iterates(before, xc) && iterates(oa, xd));
                let xd = such_that(|xd: Seq<K>| xa == xc.concat(xd)
                    && iterates(before, xc) && iterates(oa, xd));
                lemma_iterates_slice(before, xc);
                lemma_iterates_once(oa, a, xd);
                lemma_iterates_once(ob, b, xb);
                lemma_iterates_slice(rest, y);
                true
            })
    }
}

/// A line is its part before its `t`th node, that node, and its part after.
#[logic]
#[requires(0 <= t && t < line.len())]
#[ensures(line == line.subsequence(0, t).concat(Seq::singleton(line[t]))
    .concat(line.subsequence(t + 1, line.len())))]
#[ensures(text(nodes, line) == text(nodes, line.subsequence(0, t))
    .concat(nodes[line[t]].contents.units())
    .concat(text(nodes, line.subsequence(t + 1, line.len()))))]
pub fn lemma_text_around<K, T, S>(nodes: Nodes<K, T, S>, line: Seq<Id<K>>, t: Int)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let a = line.subsequence(0, t);
        let b = line.subsequence(t + 1, line.len());
        let x = Seq::singleton(line[t]);
        proof_assert!(line.ext_eq(a.concat(x).concat(b)));
        lemma_text_concat(nodes, a.concat(x), b);
        lemma_text_concat(nodes, a, x);
        lemma_text_single(nodes, line[t]);
    }
}

// Changing the graph.

/// Splitting node `x` into `x` and `y` keeps every link which doesn't come from `x`, links `x`'s
/// former children to `y` instead, and keeps the roots.
#[logic]
#[requires(old.valid())]
#[requires(split_shape(old, new, x, y))]
#[ensures(forall<k, p> old.nodes.contains(&k) && old.nodes[k]@.from.contains(p) && p != x
    ==> new.nodes[k]@.from.contains(p))]
#[ensures(forall<k> old.nodes.contains(&k) && old.nodes[k]@.from.contains(x)
    ==> new.nodes[k]@.from.contains(y))]
#[ensures(new.nodes[x]@.from == old.nodes[x]@.from)]
#[ensures(new.nodes[y]@.from == Seq::singleton(x))]
#[ensures(new.nodes[x]@.to == Seq::singleton(y))]
#[ensures(new.nodes[y]@.to == old.nodes[x]@.to)]
#[ensures(forall<k> old.nodes.contains(&k) && old.nodes[k]@.from.len() == 0
    ==> new.nodes[k]@.from.len() == 0)]
pub fn lemma_split_links<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    x: Id<K>,
    y: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k> old.nodes.contains(&k) ==> linked(old.nodes, k));
        proof_assert!(forall<k> old.nodes.contains(&k) && old.nodes[k]@.from.contains(x)
            ==> old.nodes[x]@.to.contains(k));
        proof_assert!(forall<k> old.nodes.contains(&k) && old.nodes[k]@.from.len() == 0
            ==> !old.nodes[k]@.from.contains(x) && !old.nodes[x]@.to.contains(k));
    }
}

/// Splitting the `t`th node of a line into it and a new node `y` makes a line with `y` after
/// it, and with the new contents of both in its text.
#[logic]
#[requires(old.valid())]
#[requires(downward(old.nodes, line) && no_dup(line))]
#[requires(0 <= t && t < line.len())]
#[requires(split_shape(old, new, line[t], y))]
#[requires(forall<k> old.nodes.contains(&k) && k != line[t]
    ==> new.nodes[k].contents == old.nodes[k].contents)]
#[ensures(downward(new.nodes, split_line(line, t, y)) && no_dup(split_line(line, t, y)))]
#[ensures(forall<k> split_line(line, t, y).contains(k) == (line.contains(k) || k == y))]
#[ensures(split_line(line, t, y).len() == line.len() + 1)]
#[ensures(text(new.nodes, split_line(line, t, y).subsequence(0, t + 1))
    == text(old.nodes, line.subsequence(0, t)).concat(new.nodes[line[t]].contents.units()))]
#[ensures(text(new.nodes, split_line(line, t, y).subsequence(t + 1, line.len() + 1))
    == new.nodes[y].contents.units().concat(text(old.nodes, line.subsequence(t + 1, line.len()))))]
#[ensures(text(new.nodes, split_line(line, t, y))
    == text(old.nodes, line.subsequence(0, t))
        .concat(new.nodes[line[t]].contents.units())
        .concat(new.nodes[y].contents.units())
        .concat(text(old.nodes, line.subsequence(t + 1, line.len()))))]
pub fn lemma_split_line<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    line: Seq<Id<K>>,
    t: Int,
    y: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let x = line[t];
        let l = split_line(line, t, y);
        let a = line.subsequence(0, t);
        let b = line.subsequence(t + 1, line.len());
        lemma_split_links(old, new, x, y);
        proof_assert!(!old.nodes.contains(&y) && !line.contains(y));
        proof_assert!(forall<i> 0 <= i && i <= t ==> l[i] == line[i]);
        proof_assert!(l[t + 1] == y);
        proof_assert!(forall<i> t + 1 < i && i < l.len() ==> l[i] == line[i - 1]);
        proof_assert!(forall<i> 0 <= i && i < t ==> line[i] != x);
        proof_assert!(forall<i> t < i && i < line.len() ==> line[i] != x);
        proof_assert!(forall<i> 0 <= i && i + 1 < l.len() ==> new.nodes[l[i + 1]]@.from.contains(l[i]));
        proof_assert!(downward(new.nodes, l));
        proof_assert!(forall<i, j> 0 <= i && i < j && j < l.len() ==> l[i] != l[j]);
        proof_assert!(forall<k> l.contains(k) == (line.contains(k) || k == y));
        proof_assert!(l.subsequence(0, t + 1).ext_eq(a.concat(Seq::singleton(x))));
        proof_assert!(l.ext_eq(a.concat(Seq::singleton(x)).concat(Seq::singleton(y)).concat(b)));
        lemma_text_same(new.nodes, old.nodes, a);
        lemma_text_same(new.nodes, old.nodes, b);
        proof_assert!(l.subsequence(t + 1, line.len() + 1).ext_eq(Seq::singleton(y).concat(b)));
        lemma_text_concat(new.nodes, Seq::singleton(y), b);
        lemma_text_concat(new.nodes, a.concat(Seq::singleton(x)).concat(Seq::singleton(y)), b);
        lemma_text_concat(new.nodes, a.concat(Seq::singleton(x)), Seq::singleton(y));
        lemma_text_concat(new.nodes, a, Seq::singleton(x));
        lemma_text_single(new.nodes, x);
        lemma_text_single(new.nodes, y);
    }
}

/// `line` with `y` inserted after its `t`th node.
#[logic(open)]
pub fn split_line<I>(line: Seq<I>, t: Int, y: I) -> Seq<I> {
    pearlite! {
        line.subsequence(0, t + 1).concat(Seq::singleton(y)).concat(line.subsequence(t + 1, line.len()))
    }
}

/// A sequence starting with `x` and continuing with `t` is `x` followed by `t`.
#[logic]
#[requires(s.len() == t.len() + 1 && s[0] == x)]
#[requires(forall<j> 1 <= j && j < s.len() ==> s[j] == t[j - 1])]
#[ensures(s == Seq::singleton(x).concat(t))]
pub fn lemma_cons<I>(s: Seq<I>, x: I, t: Seq<I>) {
    proof_assert!(s.ext_eq(Seq::singleton(x).concat(t)))
}

/// A part of a sequence is its part up to some index followed by its part from there.
#[logic]
#[requires(0 <= a && a <= b && b <= c && c <= s.len())]
#[ensures(s.subsequence(a, c) == s.subsequence(a, b).concat(s.subsequence(b, c)))]
#[ensures(a == 1 && c == s.len() ==> s.tail() == s.subsequence(a, b).concat(s.subsequence(b, c)))]
pub fn lemma_subsequence_split<I>(s: Seq<I>, a: Int, b: Int, c: Int) {
    pearlite! {
        proof_assert!(s.subsequence(a, c).ext_eq(s.subsequence(a, b).concat(s.subsequence(b, c))));
        proof_assert!(a == 1 && c == s.len() ==> s.tail().ext_eq(s.subsequence(a, c)));
    }
}

// Counting the active nodes along the paths which activation compares.

/// A set counts at most as many elements of a sequence as a set containing those of its elements.
#[logic]
#[requires(forall<i> 0 <= i && i < s.len() && a.contains(&s[i]) ==> b.contains(&s[i]))]
#[ensures(count_in(a, s) <= count_in(b, s))]
#[variant(s.len())]
pub fn lemma_count_mono<I>(a: FSet<I>, b: FSet<I>, s: Seq<I>) {
    pearlite! {
        if s.len() > 0 {
            proof_assert!(forall<i> 0 <= i && i < s.tail().len() ==> s.tail()[i] == s[i + 1]);
            lemma_count_mono(a, b, s.tail())
        }
    }
}

/// A sequence without duplicates counts at most as many elements of a set as it has.
#[logic]
#[requires(no_dup(s))]
#[ensures(count_in(set, s) <= set.len())]
#[variant(s.len())]
pub fn lemma_count_set_len<I>(set: FSet<I>, s: Seq<I>) {
    pearlite! {
        if s.len() > 0 {
            let x = s[0];
            proof_assert!(forall<i> 0 <= i && i < s.tail().len() ==> s.tail()[i] == s[i + 1]);
            proof_assert!(!s.tail().contains(x));
            proof_assert!(no_dup(s.tail()));
            if set.contains(&x) {
                lemma_count_add_absent(set.remove(x), s.tail(), x);
                proof_assert!(set.remove(x).add(x).ext_eq(set));
                lemma_count_set_len(set.remove(x), s.tail());
            } else {
                lemma_count_set_len(set, s.tail());
            }
        }
    }
}

/// Counting as many elements of a set as a sequence has means they are all in it.
#[logic]
#[requires(count_in(set, s) >= s.len())]
#[ensures(forall<i> 0 <= i && i < s.len() ==> set.contains(&s[i]))]
#[variant(s.len())]
pub fn lemma_count_all<I>(set: FSet<I>, s: Seq<I>) {
    pearlite! {
        if s.len() > 0 {
            proof_assert!(forall<i> 0 <= i && i < s.tail().len() ==> s.tail()[i] == s[i + 1]);
            lemma_count_in_bounds(set, s.tail());
            lemma_count_all(set, s.tail())
        }
    }
}

/// A sequence whose elements are all in a set counts all of them.
#[logic]
#[requires(forall<i> 0 <= i && i < s.len() ==> set.contains(&s[i]))]
#[ensures(count_in(set, s) == s.len())]
#[variant(s.len())]
pub fn lemma_count_full<I>(set: FSet<I>, s: Seq<I>) {
    pearlite! {
        if s.len() > 0 {
            proof_assert!(forall<i> 0 <= i && i < s.tail().len() ==> s.tail()[i] == s[i + 1]);
            lemma_count_full(set, s.tail())
        }
    }
}

/// A sequence without duplicates which counts as many elements of a set as it has contains all
/// of them.
#[logic]
#[requires(no_dup(s))]
#[requires(count_in(set, s) >= set.len())]
#[ensures(forall<x> set.contains(&x) ==> s.contains(x))]
pub fn lemma_count_cover<I>(set: FSet<I>, s: Seq<I>) {
    pearlite! {
        proof_assert!(forall<x> set.contains(&x) && !s.contains(x) ==> {
            lemma_count_add_absent(set.remove(x), s, x);
            proof_assert!(set.remove(x).add(x).ext_eq(set));
            lemma_count_set_len(set.remove(x), s);
            false
        })
    }
}

/// The nodes of `line` from `a` to `b`, as a set.
#[logic(open)]
pub fn nodes_of<K, T, S>(nodes: Nodes<K, T, S>, line: Seq<Id<K>>, a: Int, b: Int) -> FSet<Id<K>>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { nodes.keys().filter(|k: Id<K>| line.subsequence(a, b).contains(k)) }
}

/// The nodes of a part of a line are as many as the part has.
#[logic]
#[requires(downward(nodes, line) && no_dup(line))]
#[requires(0 <= a && a <= b && b <= line.len())]
#[ensures(forall<k> nodes_of(nodes, line, a, b).contains(&k) == line.subsequence(a, b).contains(k))]
#[ensures(nodes_of(nodes, line, a, b).len() == b - a)]
pub fn lemma_nodes_of<K, T, S>(nodes: Nodes<K, T, S>, line: Seq<Id<K>>, a: Int, b: Int)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let part = line.subsequence(a, b);
        proof_assert!(forall<i> 0 <= i && i < part.len() ==> part[i] == line[a + i]);
        proof_assert!(no_dup(part));
        proof_assert!(forall<k> part.contains(k) ==> nodes.contains(&k));
        lemma_lists_set_len(part, nodes_of(nodes, line, a, b))
    }
}

// Paths down to a leaf.

/// A path from `x` down to a leaf, through first children.
#[logic]
#[requires(all_linked(nodes) && ranked(nodes, rank))]
#[requires(nodes.contains(&x))]
#[requires(forall<k> nodes.contains(&k) ==> rank[k] < bound)]
#[ensures(path_down(nodes, x, result))]
#[ensures(forall<j> 0 < j && j < result.len() ==> rank[x] < rank[result[j]])]
#[variant(bound - rank[x])]
pub fn leaf_path<K, T, S>(
    nodes: Nodes<K, T, S>,
    rank: Mapping<Id<K>, Int>,
    bound: Int,
    x: Id<K>,
) -> Seq<Id<K>>
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        if nodes[x]@.to.len() == 0 {
            Seq::singleton(x)
        } else {
            let c = nodes[x]@.to[0];
            proof_assert!(linked(nodes, x) && nodes.contains(&c) && nodes[c]@.from.contains(x));
            let rest = leaf_path(nodes, rank, bound, c);
            let p = Seq::singleton(x).concat(rest);
            proof_assert!(forall<j> 0 < j && j < p.len() ==> p[j] == rest[j - 1]);
            proof_assert!(forall<j> 0 <= j && j < rest.len() ==> rank[c] <= rank[rest[j]]);
            proof_assert!(!rest.contains(x));
            p
        }
    }
}

// Activating an inserted node.

/// A node of a line ranked no higher than its `i`th node comes at most `i`th, and one ranked
/// higher comes after it.
#[logic]
#[requires(acyclic(nodes))]
#[requires(line_of(nodes, set, line))]
#[requires(0 <= i && i < line.len())]
#[requires(set.contains(&x))]
#[ensures(rank_of(nodes)[x] <= rank_of(nodes)[line[i]] ==> line.subsequence(0, i + 1).contains(x))]
#[ensures(rank_of(nodes)[line[i]] < rank_of(nodes)[x]
    ==> line.subsequence(i + 1, line.len()).contains(x))]
pub fn lemma_line_rank_le<K, T, S>(
    nodes: Nodes<K, T, S>,
    set: FSet<Id<K>>,
    line: Seq<Id<K>>,
    i: Int,
    x: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let rank = rank_of(nodes);
        lemma_line_ranks(nodes, rank, line);
        lemma_increasing(line, rank);
        let r = such_that(|r: Int| 0 <= r && r < line.len() && line[r] == x);
        if r <= i {
            proof_assert!(line.subsequence(0, i + 1)[r] == x);
            proof_assert!(rank[x] <= rank[line[i]]);
        } else {
            proof_assert!(line.subsequence(i + 1, line.len())[r - i - 1] == x);
            proof_assert!(rank[line[i]] < rank[x]);
        }
    }
}

/// The parent of an inserted node which is on the active line, and the line itself, are where a
/// best path up from the node goes: it holds the node and the line up to that parent.
#[logic]
#[requires(new.valid())]
#[requires(line_of(new.nodes, active, line))]
#[requires(0 <= i && i < line.len())]
#[requires(new.nodes.contains(&id) && new.nodes[id]@.from == Seq::singleton(line[i]))]
#[requires(!active.contains(&id))]
#[requires(best_path_up(new.nodes, active, id, up))]
#[ensures(forall<k> up.contains(k) == (k == id || line.subsequence(0, i + 1).contains(k)))]
pub fn lemma_best_up_line<K, T, S>(
    new: IndependentWeaveView<K, T, S>,
    active: FSet<Id<K>>,
    line: Seq<Id<K>>,
    i: Int,
    id: Id<K>,
    up: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let nodes = new.nodes;
        let rank = rank_of(nodes);
        lemma_line_ranks(nodes, rank, line);
        lemma_increasing(line, rank);
        let before = line.subsequence(0, i + 1);
        let q = Seq::singleton(id).concat(before.reverse());
        proof_assert!(q.len() == i + 2 && q[0] == id);
        proof_assert!(forall<j> 1 <= j && j < q.len() ==> q[j] == line[i + 1 - j]);
        proof_assert!(!line.contains(id));
        proof_assert!(forall<j> 0 <= j && j + 1 < q.len() ==> nodes[q[j]]@.from.contains(q[j + 1]));
        proof_assert!(valid_path(nodes, q));
        proof_assert!(forall<a, b> 0 <= a && a < b && b < q.len() ==> q[a] != q[b]);
        proof_assert!(path_up(nodes, id, q));
        lemma_count_full(active, q.tail());
        proof_assert!(forall<j> 0 <= j && j < q.tail().len() ==> q.tail()[j] == q[j + 1]);
        proof_assert!(count_in(active, q) == i + 1);
        proof_assert!(no_better(active, q, up));
        proof_assert!(1 < up.len() && up[1] == line[i]);
        lemma_path_ranks_all(nodes, rank, up);
        let p = nodes_of(nodes, line, 0, i + 1);
        lemma_nodes_of(nodes, line, 0, i + 1);
        proof_assert!(up[0] == id);
        proof_assert!(forall<j> 0 <= j && j < up.len() && active.contains(&up[j])
            ==> j > 0 && rank[up[j]] <= rank[line[i]]
                && {
                    lemma_line_rank_le(nodes, active, line, i, up[j]);
                    before.contains(up[j])
                }
                && p.contains(&up[j]));
        lemma_count_mono(active, p, up);
        lemma_count_set_len(p, up);
        proof_assert!(count_in(active, up) == i + 1 && up.len() <= i + 2);
        let rest = up.tail();
        proof_assert!(forall<j> 0 <= j && j < rest.len() ==> rest[j] == up[j + 1]);
        proof_assert!(!p.contains(&id) && !active.contains(&id));
        proof_assert!(count_in(p, up) == count_in(p, rest));
        proof_assert!(count_in(p, rest) >= rest.len());
        lemma_count_all(p, rest);
        proof_assert!(no_dup(rest));
        lemma_count_cover(p, rest);
        proof_assert!(forall<k> up.contains(k) == (k == id || rest.contains(k)));
    }
}

/// The nodes of `set` are ranked above the end of `line`, which isn't empty.
#[logic(open)]
pub fn below_end<K, T, S>(nodes: Nodes<K, T, S>, line: Seq<Id<K>>, set: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<c> set.contains(c)
            ==> 0 < line.len() && rank_of(nodes)[line[line.len() - 1]] < rank_of(nodes)[c]
    }
}

/// No path down from `id` reaches an active node when its children are below the end of the
/// active line.
#[logic]
#[requires(new.valid())]
#[requires(line_of(new.nodes, active, line))]
#[requires(new.nodes.contains(&id) && !active.contains(&id))]
#[requires(below_end(new.nodes, line, new.nodes[id]@.to))]
#[ensures(nothing_below(new.nodes, active, id))]
pub fn lemma_nothing_below_line<K, T, S>(
    new: IndependentWeaveView<K, T, S>,
    active: FSet<Id<K>>,
    line: Seq<Id<K>>,
    id: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let nodes = new.nodes;
        let rank = rank_of(nodes);
        lemma_line_ranks(nodes, rank, line);
        lemma_increasing(line, rank);
        proof_assert!(forall<q, i> path_down(nodes, id, q) && 0 < i && i < q.len() ==> {
            lemma_down_ranks(nodes, rank, q);
            proof_assert!(nodes[id]@.to.contains(q[1]));
            proof_assert!(rank[q[1]] <= rank[q[i]]);
            proof_assert!(forall<r> 0 <= r && r < line.len() ==> rank[line[r]] <= rank[line[line.len() - 1]]);
            true
        } && !active.contains(&q[i]))
    }
}

/// The part of a path down from its `k`th node is a path down from that node.
#[logic]
#[requires(path_down(nodes, x, q))]
#[requires(0 <= k && k < q.len())]
#[ensures(path_down(nodes, q[k], q.subsequence(k, q.len())))]
pub fn lemma_path_down_from<K, T, S>(nodes: Nodes<K, T, S>, x: Id<K>, q: Seq<Id<K>>, k: Int)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let s = q.subsequence(k, q.len());
        proof_assert!(forall<j> 0 <= j && j < s.len() ==> s[j] == q[k + j]);
        proof_assert!(s[s.len() - 1] == q[q.len() - 1]);
    }
}

/// A node ranked above the end of the active line isn't active.
#[logic]
#[requires(acyclic(nodes))]
#[requires(line_of(nodes, active, line))]
#[requires(0 < line.len())]
#[requires(rank_of(nodes)[line[line.len() - 1]] < rank_of(nodes)[x])]
#[ensures(!active.contains(&x))]
pub fn lemma_below_end_inactive<K, T, S>(
    nodes: Nodes<K, T, S>,
    active: FSet<Id<K>>,
    line: Seq<Id<K>>,
    x: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        if active.contains(&x) {
            lemma_line_rank_le(nodes, active, line, line.len() - 1, x);
            proof_assert!(line.subsequence(line.len(), line.len()).len() == 0);
        }
    }
}

/// From `id`, inserted after the `i`th node of the active line, straight down the rest of the
/// line, and on from its end along `tail`.
#[logic(open)]
pub fn straight<I>(line: Seq<I>, i: Int, id: I, tail: Seq<I>) -> Seq<I> {
    pearlite! {
        Seq::singleton(id).concat(line.subsequence(i + 1, line.len())).concat(tail.tail())
    }
}

/// Going [`straight`] down the active line from a node inserted after its `i`th node, which has
/// the next one as a child, and on to a leaf from its end, is a path down which counts the rest of
/// the line as active.
#[logic]
#[requires(new.valid())]
#[requires(line_of(new.nodes, active, line))]
#[requires(0 <= i && i + 1 < line.len())]
#[requires(new.nodes.contains(&id) && new.nodes[id]@.to.contains(line[i + 1]))]
#[requires(!active.contains(&id))]
#[requires(path_down(new.nodes, line[line.len() - 1], tail))]
#[ensures(path_down(new.nodes, id, straight(line, i, id, tail)))]
#[ensures(count_in(active, straight(line, i, id, tail)) == line.len() - i - 1)]
#[ensures(straight(line, i, id, tail).len() == line.len() - i + tail.len() - 1)]
#[ensures(straight(line, i, id, tail)[1] == line[i + 1])]
pub fn lemma_straight_down<K, T, S>(
    new: IndependentWeaveView<K, T, S>,
    active: FSet<Id<K>>,
    line: Seq<Id<K>>,
    i: Int,
    id: Id<K>,
    tail: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let nodes = new.nodes;
        let rank = rank_of(nodes);
        let n = line.len();
        let after = line.subsequence(i + 1, n);
        let more = tail.tail();
        let q = straight(line, i, id, tail);
        proof_assert!(forall<k> nodes.contains(&k) ==> linked(nodes, k));
        proof_assert!(forall<r> 0 <= r && r + 1 < n ==> nodes[line[r]]@.to.contains(line[r + 1]));
        proof_assert!(forall<r> 0 <= r && r < after.len() ==> after[r] == line[i + 1 + r]);
        proof_assert!(forall<j> 0 <= j && j < more.len() ==> more[j] == tail[j + 1]);
        proof_assert!(q.len() == n - i + tail.len() - 1);
        proof_assert!(forall<j> 0 < j && j < n - i ==> q[j] == line[i + j]);
        proof_assert!(forall<j> n - i <= j && j < q.len() ==> q[j] == tail[j - (n - i) + 1]);
        proof_assert!(tail[0] == line[n - 1]);
        proof_assert!(forall<j> 0 <= j && j < q.len() ==> nodes.contains(&q[j]));
        proof_assert!(forall<j> 0 <= j && j + 1 < q.len() ==> nodes[q[j]]@.to.contains(q[j + 1]));
        lemma_down_ranks(nodes, rank, q);
        proof_assert!(q[q.len() - 1] == tail[tail.len() - 1]);
        proof_assert!(path_down(nodes, id, q));
        lemma_down_ranks(nodes, rank, tail);
        proof_assert!(forall<j> 0 <= j && j < more.len() ==> rank[line[n - 1]] < rank[more[j]] && {
            lemma_below_end_inactive(nodes, active, line, more[j]);
            !active.contains(&more[j])
        });
        lemma_count_concat(active, Seq::singleton(id).concat(after), more);
        lemma_count_concat(active, Seq::singleton(id), after);
        lemma_count_single(active, Seq::singleton(id));
        lemma_count_full(active, after);
        lemma_count_in_zero(active, more);
    }
}

/// The best path down which activation cuts to get `below`, from a node inserted after the `i`th
/// node of the active line, counts as many active nodes as going straight down the line, so it
/// does go straight down the line.
#[logic]
#[requires(new.valid())]
#[requires(line_of(new.nodes, active, line))]
#[requires(0 <= i && i + 1 < line.len())]
#[requires(new.nodes.contains(&id) && new.nodes[id]@.from == Seq::singleton(line[i]))]
#[requires(new.nodes[id]@.to.contains(line[i + 1]))]
#[requires(!active.contains(&id))]
#[requires(best_path_down(new.nodes, active, id, path))]
#[requires(1 < below.len() && below.len() <= path.len())]
#[requires(forall<j> 0 <= j && j < below.len() ==> below[j] == path[j])]
#[requires(active.contains(&below[below.len() - 1]))]
#[requires(forall<j> below.len() <= j && j < path.len() ==> !active.contains(&path[j]))]
#[requires(count_in(active, path) >= line.len() - i - 1)]
#[ensures(forall<k> below.contains(k) == (k == id || line.subsequence(i + 1, line.len()).contains(k)))]
pub fn lemma_kept_below_path<K, T, S>(
    new: IndependentWeaveView<K, T, S>,
    active: FSet<Id<K>>,
    line: Seq<Id<K>>,
    i: Int,
    id: Id<K>,
    below: Seq<Id<K>>,
    path: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let nodes = new.nodes;
        let rank = rank_of(nodes);
        let n = line.len();
        let m = below.len();
        lemma_line_ranks(nodes, rank, line);
        lemma_increasing(line, rank);
        lemma_down_ranks(nodes, rank, path);
        proof_assert!(path[0] == id && nodes[id]@.to.contains(path[1]));
        proof_assert!(linked(nodes, id) && nodes[path[1]]@.from.contains(id));
        proof_assert!(rank[line[i]] < rank[id] && rank[id] < rank[path[1]]);
        let after = line.subsequence(i + 1, n);
        let a = nodes_of(nodes, line, i + 1, n);
        lemma_nodes_of(nodes, line, i + 1, n);
        proof_assert!(forall<k> a.contains(&k) ==> active.contains(&k));

        // The active nodes of the path are the rest of the line, all of which it holds.
        let rest = path.tail();
        proof_assert!(forall<j> 0 <= j && j < rest.len() ==> rest[j] == path[j + 1]);
        proof_assert!(forall<j> 0 <= j && j < rest.len() ==> rank[line[i]] < rank[rest[j]]);
        proof_assert!(forall<j> 0 <= j && j < rest.len() && active.contains(&rest[j]) ==> {
            lemma_line_rank_le(nodes, active, line, i, rest[j]);
            after.contains(rest[j])
        } && a.contains(&rest[j]));
        lemma_count_mono(active, a, rest);
        lemma_count_set_len(a, rest);
        proof_assert!(count_in(active, path) == count_in(active, rest));
        proof_assert!(count_in(a, rest) >= a.len());
        lemma_count_cover(a, rest);
        proof_assert!(forall<k> a.contains(&k) ==> exists<j> 1 <= j && j < m && path[j] == k);

        // Its last active node is the end of the line.
        proof_assert!(after[n - i - 2] == line[n - 1] && after.contains(line[n - 1]));
        proof_assert!(a.contains(&line[n - 1]));
        let p = such_that(|p: Int| 1 <= p && p < m && path[p] == line[n - 1]);
        proof_assert!(path[m - 1] == below[m - 1] && rest[m - 2] == path[m - 1]);
        proof_assert!(a.contains(&path[m - 1]));
        proof_assert!(exists<r> i + 1 <= r && r < n && line[r] == path[m - 1]);
        proof_assert!(rank[path[m - 1]] <= rank[line[n - 1]]);
        proof_assert!(p == m - 1);

        // Going straight down the line from there is no worse, so it makes no detour.
        let end = path.subsequence(m - 1, path.len());
        lemma_path_down_from(nodes, id, path, m - 1);
        lemma_straight_down(new, active, line, i, id, end);
        proof_assert!(no_better(active, straight(line, i, id, end), path));
        proof_assert!(m <= n - i);

        // So its part from its second node to its last active one is the rest of the line.
        let mid = path.subsequence(1, m);
        let tail = path.subsequence(m, path.len());
        proof_assert!(forall<j> 0 <= j && j < mid.len() ==> mid[j] == path[j + 1]);
        proof_assert!(forall<j> 0 <= j && j < tail.len() ==> tail[j] == path[m + j]);
        lemma_subsequence_split(path, 1, m, path.len());
        proof_assert!(rest == mid.concat(tail));
        lemma_count_concat(a, mid, tail);
        lemma_count_mono(a, active, tail);
        lemma_count_in_zero(active, tail);
        lemma_count_in_bounds(a, tail);
        proof_assert!(count_in(a, tail) == 0);
        proof_assert!(count_in(a, mid) >= mid.len());
        lemma_count_all(a, mid);
        proof_assert!(forall<k> mid.contains(k) ==> a.contains(&k));
        proof_assert!(forall<k> a.contains(&k) ==> mid.contains(k));
        proof_assert!(below[0] == id && below.len() == mid.len() + 1);
        proof_assert!(forall<j> 1 <= j && j < m ==> below[j] == mid[j - 1]);
        lemma_cons(below, id, mid);
        proof_assert!(forall<k> below.contains(k) == (k == id || mid.contains(k)) && {
            lemma_concat_contains(Seq::singleton(id), mid, k);
            true
        });
    }
}

/// What activation keeps below a node inserted after the `i`th node of the active line, with the
/// next one as a child, is that node and the rest of the line.
#[logic]
#[requires(new.valid())]
#[requires(line_of(new.nodes, active, line))]
#[requires(0 <= i && i + 1 < line.len())]
#[requires(new.nodes.contains(&id) && new.nodes[id]@.from == Seq::singleton(line[i]))]
#[requires(new.nodes[id]@.to.contains(line[i + 1]))]
#[requires(!active.contains(&id))]
#[requires(kept_below(new.nodes, active, id, below))]
#[ensures(forall<k> below.contains(k) == (k == id || line.subsequence(i + 1, line.len()).contains(k)))]
pub fn lemma_kept_below_line<K, T, S>(
    new: IndependentWeaveView<K, T, S>,
    active: FSet<Id<K>>,
    line: Seq<Id<K>>,
    i: Int,
    id: Id<K>,
    below: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let nodes = new.nodes;
        let rank = rank_of(nodes);
        let n = line.len();
        let leaf = leaf_path(nodes, rank, set_bound(nodes.keys(), rank), line[n - 1]);
        lemma_straight_down(new, active, line, i, id, leaf);
        let q = straight(line, i, id, leaf);
        proof_assert!(1 < q.len() && q[1] == line[i + 1] && active.contains(&line[i + 1]));
        proof_assert!(!nothing_below(nodes, active, id));
        let path = such_that(|path| best_path_down(nodes, active, id, path)
            && 1 < below.len()
            && below.len() <= path.len()
            && (forall<j> 0 <= j && j < below.len() ==> below[j] == path[j])
            && active.contains(&below[below.len() - 1])
            && forall<j> below.len() <= j && j < path.len() ==> !active.contains(&path[j]));
        proof_assert!(no_better(active, q, path));
        proof_assert!(count_in(active, path) >= n - i - 1);
        lemma_kept_below_path(new, active, line, i, id, below, path)
    }
}

/// Activating a node inserted after the `i`th node of the active line, with the next one as a
/// child, puts it between them.
#[logic]
#[requires(new.valid())]
#[requires(line_of(new.nodes, active, line))]
#[requires(0 <= i && i + 1 < line.len())]
#[requires(new.nodes.contains(&id) && new.nodes[id]@.from == Seq::singleton(line[i]))]
#[requires(new.nodes[id]@.to.contains(line[i + 1]))]
#[requires(!active.contains(&id))]
#[requires(activated(new.nodes, active, id, after))]
#[ensures(line_of(new.nodes, after, split_line(line, i, id)))]
pub fn lemma_activated_between<K, T, S>(
    new: IndependentWeaveView<K, T, S>,
    active: FSet<Id<K>>,
    line: Seq<Id<K>>,
    i: Int,
    id: Id<K>,
    after: FSet<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let nodes = new.nodes;
        proof_assert!(linked(nodes, line[i]) && linked(nodes, id) && linked(nodes, line[i + 1]));
        proof_assert!(nodes[line[i]]@.to.contains(line[i + 1]) && active.contains(&line[i + 1]));
        proof_assert!(has_child_in(nodes, active, line[i]));
        proof_assert!(forall<p> nodes[id]@.from.contains(p) ==> p == line[i]);
        proof_assert!(!extends_path(nodes, active, id));
        let up = such_that(|up| exists<below> best_path_up(nodes, active, id, up)
            && kept_below(nodes, active, id, below)
            && forall<k> after.contains(&k) == (up.contains(k) || below.contains(k)));
        let below = such_that(|below| best_path_up(nodes, active, id, up)
            && kept_below(nodes, active, id, below)
            && forall<k> after.contains(&k) == (up.contains(k) || below.contains(k)));
        lemma_best_up_line(new, active, line, i, id, up);
        lemma_kept_below_line(new, active, line, i, id, below);
        let l = split_line(line, i, id);
        proof_assert!(forall<j> 0 <= j && j <= i ==> l[j] == line[j]);
        proof_assert!(l[i + 1] == id);
        proof_assert!(forall<j> i + 1 < j && j < l.len() ==> l[j] == line[j - 1]);
        proof_assert!(nodes[line[i + 1]]@.from.contains(id));
        proof_assert!(downward(nodes, l));
        proof_assert!(!line.contains(id));
        proof_assert!(no_dup(l));
        let pre = line.subsequence(0, i + 1);
        let suf = line.subsequence(i + 1, line.len());
        lemma_subsequence_split(line, 0, i + 1, line.len());
        lemma_full_prefix(line);
        proof_assert!(line == pre.concat(suf));
        proof_assert!(forall<k> {
            lemma_concat_contains(pre, suf, k);
            true
        } && line.contains(k) == (pre.contains(k) || suf.contains(k)));
        proof_assert!(forall<k> l.contains(k) == (line.contains(k) || k == id));
    }
}

/// Activating a node inserted after the `i`th node of the active line, with nothing active below
/// it, makes the active line end with it there.
#[logic]
#[requires(new.valid())]
#[requires(line_of(new.nodes, active, line))]
#[requires(0 <= i && i < line.len())]
#[requires(new.nodes.contains(&id) && new.nodes[id]@.from == Seq::singleton(line[i]))]
#[requires(below_end(new.nodes, line, new.nodes[id]@.to))]
#[requires(!active.contains(&id))]
#[requires(activated(new.nodes, active, id, after))]
#[ensures(line_of(new.nodes, after, line.subsequence(0, i + 1).snoc(id)))]
pub fn lemma_activated_after<K, T, S>(
    new: IndependentWeaveView<K, T, S>,
    active: FSet<Id<K>>,
    line: Seq<Id<K>>,
    i: Int,
    id: Id<K>,
    after: FSet<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let nodes = new.nodes;
        let rank = rank_of(nodes);
        lemma_line_ranks(nodes, rank, line);
        lemma_increasing(line, rank);
        let l = line.subsequence(0, i + 1).snoc(id);
        proof_assert!(forall<j> 0 <= j && j <= i ==> l[j] == line[j]);
        proof_assert!(l[i + 1] == id && l.len() == i + 2);
        proof_assert!(downward(nodes, l));
        proof_assert!(!line.contains(id));
        proof_assert!(no_dup(l));
        proof_assert!(forall<k> l.contains(k) == (line.subsequence(0, i + 1).contains(k) || k == id));
        if i + 1 == line.len() {
            proof_assert!(forall<c> nodes[line[i]]@.to.contains(c) && active.contains(&c) ==> {
                proof_assert!(linked(nodes, c) && nodes[c]@.from.contains(line[i]));
                lemma_line_rank_le(nodes, active, line, i, c);
                false
            });
            proof_assert!(!has_child_in(nodes, active, line[i]));
            proof_assert!(extends_path(nodes, active, id));
            proof_assert!(line.subsequence(0, i + 1).ext_eq(line));
        } else {
            proof_assert!(linked(nodes, line[i]) && linked(nodes, line[i + 1]));
            proof_assert!(nodes[line[i]]@.to.contains(line[i + 1]) && active.contains(&line[i + 1]));
            proof_assert!(has_child_in(nodes, active, line[i]));
            proof_assert!(forall<p> nodes[id]@.from.contains(p) ==> p == line[i]);
            proof_assert!(!extends_path(nodes, active, id));
            let up = such_that(|up| exists<below> best_path_up(nodes, active, id, up)
                && kept_below(nodes, active, id, below)
                && forall<k> after.contains(&k) == (up.contains(k) || below.contains(k)));
            let below = such_that(|below| best_path_up(nodes, active, id, up)
                && kept_below(nodes, active, id, below)
                && forall<k> after.contains(&k) == (up.contains(k) || below.contains(k)));
            lemma_best_up_line(new, active, line, i, id, up);
            lemma_nothing_below_line(new, active, line, id);
            proof_assert!(below == Seq::singleton(id) || {
                proof_assert!(1 < below.len());
                false
            });
        }
    }
}

/// Activating a new root with nothing active below it makes it the whole active line.
#[logic]
#[requires(new.valid())]
#[requires(line_of(new.nodes, active, line))]
#[requires(new.nodes.contains(&id) && new.nodes[id]@.from.len() == 0)]
#[requires(below_end(new.nodes, line, new.nodes[id]@.to))]
#[requires(!active.contains(&id))]
#[requires(activated(new.nodes, active, id, after))]
#[ensures(line_of(new.nodes, after, Seq::singleton(id)))]
pub fn lemma_activated_root<K, T, S>(
    new: IndependentWeaveView<K, T, S>,
    active: FSet<Id<K>>,
    line: Seq<Id<K>>,
    id: Id<K>,
    after: FSet<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let nodes = new.nodes;
        proof_assert!(downward(nodes, Seq::singleton(id)) && no_dup(Seq::singleton(id)));
        if active.len() == 0 {
            proof_assert!(extends_path(nodes, active, id));
            proof_assert!(forall<k> !active.contains(&k));
            proof_assert!(forall<k> after.contains(&k) == (k == id));
        } else {
            proof_assert!(!extends_path(nodes, active, id));
            let up = such_that(|up| exists<below> best_path_up(nodes, active, id, up)
                && kept_below(nodes, active, id, below)
                && forall<k> after.contains(&k) == (up.contains(k) || below.contains(k)));
            let below = such_that(|below| best_path_up(nodes, active, id, up)
                && kept_below(nodes, active, id, below)
                && forall<k> after.contains(&k) == (up.contains(k) || below.contains(k)));
            proof_assert!(up[0] == id && valid_path(nodes, up));
            proof_assert!(up.len() == 1);
            proof_assert!(up.ext_eq(Seq::singleton(id)));
            lemma_nothing_below_line(new, active, line, id);
            proof_assert!(below == Seq::singleton(id) || {
                proof_assert!(1 < below.len());
                false
            });
            proof_assert!(forall<k> after.contains(&k) == (k == id));
        }
    }
}

// Building and inserting nodes.

/// What `IndexSet::from_iter` collects from `empty()`, `once(x)` and an `Option`, without
/// duplicates.
#[logic(prophetic)]
#[ensures(forall<e: Empty<K>, items: Seq<K>, done: &mut Empty<K>> e.produces(items, *done)
    ==> items == Seq::empty())]
#[ensures(forall<o: Once<K>, items: Seq<K>, done: &mut Once<K>>
    o.produces(items, *done) && done.completed()
    ==> items == match o@ { Some(x) => Seq::singleton(x), None => Seq::empty() })]
#[ensures(forall<o: OptionIntoIter<K>, items: Seq<K>, done: &mut OptionIntoIter<K>>
    o.produces(items, *done) && done.completed()
    ==> items == match o@ { Some(x) => Seq::singleton(x), None => Seq::empty() })]
#[ensures(dedup(ids(Seq::<K>::empty())) == Seq::empty())]
#[ensures(forall<x: K> dedup(ids(Seq::singleton(x))) == Seq::singleton(x.deep_model()))]
pub fn lemma_collected<K: DeepModel>() {
    pearlite! {
        proof_assert!(ids(Seq::<K>::empty()).len() == 0);
        proof_assert!(forall<x: K> ids(Seq::singleton(x)).len() == 1
            && ids(Seq::singleton(x))[0] == x.deep_model()
            && ids(Seq::singleton(x)).subsequence(0, 0).len() == 0
            && dedup(ids(Seq::singleton(x)).subsequence(0, 0)) == Seq::empty()
            && Seq::<Id<K>>::empty().snoc(x.deep_model()).ext_eq(Seq::singleton(x.deep_model())));
    }
}

/// A new node without parents, whose children exist, can be inserted.
#[logic]
#[requires(!nodes.contains(&n.id))]
#[requires(n.from.len() == 0)]
#[requires(no_dup(n.to) && forall<c> n.to.contains(c) ==> nodes.contains(&c))]
#[ensures(insertable(nodes, n))]
pub fn lemma_insertable_root<K, T, S>(nodes: Nodes<K, T, S>, n: NodeView<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(n.valid());
        proof_assert!(separates(nodes, n, |_k: Id<K>| false));
    }
}

/// The nodes of `set` exist and are ranked above `p`.
#[logic(open)]
pub fn ranked_above<K, T, S>(nodes: Nodes<K, T, S>, p: Id<K>, set: Seq<Id<K>>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<c> set.contains(c) ==> nodes.contains(&c) && rank_of(nodes)[p] < rank_of(nodes)[c]
    }
}

/// A new node with one parent, whose children exist and are ranked above that parent, can be
/// inserted.
#[logic]
#[requires(all_linked(nodes) && acyclic(nodes))]
#[requires(!nodes.contains(&n.id))]
#[requires(n.from == Seq::singleton(p) && nodes.contains(&p))]
#[requires(no_dup(n.to))]
#[requires(ranked_above(nodes, p, n.to))]
#[ensures(insertable(nodes, n))]
pub fn lemma_insertable_under<K, T, S>(nodes: Nodes<K, T, S>, n: NodeView<Id<K>>, p: Id<K>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let rank = rank_of(nodes);
        proof_assert!(!n.to.contains(p));
        proof_assert!(n.valid());
        proof_assert!(separates(nodes, n, |k: Id<K>| rank[k] <= rank[p]));
    }
}

/// Inserting a node after the `j`th node of a line, or before the line if `j` is -1, with the
/// next node of the line as a child, makes a line with the node there, whose text has the node's
/// contents there. With a parent on the line, the line is still one.
#[logic]
#[requires(old.valid() && new.valid())]
#[requires(inserted(old, new, n, contents))]
#[requires(!old.nodes.contains(&n.id))]
#[requires(downward(old.nodes, line) && no_dup(line))]
#[requires(-1 <= j && j < line.len())]
#[requires(0 <= j ==> n.from == Seq::singleton(line[j]))]
#[requires(j == -1 ==> n.from.len() == 0)]
#[requires(j + 1 < line.len() ==> n.to.contains(line[j + 1]))]
#[ensures(downward(new.nodes, split_line(line, j, n.id)) && no_dup(split_line(line, j, n.id)))]
#[ensures(forall<k> split_line(line, j, n.id).contains(k) == (line.contains(k) || k == n.id))]
#[ensures(0 <= j ==> downward(new.nodes, line))]
#[ensures(forall<i> 0 <= i && i < line.len() ==> new.nodes[line[i]].contents == old.nodes[line[i]].contents)]
#[ensures(text(new.nodes, line) == text(old.nodes, line))]
#[ensures(text(new.nodes, split_line(line, j, n.id))
    == text(old.nodes, line.subsequence(0, j + 1)).concat(contents.units())
        .concat(text(old.nodes, line.subsequence(j + 1, line.len()))))]
pub fn lemma_insert_line<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    n: NodeView<Id<K>>,
    contents: T,
    line: Seq<Id<K>>,
    j: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let id = n.id;
        let l = split_line(line, j, id);
        let rank = rank_of(new.nodes);
        proof_assert!(!line.contains(id));
        proof_assert!(forall<i> 0 <= i && i <= j ==> l[i] == line[i]);
        proof_assert!(l[j + 1] == id && l.len() == line.len() + 1);
        proof_assert!(forall<i> j + 1 < i && i < l.len() ==> l[i] == line[i - 1]);
        proof_assert!(forall<i> 0 <= i && i < line.len() ==> new.nodes.contains(&line[i]));
        proof_assert!(forall<k, p> old.nodes.contains(&k) && old.nodes[k]@.from.contains(p) ==> {
            lemma_snoc_contains(old.nodes[k]@.from, id, p);
            true
        } && new.nodes[k]@.from.contains(p));
        proof_assert!(forall<i> 0 <= i && i + 1 < line.len()
            ==> new.nodes[line[i + 1]]@.from.contains(line[i]));
        proof_assert!(new.nodes[id]@.from == n.from);
        proof_assert!(j + 1 < line.len() ==> new.nodes[line[j + 1]]@.from.contains(id));
        proof_assert!(0 <= j && 0 < line.len() ==> {
            lemma_line_ranks(new.nodes, rank, line.subsequence(0, 0));
            proof_assert!(increasing_steps(line, rank));
            lemma_increasing(line, rank);
            proof_assert!(rank[line[0]] <= rank[line[j]] && rank[line[j]] < rank[id]);
            proof_assert!(n.to.contains(line[0]) ==> new.nodes[line[0]]@.from.contains(id)
                && rank[id] < rank[line[0]]);
            true
        } && !n.to.contains(line[0]) && new.nodes[line[0]]@.from.len() == 0);
        proof_assert!(0 <= j ==> downward(new.nodes, line));
        proof_assert!(forall<i> 0 <= i && i < l.len() ==> new.nodes.contains(&l[i]));
        proof_assert!(forall<i> 0 <= i && i + 1 <= j ==> new.nodes[l[i + 1]]@.from.contains(l[i]));
        proof_assert!(0 <= j ==> new.nodes[l[j + 1]]@.from.contains(l[j]));
        proof_assert!(j + 2 < l.len() ==> new.nodes[l[j + 2]]@.from.contains(l[j + 1]));
        proof_assert!(forall<i> j + 1 < i && i + 1 < l.len()
            ==> l[i] == line[i - 1] && l[i + 1] == line[i]
                && new.nodes[l[i + 1]]@.from.contains(l[i]));
        proof_assert!(forall<i> 0 <= i && i + 1 < l.len() ==> new.nodes[l[i + 1]]@.from.contains(l[i]));
        proof_assert!(new.nodes[l[0]]@.from.len() == 0);
        proof_assert!(downward(new.nodes, l));
        proof_assert!(forall<a, b> 0 <= a && a < b && b < l.len() ==> l[a] != l[b]);
        proof_assert!(forall<k> l.contains(k) == (line.contains(k) || k == id));
        let pre = line.subsequence(0, j + 1);
        let suf = line.subsequence(j + 1, line.len());
        proof_assert!(forall<i> 0 <= i && i < line.len() ==> new.nodes[line[i]].contents == old.nodes[line[i]].contents);
        proof_assert!(forall<i> 0 <= i && i < pre.len() ==> pre[i] == line[i]);
        proof_assert!(forall<i> 0 <= i && i < suf.len() ==> suf[i] == line[j + 1 + i]);
        lemma_text_same(new.nodes, old.nodes, pre);
        lemma_text_same(new.nodes, old.nodes, suf);
        lemma_text_same(new.nodes, old.nodes, line);
        proof_assert!(l.ext_eq(pre.concat(Seq::singleton(id)).concat(suf)));
        lemma_text_concat(new.nodes, pre.concat(Seq::singleton(id)), suf);
        lemma_text_concat(new.nodes, pre, Seq::singleton(id));
        lemma_text_single(new.nodes, id);
    }
}

/// Graphs which differ only by which nodes are active have the same lines and texts.
#[logic]
#[requires(same_graph(new, old))]
#[ensures(downward(new, line) == downward(old, line))]
#[ensures(text(new, line) == text(old, line))]
pub fn lemma_same_graph_line<K, T, S>(new: Nodes<K, T, S>, old: Nodes<K, T, S>, line: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        lemma_text_same(new, old, line)
    }
}

// Splicing.

/// Inserting `x` at the boundary between `a` and `b`.
#[logic]
#[requires((if at < a.len() + b.len() { at } else { a.len() + b.len() }) == a.len())]
#[ensures(spliced(a.concat(b), at, at, x) == a.concat(x).concat(b))]
pub fn lemma_spliced_between<U>(a: Seq<U>, b: Seq<U>, at: Int, x: Seq<U>) {
    pearlite! {
        let t = a.concat(b);
        proof_assert!(t.subsequence(0, a.len()).ext_eq(a));
        proof_assert!(t.subsequence(a.len(), t.len()).ext_eq(b));
    }
}

/// Inserting `x` at `k` inside `c`, between `a` and `b`.
#[logic]
#[requires(0 < k && k < c.len())]
#[ensures(spliced(a.concat(c).concat(b), a.len() + k, a.len() + k, x)
    == a.concat(c.subsequence(0, k)).concat(x).concat(c.subsequence(k, c.len())).concat(b))]
pub fn lemma_spliced_inside<U>(a: Seq<U>, c: Seq<U>, b: Seq<U>, k: Int, x: Seq<U>) {
    pearlite! {
        let t = a.concat(c).concat(b);
        let at = a.len() + k;
        proof_assert!(t.subsequence(0, at).ext_eq(a.concat(c.subsequence(0, k))));
        proof_assert!(t.subsequence(at, t.len()).ext_eq(c.subsequence(k, c.len()).concat(b)));
        let p = a.concat(c.subsequence(0, k)).concat(x);
        p.concat_assoc(c.subsequence(k, c.len()), b);
    }
}

/// The models of concatenated identifiers are the concatenated models, and those of one
/// identifier are its model.
#[logic]
#[ensures(forall<a: Seq<K>, b: Seq<K>> ids(a.concat(b)) == ids(a).concat(ids(b)))]
#[ensures(forall<x: K> ids(Seq::singleton(x)) == Seq::singleton(x.deep_model()))]
pub fn lemma_ids_concat<K: DeepModel>() {
    pearlite! {
        proof_assert!(forall<a: Seq<K>, b: Seq<K>> ids(a.concat(b)).ext_eq(ids(a).concat(ids(b))));
        proof_assert!(forall<x: K> ids(Seq::singleton(x)).ext_eq(Seq::singleton(x.deep_model())));
    }
}

/// The parts of `x` followed by `t`.
#[logic]
#[ensures(Seq::singleton(x).concat(t).subsequence(0, 1) == Seq::singleton(x))]
#[ensures(Seq::singleton(x).concat(t).subsequence(1, t.len() + 1) == t)]
#[ensures(Seq::singleton(x).concat(t).len() == t.len() + 1)]
#[ensures(Seq::singleton(x).concat(t)[0] == x)]
#[ensures(forall<i> 0 <= i && i < t.len() ==> Seq::singleton(x).concat(t)[i + 1] == t[i])]
#[ensures(split_line(Seq::singleton(x).concat(t), 0, y)
    == Seq::singleton(x).concat(Seq::singleton(y)).concat(t))]
pub fn lemma_cons_parts<I>(x: I, t: Seq<I>, y: I) {
    pearlite! {
        let s = Seq::singleton(x).concat(t);
        proof_assert!(s.subsequence(0, 1).ext_eq(Seq::singleton(x)));
        proof_assert!(s.subsequence(1, t.len() + 1).ext_eq(t));
        proof_assert!(split_line(s, 0, y).ext_eq(Seq::singleton(x).concat(Seq::singleton(y)).concat(t)));
    }
}

/// Inserting `r` and then `y` after the `t`th element.
#[logic]
#[requires(0 <= t && t < line.len())]
#[ensures(split_line(split_line(line, t, r), t, y)
    == line.subsequence(0, t + 1).concat(Seq::singleton(y)).concat(Seq::singleton(r))
        .concat(line.subsequence(t + 1, line.len())))]
pub fn lemma_split_twice<I>(line: Seq<I>, t: Int, r: I, y: I) {
    pearlite! {
        let s = split_line(line, t, r);
        proof_assert!(s.subsequence(0, t + 1).ext_eq(line.subsequence(0, t + 1)));
        proof_assert!(s.subsequence(t + 1, s.len())
            .ext_eq(Seq::singleton(r).concat(line.subsequence(t + 1, line.len()))));
        proof_assert!(split_line(s, t, y).ext_eq(line.subsequence(0, t + 1).concat(Seq::singleton(y))
            .concat(Seq::singleton(r)).concat(line.subsequence(t + 1, line.len()))));
    }
}

/// The models of a part of a sequence of identifiers are that part of their models.
#[logic]
#[requires(0 <= a && a <= b && b <= s.len())]
#[ensures(ids(s.subsequence(a, b)) == ids(s).subsequence(a, b))]
pub fn lemma_ids_sub<K: DeepModel>(s: Seq<K>, a: Int, b: Int) {
    proof_assert!(ids(s.subsequence(a, b)).ext_eq(ids(s).subsequence(a, b)))
}

/// A line with `y` after its last node.
#[logic]
#[requires(0 < line.len())]
#[ensures(split_line(line, line.len() - 1, y) == line.snoc(y))]
#[ensures(line.subsequence(0, line.len()).snoc(y) == line.snoc(y))]
pub fn lemma_split_line_end<I>(line: Seq<I>, y: I) {
    pearlite! {
        proof_assert!(split_line(line, line.len() - 1, y).ext_eq(line.snoc(y)));
        proof_assert!(line.subsequence(0, line.len()).ext_eq(line));
    }
}

/// A sequence is its part up to an index followed by its part from there.
#[logic]
#[requires(0 <= k && k <= t.len())]
#[ensures(t.subsequence(0, k).concat(t.subsequence(k, t.len())) == t)]
pub fn lemma_halves<U>(t: Seq<U>, k: Int) {
    proof_assert!(t.subsequence(0, k).concat(t.subsequence(k, t.len())).ext_eq(t))
}


// Cutting and refining the active line.

/// Splitting the `t`th node of a line exactly at `at` keeps the line's text, in a line with the
/// new node after the split one.
#[logic]
#[requires(old.valid())]
#[requires(downward(old.nodes, line) && no_dup(line))]
#[requires(0 <= t && t < line.len())]
#[requires(split_shape(old, new, line[t], y))]
#[requires(forall<k> old.nodes.contains(&k) && k != line[t]
    ==> new.nodes[k].contents == old.nodes[k].contents)]
#[requires(0 <= at && at <= old.nodes[line[t]].contents.units().len())]
#[requires(new.nodes[line[t]].contents.units() == old.nodes[line[t]].contents.units().subsequence(0, at))]
#[requires(new.nodes[y].contents.units()
    == old.nodes[line[t]].contents.units().subsequence(at, old.nodes[line[t]].contents.units().len()))]
#[ensures(downward(new.nodes, split_line(line, t, y)) && no_dup(split_line(line, t, y)))]
#[ensures(split_line(line, t, y).len() == line.len() + 1)]
#[ensures(forall<k> split_line(line, t, y).contains(k) == (line.contains(k) || k == y))]
#[ensures(text(new.nodes, split_line(line, t, y)) == text(old.nodes, line))]
pub fn lemma_split_refines<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    line: Seq<Id<K>>,
    t: Int,
    y: Id<K>,
    at: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let c = old.nodes[line[t]].contents.units();
        lemma_split_line(old, new, line, t, y);
        lemma_text_around(old.nodes, line, t);
        lemma_halves(c, at);
        let a = text(old.nodes, line.subsequence(0, t));
        a.concat_assoc(c.subsequence(0, at), c.subsequence(at, c.len()));
    }
}

/// The prefixes of a line with `y` inserted after its `t`th element.
#[logic]
#[requires(0 <= t && t < line.len())]
#[requires(0 <= j && j <= line.len())]
#[ensures(j <= t + 1 ==> split_line(line, t, y).subsequence(0, j) == line.subsequence(0, j))]
#[ensures(t < j ==> split_line(line, t, y).subsequence(0, j + 1)
    == line.subsequence(0, t + 1).concat(Seq::singleton(y)).concat(line.subsequence(t + 1, j)))]
#[ensures(t < j ==> line.subsequence(0, j)
    == line.subsequence(0, t).concat(Seq::singleton(line[t])).concat(line.subsequence(t + 1, j)))]
#[ensures(line.subsequence(0, t + 1) == line.subsequence(0, t).concat(Seq::singleton(line[t])))]
pub fn lemma_split_line_prefix<I>(line: Seq<I>, t: Int, y: I, j: Int) {
    pearlite! {
        let l = split_line(line, t, y);
        proof_assert!(l.len() == line.len() + 1);
        proof_assert!(forall<i> 0 <= i && i <= t ==> l[i] == line[i]);
        proof_assert!(l[t + 1] == y);
        proof_assert!(forall<i> t + 1 < i && i < l.len() ==> l[i] == line[i - 1]);
        proof_assert!(j <= t + 1 ==> l.subsequence(0, j).ext_eq(line.subsequence(0, j)));
        let m = line.subsequence(t + 1, j);
        proof_assert!(t < j ==> forall<i> 0 <= i && i < m.len() ==> m[i] == line[t + 1 + i]);
        proof_assert!(t < j ==> l.subsequence(0, j + 1)
            .ext_eq(line.subsequence(0, t + 1).concat(Seq::singleton(y)).concat(m)));
        proof_assert!(t < j ==> line.subsequence(0, j)
            .ext_eq(line.subsequence(0, t).concat(Seq::singleton(line[t])).concat(m)));
        proof_assert!(line.subsequence(0, t + 1).ext_eq(line.subsequence(0, t).concat(Seq::singleton(line[t]))));
    }
}

/// Where the parts of a line split as in [`lemma_split_refines`] end: those up to the split node
/// where they did, the split node `at` units into its former contents, and later ones where they
/// did.
#[logic]
#[requires(old.valid())]
#[requires(downward(old.nodes, line) && no_dup(line))]
#[requires(0 <= t && t < line.len())]
#[requires(split_shape(old, new, line[t], y))]
#[requires(forall<k> old.nodes.contains(&k) && k != line[t]
    ==> new.nodes[k].contents == old.nodes[k].contents)]
#[requires(0 <= at && at <= old.nodes[line[t]].contents.units().len())]
#[requires(new.nodes[line[t]].contents.units() == old.nodes[line[t]].contents.units().subsequence(0, at))]
#[requires(new.nodes[y].contents.units()
    == old.nodes[line[t]].contents.units().subsequence(at, old.nodes[line[t]].contents.units().len()))]
#[requires(0 <= j && j <= line.len())]
#[ensures(j <= t ==> text(new.nodes, split_line(line, t, y).subsequence(0, j)).len()
    == text(old.nodes, line.subsequence(0, j)).len())]
#[ensures(j == t + 1 ==> text(new.nodes, split_line(line, t, y).subsequence(0, j)).len()
    == text(old.nodes, line.subsequence(0, t)).len() + at)]
#[ensures(t < j ==> text(new.nodes, split_line(line, t, y).subsequence(0, j + 1)).len()
    == text(old.nodes, line.subsequence(0, j)).len())]
pub fn lemma_split_offset<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    line: Seq<Id<K>>,
    t: Int,
    y: Id<K>,
    at: Int,
    j: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let l = split_line(line, t, y);
        let x = line[t];
        lemma_split_line(old, new, line, t, y);
        proof_assert!(forall<i> 0 <= i && i < t ==> line[i] != x);
        proof_assert!(forall<i> t < i && i < line.len() ==> line[i] != x);
        lemma_split_line_prefix(line, t, y, j);
        if j <= t {
            let a = line.subsequence(0, j);
            proof_assert!(forall<i> 0 <= i && i < j ==> a[i] == line[i]);
            lemma_text_same(new.nodes, old.nodes, a);
        } else if j == t + 1 {
            proof_assert!(text(new.nodes, l.subsequence(0, t + 1))
                == text(old.nodes, line.subsequence(0, t)).concat(new.nodes[x].contents.units()));
        } else {
            let mid = line.subsequence(t + 1, j);
            let pre = line.subsequence(0, t + 1);
            proof_assert!(forall<i> 0 <= i && i < mid.len() ==> mid[i] == line[t + 1 + i]);
            lemma_text_concat(new.nodes, pre.concat(Seq::singleton(y)), mid);
            lemma_text_concat(new.nodes, pre, Seq::singleton(y));
            lemma_text_single(new.nodes, y);
            lemma_text_same(new.nodes, old.nodes, mid);
            lemma_split_line_prefix(line, t, y, t + 1);
            proof_assert!(text(new.nodes, pre)
                == text(old.nodes, line.subsequence(0, t)).concat(new.nodes[x].contents.units()));
            lemma_text_concat(old.nodes, line.subsequence(0, t).concat(Seq::singleton(x)), mid);
            lemma_text_concat(old.nodes, line.subsequence(0, t), Seq::singleton(x));
            lemma_text_single(old.nodes, x);
        }
    }
}

/// The part of a line up to `p`, followed by its part from `q`, is a line when the `q`th node has
/// the one before `p` as a parent, or when nothing follows.
#[logic]
#[requires(downward(nodes, line) && no_dup(line))]
#[requires(0 <= p && p <= q && q <= line.len())]
#[requires(0 < p && q < line.len() ==> nodes[line[q]]@.from.contains(line[p - 1]))]
#[requires(p == 0 && q < line.len() ==> nodes[line[q]]@.from.len() == 0)]
#[ensures(downward(nodes, cut(line, p, q)) && no_dup(cut(line, p, q)))]
#[ensures(forall<k> cut(line, p, q).contains(k)
    == (line.subsequence(0, p).contains(k) || line.subsequence(q, line.len()).contains(k)))]
pub fn lemma_cut_line<K, T, S>(nodes: Nodes<K, T, S>, line: Seq<Id<K>>, p: Int, q: Int)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let l = cut(line, p, q);
        proof_assert!(l.len() == p + line.len() - q);
        proof_assert!(forall<i> 0 <= i && i < p ==> l[i] == line[i]);
        proof_assert!(forall<i> p <= i && i < l.len() ==> l[i] == line[i - p + q]);
        proof_assert!(forall<i> 0 <= i && i + 1 < l.len() ==> nodes[l[i + 1]]@.from.contains(l[i]));
        proof_assert!(forall<a, b> 0 <= a && a < b && b < l.len() ==> l[a] != l[b]);
        lemma_concat_contains::<Id<K>>(line.subsequence(0, p), line.subsequence(q, line.len()), such_that(|_k: Id<K>| true));
        proof_assert!(forall<k> {
            lemma_concat_contains(line.subsequence(0, p), line.subsequence(q, line.len()), k);
            true
        } && l.contains(k) == (line.subsequence(0, p).contains(k) || line.subsequence(q, line.len()).contains(k)));
    }
}

/// The part of `line` up to `p`, followed by its part from `q`.
#[logic(open)]
pub fn cut<I>(line: Seq<I>, p: Int, q: Int) -> Seq<I> {
    pearlite! { line.subsequence(0, p).concat(line.subsequence(q, line.len())) }
}

/// The text of a line cut from `p` to `q` is its text cut where those parts end and start.
#[logic]
#[requires(0 <= p && p <= q && q <= line.len())]
#[ensures(text(nodes, cut(line, p, q))
    == text(nodes, line).subsequence(0, text(nodes, line.subsequence(0, p)).len())
        .concat(text(nodes, line).subsequence(text(nodes, line.subsequence(0, q)).len(), text(nodes, line).len())))]
pub fn lemma_cut_text<K, T, S>(nodes: Nodes<K, T, S>, line: Seq<Id<K>>, p: Int, q: Int)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        lemma_text_concat(nodes, line.subsequence(0, p), line.subsequence(q, line.len()));
        lemma_text_split(nodes, line, p);
        lemma_text_split(nodes, line, q);
    }
}

/// The nodes of `parents` exist and are ranked below `id`.
#[logic(open)]
pub fn ranked_under<K, T, S>(nodes: Nodes<K, T, S>, parents: Seq<Id<K>>, id: Id<K>) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        forall<q> parents.contains(q) ==> nodes.contains(&q) && rank_of(nodes)[q] < rank_of(nodes)[id]
    }
}

/// Nodes ranked below `id` don't descend from it.
#[logic]
#[requires(all_linked(nodes) && acyclic(nodes))]
#[requires(nodes.contains(&id))]
#[requires(ranked_under(nodes, parents, id))]
#[ensures(not_below(nodes, id, parents))]
pub fn lemma_not_below_ranked<K, T, S>(nodes: Nodes<K, T, S>, id: Id<K>, parents: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<q> parents.contains(q) ==> !descends(nodes, q, id) || {
            lemma_descends_rank(nodes, rank_of(nodes), q, id);
            false
        })
    }
}

/// Moving a node under parents which include its former ones keeps every link and contents, so
/// it keeps lines which don't start at it, and their texts.
#[logic]
#[requires(old.valid())]
#[requires(moved(old, new, id, parents))]
#[requires(forall<x> old.nodes[id]@.from.contains(x) ==> parents.contains(x))]
#[requires(downward(old.nodes, line))]
#[requires(0 < line.len() ==> line[0] != id)]
#[ensures(downward(new.nodes, line))]
#[ensures(text(new.nodes, line) == text(old.nodes, line))]
#[ensures(forall<k> old.nodes.contains(&k) ==> new.nodes[k].contents == old.nodes[k].contents)]
pub fn lemma_moved_line<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    id: Id<K>,
    parents: Seq<Id<K>>,
    line: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<k, x> old.nodes.contains(&k) && old.nodes[k]@.from.contains(x)
            ==> new.nodes[k]@.from.contains(x));
        proof_assert!(forall<i> 0 <= i && i + 1 < line.len() ==> new.nodes[line[i + 1]]@.from.contains(line[i]));
        proof_assert!(0 < line.len() ==> new.nodes[line[0]]@.from == old.nodes[line[0]]@.from);
        lemma_text_same(new.nodes, old.nodes, line)
    }
}

/// Inserting a node after the `p - 1`th node of a line, or before it if `p` is 0, with its `q`th
/// node as a child, makes a line of the line cut from `p` to `q` with the node in between, whose
/// text has the node's contents there.
#[logic]
#[requires(old.valid() && new.valid())]
#[requires(inserted(old, new, n, contents))]
#[requires(!old.nodes.contains(&n.id))]
#[requires(downward(old.nodes, line) && no_dup(line))]
#[requires(0 <= p && p <= q && q <= line.len())]
#[requires(0 < p ==> n.from == Seq::singleton(line[p - 1]))]
#[requires(p == 0 ==> n.from.len() == 0)]
#[requires(q < line.len() ==> n.to.contains(line[q]))]
#[ensures(downward(new.nodes, between(line, p, q, n.id)) && no_dup(between(line, p, q, n.id)))]
#[ensures(forall<k> between(line, p, q, n.id).contains(k)
    == (line.subsequence(0, p).contains(k) || line.subsequence(q, line.len()).contains(k) || k == n.id))]
#[ensures(text(new.nodes, between(line, p, q, n.id))
    == text(old.nodes, line.subsequence(0, p)).concat(contents.units())
        .concat(text(old.nodes, line.subsequence(q, line.len()))))]
#[ensures(0 < p ==> downward(new.nodes, line))]
#[ensures(n.to.len() == 0 ==> downward(new.nodes, line))]
#[ensures(0 < line.len() && !n.to.contains(line[0]) ==> downward(new.nodes, line))]
pub fn lemma_insert_between<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    n: NodeView<Id<K>>,
    contents: T,
    line: Seq<Id<K>>,
    p: Int,
    q: Int,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let id = n.id;
        let pre = line.subsequence(0, p);
        let suf = line.subsequence(q, line.len());
        let l = between(line, p, q, id);
        let rank = rank_of(new.nodes);
        proof_assert!(!line.contains(id));
        proof_assert!(forall<i> 0 <= i && i < pre.len() ==> pre[i] == line[i]);
        proof_assert!(forall<i> 0 <= i && i < suf.len() ==> suf[i] == line[q + i]);
        proof_assert!(l.len() == p + 1 + line.len() - q);
        proof_assert!(forall<i> 0 <= i && i < p ==> l[i] == line[i]);
        proof_assert!(l[p] == id);
        proof_assert!(forall<i> p < i && i < l.len() ==> l[i] == line[i - p - 1 + q]);
        proof_assert!(forall<i> 0 <= i && i < line.len() ==> new.nodes.contains(&line[i]));
        proof_assert!(forall<k, x> old.nodes.contains(&k) && old.nodes[k]@.from.contains(x) ==> {
            lemma_snoc_contains(old.nodes[k]@.from, id, x);
            true
        } && new.nodes[k]@.from.contains(x));
        proof_assert!(forall<i> 0 <= i && i + 1 < line.len()
            ==> new.nodes[line[i + 1]]@.from.contains(line[i]));
        proof_assert!(new.nodes[id]@.from == n.from);
        proof_assert!(q < line.len() ==> new.nodes[line[q]]@.from.contains(id));
        proof_assert!(0 < p && 0 < line.len() ==> {
            proof_assert!(increasing_steps(line, rank));
            lemma_increasing(line, rank);
            proof_assert!(rank[line[0]] <= rank[line[p - 1]] && rank[line[p - 1]] < rank[id]);
            proof_assert!(n.to.contains(line[0]) ==> new.nodes[line[0]]@.from.contains(id)
                && rank[id] < rank[line[0]]);
            true
        } && !n.to.contains(line[0]) && new.nodes[line[0]]@.from.len() == 0);
        proof_assert!(0 < p ==> downward(new.nodes, line));
        proof_assert!(n.to.len() == 0 && 0 < line.len() ==> new.nodes[line[0]]@.from == old.nodes[line[0]]@.from);
        proof_assert!(n.to.len() == 0 ==> downward(new.nodes, line));
        proof_assert!(0 < line.len() && !n.to.contains(line[0])
            ==> new.nodes[line[0]]@.from == old.nodes[line[0]]@.from && downward(new.nodes, line));
        proof_assert!(forall<i> 0 <= i && i < l.len() ==> new.nodes.contains(&l[i]));
        proof_assert!(forall<i> 0 <= i && i + 1 < p ==> new.nodes[l[i + 1]]@.from.contains(l[i]));
        proof_assert!(0 < p ==> new.nodes[l[p]]@.from.contains(l[p - 1]));
        proof_assert!(p + 1 < l.len() ==> new.nodes[l[p + 1]]@.from.contains(l[p]));
        proof_assert!(forall<i> p < i && i + 1 < l.len()
            ==> l[i] == line[i - p - 1 + q] && l[i + 1] == line[i - p + q]
                && new.nodes[l[i + 1]]@.from.contains(l[i]));
        proof_assert!(forall<i> 0 <= i && i + 1 < l.len() ==> new.nodes[l[i + 1]]@.from.contains(l[i]));
        proof_assert!(new.nodes[l[0]]@.from.len() == 0);
        proof_assert!(downward(new.nodes, l));
        proof_assert!(forall<a, b> 0 <= a && a < b && b < l.len() ==> l[a] != l[b]);
        proof_assert!(forall<k> {
            lemma_concat_contains(pre.concat(Seq::singleton(id)), suf, k);
            lemma_concat_contains(pre, Seq::singleton(id), k);
            true
        } && l.contains(k) == (pre.contains(k) || suf.contains(k) || k == id));
        proof_assert!(forall<i> 0 <= i && i < line.len() ==> new.nodes[line[i]].contents == old.nodes[line[i]].contents);
        lemma_text_same(new.nodes, old.nodes, pre);
        lemma_text_same(new.nodes, old.nodes, suf);
        lemma_text_concat(new.nodes, pre.concat(Seq::singleton(id)), suf);
        lemma_text_concat(new.nodes, pre, Seq::singleton(id));
        lemma_text_single(new.nodes, id);
    }
}

/// The part of `line` up to `p`, then `id`, then its part from `q`.
#[logic(open)]
pub fn between<I>(line: Seq<I>, p: Int, q: Int, id: I) -> Seq<I> {
    pearlite! {
        line.subsequence(0, p).concat(Seq::singleton(id)).concat(line.subsequence(q, line.len()))
    }
}

/// Where the text of the first `j` nodes of `line` ends.
#[logic(open)]
pub fn offset<K, T, S>(nodes: Nodes<K, T, S>, line: Seq<Id<K>>, j: Int) -> Int
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! { text(nodes, line.subsequence(0, j)).len() }
}

/// Where the text of a line's first nodes ends doesn't decrease with their number.
#[logic]
#[requires(0 <= i && i <= j && j <= line.len())]
#[ensures(offset(nodes, line, i) <= offset(nodes, line, j))]
#[variant(j - i)]
pub fn lemma_offset_mono<K, T, S>(nodes: Nodes<K, T, S>, line: Seq<Id<K>>, i: Int, j: Int)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        if i < j {
            lemma_offset_mono(nodes, line, i, j - 1);
            lemma_text_step(nodes, line, j - 1);
        }
    }
}

/// The part of `l` up to `p`, when it is that of `line`, holds `line`'s first `p` elements, and
/// only elements of `line`.
#[logic]
#[requires(0 <= p && p <= line.len() && p <= l.len())]
#[requires(l.subsequence(0, p) == line.subsequence(0, p))]
#[ensures(forall<i> 0 <= i && i < p ==> l.subsequence(0, p).contains(line[i]))]
#[ensures(forall<k> l.subsequence(0, p).contains(k) ==> line.contains(k))]
pub fn lemma_prefix_part<I>(l: Seq<I>, line: Seq<I>, p: Int) {
    proof_assert!(forall<i> 0 <= i && i < p ==> line.subsequence(0, p)[i] == line[i]);
}

/// The part of `l` from `q`, when it is `e` followed by the elements of `line` from `s`, holds `e`
/// and those elements, and nothing else.
#[logic]
#[requires(0 <= s && s <= line.len() && 0 <= q && q <= l.len())]
#[requires(l.subsequence(q, l.len()) == Seq::singleton(e).concat(line.subsequence(s, line.len())))]
#[ensures(l.subsequence(q, l.len()).contains(e))]
#[ensures(forall<i> s <= i && i < line.len() ==> l.subsequence(q, l.len()).contains(line[i]))]
#[ensures(forall<k> l.subsequence(q, l.len()).contains(k) ==> line.contains(k) || k == e)]
pub fn lemma_suffix_part<I>(l: Seq<I>, line: Seq<I>, q: Int, s: Int, e: I) {
    pearlite! {
        let t = Seq::singleton(e).concat(line.subsequence(s, line.len()));
        proof_assert!(t[0] == e);
        proof_assert!(forall<i> s <= i && i < line.len() ==> t[i - s + 1] == line[i]);
        proof_assert!(forall<j> 1 <= j && j < t.len() ==> t[j] == line[j - 1 + s]);
    }
}

/// A line cut from `p` to `q` holds exactly the elements of its part before `p` and of its part
/// from `q`.
#[logic]
#[ensures(forall<k> cut(line, p, q).contains(k)
    == (line.subsequence(0, p).contains(k) || line.subsequence(q, line.len()).contains(k)))]
pub fn lemma_cut_contains<I>(line: Seq<I>, p: Int, q: Int) {
    proof_assert!(forall<k> {
        lemma_concat_contains(line.subsequence(0, p), line.subsequence(q, line.len()), k);
        true
    } && cut(line, p, q).contains(k)
        == (line.subsequence(0, p).contains(k) || line.subsequence(q, line.len()).contains(k)))
}

/// A line cut from `p` to `q` with `id` in between holds exactly `id` and the elements of its part
/// before `p` and of its part from `q`.
#[logic]
#[ensures(forall<k> between(line, p, q, id).contains(k)
    == (line.subsequence(0, p).contains(k) || line.subsequence(q, line.len()).contains(k) || k == id))]
pub fn lemma_between_contains<I>(line: Seq<I>, p: Int, q: Int, id: I) {
    proof_assert!(forall<k> {
        lemma_concat_contains(line.subsequence(0, p).concat(Seq::singleton(id)), line.subsequence(q, line.len()), k);
        lemma_concat_contains(line.subsequence(0, p), Seq::singleton(id), k);
        true
    } && between(line, p, q, id).contains(k)
        == (line.subsequence(0, p).contains(k) || line.subsequence(q, line.len()).contains(k) || k == id))
}

/// A map whose keys are keys of another map, or `x`, is at most one longer. The bound goes through
/// the other map's keys and `x`, so that the provers can apply it: they don't learn `x` otherwise.
#[logic]
#[requires(forall<k> a.contains(&k) ==> b.contains(&k) || k == x)]
#[ensures(a.len() <= b.keys().add(x).len())]
#[ensures(b.keys().add(x).len() <= b.len() + 1)]
pub fn lemma_keys_one_more_len<K, V, W>(a: FMap<K, V>, b: FMap<K, W>, x: K) {
    proof_assert!(a.keys().is_subset(b.keys().add(x)));
}

/// The suffixes of a line with `y` inserted after its `t`th element, from after it.
#[logic]
#[requires(0 <= t && t < line.len())]
#[requires(t < j && j <= line.len())]
#[ensures(split_line(line, t, y).subsequence(j + 1, line.len() + 1) == line.subsequence(j, line.len()))]
pub fn lemma_split_line_suffix<I>(line: Seq<I>, t: Int, y: I, j: Int) {
    pearlite! {
        let l = split_line(line, t, y);
        proof_assert!(l.len() == line.len() + 1);
        proof_assert!(forall<i> t + 1 < i && i < l.len() ==> l[i] == line[i - 1]);
        proof_assert!(l.subsequence(j + 1, line.len() + 1).ext_eq(line.subsequence(j, line.len())));
    }
}

/// The elements of a line with `y` inserted after its `t`th element, and its part from `y`.
#[logic]
#[requires(0 <= t && t < line.len())]
#[ensures(split_line(line, t, y).subsequence(t + 1, line.len() + 1)
    == Seq::singleton(y).concat(line.subsequence(t + 1, line.len())))]
#[ensures(split_line(line, t, y)[t + 1] == y)]
#[ensures(forall<i> 0 <= i && i <= t ==> split_line(line, t, y)[i] == line[i])]
#[ensures(forall<i> t < i && i < line.len() ==> split_line(line, t, y)[i + 1] == line[i])]
pub fn lemma_split_line_at<I>(line: Seq<I>, t: Int, y: I) {
    pearlite! {
        let l = split_line(line, t, y);
        proof_assert!(l.len() == line.len() + 1);
        proof_assert!(forall<i> 0 <= i && i <= t ==> l[i] == line[i]);
        proof_assert!(l[t + 1] == y);
        proof_assert!(forall<i> t + 1 < i && i < l.len() ==> l[i] == line[i - 1]);
        proof_assert!(l.subsequence(t + 1, line.len() + 1)
            .ext_eq(Seq::singleton(y).concat(line.subsequence(t + 1, line.len()))));
    }
}

/// A line's part from `j` is its `j`th node followed by its part after it.
#[logic]
#[requires(0 <= j && j < line.len())]
#[ensures(line.subsequence(j, line.len()) == Seq::singleton(line[j]).concat(line.subsequence(j + 1, line.len())))]
pub fn lemma_suffix_cons<I>(line: Seq<I>, j: Int) {
    proof_assert!(line.subsequence(j, line.len())
        .ext_eq(Seq::singleton(line[j]).concat(line.subsequence(j + 1, line.len()))))
}

/// Removing nothing, or from beyond the end, keeps a text.
#[logic]
#[requires(0 <= start)]
#[requires(end <= start || t.len() <= start)]
#[ensures(spliced(t, start, end, Seq::empty()) == t)]
pub fn lemma_spliced_same<U>(t: Seq<U>, start: Int, end: Int) {
    pearlite! {
        let s = if start < t.len() { start } else { t.len() };
        lemma_halves(t, s);
        t.subsequence(0, s).concat_empty();
    }
}

/// Splicing from `start` to `end`, when `start` isn't beyond the end of the text.
#[logic]
#[requires(0 <= start && start <= t.len() && start <= end)]
#[ensures(end <= t.len() ==> spliced(t, start, end, x)
    == t.subsequence(0, start).concat(x).concat(t.subsequence(end, t.len())))]
#[ensures(t.len() <= end ==> spliced(t, start, end, x) == t.subsequence(0, start).concat(x))]
pub fn lemma_spliced_range<U>(t: Seq<U>, start: Int, end: Int, x: Seq<U>) {
    pearlite! {
        proof_assert!(t.subsequence(t.len(), t.len()).ext_eq(Seq::empty()));
        t.subsequence(0, start).concat(x).concat_empty();
    }
}

/// The end of the active line has no active child.
#[logic]
#[requires(all_linked(nodes) && acyclic(nodes))]
#[requires(line_of(nodes, active, line))]
#[requires(0 < line.len())]
#[ensures(!has_child_in(nodes, active, line[line.len() - 1]))]
pub fn lemma_end_childless<K, T, S>(nodes: Nodes<K, T, S>, active: FSet<Id<K>>, line: Seq<Id<K>>)
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let x = line[line.len() - 1];
        proof_assert!(forall<c> nodes[x]@.to.contains(c) && active.contains(&c) ==> {
            proof_assert!(linked(nodes, x) && nodes[c]@.from.contains(x));
            proof_assert!(rank_of(nodes)[x] < rank_of(nodes)[c]);
            lemma_line_rank_le(nodes, active, line, line.len() - 1, c);
            proof_assert!(line.subsequence(line.len(), line.len()).len() == 0);
            false
        })
    }
}

/// The two parts of a concatenation.
#[logic]
#[ensures(a.concat(b).subsequence(0, a.len()) == a)]
#[ensures(a.concat(b).subsequence(a.len(), a.len() + b.len()) == b)]
pub fn lemma_concat_parts<I>(a: Seq<I>, b: Seq<I>) {
    pearlite! {
        proof_assert!(a.concat(b).subsequence(0, a.len()).ext_eq(a));
        proof_assert!(a.concat(b).subsequence(a.len(), a.len() + b.len()).ext_eq(b));
    }
}

/// Splicing from beyond the end of a text appends to it.
#[logic]
#[requires(t.len() <= start)]
#[ensures(spliced(t, start, end, x) == t.concat(x))]
pub fn lemma_spliced_end<U>(t: Seq<U>, start: Int, end: Int, x: Seq<U>) {
    pearlite! {
        proof_assert!(t.subsequence(0, t.len()).ext_eq(t));
        proof_assert!(t.subsequence(t.len(), t.len()).ext_eq(Seq::empty()));
        t.concat(x).concat_empty();
    }
}

/// An empty range splices like an insertion at its start.
#[logic]
#[requires(end <= start)]
#[ensures(spliced(t, start, end, x) == spliced(t, start, start, x))]
pub fn lemma_spliced_empty_range<U>(t: Seq<U>, start: Int, end: Int, x: Seq<U>) {}

/// What `locate_range` finds for the range from `start` to `end` in a line, whose identifiers are
/// `keys`: `prefix_len` nodes come before the range, which starts `at` units into the
/// `index`th node if `start_split` is `Some((index, at))`, and ends `at` units into the `index`th
/// node, whose identifier is `id`, if `end` is `Some((index, at, id))`, and the text of every
/// node before that one starts before the end; if `end` is `None`, the text of every node does. A
/// range starting at 0 starts after the empty nodes at the start of the line.
#[logic(open(self))]
pub fn located<K, T, S>(
    nodes: Nodes<K, T, S>,
    line: Seq<Id<K>>,
    keys: Seq<K>,
    start: Int,
    end: Int,
    prefix_len: Int,
    start_split: Option<(usize, usize)>,
    found_end: Option<(usize, usize, K)>,
) -> bool
where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        prefix_len <= line.len()
            && (start == 0 ==> start_split == None
                && offset(nodes, line, prefix_len) == 0
                && prefix_len < line.len()
                && 0 < offset(nodes, line, prefix_len + 1))
            && (start > 0 ==> 1 <= prefix_len
                && offset(nodes, line, prefix_len - 1) < start
                && start <= offset(nodes, line, prefix_len)
                && match start_split {
                    Some((index, at)) => index@ == prefix_len - 1
                        && at@ + offset(nodes, line, prefix_len - 1) == start
                        && start < offset(nodes, line, prefix_len),
                    None => start == offset(nodes, line, prefix_len),
                })
            && match found_end {
                Some((index, at, id)) => index@ < line.len() && id == keys[index@]
                    && at@ + offset(nodes, line, index@) == end
                    && (at@ == 0 || end < offset(nodes, line, index@ + 1))
                    && (start == 0 ==> prefix_len <= index@)
                    && (start > 0 ==> prefix_len - 1 <= index@)
                    && forall<j> 0 <= j && j < index@ ==> offset(nodes, line, j) < end,
                None => text(nodes, line).len() <= end
                    && start <= text(nodes, line).len()
                    && (start == text(nodes, line).len() ==> prefix_len < line.len())
                    && forall<j> 0 <= j && j < line.len() ==> offset(nodes, line, j) < end,
            }
    }
}

/// What `located` is. Only `locate_range` and `split_bounds` use this: outside of this module,
/// `located` is opaque, so that the provers don't unfold it where their callers pass it on.
#[logic]
#[ensures(located(nodes, line, keys, start, end, prefix_len, start_split, found_end) == (
    prefix_len <= line.len()
        && (start == 0 ==> start_split == None
            && offset(nodes, line, prefix_len) == 0
            && prefix_len < line.len()
            && 0 < offset(nodes, line, prefix_len + 1))
        && (start > 0 ==> 1 <= prefix_len
            && offset(nodes, line, prefix_len - 1) < start
            && start <= offset(nodes, line, prefix_len)
            && match start_split {
                Some((index, at)) => index@ == prefix_len - 1
                    && at@ + offset(nodes, line, prefix_len - 1) == start
                    && start < offset(nodes, line, prefix_len),
                None => start == offset(nodes, line, prefix_len),
            })
        && match found_end {
            Some((index, at, id)) => index@ < line.len() && id == keys[index@]
                && at@ + offset(nodes, line, index@) == end
                && (at@ == 0 || end < offset(nodes, line, index@ + 1))
                && (start == 0 ==> prefix_len <= index@)
                && (start > 0 ==> prefix_len - 1 <= index@)
                && forall<j> 0 <= j && j < index@ ==> offset(nodes, line, j) < end,
            None => text(nodes, line).len() <= end
                && start <= text(nodes, line).len()
                && (start == text(nodes, line).len() ==> prefix_len < line.len())
                && forall<j> 0 <= j && j < line.len() ==> offset(nodes, line, j) < end,
        }
))]
pub fn lemma_located<K, T, S>(
    nodes: Nodes<K, T, S>,
    line: Seq<Id<K>>,
    keys: Seq<K>,
    start: Int,
    end: Int,
    prefix_len: Int,
    start_split: Option<(usize, usize)>,
    found_end: Option<(usize, usize, K)>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
}

/// The line which `split_bounds` leaves of `line`, once it splits the nodes a range starts and
/// ends inside of, as `located` describes them: `end_id` follows the `index`th node if the range
/// ends `at > 0` units into it, and then `start_id` follows the `index`th node if the range starts
/// inside of it.
#[logic(open(self))]
pub fn bounded<K: DeepModel>(
    line: Seq<Id<K>>,
    start_split: Option<(usize, usize)>,
    end: Option<(usize, usize, K)>,
    end_id: Option<K>,
    start_id: Option<K>,
) -> Seq<Id<K>> {
    pearlite! {
        let ended = match end {
            Some((index, at, _)) => match end_id {
                Some(r) => if at@ == 0 { line } else { split_line(line, index@, r.deep_model()) },
                None => line,
            },
            None => line,
        };
        match start_split {
            Some((index, _)) => match start_id {
                Some(r) => split_line(ended, index@, r.deep_model()),
                None => ended,
            },
            None => ended,
        }
    }
}

/// Where the range ends in `bounded`'s line, for a line of `len` nodes: at the node it ends before,
/// or at the end of the line.
#[logic(open(self))]
pub fn bounded_end<K>(len: Int, start_split: Option<(usize, usize)>, end: Option<(usize, usize, K)>) -> Int {
    pearlite! {
        (match end {
            Some((index, at, _)) => if at@ == 0 { index@ } else { index@ + 1 },
            None => len,
        }) + match start_split {
            Some(_) => 1,
            None => 0,
        }
    }
}

/// What `bounded` and `bounded_end` are. Only `split_bounds` uses this: outside of this module,
/// they are opaque, so that the provers don't unfold them where its callers use its contract.
#[logic]
#[ensures(bounded(line, start_split, end, end_id, start_id) == {
    let ended = match end {
        Some((index, at, _)) => match end_id {
            Some(r) => if at@ == 0 { line } else { split_line(line, index@, r.deep_model()) },
            None => line,
        },
        None => line,
    };
    match start_split {
        Some((index, _)) => match start_id {
            Some(r) => split_line(ended, index@, r.deep_model()),
            None => ended,
        },
        None => ended,
    }
})]
#[ensures(bounded_end(line.len(), start_split, end) == (match end {
    Some((index, at, _)) => if at@ == 0 { index@ } else { index@ + 1 },
    None => line.len(),
}) + match start_split {
    Some(_) => 1,
    None => 0,
})]
pub fn lemma_bounded<K: DeepModel>(
    line: Seq<Id<K>>,
    start_split: Option<(usize, usize)>,
    end: Option<(usize, usize, K)>,
    end_id: Option<K>,
    start_id: Option<K>,
) {
}

/// Inserting a node which isn't a parent of a line's root keeps the line downward.
#[logic]
#[requires(inserted(old, new, n, contents))]
#[requires(!old.nodes.contains(&n.id))]
#[requires(downward(old.nodes, line))]
#[requires(0 < line.len() ==> !n.to.contains(line[0]))]
#[ensures(inserted(old, new, n, contents) && downward(new.nodes, line))]
pub fn lemma_insert_keeps_line<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    n: NodeView<Id<K>>,
    contents: T,
    line: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<i> 0 <= i && i < line.len() ==> new.nodes.contains(&line[i]));
        proof_assert!(forall<k, x> old.nodes.contains(&k) && old.nodes[k]@.from.contains(x) ==> {
            lemma_snoc_contains(old.nodes[k]@.from, n.id, x);
            true
        } && new.nodes[k]@.from.contains(x));
        proof_assert!(forall<i> 0 <= i && i + 1 < line.len()
            ==> new.nodes[line[i + 1]]@.from.contains(line[i]));
        proof_assert!(0 < line.len() ==> new.nodes[line[0]]@.from == old.nodes[line[0]]@.from);
    }
}

/// Inserting a node keeps the text of a line of the old weave.
#[logic]
#[requires(inserted(old, new, n, contents))]
#[requires(downward(old.nodes, line))]
#[ensures(inserted(old, new, n, contents) && text(new.nodes, line) == text(old.nodes, line))]
pub fn lemma_insert_keeps_text<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    n: NodeView<Id<K>>,
    contents: T,
    line: Seq<Id<K>>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        proof_assert!(forall<i> 0 <= i && i < line.len()
            ==> new.nodes[line[i]].contents == old.nodes[line[i]].contents);
        lemma_text_same(new.nodes, old.nodes, line)
    }
}

/// Every element of `a` is one of `b`.
#[logic(open)]
pub fn within<I>(a: Seq<I>, b: Seq<I>) -> bool {
    pearlite! { forall<c> a.contains(c) ==> b.contains(c) }
}

/// Inserting an active node after the `p - 1`th node of the active line makes the line up to that
/// node, followed by the new node, the active line, when the new node's children are children of
/// `r`, which is the line's last node or a child of it.
#[logic]
#[requires(old.valid() && new.valid())]
#[requires(inserted(old, new, n, contents))]
#[requires(!old.nodes.contains(&n.id))]
#[requires(activated(new.nodes, old.active, n.id, new.active))]
#[requires(line_of(old.nodes, old.active, line))]
#[requires(0 < p && p <= line.len())]
#[requires(n.from == Seq::singleton(line[p - 1]))]
#[requires(old.nodes.contains(&r))]
#[requires(r == line[line.len() - 1] || old.nodes[line[line.len() - 1]]@.to.contains(r))]
#[requires(within(n.to, old.nodes[r]@.to))]
#[ensures(old.nodes.contains(&r)
    && active_line(new) == line.subsequence(0, p).snoc(n.id)
    && text(new.nodes, line.subsequence(0, p).snoc(n.id))
        == text(old.nodes, line.subsequence(0, p)).concat(contents.units()))]
pub fn lemma_insert_active_after<K, T, S>(
    old: IndependentWeaveView<K, T, S>,
    new: IndependentWeaveView<K, T, S>,
    n: NodeView<Id<K>>,
    contents: T,
    line: Seq<Id<K>>,
    p: Int,
    r: Id<K>,
) where
    K: Hash + Copy + Eq + Ord + DeepModel,
    T: IndependentContents + DiscreteContents,
    S: BuildHasher + Default + Clone,
{
    pearlite! {
        let y = n.id;
        let last = line[line.len() - 1];
        let pre = line.subsequence(0, p);
        proof_assert!(forall<k> old.nodes.contains(&k) ==> linked(old.nodes, k));
        proof_assert!(downward(old.nodes, line) && no_dup(line));
        proof_assert!(!line.contains(y));
        proof_assert!(0 < line.len() ==> old.nodes[line[0]]@.from.len() == 0);
        proof_assert!(forall<c> n.to.contains(c) ==> old.nodes[c]@.from.contains(r));
        proof_assert!(r != last ==> old.nodes[r]@.from.contains(last));
        proof_assert!(!n.to.contains(line[0]));
        proof_assert! {
            lemma_insert_keeps_line(old, new, n, contents, line);
            downward(new.nodes, line)
        };
        proof_assert!(new.nodes[y]@.to == n.to && new.nodes[y]@.from == Seq::singleton(line[p - 1]));
        proof_assert!(forall<c> n.to.contains(c) ==> {
            lemma_snoc_contains(old.nodes[c]@.from, y, r);
            true
        } && new.nodes[c]@.from.contains(r));
        proof_assert!(r != last ==> {
            lemma_snoc_contains(old.nodes[r]@.from, y, last);
            true
        } && new.nodes[r]@.from.contains(last));
        proof_assert!(ranked(new.nodes, rank_of(new.nodes)));
        proof_assert!(new.nodes.contains(&r) && new.nodes.contains(&last));
        proof_assert!(forall<c> n.to.contains(c) ==> new.nodes.contains(&c)
            && rank_of(new.nodes)[r] < rank_of(new.nodes)[c]);
        proof_assert!(r == last || rank_of(new.nodes)[last] < rank_of(new.nodes)[r]);
        proof_assert!(below_end(new.nodes, line, new.nodes[y]@.to));
        proof_assert!(!old.active.contains(&y));
        proof_assert!(forall<k> line.contains(k) ==> old.nodes.contains(&k));
        proof_assert!(line_of(new.nodes, old.active, line));
        proof_assert! {
            lemma_activated_after(new, old.active, line, p - 1, y, new.active);
            line_of(new.nodes, new.active, line.subsequence(0, p).snoc(y))
        };
        proof_assert! {
            lemma_active_line(new);
            active_line(new) == line.subsequence(0, p).snoc(y)
        };
        proof_assert!(pre.snoc(y).ext_eq(pre.concat(Seq::singleton(y))));
        proof_assert!(forall<i> 0 <= i && i < p ==> pre[i] == line[i] && old.nodes.contains(&pre[i]));
        proof_assert!(forall<i> 0 <= i && i < p ==> new.nodes[pre[i]].contents == old.nodes[pre[i]].contents);
        proof_assert! {
            lemma_text_same(new.nodes, old.nodes, pre);
            text(new.nodes, pre) == text(old.nodes, pre)
        };
        proof_assert! {
            lemma_text_single(new.nodes, y);
            text(new.nodes, Seq::singleton(y)) == contents.units()
        };
        proof_assert! {
            lemma_text_concat(new.nodes, pre, Seq::singleton(y));
            text(new.nodes, pre.snoc(y)) == text(old.nodes, pre).concat(contents.units())
        };
    }
}

/// The line which stays active when a range reaches the end of the active line `line`, once
/// `split_bounds` splits it into `split`: `split` if nodes of `line` follow the range's first `p`
/// nodes, or else `line`, whose last node the range starts inside of. It is opaque outside of this
/// module, so that it doesn't appear as a conditional, which the provers handle poorly.
#[logic(open(self))]
pub fn kept_line<I>(split: Seq<I>, line: Seq<I>, p: Int) -> Seq<I> {
    pearlite! { if p < line.len() { split } else { line } }
}

/// What `kept_line` is.
#[logic]
#[ensures(p < line.len() ==> kept_line(split, line, p) == split)]
#[ensures(line.len() <= p ==> kept_line(split, line, p) == line)]
pub fn lemma_kept_line<I>(split: Seq<I>, line: Seq<I>, p: Int) {}
