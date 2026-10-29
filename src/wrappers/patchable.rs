//! [`PatchablePathWeave`], a [`Weave`] wrapper which adds content patch operations to the active path.
//!
//! Its patch operations are verified for [`IndependentWeave`](crate::independent::IndependentWeave). See `VERIFICATION.md`.

use alloc::vec::Vec;
use core::{cmp::Ordering, hash::Hash, iter, marker::PhantomData, ops::Range};

use creusot_std::macros::*;

#[cfg(creusot)]
use crate::DiscreteContentResult;

use crate::{
    ActivePathWeave, ActiveSingularWeave, BookmarkableWeave, BuildableNode, DiscreteContents,
    DiscreteWeave, IndependentContents, IndependentWeave, MetadataWeave, Node,
    SemiIndependentWeave, SortableBookmarkableWeave, SortableWeave, VerifiableIterator, Weave,
};

#[cfg(creusot)]
use core::hash::BuildHasher;

#[cfg(creusot)]
use creusot_std::{
    invariant::inv,
    logic::FSet,
    prelude::*,
    std::ops::{FnMutExt as _, FnOnceExt as _},
};

#[cfg(creusot)]
use crate::{
    Verifiable,
    independent::{self, IndependentNode},
    independent::verification::{
        NodeView, activated, descends, extends_path, has_child_in, insertable, inserted, linked,
        longest_path_within, not_below, rank_of, ranked, same_graph, split_shape, valid_path,
    },
    verification::{
        Id, Model, first, ids, increasing_steps, iterates, lemma_dedup_no_dup, lemma_full_prefix,
        lemma_ids_prefix, lemma_ids_reverse, lemma_increasing, lemma_no_dup_snoc, lemma_snoc_contains,
        no_dup,
    },
};

#[cfg(creusot)]
#[allow(clippy::wildcard_imports, reason = "Proof definitions")]
use verification::*;

#[cfg(creusot)]
pub mod verification;

/// A [`Weave`] wrapper which adds content patch operations to the active path.
///
/// Unless documented otherwise, the wrapper's exact indexing behavior for zero-width nodes is an implementation detail which may be changed in the future. However, indexing behavior is consistent between functions.
///
/// # Requirements
///
/// The underlying [`Weave`] must meet all of the following requirements for patch operations to function properly:
///
/// 1. [`IndependentWeave`](crate::independent::IndependentWeave)-like activation semantics.
/// 2. [`Weave::insert`], [`DiscreteWeave::split`], and [`IndependentWeave::move_to`] must never refuse a structurally valid operation.
/// 3. `T::split()` must always split at the exact specified index and should never refuse a structurally valid operation.
/// 4. `T::default()` must have a length of zero.
///
/// # Panics
///
/// May panic if the underlying [`Weave`] violates any of the specified [requirements](#requirements).
///
/// All panics should be assumed to leave the underlying Weave in a malformed state.
#[cfg_attr(not(creusot), derive(Debug, Clone))]
#[must_use]
pub struct PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    /// The [`Weave`] being wrapped.
    pub weave: W,

    scratchpad: Vec<K>,

    _phantom_n: PhantomData<N>,
    _phantom_t: PhantomData<T>,
}

impl<W, K, N, T> Default for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T> + Default,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn default() -> Self {
        Self::new(W::default())
    }
}

#[cfg(not(creusot))]
#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<W, K, N, T> PartialEq for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T> + PartialEq,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.weave == other.weave
    }
}

#[cfg(not(creusot))]
#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<W, K, N, T> Eq for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T> + Eq,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
}

impl<W, K, N, T> AsRef<W> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn as_ref(&self) -> &W {
        &self.weave
    }
}

impl<W, K, N, T> From<W> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn from(value: W) -> Self {
        Self::new(value)
    }
}

impl<W, K, N, T> PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    /// Creates a [`PatchablePathWeave`] from a [`Weave`].
    #[inline]
    pub const fn new(weave: W) -> Self {
        Self {
            weave,
            scratchpad: Vec::new(),
            _phantom_n: PhantomData,
            _phantom_t: PhantomData,
        }
    }
    /// Converts a [`PatchablePathWeave`] into its inner [`Weave`].
    #[inline]
    pub fn into_inner(self) -> W {
        self.weave
    }
    /// Returns a reference to the inner [`Weave`].
    #[inline]
    pub const fn as_inner(&self) -> &W {
        &self.weave
    }
}

impl<W, K, N, T> Weave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    type Nodes = W::Nodes;
    type Roots = W::Roots;

    #[inline]
    fn len(&self) -> usize {
        self.weave.len()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.weave.is_empty()
    }
    #[inline]
    fn nodes(&self) -> &Self::Nodes {
        self.weave.nodes()
    }
    #[inline]
    fn roots(&self) -> &Self::Roots {
        self.weave.roots()
    }
    #[inline]
    fn contains(&self, id: &K) -> bool {
        self.weave.contains(id)
    }
    #[inline]
    fn contains_active(&self, id: &K) -> bool {
        self.weave.contains_active(id)
    }
    #[inline]
    fn get(&self, id: &K) -> Option<&N> {
        self.weave.get(id)
    }
    #[inline]
    fn get_parents(&self, id: &K) -> Option<&N::From> {
        self.weave.get_parents(id)
    }
    #[inline]
    fn get_children(&self, id: &K) -> Option<&N::To> {
        self.weave.get_children(id)
    }
    #[inline]
    fn get_contents(&self, id: &K) -> Option<&T> {
        self.weave.get_contents(id)
    }
    #[inline]
    fn get_ordered_identifiers(&mut self, output: &mut Vec<K>) {
        self.weave.get_ordered_identifiers(output);
    }
    #[inline]
    fn get_ordered_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_ordered_identifiers_from(id, output);
    }
    #[inline]
    fn get_active_path(&mut self, output: &mut Vec<K>) {
        self.weave.get_active_path(output);
    }
    #[inline]
    fn get_path_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_path_from(id, output);
    }
    #[inline]
    fn insert(&mut self, node: N) -> bool {
        self.weave.insert(node)
    }
    #[inline]
    fn set_active(&mut self, id: &K, value: bool) -> bool {
        self.weave.set_active(id, value)
    }
    #[inline]
    fn remove(&mut self, id: &K) -> Option<N> {
        self.weave.remove(id)
    }
    #[inline]
    #[requires(forall<f> on_removal.hist_inv(f) ==> forall<n: N> inv(n) ==> f.precondition((n,)))]
    fn remove_tracked(&mut self, id: &K, on_removal: impl FnMut(N)) -> bool {
        self.weave.remove_tracked(id, on_removal)
    }
    #[inline]
    fn clear(&mut self) {
        self.weave.clear();
    }
}

impl<W, K, N, T, M> MetadataWeave<K, N, T, M> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T>
        + ActivePathWeave<K, N, T>
        + IndependentWeave<K, N, T>
        + MetadataWeave<K, N, T, M>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn metadata(&self) -> &M {
        self.weave.metadata()
    }
    #[inline]
    #[requires(forall<m: &mut M> inv(m) ==> callback.precondition((m,)))]
    fn metadata_mut<O>(&mut self, callback: impl FnOnce(&mut M) -> O) -> O {
        self.weave.metadata_mut(callback)
    }
}

impl<W, K, N, T> BookmarkableWeave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T>
        + ActivePathWeave<K, N, T>
        + IndependentWeave<K, N, T>
        + BookmarkableWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    type Bookmarks = W::Bookmarks;

    #[inline]
    fn bookmarks(&self) -> &Self::Bookmarks {
        self.weave.bookmarks()
    }
    #[inline]
    fn contains_bookmark(&self, id: &K) -> bool {
        self.weave.contains_bookmark(id)
    }
    #[inline]
    fn set_bookmarked(&mut self, id: &K, value: bool) -> bool {
        self.weave.set_bookmarked(id, value)
    }
}

impl<W, K, N, T> SortableWeave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T>
        + ActivePathWeave<K, N, T>
        + IndependentWeave<K, N, T>
        + SortableWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    #[requires(forall<f> cmp.hist_inv(f) ==> forall<a: &N, b: &N> inv(a) && inv(b) ==> f.precondition((a, b)))]
    fn sort_children_by(&mut self, id: &K, cmp: impl FnMut(&N, &N) -> Ordering) -> bool {
        self.weave.sort_children_by(id, cmp)
    }
    #[inline]
    #[requires(forall<f> cmp.hist_inv(f) ==> forall<a: &K, b: &K> inv(a) && inv(b) ==> f.precondition((a, b)))]
    fn sort_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool {
        self.weave.sort_children_by_id(id, cmp)
    }
    #[inline]
    #[requires(forall<f> cmp.hist_inv(f) ==> forall<a: &N, b: &N> inv(a) && inv(b) ==> f.precondition((a, b)))]
    fn sort_roots_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering) {
        self.weave.sort_roots_by(cmp);
    }
    #[inline]
    #[requires(forall<f> cmp.hist_inv(f) ==> forall<a: &K, b: &K> inv(a) && inv(b) ==> f.precondition((a, b)))]
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.weave.sort_roots_by_id(cmp);
    }
}

impl<W, K, N, T> SortableBookmarkableWeave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T>
        + ActivePathWeave<K, N, T>
        + IndependentWeave<K, N, T>
        + SortableBookmarkableWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    #[requires(forall<f> cmp.hist_inv(f) ==> forall<a: &N, b: &N> inv(a) && inv(b) ==> f.precondition((a, b)))]
    fn sort_bookmarks_by(&mut self, cmp: impl FnMut(&N, &N) -> Ordering) {
        self.weave.sort_bookmarks_by(cmp);
    }
    #[inline]
    #[requires(forall<f> cmp.hist_inv(f) ==> forall<a: &K, b: &K> inv(a) && inv(b) ==> f.precondition((a, b)))]
    fn sort_bookmarks_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.weave.sort_bookmarks_by_id(cmp);
    }
}

impl<W, K, N, T> ActiveSingularWeave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T>
        + ActivePathWeave<K, N, T>
        + IndependentWeave<K, N, T>
        + ActiveSingularWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn active(&self) -> Option<K> {
        ActiveSingularWeave::active(&self.weave)
    }
}

impl<W, K, N, T> ActivePathWeave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    type Active = W::Active;

    #[inline]
    fn active(&self) -> &Self::Active {
        self.weave.active()
    }
    #[inline]
    fn set_active_path(&mut self, active: impl VerifiableIterator<Item = K>) {
        self.weave.set_active_path(active);
    }
}

impl<W, K, N, T> IndependentWeave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn move_to(&mut self, id: &K, new_parents: &[K]) -> bool {
        self.weave.move_to(id, new_parents)
    }
}

impl<W, K, N, T> SemiIndependentWeave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    #[requires(forall<c: &mut T> inv(c) ==> callback.precondition((c,)))]
    fn get_contents_mut<O>(&mut self, id: &K, callback: impl FnOnce(&mut T) -> O) -> Option<O> {
        self.weave.get_contents_mut(id, callback)
    }
}

impl<W, K, N, T> DiscreteWeave<K, N, T> for PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    #[inline]
    fn split(&mut self, id: &K, at: usize, new_id: K) -> bool {
        self.weave.split(id, at, new_id)
    }
    #[inline]
    fn merge_with_parent(&mut self, id: &K) -> Option<K> {
        self.weave.merge_with_parent(id)
    }
}


impl<W, K, N, T> PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    /// Convenience function which provides an empty `&mut Vec<K>` for path operations.
    #[inline]
    #[requires(forall<w: &mut W, v: &mut Vec<K>> inv(w) && inv(v) && v@.len() == 0
        ==> callback.precondition((w, v)))]
    // The callback is called on the weave and an empty vector, and leaves the weave as the
    // wrapper's new weave.
    #[ensures(exists<w: &mut W, v: &mut Vec<K>> *w == self.weave && ^w == (^self).weave
        && v@.len() == 0 && callback.postcondition_once((w, v), result))]
    pub fn inner_scratchpad_mut<F, O>(&mut self, callback: F) -> O
    where
        F: FnOnce(&mut W, &mut Vec<K>) -> O,
    {
        self.scratchpad.clear();
        callback(&mut self.weave, &mut self.scratchpad)
    }
}

/// The methods of [`PatchablePathWeave`] which read or patch the active path, for the weave type
/// `$W` and the node type `$N`.
///
/// They are generic over the weave, but Creusot verifies them for
/// [`IndependentWeave`](crate::independent::IndependentWeave), so they are written once here and
/// implemented for every weave, or only for that one while Creusot is running. See
/// `VERIFICATION.md`.
macro_rules! path_methods {
    ($W:ty, $N:ty) => {
        /// Convenience function which returns the output of [`Weave::get_active_path`].
        #[inline]
        // The active path, from its end up to its root, and the weave is unchanged.
        #[ensures(ids(result@).reverse() == active_line(self.weave@))]
        #[ensures((^self).weave@ == self.weave@)]
        #[ensures((^self).weave.metadata == self.weave.metadata)]
        pub fn active_path_vec(&mut self) -> &mut Vec<K> {
            self.scratchpad.clear();
            self.weave.get_active_path(&mut self.scratchpad);
            proof_assert! {
                lemma_output_line(self.weave@, ids(self.scratchpad@));
                ids(self.scratchpad@).reverse() == active_line(self.weave@)
            };
            &mut self.scratchpad
        }
        /// Convenience function which returns the output of [`Weave::get_path_from`].
        #[inline]
        // A path from `id` up to a root, which holds every active node `id` descends from, after
        // its inactive nodes, or nothing if there is no such node, and the weave is unchanged.
        #[ensures(self.weave@.nodes.contains(&id.deep_model())
            ==> first(ids(result@)) == Some(id.deep_model()))]
        #[ensures(!self.weave@.nodes.contains(&id.deep_model()) ==> result@.len() == 0)]
        #[ensures(no_dup(ids(result@)))]
        #[ensures(valid_path(self.weave@.nodes, ids(result@)))]
        #[ensures(forall<k> self.weave@.active.contains(&k) && descends(self.weave@.nodes, id.deep_model(), k)
            ==> ids(result@).contains(k))]
        #[ensures(forall<i, j> 0 <= i && i < j && j < result@.len()
            && self.weave@.active.contains(&ids(result@)[i])
            ==> self.weave@.active.contains(&ids(result@)[j]))]
        #[ensures((^self).weave@ == self.weave@)]
        #[ensures((^self).weave.metadata == self.weave.metadata)]
        pub fn path_vec(&mut self, id: &K) -> &mut Vec<K> {
            self.scratchpad.clear();
            self.weave.get_path_from(id, &mut self.scratchpad);
            &mut self.scratchpad
        }
        /// Convenience function which returns an iterator over the content corresponding to the active path.
        #[cfg(not(creusot))]
        #[allow(
            clippy::missing_panics_doc,
            reason = "Should never panic when underlying Weave is correctly implemented"
        )]
        pub fn active_content(&mut self) -> impl Iterator<Item = &T> {
            self.scratchpad.clear();
            self.weave.get_active_path(&mut self.scratchpad);

            self.scratchpad
                .drain(..)
                .rev()
                .map(|id| self.weave.get_contents(&id).unwrap())
        }
        /// Convenience function which returns an iterator over the content corresponding to `id`'s path.
        #[cfg(not(creusot))]
        #[allow(
            clippy::missing_panics_doc,
            reason = "Should never panic when underlying Weave is correctly implemented"
        )]
        pub fn path_content(&mut self, id: &K) -> impl Iterator<Item = &T> {
            self.scratchpad.clear();
            self.weave.get_path_from(id, &mut self.scratchpad);

            self.scratchpad
                .drain(..)
                .rev()
                .map(|id| self.weave.get_contents(&id).unwrap())
        }
        /// Removes the specified range from the active path without removing the content from the underlying Weave.
        ///
        /// If the range is empty or starts past the end of the active path, this function does nothing. If the range extends beyond the active path, its length is clamped to the active path's length.
        ///
        /// If the range starts at zero, an empty root node (`T::default()`) will be inserted at the start of the active path if one does not already exist.
        ///
        /// This function may split up to 2 nodes, may move up to 1 node, and may insert up to 1 node if necessary to apply the operation.
        ///
        /// # Panics
        ///
        /// May panic if the underlying [`Weave`] violates any of the wrapper's [requirements](#requirements).
        ///
        /// May panic if `generate_id` panics or returns an identifier already in the Weave.
        #[requires(generates(generate_id, self.weave@.nodes))]
        #[requires(exact_splits::<T>() && total_splits::<T>())]
        #[requires(active_text(self.weave@).len() <= usize::MAX@)]
        // The units from `range.start` to `range.end` are removed from the active path's text, which
        // the range is clamped to, and no node is removed.
        #[ensures(active_text((^self).weave@)
            == spliced(active_text(self.weave@), range.start@, range.end@, Seq::empty()))]
        #[ensures(forall<k> self.weave@.nodes.contains(&k) ==> (^self).weave@.nodes.contains(&k))]
        #[ensures((^self).weave.metadata == self.weave.metadata)]
        #[allow(clippy::too_many_lines, reason = "Most of its length is proof code")]
        pub fn split_out<F>(&mut self, range: Range<usize>, mut generate_id: F)
        where
            F: FnMut() -> K,
        {
            proof! {
                let old = snapshot!(self.weave@);
            }
            snapshot! { lemma_collected::<K> };
            snapshot! { lemma_ids_concat::<K> };
            proof_assert! { lemma_active_line(*old); line_of(old.nodes, old.active, active_line(*old)) };

            if range.is_empty() {
                proof_assert! {
                    lemma_spliced_same(active_text(*old), range.start@, range.end@);
                    spliced(active_text(*old), range.start@, range.end@, Seq::empty()) == active_text(*old)
                };
                return;
            }

            self.scratchpad.clear();
            self.weave.get_active_path(&mut self.scratchpad);
            proof! {
                let path = snapshot!(self.scratchpad@);
            }
            proof_assert! { lemma_output_line(*old, ids(*path)); ids(*path).reverse() == active_line(*old) };
            self.scratchpad.reverse();
            proof! {
                let line = snapshot!(ids(self.scratchpad@));
                let total = snapshot!(active_text(*old));
            }
            proof_assert! { lemma_ids_reverse(*path); *line == active_line(*old) };
            proof_assert!(forall<i> 0 <= i && i < line.len() ==> old.nodes.contains(&line[i]));
            proof_assert!(*total == text(old.nodes, *line));
            proof_assert!(downward(old.nodes, *line) && no_dup(*line));
            proof_assert!(range.start@ < range.end@);
            proof_assert! { lemma_full_prefix(*line); line.subsequence(0, line.len()) == *line };

            let Some((prefix_len, start_split, end)) = self.locate_range(&range) else {
                proof_assert! {
                    lemma_spliced_same(*total, range.start@, range.end@);
                    spliced(*total, range.start@, range.end@, Seq::empty()) == *total
                };
                return;
            };
            proof_assert!(self.weave@ == *old && ids(self.scratchpad@) == *line);

            proof! {
                let found = snapshot!(end);
            }
            let bounds = self.split_bounds(&range, prefix_len, start_split, end, &mut generate_id);
            proof! {
                let v2 = snapshot!(self.weave@);
                let l2 = snapshot!(bounded(*line, start_split, *found, bounds.2, bounds.3));
                let q2 = snapshot!(bounded_end(line.len(), start_split, *found));
            }
            let (_, suffix_index, end, _) = bounds;
            proof_assert!(ids(self.scratchpad@) == *line);
            proof_assert!(v2.valid());
            proof_assert!(generates(generate_id, v2.nodes));
            proof_assert!(forall<k> old.nodes.contains(&k) ==> v2.nodes.contains(&k));
            proof_assert!(downward(v2.nodes, *l2) && no_dup(*l2) && text(v2.nodes, *l2) == *total);
            proof_assert!(prefix_len@ <= l2.len()
                && l2.subsequence(0, prefix_len@) == line.subsequence(0, prefix_len@)
                && offset(v2.nodes, *l2, prefix_len@) == range.start@);
            proof_assert!(match end {
                Some(e) => prefix_len@ <= *q2 && 0 < *q2 && *q2 < l2.len() && l2[*q2] == e.deep_model()
                    && offset(v2.nodes, *l2, *q2) == range.end@
                    && l2.subsequence(*q2, l2.len())
                        == Seq::singleton(e.deep_model()).concat(line.subsequence(suffix_index@, line.len())),
                None => *q2 == l2.len() && total.len() <= range.end@,
            });

            let prefix = &self.scratchpad[..prefix_len];
            proof_assert!(prefix@ == self.scratchpad@.subsequence(0, prefix_len@) && prefix@.len() == prefix_len@);

            if let Some(prefix_tail) = prefix.last().copied() {
                proof_assert!(0 < prefix_len@ && prefix_tail == prefix@[prefix_len@ - 1]);
                proof_assert!(prefix@[prefix_len@ - 1] == self.scratchpad@[prefix_len@ - 1]);
                proof_assert!(ids(self.scratchpad@)[prefix_len@ - 1] == self.scratchpad@[prefix_len@ - 1].deep_model());
                proof_assert!(prefix_tail.deep_model() == line[prefix_len@ - 1]);
                proof_assert!(l2.subsequence(0, prefix_len@)[prefix_len@ - 1] == l2[prefix_len@ - 1]
                    && line.subsequence(0, prefix_len@)[prefix_len@ - 1] == line[prefix_len@ - 1]);
                proof_assert!(prefix_tail.deep_model() == l2[prefix_len@ - 1]);
                if let Some(end) = end {
                    let parents = self.weave.get_parents(&end).unwrap();
                    proof_assert!(parents.model() == v2.nodes[end.deep_model()]@.from);

                    if !Self::lists(parents, &prefix_tail) {
                        let start = self.scratchpad.len();

                        Self::extend_with_parents(&mut self.scratchpad, parents, prefix_tail);
                        proof! {
                            let moved_to = snapshot!(v2.nodes[end.deep_model()]@.from.snoc(prefix_tail.deep_model()));
                        }
                        proof_assert!(start@ == line.len());
                        proof_assert! {
                            crate::verification::lemma_concat_snoc(*line, v2.nodes[end.deep_model()]@.from, prefix_tail.deep_model());
                            ids(self.scratchpad@) == line.concat(*moved_to)
                        };
                        proof_assert! {
                            lemma_concat_parts(*line, *moved_to);
                            lemma_ids_sub(self.scratchpad@, start@, self.scratchpad@.len());
                            ids(self.scratchpad@.subsequence(start@, self.scratchpad@.len())) == *moved_to
                        };
                        proof_assert!(forall<k> v2.nodes.contains(&k) ==> linked(v2.nodes, k));
                        proof_assert!(ranked(v2.nodes, rank_of(v2.nodes)));
                        proof_assert!(0 <= prefix_len@ - 1 && prefix_len@ - 1 < *q2 && *q2 < l2.len());
                        proof_assert! {
                            lemma_line_ranks(v2.nodes, rank_of(v2.nodes), *l2);
                            increasing_steps(*l2, rank_of(v2.nodes))
                        };
                        proof_assert! {
                            lemma_increasing(*l2, rank_of(v2.nodes));
                            rank_of(v2.nodes)[l2[prefix_len@ - 1]] < rank_of(v2.nodes)[l2[*q2]]
                        };
                        proof_assert!(v2.nodes.contains(&end.deep_model()) && v2.nodes.contains(&prefix_tail.deep_model()));
                        proof_assert!(forall<q> v2.nodes[end.deep_model()]@.from.contains(q)
                            ==> v2.nodes.contains(&q) && rank_of(v2.nodes)[q] < rank_of(v2.nodes)[end.deep_model()]);
                        proof_assert!(forall<q> moved_to.contains(q) ==> {
                            lemma_snoc_contains(v2.nodes[end.deep_model()]@.from, prefix_tail.deep_model(), q);
                            true
                        } && (v2.nodes[end.deep_model()]@.from.contains(q) || q == prefix_tail.deep_model()));
                        proof_assert!(ranked_under(v2.nodes, *moved_to, end.deep_model()));
                        proof_assert! {
                            lemma_not_below_ranked(v2.nodes, end.deep_model(), *moved_to);
                            not_below(v2.nodes, end.deep_model(), *moved_to)
                        };

                        assert!(
                            self.weave.move_to(&end, &self.scratchpad[start..]),
                            "Moving node failed"
                        );
                        proof_assert! {
                            lemma_no_dup_snoc(v2.nodes[end.deep_model()]@.from, prefix_tail.deep_model());
                            lemma_dedup_no_dup(*moved_to);
                            self.weave@.nodes[end.deep_model()]@.from == *moved_to
                        };
                        proof_assert!(forall<x> v2.nodes[end.deep_model()]@.from.contains(x) ==> {
                            lemma_snoc_contains(v2.nodes[end.deep_model()]@.from, prefix_tail.deep_model(), x);
                            true
                        } && moved_to.contains(x));
                        proof_assert!(l2[0] != end.deep_model());
                        proof_assert! {
                            lemma_moved_line(*v2, self.weave@, end.deep_model(), *moved_to, *l2);
                            downward(self.weave@.nodes, *l2) && text(self.weave@.nodes, *l2) == *total
                                && forall<k> v2.nodes.contains(&k)
                                    ==> self.weave@.nodes[k].contents == v2.nodes[k].contents
                        };
                        proof_assert! {
                            lemma_snoc_contains(v2.nodes[end.deep_model()]@.from, prefix_tail.deep_model(), prefix_tail.deep_model());
                            self.weave@.nodes[end.deep_model()]@.from.contains(prefix_tail.deep_model())
                        };

                        proof! {
                            let extended = snapshot!(self.scratchpad@);
                        }
                        self.scratchpad.truncate(start);
                        proof_assert! {
                            lemma_ids_prefix(*extended, start@);
                            lemma_concat_parts(*line, *moved_to);
                            ids(self.scratchpad@) == *line
                        };
                    }
                    proof! {
                        let linked_view = snapshot!(self.weave@);
                        let after = snapshot!(cut(*l2, prefix_len@, *q2));
                    }
                    proof_assert!(downward(linked_view.nodes, *l2) && text(linked_view.nodes, *l2) == *total);
                    proof_assert!(forall<k> v2.nodes.contains(&k)
                        ==> linked_view.nodes[k].contents == v2.nodes[k].contents);
                    proof_assert!(forall<i> 0 <= i && i < l2.len() ==> v2.nodes.contains(&l2[i]));
                    proof_assert!(prefix_len@ <= *q2 && *q2 < l2.len());
                    proof_assert!(forall<i> 0 <= i && i < prefix_len@ ==> l2.subsequence(0, prefix_len@)[i] == l2[i]
                        && linked_view.nodes[l2.subsequence(0, prefix_len@)[i]].contents
                            == v2.nodes[l2.subsequence(0, prefix_len@)[i]].contents);
                    proof_assert!(forall<i> 0 <= i && i < *q2 ==> l2.subsequence(0, *q2)[i] == l2[i]
                        && linked_view.nodes[l2.subsequence(0, *q2)[i]].contents
                            == v2.nodes[l2.subsequence(0, *q2)[i]].contents);
                    proof_assert! {
                        lemma_text_same(linked_view.nodes, v2.nodes, l2.subsequence(0, prefix_len@));
                        text(linked_view.nodes, l2.subsequence(0, prefix_len@))
                            == text(v2.nodes, l2.subsequence(0, prefix_len@))
                    };
                    proof_assert! {
                        lemma_text_same(linked_view.nodes, v2.nodes, l2.subsequence(0, *q2));
                        text(linked_view.nodes, l2.subsequence(0, *q2)) == text(v2.nodes, l2.subsequence(0, *q2))
                    };
                    proof_assert!(offset(linked_view.nodes, *l2, prefix_len@) == range.start@
                        && offset(linked_view.nodes, *l2, *q2) == range.end@);
                    proof_assert!(linked_view.nodes[l2[*q2]]@.from.contains(l2[prefix_len@ - 1]));
                    proof_assert! {
                        lemma_cut_line(linked_view.nodes, *l2, prefix_len@, *q2);
                        downward(linked_view.nodes, *after) && no_dup(*after)
                    };
                    proof_assert! {
                        line.subsequence(0, prefix_len@).concat_assoc(
                            Seq::singleton(end.deep_model()),
                            line.subsequence(suffix_index@, line.len()),
                        );
                        *after == line.subsequence(0, prefix_len@).concat(Seq::singleton(end.deep_model()))
                            .concat(line.subsequence(suffix_index@, line.len()))
                    };
                    proof_assert! {
                        lemma_ids_sub(self.scratchpad@, 0, prefix_len@);
                        lemma_ids_sub(self.scratchpad@, suffix_index@, line.len());
                        ids(self.scratchpad@.subsequence(0, prefix_len@)) == line.subsequence(0, prefix_len@)
                            && ids(self.scratchpad@.subsequence(suffix_index@, line.len()))
                                == line.subsequence(suffix_index@, line.len())
                    };

                    let active = self.scratchpad[..prefix_len]
                        .iter()
                        .copied()
                        .chain(iter::once(end))
                        .chain(self.scratchpad[suffix_index..].iter().copied());
                    proof! {
                        let path = snapshot!(active);
                    }
                    proof_assert! {
                        lemma_iterates_one_between::<K>();
                        forall<items: Seq<K>> iterates(*path, items)
                            ==> items == self.scratchpad@.subsequence(0, prefix_len@).concat(Seq::singleton(end))
                                .concat(self.scratchpad@.subsequence(suffix_index@, line.len()))
                    };
                    proof_assert!(forall<items: Seq<K>> iterates(*path, items) ==> ids(items) == *after);
                    Self::set_active_line(&mut self.weave, active);
                    proof_assert!(exists<listed: Seq<K>> iterates(*path, listed)
                        && active_line(self.weave@) == ids(listed));
                    proof_assert!(active_line(self.weave@) == *after);
                    proof_assert! {
                        lemma_same_graph_line(self.weave@.nodes, linked_view.nodes, *after);
                        text(self.weave@.nodes, *after) == text(linked_view.nodes, *after)
                    };
                    proof_assert! {
                        lemma_cut_text(linked_view.nodes, *l2, prefix_len@, *q2);
                        text(linked_view.nodes, *after)
                            == total.subsequence(0, range.start@).concat(total.subsequence(range.end@, total.len()))
                    };
                    proof_assert! {
                        lemma_spliced_range(*total, range.start@, range.end@, Seq::empty());
                        total.subsequence(0, range.start@).concat_empty();
                        active_text(self.weave@) == spliced(*total, range.start@, range.end@, Seq::empty())
                    };
                } else if prefix_len != self.scratchpad.len() {
                    proof! {
                        let after = snapshot!(cut(*l2, prefix_len@, l2.len()));
                    }
                    proof_assert! {
                        lemma_cut_line(v2.nodes, *l2, prefix_len@, l2.len());
                        downward(v2.nodes, *after) && no_dup(*after)
                    };
                    proof_assert! {
                        l2.subsequence(0, prefix_len@).concat_empty();
                        l2.subsequence(l2.len(), l2.len()).ext_eq(Seq::empty());
                        lemma_ids_sub(self.scratchpad@, 0, prefix_len@);
                        *after == l2.subsequence(0, prefix_len@)
                            && ids(prefix@) == *after
                    };

                    let active = prefix.iter().copied();
                    proof! {
                        let path = snapshot!(active);
                    }
                    proof_assert!(forall<items: Seq<K>> iterates(*path, items) ==> {
                        lemma_iterates_slice(*path, items);
                        items == prefix@
                    } && ids(items) == *after);
                    Self::set_active_line(&mut self.weave, active);
                    proof_assert!(exists<listed: Seq<K>> iterates(*path, listed)
                        && active_line(self.weave@) == ids(listed));
                    proof_assert!(active_line(self.weave@) == *after);
                    proof_assert! {
                        lemma_same_graph_line(self.weave@.nodes, v2.nodes, *after);
                        text(self.weave@.nodes, *after) == text(v2.nodes, *after)
                    };
                    proof_assert! {
                        lemma_cut_text(v2.nodes, *l2, prefix_len@, l2.len());
                        lemma_full_prefix(*l2);
                        total.subsequence(total.len(), total.len()).ext_eq(Seq::empty());
                        total.subsequence(0, range.start@).concat_empty();
                        text(self.weave@.nodes, *after) == total.subsequence(0, range.start@)
                    };
                    proof_assert! {
                        lemma_spliced_range(*total, range.start@, range.end@, Seq::empty());
                        total.subsequence(0, range.start@).concat_empty();
                        active_text(self.weave@) == spliced(*total, range.start@, range.end@, Seq::empty())
                    };
                } else {
                    proof_assert!(prefix_len@ == line.len());
                    proof_assert!(bounds.3 != None);
                    proof_assert!(match bounds.3 {
                        Some(r) => split_shape(*old, *v2, line[line.len() - 1], r.deep_model())
                            && *l2 == split_line(*line, line.len() - 1, r.deep_model()),
                        None => false,
                    });
                    proof_assert! {
                        lemma_end_childless(old.nodes, old.active, *line);
                        !has_child_in(old.nodes, old.active, line[line.len() - 1])
                    };
                    proof_assert!(v2.active == old.active);
                    proof_assert!(l2.subsequence(0, prefix_len@) == *line);
                    proof_assert!(l2.subsequence(l2.len(), l2.len()).ext_eq(Seq::empty()));
                    proof_assert! {
                        l2.subsequence(0, prefix_len@).concat_empty();
                        cut(*l2, prefix_len@, l2.len()) == *line
                    };
                    proof_assert! {
                        lemma_cut_line(v2.nodes, *l2, prefix_len@, l2.len());
                        downward(v2.nodes, cut(*l2, prefix_len@, l2.len()))
                            && no_dup(cut(*l2, prefix_len@, l2.len()))
                    };
                    proof_assert!(downward(v2.nodes, *line) && no_dup(*line));
                    proof_assert!(line_of(v2.nodes, v2.active, *line));
                    proof_assert! {
                        lemma_active_line(*v2);
                        active_line(*v2) == *line
                    };
                    proof_assert! {
                        lemma_text_split(v2.nodes, *l2, prefix_len@);
                        lemma_full_prefix(*l2);
                        text(v2.nodes, *line) == total.subsequence(0, range.start@)
                    };
                    proof_assert! {
                        lemma_spliced_range(*total, range.start@, range.end@, Seq::empty());
                        total.subsequence(0, range.start@).concat_empty();
                        active_text(self.weave@) == spliced(*total, range.start@, range.end@, Seq::empty())
                    };
                }
            } else {
                proof_assert!(prefix@.len() == 0 && prefix_len@ == 0);
                proof_assert!(range.start@ == 0);
                proof! {
                    let gen2 = snapshot!(generate_id);
                }
                let id = generate_id();
                proof_assert! {
                    lemma_generates_call(*gen2, generate_id, id, v2.nodes);
                    !v2.nodes.contains(&id.deep_model())
                };
                let contents = Self::default_contents();

                assert!(
                    contents.is_empty(),
                    "`T::default()` must have a length of zero"
                );

                proof! {
                    let node = snapshot!(NodeView {
                        id: id.deep_model(),
                        from: Seq::empty(),
                        to: match end { Some(e) => Seq::singleton(e.deep_model()), None => Seq::empty() },
                        active: match end { Some(_) => false, None => true },
                        bookmarked: false,
                    });
                    let default = snapshot!(contents);
                }
                proof_assert!(match end {
                    Some(e) => v2.nodes.contains(&e.deep_model()) && e.deep_model() != id.deep_model(),
                    None => true,
                });
                proof_assert!(no_dup(node.to) && node.from.len() == 0);
                proof_assert! {
                    lemma_insertable_root(v2.nodes, *node);
                    insertable(v2.nodes, *node)
                };
                assert!(
                    self.weave.insert(<$N as BuildableNode<K, T>>::new(
                        id,
                        <$N as Node<K, T>>::From::from_iter(iter::empty()),
                        <$N as Node<K, T>>::To::from_iter(end),
                        end.is_none(),
                        contents
                    )),
                    "Inserting node failed"
                );
                proof_assert!(inserted(*v2, self.weave@, *node, *default));
                proof_assert!(default.units().ext_eq(Seq::empty()));
                proof! {
                    let linked_view = snapshot!(self.weave@);
                    let after = snapshot!(between(*l2, 0, *q2, id.deep_model()));
                }
                proof_assert! {
                    lemma_insert_between(*v2, self.weave@, *node, *default, *l2, 0, *q2);
                    downward(self.weave@.nodes, *after) && no_dup(*after)
                        && text(self.weave@.nodes, *after)
                            == text(v2.nodes, l2.subsequence(0, 0)).concat(default.units())
                                .concat(text(v2.nodes, l2.subsequence(*q2, l2.len())))
                };
                proof_assert!(match end {
                    Some(_) => true,
                    None => node.to.len() == 0 && {
                        lemma_insert_keeps_line(*v2, self.weave@, *node, *default, *l2);
                        downward(self.weave@.nodes, *l2)
                    },
                });
                proof_assert!(text(v2.nodes, l2.subsequence(0, 0)) == Seq::empty() && {
                    proof_assert!(l2.subsequence(0, 0).len() == 0);
                    true
                });
                proof_assert! {
                    lemma_text_split(v2.nodes, *l2, *q2);
                    text(v2.nodes, l2.subsequence(*q2, l2.len())) == total.subsequence(
                        if *q2 == l2.len() { total.len() } else { range.end@ },
                        total.len(),
                    ) || *q2 == l2.len()
                };

                if let Some(end) = end {
                    proof_assert! {
                        lemma_ids_sub(self.scratchpad@, suffix_index@, line.len());
                        l2.subsequence(0, 0).concat_empty();
                        *after == Seq::singleton(id.deep_model()).concat(Seq::singleton(end.deep_model()))
                            .concat(line.subsequence(suffix_index@, line.len()))
                    };

                    let active = iter::once(id)
                        .chain(iter::once(end))
                        .chain(self.scratchpad[suffix_index..].iter().copied());
                    proof! {
                        let path = snapshot!(active);
                    }
                    proof_assert! {
                        lemma_iterates_two_then::<K>();
                        forall<items: Seq<K>> iterates(*path, items)
                            ==> items == Seq::singleton(id).concat(Seq::singleton(end))
                                .concat(self.scratchpad@.subsequence(suffix_index@, line.len()))
                    };
                    proof_assert!(forall<items: Seq<K>> iterates(*path, items) ==> ids(items) == *after);
                    Self::set_active_line(&mut self.weave, active);
                    proof_assert!(exists<listed: Seq<K>> iterates(*path, listed)
                        && active_line(self.weave@) == ids(listed));
                    proof_assert!(active_line(self.weave@) == *after);
                    proof_assert! {
                        lemma_same_graph_line(self.weave@.nodes, linked_view.nodes, *after);
                        text(self.weave@.nodes, *after) == text(linked_view.nodes, *after)
                    };
                    proof_assert! {
                        lemma_same_graph_line(self.weave@.nodes, linked_view.nodes, *after);
                        Seq::<T::Unit>::empty().concat_empty();
                        active_text(self.weave@) == total.subsequence(range.end@, total.len())
                    };
                    proof_assert! {
                        lemma_spliced_range(*total, range.start@, range.end@, Seq::empty());
                        total.subsequence(0, 0).ext_eq(Seq::empty());
                        Seq::<T::Unit>::empty().concat_empty();
                        active_text(self.weave@) == spliced(*total, range.start@, range.end@, Seq::empty())
                    };
                } else {
                    proof_assert!(start_split == None && bounds.3 == None);
                    proof_assert!(*v2 == *old && *l2 == *line);
                    proof_assert!(activated(self.weave@.nodes, old.active, id.deep_model(), self.weave@.active));
                    proof_assert!(line_of(self.weave@.nodes, old.active, *line));
                    proof_assert!(below_end(self.weave@.nodes, *line, self.weave@.nodes[id.deep_model()]@.to));
                    proof_assert! {
                        lemma_activated_root(self.weave@, old.active, *line, id.deep_model(), self.weave@.active);
                        line_of(self.weave@.nodes, self.weave@.active, Seq::singleton(id.deep_model()))
                    };
                    proof_assert! {
                        lemma_active_line(self.weave@);
                        active_line(self.weave@) == Seq::singleton(id.deep_model())
                    };
                    proof_assert! {
                        lemma_text_single(self.weave@.nodes, id.deep_model());
                        active_text(self.weave@) == Seq::empty()
                    };
                    proof_assert! {
                        lemma_spliced_range(*total, range.start@, range.end@, Seq::empty());
                        total.subsequence(0, 0).ext_eq(Seq::empty());
                        Seq::<T::Unit>::empty().concat_empty();
                        spliced(*total, range.start@, range.end@, Seq::empty()) == Seq::empty()
                    };
                }
            }
        }
        /// Splits the active path at the specified index without deactivating the right side of the split.
        ///
        /// If `at` is zero or beyond the active path's length, this function does nothing.
        ///
        /// Returns `false` if splitting the path failed or if `generate_id` returns an identifier already in the Weave.
        ///
        /// # Panics
        ///
        /// May panic if `T::split()` or `generate_id` panics.
        #[requires(callable(generate_id))]
        #[requires(exact_splits::<T>())]
        #[requires(active_text(self.weave@).len() <= usize::MAX@)]
        // The active path keeps its text, and, if splitting succeeds, the text of some of its
        // first nodes ends at `at`, or at its end if `at` is beyond it.
        #[ensures(active_text((^self).weave@) == active_text(self.weave@))]
        #[ensures(result ==> exists<i> 0 <= i && i <= active_line((^self).weave@).len()
            && text((^self).weave@.nodes, active_line((^self).weave@).subsequence(0, i)).len()
                == if at@ < active_text(self.weave@).len() { at@ } else { active_text(self.weave@).len() })]
        // A failed split changes nothing, and splitting only fails when the contents refuse to
        // split or the identifier isn't new.
        #[ensures(!result ==> (^self).weave@ == self.weave@)]
        #[ensures(total_splits::<T>() && generates(generate_id, self.weave@.nodes) ==> result)]
        #[ensures(forall<k> self.weave@.nodes.contains(&k) ==> (^self).weave@.nodes.contains(&k))]
        #[ensures((^self).weave.metadata == self.weave.metadata)]
        pub fn split_at<F>(&mut self, at: usize, mut generate_id: F) -> bool
        where
            F: FnMut() -> K,
        {
            proof! {
                let old = snapshot!(self.weave@);
                let start = snapshot!(generate_id);
            }
            proof_assert! { lemma_active_line(*old); line_of(old.nodes, old.active, active_line(*old)) };

            if at == 0 {
                proof_assert!(active_line(*old).subsequence(0, 0).len() == 0);
                return true;
            }

            self.scratchpad.clear();
            self.weave.get_active_path(&mut self.scratchpad);
            proof! {
                let path = snapshot!(self.scratchpad@);
            }
            proof_assert! { lemma_output_line(*old, ids(*path)); ids(*path).reverse() == active_line(*old) };
            self.scratchpad.reverse();
            proof! {
                let line = snapshot!(ids(self.scratchpad@));
            }
            proof_assert! { lemma_ids_reverse(*path); *line == active_line(*old) };
            proof_assert!(forall<i> 0 <= i && i < line.len() ==> old.nodes.contains(&line[i]));

            let mut cursor: usize = 0;
            let mut target = None;
            proof! {
                let mut stop = snapshot!(0);
            }

            #[invariant(self.weave@ == *old)]
            #[invariant(cursor@ == text(old.nodes, line.subsequence(0, produced.len())).len())]
            #[invariant(cursor@ < at@)]
            #[invariant(target == None)]
            for id in self.scratchpad.iter() {
                proof! {
                    let j = snapshot!(produced.len() - 1);
                }
                proof_assert!(0 <= *j && *j < line.len() && id.deep_model() == line[*j]);
                let length = self.weave.get_contents(id).unwrap().len();
                proof_assert! {
                    lemma_text_step(old.nodes, *line, *j);
                    lemma_text_split(old.nodes, *line, *j + 1);
                    cursor@ + length@ == text(old.nodes, line.subsequence(0, *j + 1)).len()
                        && cursor@ + length@ <= active_text(*old).len()
                };
                let next = cursor.strict_add(length);

                if next >= at {
                    proof! {
                        stop = snapshot!(*j);
                    }
                    target = Some((*id, length));
                    break;
                }

                cursor = next;
            }
            proof_assert!(match target {
                Some((p, length)) => 0 <= *stop && *stop < line.len() && p.deep_model() == line[*stop]
                    && cursor@ == text(old.nodes, line.subsequence(0, *stop)).len()
                    && cursor@ + length@ == text(old.nodes, line.subsequence(0, *stop + 1)).len()
                    && length@ == old.nodes[line[*stop]].contents.units().len()
                    && cursor@ < at@ && at@ <= cursor@ + length@,
                None => {
                    lemma_full_prefix(*line);
                    cursor@ == active_text(*old).len()
                },
            });

            if let Some((parent, length)) = target {
                #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                let split_at = at - cursor;
                proof_assert! { lemma_text_split(old.nodes, *line, *stop + 1); at@ <= active_text(*old).len() };

                if split_at == length {
                    true
                } else {
                    let new_id = generate_id();
                    proof_assert! {
                        lemma_callable_call(*start, generate_id, new_id);
                        total_splits::<T>() && generates(*start, old.nodes) ==> {
                            lemma_generates_call(*start, generate_id, new_id, old.nodes);
                            !old.nodes.contains(&new_id.deep_model())
                        }
                    };

                    if self.weave.split(&parent, split_at, new_id) {
                        proof! {
                            let split = snapshot!(self.weave@);
                            let y = snapshot!(new_id.deep_model());
                            let after = snapshot!(split_line(*line, *stop, *y));
                        }
                        proof_assert! {
                            lemma_split_line(*old, *split, *line, *stop, *y);
                            downward(split.nodes, *after) && no_dup(*after)
                        };
                        proof_assert!(split.nodes[parent.deep_model()]@.to == Seq::singleton(*y));
                        proof_assert!(old.active.contains(&parent.deep_model())
                            && old.nodes[parent.deep_model()]@.active);
                        proof_assert!(split.active.contains(&*y)
                            || (split.active == old.active
                                && !has_child_in(split.nodes, split.active, parent.deep_model())
                                && split.nodes[*y]@.from.contains(parent.deep_model())
                                && extends_path(split.nodes, split.active, *y)));
                        proof_assert!(forall<k> split.active.contains(&k) ==> after.contains(k));
                        assert!(
                            self.weave.set_active(&new_id, true),
                            "Setting node active status failed"
                        );
                        proof_assert!(self.weave@.active == old.active.add(*y));
                        proof_assert!(forall<k> self.weave@.active.contains(&k) == after.contains(k));
                        proof_assert!(self.weave@.valid());
                        proof_assert!(downward(self.weave@.nodes, *after) && no_dup(*after));
                        proof_assert!(line_of(self.weave@.nodes, self.weave@.active, *after));
                        proof_assert! {
                            lemma_active_line(self.weave@);
                            active_line(self.weave@) == *after
                        };
                        proof! {
                            let c = snapshot!(old.nodes[line[*stop]].contents.units());
                            let left = snapshot!(split.nodes[line[*stop]].contents.units());
                            let right = snapshot!(split.nodes[*y].contents.units());
                            let before = snapshot!(text(old.nodes, line.subsequence(0, *stop)));
                        }
                        proof_assert!(0 < split_at@ && split_at@ < c.len());
                        proof_assert!(old.nodes[line[*stop]].contents.splits(
                            split_at,
                            DiscreteContentResult::Two(
                                split.nodes[line[*stop]].contents,
                                split.nodes[*y].contents,
                            ),
                        ));
                        proof_assert!(*left == c.subsequence(0, split_at@));
                        proof_assert!(*right == c.subsequence(split_at@, c.len()));
                        proof_assert!(left.concat(*right).ext_eq(*c));
                        proof_assert! {
                            before.concat_assoc(*left, *right);
                            before.concat(*left).concat(*right) == before.concat(*c)
                        };
                        proof_assert! {
                            lemma_text_same(self.weave@.nodes, split.nodes, *after);
                            lemma_text_same(self.weave@.nodes, split.nodes, after.subsequence(0, *stop + 1));
                            lemma_text_around(old.nodes, *line, *stop);
                            text(self.weave@.nodes, *after) == before.concat(*c)
                                .concat(text(old.nodes, line.subsequence(*stop + 1, line.len())))
                        };
                        proof_assert!(active_text(self.weave@) == active_text(*old));

                        true
                    } else {
                        false
                    }
                }
            } else {
                true
            }
        }
        /// Inserts a new node into the active path at the specified index without removing existing node connections.
        ///
        /// If `prefix_all` is true, the inserted node prefixes all continuations, not just the active continuation. This may result in quadratic connection growth when repeatedly inserting at the same position.
        ///
        /// If the index extends past the end of the active path, it is clamped to the active path's length.
        ///
        /// If the index is zero and the active path is non-empty, an empty root node (`T::default()`) will be inserted at the start of the active path if one does not already exist. When this new root node is inserted, `prefix_all` has no effect.
        ///
        /// This function may split up to 1 node and may insert up to 1 additional node if necessary to apply the operation.
        ///
        /// # Panics
        ///
        /// May panic if the underlying [`Weave`] violates any of the wrapper's [requirements](#requirements).
        ///
        /// May panic if `generate_id` panics or returns an identifier already in the Weave.
        #[requires(generates(generate_id, self.weave@.nodes))]
        #[requires(exact_splits::<T>() && total_splits::<T>())]
        #[requires(active_text(self.weave@).len() <= usize::MAX@)]
        // The contents are inserted into the active path's text at `at`, or at its end if `at` is
        // beyond it, and no node is removed.
        #[ensures(active_text((^self).weave@)
            == spliced(active_text(self.weave@), at@, at@, contents.units()))]
        #[ensures(forall<k> self.weave@.nodes.contains(&k) ==> (^self).weave@.nodes.contains(&k))]
        #[ensures((^self).weave.metadata == self.weave.metadata)]
        #[allow(clippy::too_many_lines, reason = "Most of its length is proof code")]
        pub fn insert_at<F>(&mut self, at: usize, contents: T, prefix_all: bool, mut generate_id: F)
        where
            F: FnMut() -> K,
        {
            enum Action {
                InsertAnchor,
                SplitNode,
                Boundary,
            }

            proof! {
                let old = snapshot!(self.weave@);
                let x = snapshot!(contents.units());
                let c0 = snapshot!(contents);
                let start = snapshot!(generate_id);
                let mut mid = snapshot!(self.weave@);
                let mut first = snapshot!(contents.units());
                let mut cut = snapshot!(0);
            }
            snapshot! { lemma_collected::<K> };
            snapshot! { lemma_ids_concat::<K> };
            proof_assert! { lemma_active_line(*old); line_of(old.nodes, old.active, active_line(*old)) };

            self.scratchpad.clear();
            self.weave.get_active_path(&mut self.scratchpad);
            proof! {
                let path = snapshot!(self.scratchpad@);
            }
            proof_assert! { lemma_output_line(*old, ids(*path)); ids(*path).reverse() == active_line(*old) };
            self.scratchpad.reverse();
            proof! {
                let line = snapshot!(ids(self.scratchpad@));
                let total = snapshot!(active_text(*old));
            }
            proof_assert! { lemma_ids_reverse(*path); *line == active_line(*old) };
            proof_assert!(forall<i> 0 <= i && i < line.len() ==> old.nodes.contains(&line[i]));
            proof_assert!(*total == text(old.nodes, *line));

            let (index, parent, child, action) = if at == 0 {
                let mut prefix_len = 0;

                #[invariant(self.weave@ == *old)]
                #[invariant(ids(self.scratchpad@) == *line)]
                #[invariant(prefix_len@ <= line.len())]
                #[invariant(text(old.nodes, line.subsequence(0, prefix_len@)).len() == 0)]
                while prefix_len < self.scratchpad.len()
                    && self
                        .weave
                        .get_contents(&self.scratchpad[prefix_len])
                        .unwrap()
                        .is_empty()
                {
                    proof_assert! {
                        lemma_text_step(old.nodes, *line, prefix_len@);
                        text(old.nodes, line.subsequence(0, prefix_len@ + 1)).len() == 0
                    };
                    #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                    {
                        prefix_len += 1;
                    }
                }

                if prefix_len == 0
                    && let Some(root) = self.scratchpad.first().copied()
                {
                    let id = generate_id();
                    proof_assert! {
                        lemma_generates_call(*start, generate_id, id, old.nodes);
                        !old.nodes.contains(&id.deep_model())
                    };
                    let contents = Self::default_contents();

                    assert!(
                        contents.is_empty(),
                        "`T::default()` must have a length of zero"
                    );

                    proof! {
                        let anchor = snapshot!(NodeView {
                            id: id.deep_model(),
                            from: Seq::empty(),
                            to: Seq::singleton(root.deep_model()),
                            active: false,
                            bookmarked: false,
                        });
                        let default = snapshot!(contents);
                    }
                    proof_assert! {
                        lemma_insertable_root(old.nodes, *anchor);
                        insertable(old.nodes, *anchor)
                    };
                    proof_assert!(split_line(*line, -1, id.deep_model())
                        .ext_eq(Seq::singleton(id.deep_model()).concat(*line)));
                    proof_assert!(root.deep_model() == line[0]);
                    assert!(
                        self.weave.insert(<$N as BuildableNode<K, T>>::new(
                            id,
                            <$N as Node<K, T>>::From::from_iter(iter::empty()),
                            <$N as Node<K, T>>::To::from_iter(iter::once(root)),
                            false,
                            contents
                        )),
                        "Inserting node failed"
                    );
                    proof_assert!(self.weave@.valid());
                    proof_assert!(old.valid());
                    proof_assert!(inserted(*old, self.weave@, *anchor, *default));
                    proof_assert!(line.subsequence(0, 0).ext_eq(Seq::empty()));
                    proof_assert! { lemma_full_prefix(*line); line.subsequence(0, line.len()) == *line };
                    proof_assert! {
                        lemma_insert_line(*old, self.weave@, *anchor, *default, *line, -1);
                        downward(self.weave@.nodes, split_line(*line, -1, id.deep_model()))
                            && no_dup(split_line(*line, -1, id.deep_model()))
                            && text(self.weave@.nodes, split_line(*line, -1, id.deep_model()))
                                == text(old.nodes, line.subsequence(0, 0)).concat(default.units())
                                    .concat(text(old.nodes, line.subsequence(0, line.len())))
                    };
                    proof_assert! {
                        lemma_text_same(self.weave@.nodes, old.nodes, *line);
                        text(self.weave@.nodes, *line) == text(old.nodes, *line)
                    };
                    proof_assert!(text(old.nodes, line.subsequence(0, 0)) == Seq::empty());
                    proof_assert!(downward(self.weave@.nodes, Seq::singleton(id.deep_model()).concat(*line))
                        && no_dup(Seq::singleton(id.deep_model()).concat(*line))
                        && text(self.weave@.nodes, Seq::singleton(id.deep_model()).concat(*line))
                            == Seq::empty().concat(default.units()).concat(*total)
                        && text(self.weave@.nodes, *line) == *total);
                    proof_assert! {
                        Seq::<T::Unit>::empty().concat_empty();
                        default.units().ext_eq(Seq::empty());
                        text(self.weave@.nodes, Seq::singleton(id.deep_model()).concat(*line)) == *total
                    };
                    proof! {
                        mid = snapshot!(self.weave@);
                        first = snapshot!(default.units());
                    }

                    (0, Some(id), Some(root), Action::InsertAnchor)
                } else {
                    (
                        prefix_len,
                        self.scratchpad[..prefix_len].last().copied(),
                        self.scratchpad.get(prefix_len).copied(),
                        Action::Boundary,
                    )
                }
            } else {
                let mut cursor: usize = 0;
                let mut target = None;
                proof! {
                    let mut stop = snapshot!(0);
                }

                #[invariant(self.weave@ == *old)]
                #[invariant(cursor@ == text(old.nodes, line.subsequence(0, produced.len())).len())]
                #[invariant(cursor@ < at@)]
                #[invariant(target == None)]
                for index in 0..self.scratchpad.len() {
                    let id = &self.scratchpad[index];
                    proof! {
                        let j = snapshot!(index@);
                    }
                    proof_assert!(produced.len() == *j + 1);
                    proof_assert!(0 <= *j && *j < line.len() && id.deep_model() == line[*j]);
                    let length = self.weave.get_contents(id).unwrap().len();
                    proof_assert! {
                        lemma_text_step(old.nodes, *line, *j);
                        lemma_text_split(old.nodes, *line, *j + 1);
                        cursor@ + length@ == text(old.nodes, line.subsequence(0, *j + 1)).len()
                            && cursor@ + length@ <= total.len()
                    };
                    let next = cursor.strict_add(length);

                    if next >= at {
                        proof! {
                            stop = snapshot!(*j);
                        }
                        target = Some((index, length, *id));
                        break;
                    }

                    cursor = next;
                }
                proof_assert!(match target {
                    Some((index, length, p)) => 0 <= *stop && *stop < line.len() && index@ == *stop
                        && p == self.scratchpad@[*stop] && p.deep_model() == line[*stop]
                        && cursor@ == text(old.nodes, line.subsequence(0, *stop)).len()
                        && cursor@ + length@ == text(old.nodes, line.subsequence(0, *stop + 1)).len()
                        && length@ == old.nodes[line[*stop]].contents.units().len()
                        && cursor@ < at@ && at@ <= cursor@ + length@,
                    None => {
                        lemma_full_prefix(*line);
                        cursor@ == total.len()
                    },
                });

                if let Some((index, length, parent)) = target {
                    #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                    let split_at = at - cursor;
                    proof_assert! { lemma_text_split(old.nodes, *line, *stop + 1); at@ <= total.len() };
                    let next = index.strict_add(1);

                    if split_at == length {
                        (
                            next,
                            Some(parent),
                            self.scratchpad.get(next).copied(),
                            Action::Boundary,
                        )
                    } else {
                        let right = generate_id();
                        proof_assert! {
                            lemma_generates_call(*start, generate_id, right, old.nodes);
                            !old.nodes.contains(&right.deep_model())
                        };
                        proof_assert!(parent.deep_model() != right.deep_model());
                        proof_assert!(0 < split_at@ && split_at@ < old.nodes[line[*stop]].contents.units().len());

                        assert!(
                            self.weave.split(&parent, split_at, right),
                            "Splitting node failed"
                        );
                        proof_assert!(old.nodes[line[*stop]].contents.splits(
                            split_at,
                            DiscreteContentResult::Two(
                                self.weave@.nodes[line[*stop]].contents,
                                self.weave@.nodes[right.deep_model()].contents,
                            ),
                        ));
                        proof_assert!(self.weave@.nodes[line[*stop]].contents.units()
                            == old.nodes[line[*stop]].contents.units().subsequence(0, split_at@));
                        proof_assert!(self.weave@.nodes[right.deep_model()].contents.units()
                            == old.nodes[line[*stop]].contents.units().subsequence(
                                split_at@,
                                old.nodes[line[*stop]].contents.units().len(),
                            ));
                        proof_assert! {
                            lemma_split_line(*old, self.weave@, *line, *stop, right.deep_model());
                            downward(self.weave@.nodes, split_line(*line, *stop, right.deep_model()))
                                && no_dup(split_line(*line, *stop, right.deep_model()))
                                && text(self.weave@.nodes, split_line(*line, *stop, right.deep_model()).subsequence(0, *stop + 1))
                                    == text(old.nodes, line.subsequence(0, *stop))
                                        .concat(self.weave@.nodes[line[*stop]].contents.units())
                                && text(self.weave@.nodes, split_line(*line, *stop, right.deep_model()).subsequence(*stop + 1, line.len() + 1))
                                    == self.weave@.nodes[right.deep_model()].contents.units()
                                        .concat(text(old.nodes, line.subsequence(*stop + 1, line.len())))
                        };
                        proof_assert!(split_shape(*old, self.weave@, line[*stop], right.deep_model()));
                        proof! {
                            mid = snapshot!(self.weave@);
                            cut = snapshot!(split_at@);
                        }

                        (next, Some(parent), Some(right), Action::SplitNode)
                    }
                } else {
                    (
                        self.scratchpad.len(),
                        self.scratchpad.last().copied(),
                        None,
                        Action::Boundary,
                    )
                }
            };
            proof_assert!(ids(self.scratchpad@) == *line);
            proof_assert!(generates(generate_id, self.weave@.nodes));
            proof_assert!(match action {
                Action::InsertAnchor => index@ == 0 && 0 < line.len() && at@ == 0
                    && first.len() == 0
                    && child == Some(self.scratchpad@[0]) && self.weave@ == *mid
                    && match parent {
                        Some(a) => !old.nodes.contains(&a.deep_model())
                            && downward(mid.nodes, Seq::singleton(a.deep_model()).concat(*line))
                            && no_dup(Seq::singleton(a.deep_model()).concat(*line))
                            && text(mid.nodes, Seq::singleton(a.deep_model()).concat(*line)) == *total
                            && mid.nodes[line[0]]@.from.contains(a.deep_model())
                            && mid.nodes[a.deep_model()]@.from.len() == 0
                            && forall<k> mid.nodes.contains(&k) == (old.nodes.contains(&k) || k == a.deep_model()),
                        None => false,
                    },
                Action::SplitNode => 0 < index@ && index@ <= line.len()
                    && parent == Some(self.scratchpad@[index@ - 1]) && self.weave@ == *mid
                    && match child {
                        Some(r) => split_shape(*old, *mid, line[index@ - 1], r.deep_model())
                            && downward(mid.nodes, split_line(*line, index@ - 1, r.deep_model()))
                            && no_dup(split_line(*line, index@ - 1, r.deep_model()))
                            && 0 < *cut && *cut < old.nodes[line[index@ - 1]].contents.units().len()
                            && at@ == text(old.nodes, line.subsequence(0, index@ - 1)).len() + *cut
                            && text(mid.nodes, split_line(*line, index@ - 1, r.deep_model()).subsequence(0, index@))
                                == text(old.nodes, line.subsequence(0, index@ - 1))
                                    .concat(old.nodes[line[index@ - 1]].contents.units().subsequence(0, *cut))
                            && text(mid.nodes, split_line(*line, index@ - 1, r.deep_model()).subsequence(index@, line.len() + 1))
                                == old.nodes[line[index@ - 1]].contents.units()
                                    .subsequence(*cut, old.nodes[line[index@ - 1]].contents.units().len())
                                    .concat(text(old.nodes, line.subsequence(index@, line.len())))
                            && mid.nodes[r.deep_model()]@.from == Seq::singleton(line[index@ - 1])
                            && forall<k> mid.nodes.contains(&k) == (old.nodes.contains(&k) || k == r.deep_model()),
                        None => false,
                    },
                Action::Boundary => self.weave@ == *old && index@ <= line.len()
                    && (index@ == 0 ==> line.len() == 0)
                    && parent == (if index@ > 0 { Some(self.scratchpad@[index@ - 1]) } else { None })
                    && child == (if index@ < line.len() { Some(self.scratchpad@[index@]) } else { None })
                    && text(old.nodes, line.subsequence(0, index@)).len()
                        == if at@ < total.len() { at@ } else { total.len() },
            });

            let id = generate_id();
            proof_assert! {
                lemma_generates_call(*start, generate_id, id, old.nodes);
                !self.weave@.nodes.contains(&id.deep_model())
            };
            proof! {
                let y = snapshot!(id.deep_model());
                let before = snapshot!(self.weave@);
                let rank = snapshot!(rank_of(before.nodes));
                let par = snapshot!(parent);
                let chi = snapshot!(child);
            }
            proof_assert!(before.valid());
            proof_assert!(forall<k> before.nodes.contains(&k) ==> linked(before.nodes, k));
            proof_assert!(ranked(before.nodes, *rank));

            match action {
                Action::InsertAnchor => {
                    proof! {
                        let a = snapshot!(match *par { Some(a) => a.deep_model(), None => *y });
                        let anchored = snapshot!(Seq::singleton(*a).concat(*line));
                        let node = snapshot!(NodeView {
                            id: *y,
                            from: Seq::singleton(*a),
                            to: Seq::singleton(line[0]),
                            active: false,
                            bookmarked: false,
                        });
                    }
                    proof_assert!(rank[*a] < rank[line[0]]);
                    proof_assert!(ranked_above(before.nodes, *a, Seq::singleton(line[0])));
                    proof_assert! {
                        lemma_insertable_under(before.nodes, *node, *a);
                        insertable(before.nodes, *node)
                    };
                    proof_assert! {
                        lemma_cons_parts(*a, *line, *y);
                        anchored[0] == *a && anchored[1] == line[0]
                            && anchored.subsequence(0, 1) == Seq::singleton(*a)
                            && anchored.subsequence(1, anchored.len()) == *line
                            && split_line(*anchored, 0, *y)
                                == Seq::singleton(*a).concat(Seq::singleton(*y)).concat(*line)
                    };
                    assert!(
                        self.weave.insert(<$N as BuildableNode<K, T>>::new(
                            id,
                            <$N as Node<K, T>>::From::from_iter(parent),
                            <$N as Node<K, T>>::To::from_iter(child),
                            false,
                            contents
                        )),
                        "Inserting node failed"
                    );
                    proof_assert!(inserted(*before, self.weave@, *node, *c0));
                    proof_assert! {
                        lemma_insert_line(*before, self.weave@, *node, *c0, *anchored, 0);
                        downward(self.weave@.nodes, split_line(*anchored, 0, *y))
                            && no_dup(split_line(*anchored, 0, *y))
                            && text(self.weave@.nodes, split_line(*anchored, 0, *y))
                                == text(before.nodes, anchored.subsequence(0, 1)).concat(*x)
                                    .concat(text(before.nodes, anchored.subsequence(1, anchored.len())))
                    };
                    proof! {
                        let after = snapshot!(split_line(*anchored, 0, *y));
                        let linked_view = snapshot!(self.weave@);
                    }
                    proof_assert!(*after == Seq::singleton(*a).concat(Seq::singleton(*y)).concat(*line));
                    proof! {
                        let pk = snapshot!(match *par { Some(p) => p, None => id });
                    }

                    let active = iter::once(parent.unwrap())
                        .chain(iter::once(id))
                        .chain(self.scratchpad.iter().copied());
                    proof! {
                        let path = snapshot!(active);
                    }
                    proof_assert! {
                        lemma_iterates_two_then::<K>();
                        forall<items: Seq<K>> iterates(*path, items)
                            ==> items == Seq::singleton(*pk).concat(Seq::singleton(id))
                                .concat(self.scratchpad@)
                    };
                    proof_assert!(forall<items: Seq<K>> iterates(*path, items) ==> ids(items) == *after);
                    self.weave.set_active_path(active);
                    proof_assert!(exists<listed: Seq<K>, requested: FSet<Id<K>>> iterates(*path, listed)
                        && (forall<k> requested.contains(&k) == (ids(listed).contains(k) && linked_view.nodes.contains(&k)))
                        && longest_path_within(linked_view.nodes, requested, self.weave@.active));
                    proof_assert!(downward(linked_view.nodes, *after) && no_dup(*after));
                    proof_assert!(forall<items: Seq<K>> iterates(*path, items) ==> ids(items) == *after);
                    proof_assert!(forall<k> after.contains(k) ==> linked_view.nodes.contains(&k));
                    proof_assert!(forall<listed: Seq<K>, requested: FSet<Id<K>>> iterates(*path, listed)
                        && (forall<k> requested.contains(&k) == (ids(listed).contains(k) && linked_view.nodes.contains(&k)))
                        && longest_path_within(linked_view.nodes, requested, self.weave@.active)
                        ==> ids(listed) == *after
                            && (forall<k> requested.contains(&k) == after.contains(k))
                            && {
                                lemma_longest_line(linked_view.nodes, requested, self.weave@.active, *after);
                                line_of(linked_view.nodes, self.weave@.active, *after)
                            });
                    proof_assert!(line_of(linked_view.nodes, self.weave@.active, *after));
                    proof_assert! {
                        lemma_same_graph_line(self.weave@.nodes, linked_view.nodes, *after);
                        line_of(self.weave@.nodes, self.weave@.active, *after)
                    };
                    proof_assert! {
                        lemma_active_line(self.weave@);
                        active_line(self.weave@) == *after
                    };
                    proof_assert! {
                        lemma_text_single(before.nodes, *a);
                        text(before.nodes, anchored.subsequence(0, 1)) == first.concat(Seq::empty())
                    };
                    proof_assert!(first.ext_eq(Seq::empty()));
                    proof_assert!(text(before.nodes, *line) == *total);
                    proof_assert! {
                        lemma_same_graph_line(self.weave@.nodes, linked_view.nodes, *after);
                        active_text(self.weave@) == Seq::empty().concat(*x).concat(*total)
                    };
                    proof_assert! {
                        lemma_spliced_between(Seq::empty(), *total, at@, *x);
                        total.concat_empty();
                        active_text(self.weave@) == spliced(*total, at@, at@, *x)
                    };
                }
                Action::SplitNode => {
                    proof! {
                        let t = snapshot!(index@ - 1);
                        let r = snapshot!(match *chi { Some(r) => r.deep_model(), None => *y });
                        let split = snapshot!(split_line(*line, *t, *r));
                        let node = snapshot!(NodeView {
                            id: *y,
                            from: Seq::singleton(line[*t]),
                            to: Seq::singleton(*r),
                            active: false,
                            bookmarked: false,
                        });
                    }
                    proof_assert!(rank[line[*t]] < rank[*r]);
                    proof_assert!(ranked_above(before.nodes, line[*t], Seq::singleton(*r)));
                    proof_assert! {
                        lemma_insertable_under(before.nodes, *node, line[*t]);
                        insertable(before.nodes, *node)
                    };
                    proof_assert!(split[*t] == line[*t] && split[*t + 1] == *r && split.len() == line.len() + 1);
                    proof_assert!(*t + 1 == index@);
                    assert!(
                        self.weave.insert(<$N as BuildableNode<K, T>>::new(
                            id,
                            <$N as Node<K, T>>::From::from_iter(parent),
                            <$N as Node<K, T>>::To::from_iter(child),
                            false,
                            contents
                        )),
                        "Inserting node failed"
                    );
                    proof_assert!(inserted(*before, self.weave@, *node, *c0));
                    proof_assert! {
                        lemma_insert_line(*before, self.weave@, *node, *c0, *split, *t);
                        downward(self.weave@.nodes, split_line(*split, *t, *y))
                            && no_dup(split_line(*split, *t, *y))
                            && text(self.weave@.nodes, split_line(*split, *t, *y))
                                == text(before.nodes, split.subsequence(0, *t + 1)).concat(*x)
                                    .concat(text(before.nodes, split.subsequence(*t + 1, split.len())))
                    };
                    proof! {
                        let after = snapshot!(split_line(*split, *t, *y));
                        let linked_view = snapshot!(self.weave@);
                    }
                    proof_assert! {
                        lemma_split_twice(*line, *t, *r, *y);
                        *after == line.subsequence(0, index@).concat(Seq::singleton(*y))
                            .concat(Seq::singleton(*r)).concat(line.subsequence(index@, line.len()))
                    };
                    proof_assert! {
                        lemma_ids_sub(self.scratchpad@, 0, index@);
                        lemma_ids_sub(self.scratchpad@, index@, line.len());
                        ids(self.scratchpad@.subsequence(0, index@)) == line.subsequence(0, index@)
                            && ids(self.scratchpad@.subsequence(index@, line.len())) == line.subsequence(index@, line.len())
                    };
                    proof! {
                        let ck = snapshot!(match *chi { Some(c) => c, None => id });
                    }

                    let active = self.scratchpad[..index]
                        .iter()
                        .copied()
                        .chain(iter::once(id))
                        .chain(iter::once(child.unwrap()))
                        .chain(self.scratchpad[index..].iter().copied());
                    proof! {
                        let path = snapshot!(active);
                    }
                    proof_assert! {
                        lemma_iterates_two_between::<K>();
                        forall<items: Seq<K>> iterates(*path, items)
                            ==> items == self.scratchpad@.subsequence(0, index@).concat(Seq::singleton(id))
                                .concat(Seq::singleton(*ck))
                                .concat(self.scratchpad@.subsequence(index@, line.len()))
                    };
                    proof_assert!(forall<items: Seq<K>> iterates(*path, items) ==> ids(items) == *after);
                    self.weave.set_active_path(active);
                    proof_assert!(exists<listed: Seq<K>, requested: FSet<Id<K>>> iterates(*path, listed)
                        && (forall<k> requested.contains(&k) == (ids(listed).contains(k) && linked_view.nodes.contains(&k)))
                        && longest_path_within(linked_view.nodes, requested, self.weave@.active));
                    proof_assert!(downward(linked_view.nodes, *after) && no_dup(*after));
                    proof_assert!(forall<items: Seq<K>> iterates(*path, items) ==> ids(items) == *after);
                    proof_assert!(forall<k> after.contains(k) ==> linked_view.nodes.contains(&k));
                    proof_assert!(forall<listed: Seq<K>, requested: FSet<Id<K>>> iterates(*path, listed)
                        && (forall<k> requested.contains(&k) == (ids(listed).contains(k) && linked_view.nodes.contains(&k)))
                        && longest_path_within(linked_view.nodes, requested, self.weave@.active)
                        ==> ids(listed) == *after
                            && (forall<k> requested.contains(&k) == after.contains(k))
                            && {
                                lemma_longest_line(linked_view.nodes, requested, self.weave@.active, *after);
                                line_of(linked_view.nodes, self.weave@.active, *after)
                            });
                    proof_assert!(line_of(linked_view.nodes, self.weave@.active, *after));
                    proof_assert! {
                        lemma_same_graph_line(self.weave@.nodes, linked_view.nodes, *after);
                        line_of(self.weave@.nodes, self.weave@.active, *after)
                    };
                    proof_assert! {
                        lemma_active_line(self.weave@);
                        active_line(self.weave@) == *after
                    };
                    proof! {
                        let a = snapshot!(text(old.nodes, line.subsequence(0, *t)));
                        let c = snapshot!(old.nodes[line[*t]].contents.units());
                        let b = snapshot!(text(old.nodes, line.subsequence(*t + 1, line.len())));
                    }
                    proof_assert! { lemma_text_around(old.nodes, *line, *t); *total == a.concat(*c).concat(*b) };
                    proof_assert!(split.len() == line.len() + 1);
                    proof_assert! {
                        lemma_same_graph_line(self.weave@.nodes, linked_view.nodes, *after);
                        active_text(self.weave@)
                            == a.concat(c.subsequence(0, *cut)).concat(*x)
                                .concat(c.subsequence(*cut, c.len()).concat(*b))
                    };
                    proof_assert! {
                        lemma_spliced_inside(*a, *c, *b, *cut, *x);
                        a.concat(c.subsequence(0, *cut)).concat(*x).concat_assoc(c.subsequence(*cut, c.len()), *b);
                        active_text(self.weave@) == spliced(*total, at@, at@, *x)
                    };
                }
                Action::Boundary => {
                    proof! {
                        let to_model = snapshot!(if prefix_all && index@ > 0 {
                            old.nodes[line[index@ - 1]]@.to
                        } else if index@ < line.len() {
                            Seq::singleton(line[index@])
                        } else {
                            Seq::empty()
                        });
                        let node = snapshot!(NodeView {
                            id: *y,
                            from: if index@ > 0 { Seq::singleton(line[index@ - 1]) } else { Seq::empty() },
                            to: *to_model,
                            active: true,
                            bookmarked: false,
                        });
                    }
                    proof_assert!(before.nodes == old.nodes);
                    proof_assert!(index@ > 0 ==> linked(old.nodes, line[index@ - 1]));
                    proof_assert!(index@ > 0 ==> forall<c> old.nodes[line[index@ - 1]]@.to.contains(c)
                        ==> old.nodes.contains(&c) && linked(old.nodes, c)
                            && old.nodes[c]@.from.contains(line[index@ - 1])
                            && rank[line[index@ - 1]] < rank[c]);
                    proof_assert!(index@ > 0 && index@ < line.len()
                        ==> old.nodes[line[index@]]@.from.contains(line[index@ - 1])
                            && old.nodes[line[index@ - 1]]@.to.contains(line[index@])
                            && rank[line[index@ - 1]] < rank[line[index@]]);
                    proof_assert!(index@ > 0 ==> forall<c> to_model.contains(c)
                        ==> old.nodes.contains(&c) && rank[line[index@ - 1]] < rank[c]);
                    proof_assert!(index@ > 0 ==> ranked_above(before.nodes, line[index@ - 1], *to_model));
                    proof_assert!(no_dup(*to_model));
                    proof_assert!(match *par {
                        Some(p) => index@ > 0 && p.deep_model() == line[index@ - 1],
                        None => index@ == 0,
                    });
                    proof_assert!(match *chi {
                        Some(c) => index@ < line.len() && c.deep_model() == line[index@],
                        None => index@ == line.len(),
                    });
                    proof_assert!(index@ > 0 ==> {
                        lemma_insertable_under(before.nodes, *node, line[index@ - 1]);
                        insertable(before.nodes, *node)
                    });
                    proof_assert!(index@ == 0 ==> {
                        lemma_insertable_root(before.nodes, *node);
                        insertable(before.nodes, *node)
                    });
                    proof_assert!(insertable(before.nodes, *node));

                    let to = if prefix_all && let Some(parent) = parent {
                        Self::copied_ids(self.weave.get_children(&parent).unwrap())
                    } else {
                        <$N as Node<K, T>>::To::from_iter(child)
                    };
                    proof_assert!(to.model() == *to_model);
                    let from = <$N as Node<K, T>>::From::from_iter(parent);
                    proof_assert!(from.model() == node.from);

                    assert!(
                        self.weave.insert(<$N as BuildableNode<K, T>>::new(
                            id, from, to, true, contents,
                        )),
                        "Inserting node failed"
                    );
                    proof_assert!(inserted(*before, self.weave@, *node, *c0));
                    proof_assert!(activated(self.weave@.nodes, before.active, *y, self.weave@.active));
                    proof! {
                        let after = snapshot!(split_line(*line, index@ - 1, *y));
                    }
                    proof_assert!(downward(before.nodes, *line) && no_dup(*line));
                    proof_assert!(index@ < line.len() ==> to_model.contains(line[index@]));
                    proof_assert!(index@ > 0 ==> node.from == Seq::singleton(line[index@ - 1]));
                    proof_assert!(index@ == 0 ==> node.from.len() == 0);
                    proof_assert!(!before.nodes.contains(&node.id));
                    proof_assert! {
                        lemma_insert_line(*before, self.weave@, *node, *c0, *line, index@ - 1);
                        downward(self.weave@.nodes, *after) && no_dup(*after)
                            && (0 < index@ ==> downward(self.weave@.nodes, *line))
                            && text(self.weave@.nodes, split_line(*line, index@ - 1, *y))
                                == text(before.nodes, line.subsequence(0, index@ - 1 + 1)).concat(c0.units())
                                    .concat(text(before.nodes, line.subsequence(index@ - 1 + 1, line.len())))
                    };
                    proof_assert!(index@ - 1 + 1 == index@ && c0.units() == *x);
                    proof_assert!(text(self.weave@.nodes, *after)
                        == text(old.nodes, line.subsequence(0, index@)).concat(*x)
                            .concat(text(old.nodes, line.subsequence(index@, line.len()))));
                    proof_assert!(index@ == 0 ==> line.len() == 0 && downward(self.weave@.nodes, *line));
                    proof_assert!(downward(self.weave@.nodes, *line));
                    proof_assert!(before.active == old.active);
                    proof_assert!(line_of(self.weave@.nodes, old.active, *line));
                    proof_assert!(!old.active.contains(&*y));
                    proof_assert!(self.weave@.nodes[*y]@.to == *to_model);
                    proof_assert!(index@ == 0 ==> to_model.len() == 0
                        && below_end(self.weave@.nodes, *line, self.weave@.nodes[*y]@.to));
                    proof_assert!(index@ == 0 ==> {
                        lemma_activated_root(self.weave@, old.active, *line, *y, self.weave@.active);
                        after.ext_eq(Seq::singleton(*y))
                            && line_of(self.weave@.nodes, self.weave@.active, Seq::singleton(*y))
                    } && line_of(self.weave@.nodes, self.weave@.active, *after));
                    proof_assert!(0 < index@ ==> forall<c> to_model.contains(c)
                        ==> old.nodes[c]@.from.contains(line[index@ - 1]) && old.nodes.contains(&c));
                    proof_assert!(0 < index@ ==> forall<c> to_model.contains(c) ==> {
                        lemma_snoc_contains(old.nodes[c]@.from, *y, line[index@ - 1]);
                        true
                    } && self.weave@.nodes[c]@.from.contains(line[index@ - 1]));
                    proof_assert!(ranked(self.weave@.nodes, rank_of(self.weave@.nodes)));
                    proof_assert!(0 < index@ ==> forall<c> to_model.contains(c)
                        ==> rank_of(self.weave@.nodes)[line[index@ - 1]] < rank_of(self.weave@.nodes)[c]);
                    proof_assert!(index@ == line.len() && 0 < index@
                        ==> below_end(self.weave@.nodes, *line, self.weave@.nodes[*y]@.to));
                    proof_assert!(index@ == line.len() && 0 < index@ ==> {
                        lemma_activated_after(self.weave@, old.active, *line, index@ - 1, *y, self.weave@.active);
                        lemma_split_line_end(*line, *y);
                        *after == line.subsequence(0, index@).snoc(*y)
                            && line_of(self.weave@.nodes, self.weave@.active, line.subsequence(0, index@).snoc(*y))
                    } && line_of(self.weave@.nodes, self.weave@.active, *after));
                    proof_assert!(0 < index@ && index@ < line.len()
                        ==> self.weave@.nodes[*y]@.to.contains(line[index@]));
                    proof_assert!(0 < index@ ==> self.weave@.nodes[*y]@.from == Seq::singleton(line[index@ - 1]));
                    proof_assert!(0 < index@ && index@ < line.len() ==> {
                        lemma_activated_between(self.weave@, old.active, *line, index@ - 1, *y, self.weave@.active);
                        line_of(self.weave@.nodes, self.weave@.active, *after)
                    });
                    proof_assert!(line_of(self.weave@.nodes, self.weave@.active, *after));
                    proof_assert! {
                        lemma_active_line(self.weave@);
                        active_line(self.weave@) == *after
                    };
                    proof_assert! {
                        lemma_text_split(old.nodes, *line, index@);
                        lemma_halves(*total, text(old.nodes, line.subsequence(0, index@)).len());
                        text(old.nodes, line.subsequence(0, index@))
                            .concat(text(old.nodes, line.subsequence(index@, line.len())))
                            == *total
                    };
                    proof_assert! {
                        lemma_spliced_between(
                            text(old.nodes, line.subsequence(0, index@)),
                            text(old.nodes, line.subsequence(index@, line.len())),
                            at@,
                            *x,
                        );
                        active_text(self.weave@) == spliced(*total, at@, at@, *x)
                    };
                }
            }
        }
        /// Replaces the specified range of the active path without removing the old content from the underlying Weave.
        ///
        /// If `prefix_all` is true, the inserted content prefixes all continuations of the last replaced node, not just the active continuation. This may result in quadratic connection growth when repeatedly replacing the same range.
        ///
        /// If the range extends beyond the active path, its length is clamped to the active path's length.
        ///
        /// If `range` is empty, this function behaves identically to [`Self::insert_at()`].
        ///
        /// This function may split up to 2 nodes and may insert up to 1 additional node if necessary to apply the operation.
        ///
        /// # Panics
        ///
        /// May panic if the underlying [`Weave`] violates any of the wrapper's [requirements](#requirements).
        ///
        /// May panic if `generate_id` panics or returns an identifier already in the Weave.
        #[requires(generates(generate_id, self.weave@.nodes))]
        #[requires(exact_splits::<T>() && total_splits::<T>())]
        #[requires(active_text(self.weave@).len() <= usize::MAX@)]
        // The units from `range.start` to `range.end` of the active path's text, which the range is
        // clamped to, are replaced by the contents, and no node is removed.
        #[ensures(active_text((^self).weave@)
            == spliced(active_text(self.weave@), range.start@, range.end@, contents.units()))]
        #[ensures(forall<k> self.weave@.nodes.contains(&k) ==> (^self).weave@.nodes.contains(&k))]
        #[ensures((^self).weave.metadata == self.weave.metadata)]
        #[allow(clippy::too_many_lines, reason = "Most of its length is proof code")]
        pub fn replace<F>(
            &mut self,
            range: Range<usize>,
            contents: T,
            prefix_all: bool,
            mut generate_id: F,
        ) where
            F: FnMut() -> K,
        {
            proof! {
                let old = snapshot!(self.weave@);
                let x = snapshot!(contents.units());
                let c0 = snapshot!(contents);
            }
            snapshot! { lemma_collected::<K> };
            snapshot! { lemma_ids_concat::<K> };
            proof_assert! { lemma_active_line(*old); line_of(old.nodes, old.active, active_line(*old)) };

            if range.is_empty() {
                proof_assert! {
                    lemma_spliced_empty_range(active_text(*old), range.start@, range.end@, *x);
                    spliced(active_text(*old), range.start@, range.end@, *x)
                        == spliced(active_text(*old), range.start@, range.start@, *x)
                };
                self.insert_at(range.start, contents, prefix_all, generate_id);
                return;
            }

            self.scratchpad.clear();
            self.weave.get_active_path(&mut self.scratchpad);
            proof! {
                let path = snapshot!(self.scratchpad@);
            }
            proof_assert! { lemma_output_line(*old, ids(*path)); ids(*path).reverse() == active_line(*old) };
            self.scratchpad.reverse();
            proof! {
                let line = snapshot!(ids(self.scratchpad@));
                let total = snapshot!(active_text(*old));
            }
            proof_assert! { lemma_ids_reverse(*path); *line == active_line(*old) };
            proof_assert!(forall<i> 0 <= i && i < line.len() ==> old.nodes.contains(&line[i]));
            proof_assert!(*total == text(old.nodes, *line));
            proof_assert!(downward(old.nodes, *line) && no_dup(*line));
            proof_assert!(range.start@ < range.end@);
            proof_assert! { lemma_full_prefix(*line); line.subsequence(0, line.len()) == *line };

            let Some((prefix_len, start_split, end)) = self.locate_range(&range) else {
                proof_assert!(self.weave@ == *old);
                proof_assert!(0 < line.len() ==> self.scratchpad@[line.len() - 1].deep_model() == line[line.len() - 1]);
                Self::insert_tail_node(
                    &mut self.weave,
                    self.scratchpad.last().copied(),
                    contents,
                    prefix_all,
                    &mut generate_id,
                );
                proof_assert! {
                    lemma_spliced_end(*total, range.start@, range.end@, *x);
                    spliced(*total, range.start@, range.end@, *x) == total.concat(*x)
                };

                return;
            };
            proof_assert!(self.weave@ == *old && ids(self.scratchpad@) == *line);

            proof! {
                let found = snapshot!(end);
            }
            let (replaced_end, suffix_index, end, start_right) =
                self.split_bounds(&range, prefix_len, start_split, end, &mut generate_id);
            proof! {
                let v2 = snapshot!(self.weave@);
                let l2 = snapshot!(bounded(*line, start_split, *found, end, start_right));
                let q2 = snapshot!(bounded_end(line.len(), start_split, *found));
            }
            proof_assert!(ids(self.scratchpad@) == *line);
            proof_assert!(v2.valid());
            proof_assert!(generates(generate_id, v2.nodes));
            proof_assert!(forall<k> old.nodes.contains(&k) ==> v2.nodes.contains(&k));
            proof_assert!(downward(v2.nodes, *l2) && no_dup(*l2) && text(v2.nodes, *l2) == *total);
            proof_assert!(prefix_len@ <= l2.len()
                && l2.subsequence(0, prefix_len@) == line.subsequence(0, prefix_len@)
                && offset(v2.nodes, *l2, prefix_len@) == range.start@);
            proof_assert!(match end {
                Some(e) => prefix_len@ <= *q2 && 0 < *q2 && *q2 < l2.len() && l2[*q2] == e.deep_model()
                    && offset(v2.nodes, *l2, *q2) == range.end@
                    && l2.subsequence(*q2, l2.len())
                        == Seq::singleton(e.deep_model()).concat(line.subsequence(suffix_index@, line.len())),
                None => *q2 == l2.len() && total.len() <= range.end@,
            });
            proof_assert!(prefix_len@ <= replaced_end@ && replaced_end@ <= line.len());
            proof_assert!(prefix_len@ < replaced_end@
                ==> self.scratchpad@[replaced_end@ - 1].deep_model() == line[replaced_end@ - 1]);

            let replaced = if replaced_end > prefix_len {
                #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                Some(self.scratchpad[replaced_end - 1])
            } else {
                start_right
            };
            proof! {
                let rep = snapshot!(replaced);
            }
            proof_assert!(match end {
                Some(_) => match *rep {
                    Some(r) => prefix_len@ < *q2 && r.deep_model() == l2[*q2 - 1],
                    None => *q2 == prefix_len@,
                },
                None => match *rep {
                    Some(r) => prefix_len@ < l2.len() && r.deep_model() == l2[l2.len() - 1],
                    None => false,
                },
            });

            let prefix = &self.scratchpad[..prefix_len];
            let suffix = &self.scratchpad[suffix_index..];
            proof_assert!(prefix@ == self.scratchpad@.subsequence(0, prefix_len@) && prefix@.len() == prefix_len@);

            proof! {
                let gen2 = snapshot!(generate_id);
            }
            let id = generate_id();
            proof_assert! {
                lemma_generates_call(*gen2, generate_id, id, v2.nodes);
                !v2.nodes.contains(&id.deep_model())
            };
            proof! {
                let y = snapshot!(id.deep_model());
                let rank = snapshot!(rank_of(v2.nodes));
                let end_set = snapshot!(match end { Some(e) => Seq::singleton(e.deep_model()), None => Seq::empty() });
                let copied = snapshot!(if prefix_all { *rep } else { None });
                let to_model = snapshot!(match *copied {
                    Some(r) => v2.nodes[r.deep_model()]@.to,
                    None => *end_set,
                });
                let node = snapshot!(NodeView {
                    id: *y,
                    from: if prefix_len@ > 0 { Seq::singleton(l2[prefix_len@ - 1]) } else { Seq::empty() },
                    to: *to_model,
                    active: match end { Some(_) => false, None => true },
                    bookmarked: false,
                });
            }
            proof_assert!(v2.valid());
            proof_assert!(forall<k> v2.nodes.contains(&k) ==> linked(v2.nodes, k));
            proof_assert!(ranked(v2.nodes, *rank));
            proof_assert!(downward(v2.nodes, *l2));
            proof_assert! {
                lemma_line_ranks(v2.nodes, *rank, *l2);
                increasing_steps(*l2, *rank)
            };
            proof_assert! {
                lemma_increasing(*l2, *rank);
                forall<a, b> 0 <= a && a < b && b < l2.len() ==> rank[l2[a]] < rank[l2[b]]
            };
            proof_assert!(forall<i> 0 <= i && i < l2.len() ==> v2.nodes.contains(&l2[i]));
            proof! {
                let rq = snapshot!(match end { Some(_) => *q2 - 1, None => l2.len() - 1 });
            }
            proof_assert!(match *rep {
                Some(r) => prefix_len@ <= *rq && *rq < l2.len() && r.deep_model() == l2[*rq],
                None => true,
            });
            proof_assert!(match *rep {
                Some(r) => v2.nodes.contains(&r.deep_model()) && linked(v2.nodes, r.deep_model()),
                None => true,
            });
            proof_assert!(match *rep {
                Some(r) => forall<c> v2.nodes[r.deep_model()]@.to.contains(c)
                    ==> v2.nodes.contains(&c) && v2.nodes[c]@.from.contains(r.deep_model()),
                None => true,
            });
            proof_assert!(match *rep {
                Some(r) => forall<c> v2.nodes[r.deep_model()]@.to.contains(c) ==> rank[r.deep_model()] < rank[c],
                None => true,
            });
            proof_assert!(match *rep {
                Some(_) => 0 < prefix_len@ ==> 0 <= prefix_len@ - 1 && prefix_len@ - 1 < *rq
                    && rank[l2[prefix_len@ - 1]] < rank[l2[*rq]],
                None => true,
            });
            proof_assert!(match end {
                Some(e) => match *rep {
                    Some(_) => *rq + 1 == *q2 && l2[*q2] == e.deep_model()
                        && v2.nodes[l2[*q2]]@.from.contains(l2[*rq]),
                    None => true,
                },
                None => true,
            });
            proof_assert!(match end {
                Some(_) => match *rep {
                    Some(_) => linked(v2.nodes, l2[*q2]) && v2.nodes[l2[*rq]]@.to.contains(l2[*q2]),
                    None => true,
                },
                None => true,
            });
            proof_assert!(match end {
                Some(e) => match *rep {
                    Some(r) => v2.nodes[e.deep_model()]@.from.contains(r.deep_model())
                        && v2.nodes[r.deep_model()]@.to.contains(e.deep_model()),
                    None => true,
                },
                None => true,
            });
            proof_assert!(prefix_all ==> *copied == *rep);
            proof_assert!(!prefix_all ==> *copied == None);
            proof_assert!(match *rep {
                Some(r) => prefix_all ==> *to_model == v2.nodes[r.deep_model()]@.to,
                None => true,
            });
            proof_assert!(!prefix_all ==> *to_model == *end_set);
            proof_assert!(*rep == None ==> *to_model == *end_set);
            proof_assert!(!(prefix_all && *rep != None) ==> *to_model == *end_set);
            proof_assert!(match end {
                Some(e) => to_model.contains(e.deep_model()),
                None => true,
            });
            proof_assert!(match end {
                Some(e) => 0 < prefix_len@ ==> 0 <= prefix_len@ - 1 && prefix_len@ - 1 < *q2
                    && rank[l2[prefix_len@ - 1]] < rank[e.deep_model()],
                None => true,
            });
            proof_assert!(0 < prefix_len@ ==> match *rep {
                Some(r) => prefix_all ==> rank[l2[prefix_len@ - 1]] < rank[r.deep_model()]
                    && forall<c> to_model.contains(c) ==> v2.nodes.contains(&c) && rank[l2[prefix_len@ - 1]] < rank[c],
                None => true,
            });
            proof_assert!(0 < prefix_len@ && !(prefix_all && *rep != None) ==> forall<c> to_model.contains(c)
                ==> v2.nodes.contains(&c) && rank[l2[prefix_len@ - 1]] < rank[c]);
            proof_assert!(0 < prefix_len@ ==> forall<c> to_model.contains(c)
                ==> v2.nodes.contains(&c) && rank[l2[prefix_len@ - 1]] < rank[c]);
            proof_assert!(0 < prefix_len@ ==> ranked_above(v2.nodes, l2[prefix_len@ - 1], *to_model));
            proof_assert!(match *rep {
                Some(_) => prefix_all ==> no_dup(*to_model),
                None => true,
            });
            proof_assert!(!(prefix_all && *rep != None) ==> no_dup(*to_model));
            proof_assert!(no_dup(*to_model));
            proof_assert!(forall<c> to_model.contains(c) ==> v2.nodes.contains(&c));
            proof_assert!(0 < prefix_len@ ==> {
                lemma_insertable_under(v2.nodes, *node, l2[prefix_len@ - 1]);
                insertable(v2.nodes, *node)
            });
            proof_assert!(prefix_len@ == 0 ==> {
                lemma_insertable_root(v2.nodes, *node);
                insertable(v2.nodes, *node)
            });
            proof_assert!(insertable(v2.nodes, *node));

            let to = if prefix_all && let Some(replaced) = replaced {
                Self::copied_ids(self.weave.get_children(&replaced).unwrap())
            } else {
                <$N as Node<K, T>>::To::from_iter(end)
            };
            proof_assert!(to.model() == *to_model);

            if let Some(prefix_tail) = prefix.last().copied() {
                proof_assert!(0 < prefix_len@ && prefix_tail == prefix@[prefix_len@ - 1]);
                proof_assert!(prefix@[prefix_len@ - 1] == self.scratchpad@[prefix_len@ - 1]);
                proof_assert!(ids(self.scratchpad@)[prefix_len@ - 1] == self.scratchpad@[prefix_len@ - 1].deep_model());
                proof_assert!(prefix_tail.deep_model() == line[prefix_len@ - 1]);
                proof_assert!(l2.subsequence(0, prefix_len@)[prefix_len@ - 1] == l2[prefix_len@ - 1]
                    && line.subsequence(0, prefix_len@)[prefix_len@ - 1] == line[prefix_len@ - 1]);
                proof_assert!(prefix_tail.deep_model() == l2[prefix_len@ - 1]);
                let from = <$N as Node<K, T>>::From::from_iter(iter::once(prefix_tail));
                proof_assert!(from.model() == node.from);

                assert!(
                    self.weave.insert(<$N as BuildableNode<K, T>>::new(
                        id,
                        from,
                        to,
                        end.is_none(),
                        contents
                    )),
                    "Inserting node failed"
                );
                proof_assert!(inserted(*v2, self.weave@, *node, *c0));

                if let Some(end) = end {
                    proof! {
                        let linked_view = snapshot!(self.weave@);
                        let after = snapshot!(between(*l2, prefix_len@, *q2, *y));
                    }
                    proof_assert! {
                        lemma_insert_between(*v2, self.weave@, *node, *c0, *l2, prefix_len@, *q2);
                        downward(self.weave@.nodes, *after) && no_dup(*after)
                            && text(self.weave@.nodes, *after)
                                == text(v2.nodes, l2.subsequence(0, prefix_len@)).concat(c0.units())
                                    .concat(text(v2.nodes, l2.subsequence(*q2, l2.len())))
                    };
                    proof_assert! {
                        line.subsequence(0, prefix_len@).concat(Seq::singleton(*y)).concat_assoc(
                            Seq::singleton(end.deep_model()),
                            line.subsequence(suffix_index@, line.len()),
                        );
                        *after == line.subsequence(0, prefix_len@).concat(Seq::singleton(*y))
                            .concat(Seq::singleton(end.deep_model()))
                            .concat(line.subsequence(suffix_index@, line.len()))
                    };
                    proof_assert! {
                        lemma_ids_sub(self.scratchpad@, 0, prefix_len@);
                        lemma_ids_sub(self.scratchpad@, suffix_index@, line.len());
                        ids(self.scratchpad@.subsequence(0, prefix_len@)) == line.subsequence(0, prefix_len@)
                            && ids(self.scratchpad@.subsequence(suffix_index@, line.len()))
                                == line.subsequence(suffix_index@, line.len())
                    };

                    let active = prefix
                        .iter()
                        .copied()
                        .chain(iter::once(id))
                        .chain(iter::once(end))
                        .chain(suffix.iter().copied());
                    proof! {
                        let path = snapshot!(active);
                    }
                    proof_assert! {
                        lemma_iterates_two_between::<K>();
                        forall<items: Seq<K>> iterates(*path, items)
                            ==> items == prefix@.concat(Seq::singleton(id)).concat(Seq::singleton(end))
                                .concat(suffix@)
                    };
                    proof_assert!(ids(prefix@) == line.subsequence(0, prefix_len@));
                    proof_assert!(ids(suffix@) == line.subsequence(suffix_index@, line.len()));
                    proof_assert!(forall<items: Seq<K>> iterates(*path, items) ==> ids(items) == *after);
                    Self::set_active_line(&mut self.weave, active);
                    proof_assert!(exists<listed: Seq<K>> iterates(*path, listed)
                        && active_line(self.weave@) == ids(listed));
                    proof_assert!(active_line(self.weave@) == *after);
                    proof_assert! {
                        lemma_same_graph_line(self.weave@.nodes, linked_view.nodes, *after);
                        text(self.weave@.nodes, *after) == text(linked_view.nodes, *after)
                    };
                    proof_assert!(active_text(self.weave@) == text(linked_view.nodes, *after));
                    proof_assert!(offset(v2.nodes, *l2, prefix_len@) == range.start@
                        && offset(v2.nodes, *l2, *q2) == range.end@
                        && prefix_len@ <= *q2 && *q2 < l2.len() && text(v2.nodes, *l2) == *total);
                    proof_assert! {
                        lemma_text_split(v2.nodes, *l2, prefix_len@);
                        text(v2.nodes, l2.subsequence(0, prefix_len@)) == total.subsequence(0, range.start@)
                            && range.start@ <= total.len()
                    };
                    proof_assert! {
                        lemma_text_split(v2.nodes, *l2, *q2);
                        text(v2.nodes, l2.subsequence(*q2, l2.len())) == total.subsequence(range.end@, total.len())
                            && range.end@ <= total.len()
                    };
                    proof_assert!(active_text(self.weave@)
                        == total.subsequence(0, range.start@).concat(*x)
                            .concat(total.subsequence(range.end@, total.len())));
                    proof_assert! {
                        lemma_spliced_range(*total, range.start@, range.end@, *x);
                        active_text(self.weave@) == spliced(*total, range.start@, range.end@, *x)
                    };
                } else {
                    proof! {
                        let la = snapshot!(kept_line(*l2, *line, prefix_len@));
                    }
                    proof_assert! {
                        lemma_kept_line(*l2, *line, prefix_len@);
                        (prefix_len@ < line.len() ==> *la == *l2) && (line.len() <= prefix_len@ ==> *la == *line)
                    };
                    proof_assert!(self.scratchpad@.len() == line.len());
                    proof_assert!(line_of(v2.nodes, v2.active, *la));
                    proof_assert!(l2.subsequence(0, prefix_len@)[prefix_len@ - 1] == l2[prefix_len@ - 1]
                        && line.subsequence(0, prefix_len@)[prefix_len@ - 1] == line[prefix_len@ - 1]);
                    proof_assert!(prefix_len@ == line.len() ==> {
                        lemma_full_prefix(*line);
                        la.subsequence(0, prefix_len@) == *line
                    });
                    proof_assert!(la.subsequence(0, prefix_len@) == l2.subsequence(0, prefix_len@)
                        && la[prefix_len@ - 1] == l2[prefix_len@ - 1] && prefix_len@ <= la.len());
                    proof_assert!(node.from == Seq::singleton(la[prefix_len@ - 1]));
                    proof_assert!(match *rep {
                        Some(r) => r.deep_model() == la[la.len() - 1]
                            || v2.nodes[la[la.len() - 1]]@.to.contains(r.deep_model()),
                        None => false,
                    });
                    proof_assert!(match *rep {
                        Some(r) => v2.nodes.contains(&r.deep_model())
                            && forall<c> to_model.contains(c) ==> v2.nodes[r.deep_model()]@.to.contains(c),
                        None => false,
                    });
                    proof_assert!(match *rep { Some(r) => r.deep_model() == l2[*rq], None => false });
                    proof_assert!(l2[*rq] == la[la.len() - 1] || v2.nodes[la[la.len() - 1]]@.to.contains(l2[*rq]));
                    proof_assert!(v2.nodes.contains(&l2[*rq]));
                    proof_assert!(node.to == *to_model && node.id == *y);
                    proof_assert!(forall<c> node.to.contains(c) ==> v2.nodes[l2[*rq]]@.to.contains(c));
                    proof_assert!(within(node.to, v2.nodes[l2[*rq]]@.to));
                    proof_assert!(0 < prefix_len@ && prefix_len@ <= la.len() && !v2.nodes.contains(&node.id));
                    proof_assert!(activated(self.weave@.nodes, v2.active, node.id, self.weave@.active));
                    proof_assert!(v2.valid() && self.weave@.valid());
                    proof_assert! {
                        lemma_insert_active_after(*v2, self.weave@, *node, *c0, *la, prefix_len@, l2[*rq]);
                        active_line(self.weave@) == la.subsequence(0, prefix_len@).snoc(*y)
                            && text(self.weave@.nodes, la.subsequence(0, prefix_len@).snoc(*y))
                                == text(v2.nodes, la.subsequence(0, prefix_len@)).concat(*x)
                    };
                    proof_assert! {
                        lemma_text_split(v2.nodes, *l2, prefix_len@);
                        text(v2.nodes, l2.subsequence(0, prefix_len@)) == total.subsequence(0, range.start@)
                    };
                    proof_assert!(active_text(self.weave@) == total.subsequence(0, range.start@).concat(*x));
                    proof_assert! {
                        lemma_spliced_range(*total, range.start@, range.end@, *x);
                        active_text(self.weave@) == spliced(*total, range.start@, range.end@, *x)
                    };
                }
            } else {
                proof_assert!(prefix@.len() == 0 && prefix_len@ == 0);
                proof_assert!(range.start@ == 0);
                let from = <$N as Node<K, T>>::From::from_iter(iter::empty());
                proof_assert!(from.model() == node.from);

                assert!(
                    self.weave.insert(<$N as BuildableNode<K, T>>::new(
                        id,
                        from,
                        to,
                        end.is_none(),
                        contents
                    )),
                    "Inserting node failed"
                );
                proof_assert!(inserted(*v2, self.weave@, *node, *c0));

                if let Some(end) = end {
                    proof! {
                        let linked_view = snapshot!(self.weave@);
                        let after = snapshot!(between(*l2, 0, *q2, *y));
                    }
                    proof_assert! {
                        lemma_insert_between(*v2, self.weave@, *node, *c0, *l2, 0, *q2);
                        downward(self.weave@.nodes, *after) && no_dup(*after)
                            && text(self.weave@.nodes, *after)
                                == text(v2.nodes, l2.subsequence(0, 0)).concat(c0.units())
                                    .concat(text(v2.nodes, l2.subsequence(*q2, l2.len())))
                    };
                    proof_assert!(l2.subsequence(0, 0).ext_eq(Seq::empty()));
                    proof_assert! {
                        Seq::singleton(*y).concat_empty();
                        Seq::singleton(*y).concat_assoc(
                            Seq::singleton(end.deep_model()),
                            line.subsequence(suffix_index@, line.len()),
                        );
                        *after == Seq::singleton(*y).concat(Seq::singleton(end.deep_model()))
                            .concat(line.subsequence(suffix_index@, line.len()))
                    };
                    proof_assert! {
                        lemma_ids_sub(self.scratchpad@, suffix_index@, line.len());
                        ids(self.scratchpad@.subsequence(suffix_index@, line.len()))
                            == line.subsequence(suffix_index@, line.len())
                    };

                    let active = iter::once(id)
                        .chain(iter::once(end))
                        .chain(suffix.iter().copied());
                    proof! {
                        let path = snapshot!(active);
                    }
                    proof_assert! {
                        lemma_iterates_two_then::<K>();
                        forall<items: Seq<K>> iterates(*path, items)
                            ==> items == Seq::singleton(id).concat(Seq::singleton(end)).concat(suffix@)
                    };
                    proof_assert!(ids(suffix@) == line.subsequence(suffix_index@, line.len()));
                    proof_assert!(forall<items: Seq<K>> iterates(*path, items) ==> ids(items) == *after);
                    Self::set_active_line(&mut self.weave, active);
                    proof_assert!(exists<listed: Seq<K>> iterates(*path, listed)
                        && active_line(self.weave@) == ids(listed));
                    proof_assert!(active_line(self.weave@) == *after);
                    proof_assert! {
                        lemma_same_graph_line(self.weave@.nodes, linked_view.nodes, *after);
                        text(self.weave@.nodes, *after) == text(linked_view.nodes, *after)
                    };
                    proof_assert!(active_text(self.weave@) == text(linked_view.nodes, *after));
                    proof_assert!(offset(v2.nodes, *l2, prefix_len@) == range.start@
                        && offset(v2.nodes, *l2, *q2) == range.end@
                        && prefix_len@ <= *q2 && *q2 < l2.len() && text(v2.nodes, *l2) == *total);
                    proof_assert! {
                        lemma_text_split(v2.nodes, *l2, prefix_len@);
                        text(v2.nodes, l2.subsequence(0, prefix_len@)) == total.subsequence(0, range.start@)
                            && range.start@ <= total.len()
                    };
                    proof_assert! {
                        lemma_text_split(v2.nodes, *l2, *q2);
                        text(v2.nodes, l2.subsequence(*q2, l2.len())) == total.subsequence(range.end@, total.len())
                            && range.end@ <= total.len()
                    };
                    proof_assert!(active_text(self.weave@)
                        == total.subsequence(0, range.start@).concat(*x)
                            .concat(total.subsequence(range.end@, total.len())));
                    proof_assert! {
                        lemma_spliced_range(*total, range.start@, range.end@, *x);
                        active_text(self.weave@) == spliced(*total, range.start@, range.end@, *x)
                    };
                } else {
                    proof_assert!(start_split == None && start_right == None);
                    proof_assert!(*v2 == *old && *l2 == *line);
                    proof_assert!(0 < line.len() && line[0] == l2[0]);
                    proof_assert!(match *rep {
                        Some(r) => r.deep_model() == line[line.len() - 1],
                        None => false,
                    });
                    proof_assert!(old.nodes[line[0]]@.from.len() == 0);
                    proof_assert!(forall<c> to_model.contains(c) ==> v2.nodes[c]@.from.contains(line[line.len() - 1]));
                    proof_assert!(!to_model.contains(line[0]));
                    proof_assert! {
                        lemma_insert_keeps_line(*v2, self.weave@, *node, *c0, *line);
                        downward(self.weave@.nodes, *line)
                    };
                    proof_assert!(self.weave@.nodes[*y]@.to == *to_model);
                    proof_assert!(match *rep {
                        Some(r) => r.deep_model() == line[line.len() - 1],
                        None => false,
                    });
                    proof_assert!(forall<c> to_model.contains(c) ==> {
                        lemma_snoc_contains(v2.nodes[c]@.from, *y, line[line.len() - 1]);
                        true
                    } && self.weave@.nodes[c]@.from.contains(line[line.len() - 1]));
                    proof_assert!(ranked(self.weave@.nodes, rank_of(self.weave@.nodes)));
                    proof_assert!(below_end(self.weave@.nodes, *line, self.weave@.nodes[*y]@.to));
                    proof_assert!(activated(self.weave@.nodes, old.active, *y, self.weave@.active));
                    proof_assert!(line_of(self.weave@.nodes, old.active, *line));
                    proof_assert! {
                        lemma_activated_root(self.weave@, old.active, *line, *y, self.weave@.active);
                        line_of(self.weave@.nodes, self.weave@.active, Seq::singleton(*y))
                    };
                    proof_assert! {
                        lemma_active_line(self.weave@);
                        active_line(self.weave@) == Seq::singleton(*y)
                    };
                    proof_assert! {
                        lemma_text_single(self.weave@.nodes, *y);
                        active_text(self.weave@) == *x
                    };
                    proof_assert! {
                        lemma_spliced_range(*total, range.start@, range.end@, *x);
                        total.subsequence(0, 0).ext_eq(Seq::empty());
                        x.concat_empty();
                        spliced(*total, range.start@, range.end@, *x) == *x
                    };
                }
            }
        }
        /// Splits the nodes which `range` starts and ends inside of, as `locate_range` found them, so
        /// that it starts and ends at node boundaries. Returns the index in `self.scratchpad` after
        /// the last node the range covers units of, the index of the first node after the range,
        /// the node the range ends before, if any, and the node split off of the one the range
        /// starts inside of, if any.
        #[requires(ids(self.scratchpad@) == active_line(self.weave@))]
        #[requires(active_text(self.weave@).len() <= usize::MAX@)]
        #[requires(range.start@ < range.end@)]
        #[requires(located(
            self.weave@.nodes,
            ids(self.scratchpad@),
            self.scratchpad@,
            range.start@,
            range.end@,
            prefix_len@,
            start_split,
            end,
        ))]
        #[requires(generates(*generate_id, self.weave@.nodes))]
        #[requires(exact_splits::<T>() && total_splits::<T>())]
        #[ensures((^self).scratchpad@ == self.scratchpad@)]
        #[ensures((^self).weave.metadata == self.weave.metadata)]
        #[ensures(generates(^generate_id, (^self).weave@.nodes))]
        #[ensures(forall<k> self.weave@.nodes.contains(&k) ==> (^self).weave@.nodes.contains(&k))]
        #[ensures((result.3 == None) == (start_split == None))]
        #[ensures(prefix_len@ <= self.scratchpad@.len())]
        #[ensures(prefix_len@ == 0 ==> range.start@ == 0 && start_split == None)]
        #[ensures(result.2 == None && prefix_len@ == self.scratchpad@.len() ==> result.3 != None)]
        // The text of the line the splits leave is that of the active line, and the range starts
        // after its first `prefix_len` nodes, which are the active line's, and ends before the node
        // `bounded_end` gives.
        #[ensures({
            let l = bounded(ids(self.scratchpad@), start_split, end, result.2, result.3);
            downward((^self).weave@.nodes, l) && no_dup(l)
                && text((^self).weave@.nodes, l) == active_text(self.weave@)
        })]
        #[ensures({
            let l = bounded(ids(self.scratchpad@), start_split, end, result.2, result.3);
            prefix_len@ <= l.len()
                && l.subsequence(0, prefix_len@) == ids(self.scratchpad@).subsequence(0, prefix_len@)
                && offset((^self).weave@.nodes, l, prefix_len@) == range.start@
        })]
        #[ensures({
            let l = bounded(ids(self.scratchpad@), start_split, end, result.2, result.3);
            let q = bounded_end(ids(self.scratchpad@).len(), start_split, end);
            match result.2 {
                Some(e) => prefix_len@ <= q && 0 < q && q < l.len() && l[q] == e.deep_model()
                    && offset((^self).weave@.nodes, l, q) == range.end@
                    && 0 < result.1@ && result.1@ <= self.scratchpad@.len()
                    && l.subsequence(q, l.len()) == Seq::singleton(e.deep_model())
                        .concat(ids(self.scratchpad@).subsequence(result.1@, self.scratchpad@.len())),
                None => q == l.len() && prefix_len@ < l.len()
                    && active_text(self.weave@).len() <= range.end@
                    && result.1@ == self.scratchpad@.len(),
            }
        })]
        // A range which reaches the end of the active line only splits the node it starts inside of.
        #[ensures(result.2 == None ==> match result.3 {
            Some(r) => 0 < prefix_len@
                && bounded(ids(self.scratchpad@), start_split, end, result.2, result.3)
                    == split_line(ids(self.scratchpad@), prefix_len@ - 1, r.deep_model())
                && (forall<k> split_line(ids(self.scratchpad@), prefix_len@ - 1, r.deep_model()).contains(k)
                    == (ids(self.scratchpad@).contains(k) || k == r.deep_model()))
                && split_shape(self.weave@, (^self).weave@, ids(self.scratchpad@)[prefix_len@ - 1], r.deep_model()),
            None => bounded(ids(self.scratchpad@), start_split, end, result.2, result.3) == ids(self.scratchpad@)
                && (^self).weave@ == self.weave@,
        })]
        // When the range reaches the end of the active line, the split line is the new active line,
        // except for the node split off of the active line's last node, which isn't active.
        #[ensures(result.2 == None ==> line_of(
            (^self).weave@.nodes,
            (^self).weave@.active,
            kept_line(
                bounded(ids(self.scratchpad@), start_split, end, result.2, result.3),
                ids(self.scratchpad@),
                prefix_len@,
            ),
        ))]
        // The last node the range covers units of.
        #[ensures({
            let l = bounded(ids(self.scratchpad@), start_split, end, result.2, result.3);
            let q = bounded_end(ids(self.scratchpad@).len(), start_split, end);
            prefix_len@ <= result.0@ && result.0@ <= self.scratchpad@.len()
                && (prefix_len@ < result.0@ ==> prefix_len@ < q && ids(self.scratchpad@)[result.0@ - 1] == l[q - 1])
                && (result.0@ == prefix_len@ ==> match result.3 {
                    Some(r) => prefix_len@ < q && r.deep_model() == l[q - 1],
                    None => q == prefix_len@,
                })
        })]
        #[allow(clippy::type_complexity, reason = "Each part is documented")]
        #[allow(clippy::too_many_lines, reason = "Most of its length is proof code")]
        #[cfg_attr(not(creusot), allow(unused_variables, reason = "Only its contract and proof use them"))]
        fn split_bounds<F>(
            &mut self,
            range: &Range<usize>,
            prefix_len: usize,
            start_split: Option<(usize, usize)>,
            end: Option<(usize, usize, K)>,
            generate_id: &mut F,
        ) -> (usize, usize, Option<K>, Option<K>)
        where
            F: FnMut() -> K,
        {
            proof! {
                let old = snapshot!(self.weave@);
                let gen0 = snapshot!(*generate_id);
                let line = snapshot!(ids(self.scratchpad@));
                let total = snapshot!(active_text(*old));
            }
            proof_assert! { lemma_active_line(*old); line_of(old.nodes, old.active, *line) };
            proof_assert!(forall<i> 0 <= i && i < line.len() ==> old.nodes.contains(&line[i]));
            proof_assert!(*total == text(old.nodes, *line));
            proof_assert!(downward(old.nodes, *line) && no_dup(*line));
            proof_assert! { lemma_full_prefix(*line); line.subsequence(0, line.len()) == *line };
            proof_assert! {
                lemma_located(old.nodes, *line, self.scratchpad@, range.start@, range.end@, prefix_len@, start_split, end);
                prefix_len@ <= line.len()
            };
            proof_assert! {
                lemma_located(old.nodes, *line, self.scratchpad@, range.start@, range.end@, prefix_len@, start_split, end);
                range.start@ == 0 ==> start_split == None
                    && offset(old.nodes, *line, prefix_len@) == 0
                    && prefix_len@ < line.len() && 0 < offset(old.nodes, *line, prefix_len@ + 1)
            };
            proof_assert! {
                lemma_located(old.nodes, *line, self.scratchpad@, range.start@, range.end@, prefix_len@, start_split, end);
                range.start@ > 0 ==> 1 <= prefix_len@
                    && offset(old.nodes, *line, prefix_len@ - 1) < range.start@
                    && range.start@ <= offset(old.nodes, *line, prefix_len@)
                    && match start_split {
                        Some((l, a)) => l@ == prefix_len@ - 1
                            && a@ + offset(old.nodes, *line, prefix_len@ - 1) == range.start@
                            && range.start@ < offset(old.nodes, *line, prefix_len@),
                        None => range.start@ == offset(old.nodes, *line, prefix_len@),
                    }
            };
            proof_assert! {
                lemma_located(old.nodes, *line, self.scratchpad@, range.start@, range.end@, prefix_len@, start_split, end);
                match end {
                    Some((e, at, l)) => e@ < line.len() && l == self.scratchpad@[e@]
                        && at@ + offset(old.nodes, *line, e@) == range.end@
                        && (at@ == 0 || range.end@ < offset(old.nodes, *line, e@ + 1))
                        && (range.start@ == 0 ==> prefix_len@ <= e@)
                        && (range.start@ > 0 ==> prefix_len@ - 1 <= e@),
                    None => total.len() <= range.end@
                        && range.start@ <= total.len()
                        && (range.start@ == total.len() ==> prefix_len@ < line.len()),
                }
            };
            proof_assert!(forall<j> 0 <= j && j < line.len() ==> {
                lemma_text_step(old.nodes, *line, j);
                true
            } && offset(old.nodes, *line, j + 1)
                == offset(old.nodes, *line, j) + old.nodes[line[j]].contents.units().len());

            proof! {
                let mut l1 = snapshot!(*line);
                let mut q1 = snapshot!(line.len());
                let mut cut_end = snapshot!(0);
            }
            let (replaced_end, suffix_index, end_id) = if let Some((index, at, left)) = end {
                let next = index.strict_add(1);

                if at == 0 {
                    proof_assert!(left.deep_model() == line[index@]);
                    proof_assert! {
                        lemma_suffix_cons(*line, index@);
                        line.subsequence(index@, line.len())
                            == Seq::singleton(line[index@]).concat(line.subsequence(index@ + 1, line.len()))
                    };
                    proof! {
                        q1 = snapshot!(index@);
                    }
                    (index, next, Some(left))
                } else {
                    let right = generate_id();
                    proof_assert! {
                        lemma_generates_call(*gen0, *generate_id, right, old.nodes);
                        !old.nodes.contains(&right.deep_model())
                    };
                    proof_assert!(left.deep_model() == line[index@] && left.deep_model() != right.deep_model());
                    proof_assert!(0 < at@ && at@ < old.nodes[line[index@]].contents.units().len());

                    assert!(self.weave.split(&left, at, right), "Splitting node failed");
                    proof_assert!(old.nodes[line[index@]].contents.splits(
                        at,
                        DiscreteContentResult::Two(
                            self.weave@.nodes[line[index@]].contents,
                            self.weave@.nodes[right.deep_model()].contents,
                        ),
                    ));
                    proof_assert!(self.weave@.nodes[line[index@]].contents.units()
                        == old.nodes[line[index@]].contents.units().subsequence(0, at@));
                    proof_assert!(self.weave@.nodes[right.deep_model()].contents.units()
                        == old.nodes[line[index@]].contents.units().subsequence(
                            at@,
                            old.nodes[line[index@]].contents.units().len(),
                        ));
                    proof_assert! {
                        lemma_split_refines(*old, self.weave@, *line, index@, right.deep_model(), at@);
                        lemma_split_offset(*old, self.weave@, *line, index@, right.deep_model(), at@, index@ + 1);
                        downward(self.weave@.nodes, split_line(*line, index@, right.deep_model()))
                            && no_dup(split_line(*line, index@, right.deep_model()))
                            && text(self.weave@.nodes, split_line(*line, index@, right.deep_model())) == *total
                            && offset(self.weave@.nodes, split_line(*line, index@, right.deep_model()), index@ + 1)
                                == range.end@
                    };
                    proof_assert!(index@ + 1 <= line.len());
                    proof_assert! {
                        lemma_split_line_at(*line, index@, right.deep_model());
                        split_line(*line, index@, right.deep_model())[index@ + 1] == right.deep_model()
                            && split_line(*line, index@, right.deep_model()).subsequence(index@ + 1, line.len() + 1)
                                == Seq::singleton(right.deep_model()).concat(line.subsequence(index@ + 1, line.len()))
                    };
                    proof! {
                        l1 = snapshot!(split_line(*line, index@, right.deep_model()));
                        q1 = snapshot!(index@ + 1);
                        cut_end = snapshot!(at@);
                    }

                    (next, next, Some(right))
                }
            } else {
                (self.scratchpad.len(), self.scratchpad.len(), None)
            };
            proof! {
                let v1 = snapshot!(self.weave@);
            }
            proof_assert!(v1.valid());
            proof_assert!(generates(*generate_id, v1.nodes));
            proof_assert!(forall<k> old.nodes.contains(&k) ==> v1.nodes.contains(&k));
            proof_assert!(downward(v1.nodes, *l1) && no_dup(*l1) && text(v1.nodes, *l1) == *total);
            proof_assert!(match end_id {
                Some(e) => 0 < suffix_index@ && suffix_index@ <= line.len()
                    && *q1 < l1.len() && l1[*q1] == e.deep_model()
                    && offset(v1.nodes, *l1, *q1) == range.end@
                    && l1.subsequence(*q1, l1.len())
                        == Seq::singleton(e.deep_model()).concat(line.subsequence(suffix_index@, line.len()))
                    && (*cut_end == 0 ==> *l1 == *line && *v1 == *old && *q1 == suffix_index@ - 1)
                    && (*cut_end != 0 ==> *l1 == split_line(*line, suffix_index@ - 1, e.deep_model())
                        && *q1 == suffix_index@
                        && split_shape(*old, *v1, line[suffix_index@ - 1], e.deep_model())
                        && 0 < *cut_end && *cut_end < old.nodes[line[suffix_index@ - 1]].contents.units().len()
                        && v1.nodes[line[suffix_index@ - 1]].contents.units()
                            == old.nodes[line[suffix_index@ - 1]].contents.units().subsequence(0, *cut_end)
                        && v1.nodes[e.deep_model()].contents.units()
                            == old.nodes[line[suffix_index@ - 1]].contents.units()
                                .subsequence(*cut_end, old.nodes[line[suffix_index@ - 1]].contents.units().len())
                        && (forall<k> old.nodes.contains(&k) && k != line[suffix_index@ - 1]
                            ==> v1.nodes[k].contents == old.nodes[k].contents)),
                None => suffix_index@ == line.len() && *l1 == *line && *v1 == *old && *q1 == line.len(),
            });

            proof_assert!(range.start@ > 0 && match end_id {
                Some(_) => *q1 + 1 == suffix_index@ + if *cut_end == 0 { 0 } else { 1 },
                None => true,
            } ==> match start_split {
                Some(_) => true,
                None => prefix_len@ - 1 < suffix_index@ - 1 || match end_id { Some(_) => false, None => true },
            });
            proof_assert!(match end_id {
                Some(_) => forall<j> 0 <= j && j <= suffix_index@ - 1 ==> {
                    lemma_split_line_prefix(*line, suffix_index@ - 1, l1[suffix_index@ - 1 + 1], j);
                    true
                } && (*cut_end == 0 || {
                    lemma_split_offset(*old, *v1, *line, suffix_index@ - 1, l1[suffix_index@], *cut_end, j);
                    true
                }) && l1.subsequence(0, j) == line.subsequence(0, j)
                    && offset(v1.nodes, *l1, j) == offset(old.nodes, *line, j),
                None => true,
            });

            proof! {
                let mut l2 = snapshot!(*l1);
                let mut q2 = snapshot!(*q1);
                let gen1 = snapshot!(*generate_id);
            }
            let start_right = if let Some((index, at)) = start_split {
                let left = &self.scratchpad[index];
                let right = generate_id();
                proof_assert! {
                    lemma_generates_call(*gen1, *generate_id, right, v1.nodes);
                    !v1.nodes.contains(&right.deep_model())
                };
                proof! {
                    let sp = snapshot!(prefix_len@ - 1);
                    let c = snapshot!(v1.nodes[line[*sp]].contents.units());
                }
                proof_assert!(index@ == *sp && 0 <= *sp && *sp < line.len() && line.len() == self.scratchpad@.len());
                proof_assert!(ids(self.scratchpad@)[index@] == self.scratchpad@[index@].deep_model());
                proof_assert!(left.deep_model() == line[*sp]);
                proof_assert!(match end_id {
                    Some(e) => *sp <= suffix_index@ - 1 && (*cut_end != 0 ==> {
                        lemma_split_line_at(*line, suffix_index@ - 1, e.deep_model());
                        true
                    } && l1[*sp] == line[*sp]),
                    None => true,
                });
                proof_assert!(l1[*sp] == line[*sp] && *sp < l1.len());
                proof_assert!(left.deep_model() != right.deep_model());
                proof_assert!(offset(v1.nodes, *l1, *sp) == offset(old.nodes, *line, *sp));
                proof_assert!(0 < at@ && at@ < c.len());

                assert!(self.weave.split(left, at, right), "Splitting node failed");
                proof_assert!(split_shape(*v1, self.weave@, l1[*sp], right.deep_model()));
                proof_assert!(forall<k> v1.nodes.contains(&k) && k != l1[*sp]
                    ==> self.weave@.nodes[k].contents == v1.nodes[k].contents);
                proof_assert!(v1.nodes[line[*sp]].contents.splits(
                    at,
                    DiscreteContentResult::Two(
                        self.weave@.nodes[line[*sp]].contents,
                        self.weave@.nodes[right.deep_model()].contents,
                    ),
                ));
                proof_assert!(self.weave@.nodes[line[*sp]].contents.units() == c.subsequence(0, at@));
                proof_assert!(self.weave@.nodes[right.deep_model()].contents.units()
                    == c.subsequence(at@, c.len()));
                proof_assert!(self.weave@.nodes[l1[*sp]].contents.units()
                    == v1.nodes[l1[*sp]].contents.units().subsequence(0, at@)
                    && self.weave@.nodes[right.deep_model()].contents.units()
                        == v1.nodes[l1[*sp]].contents.units()
                            .subsequence(at@, v1.nodes[l1[*sp]].contents.units().len()));
                proof_assert!(0 <= *q1 - if *q1 == l1.len() { 1 } else { 0 }
                    && *q1 - if *q1 == l1.len() { 1 } else { 0 } <= l1.len());
                proof_assert! {
                    lemma_split_refines(*v1, self.weave@, *l1, *sp, right.deep_model(), at@);
                    lemma_split_offset(*v1, self.weave@, *l1, *sp, right.deep_model(), at@, *sp + 1);
                    lemma_split_offset(*v1, self.weave@, *l1, *sp, right.deep_model(), at@, *q1 - if *q1 == l1.len() { 1 } else { 0 });
                    lemma_split_line_prefix(*l1, *sp, right.deep_model(), *sp + 1);
                    downward(self.weave@.nodes, split_line(*l1, *sp, right.deep_model()))
                        && no_dup(split_line(*l1, *sp, right.deep_model()))
                        && text(self.weave@.nodes, split_line(*l1, *sp, right.deep_model())) == *total
                        && offset(self.weave@.nodes, split_line(*l1, *sp, right.deep_model()), *sp + 1)
                            == range.start@
                        && split_line(*l1, *sp, right.deep_model()).subsequence(0, *sp + 1)
                            == l1.subsequence(0, *sp + 1)
                };
                proof_assert!(match end_id {
                    Some(_) => *sp < *q1 && *q1 < l1.len() && offset(v1.nodes, *l1, *q1) == range.end@,
                    None => true,
                });
                proof_assert!(match end_id {
                    Some(_) => {
                        lemma_split_offset(*v1, self.weave@, *l1, *sp, right.deep_model(), at@, *q1);
                        offset(self.weave@.nodes, split_line(*l1, *sp, right.deep_model()), *q1 + 1)
                            == offset(v1.nodes, *l1, *q1)
                    },
                    None => true,
                });
                proof_assert!(match end_id {
                    Some(_) => {
                        lemma_split_line_suffix(*l1, *sp, right.deep_model(), *q1);
                        split_line(*l1, *sp, right.deep_model()).subsequence(*q1 + 1, l1.len() + 1)
                            == l1.subsequence(*q1, l1.len())
                    },
                    None => true,
                });
                proof_assert!(match end_id {
                    Some(_) => {
                        lemma_split_line_at(*l1, *sp, right.deep_model());
                        split_line(*l1, *sp, right.deep_model())[*q1 + 1] == l1[*q1]
                    },
                    None => true,
                });
                proof_assert! {
                    lemma_split_line_at(*l1, *sp, right.deep_model());
                    split_line(*l1, *sp, right.deep_model())[*sp + 1] == right.deep_model()
                        && (forall<i> *sp < i && i < l1.len()
                            ==> split_line(*l1, *sp, right.deep_model())[i + 1] == l1[i])
                        && (forall<i> 0 <= i && i <= *sp
                            ==> split_line(*l1, *sp, right.deep_model())[i] == l1[i])
                };
                proof_assert!(self.weave@.nodes[right.deep_model()]@.from == Seq::singleton(line[*sp]));
                proof_assert!(self.weave@.nodes[right.deep_model()]@.to == v1.nodes[line[*sp]]@.to);
                proof_assert!(self.weave@.nodes[line[*sp]]@.to == Seq::singleton(right.deep_model()));
                proof_assert! {
                    lemma_split_refines(*v1, self.weave@, *l1, *sp, right.deep_model(), at@);
                    forall<k> split_line(*l1, *sp, right.deep_model()).contains(k)
                        == (l1.contains(k) || k == right.deep_model())
                };
                proof! {
                    l2 = snapshot!(split_line(*l1, *sp, right.deep_model()));
                    q2 = snapshot!(*q1 + 1);
                }

                Some(right)
            } else {
                None
            };
            proof! {
                let v2 = snapshot!(self.weave@);
            }
            proof_assert!(v2.valid());
            proof_assert!(generates(*generate_id, v2.nodes));
            proof_assert!(forall<k> old.nodes.contains(&k) ==> v2.nodes.contains(&k));
            proof_assert!(downward(v2.nodes, *l2) && no_dup(*l2) && text(v2.nodes, *l2) == *total);
            proof_assert!(prefix_len@ <= l2.len()
                && l2.subsequence(0, prefix_len@) == line.subsequence(0, prefix_len@)
                && offset(v2.nodes, *l2, prefix_len@) == range.start@);
            proof_assert!(match end_id {
                Some(e) => prefix_len@ <= *q2 && 0 < *q2 && *q2 < l2.len() && l2[*q2] == e.deep_model()
                    && offset(v2.nodes, *l2, *q2) == range.end@
                    && l2.subsequence(*q2, l2.len())
                        == Seq::singleton(e.deep_model()).concat(line.subsequence(suffix_index@, line.len())),
                None => *q2 == l2.len() && total.len() <= range.end@,
            });

            proof_assert!(match end_id {
                Some(_) => *q1 == replaced_end@ && replaced_end@ < l1.len()
                    && (*cut_end == 0 ==> forall<i> 0 <= i && i < line.len() ==> l1[i] == line[i])
                    && (*cut_end != 0 ==> {
                        lemma_split_line_at(*line, suffix_index@ - 1, l1[suffix_index@]);
                        true
                    } && l1[replaced_end@ - 1] == line[replaced_end@ - 1]),
                None => replaced_end@ == line.len(),
            });
            proof_assert!(prefix_len@ <= replaced_end@);
            proof_assert!(match end_id {
                Some(_) => *cut_end == 0 ==> l1[replaced_end@ - 1] == line[replaced_end@ - 1] || replaced_end@ == 0,
                None => true,
            });
            proof_assert!(match end {
                Some((e, at, _)) => if at@ == 0 {
                    *l1 == *line && *q1 == e@ && *v1 == *old
                } else {
                    match end_id {
                        Some(r) => *l1 == split_line(*line, e@, r.deep_model()) && *q1 == e@ + 1,
                        None => false,
                    }
                },
                None => *l1 == *line && *v1 == *old && *q1 == line.len() && end_id == None,
            });
            proof_assert!(match start_right {
                Some(r) => match start_split {
                    Some((index, _)) => index@ == prefix_len@ - 1 && 0 < prefix_len@
                        && *l2 == split_line(*l1, index@, r.deep_model()) && *q2 == *q1 + 1
                        && split_shape(*v1, *v2, l1[prefix_len@ - 1], r.deep_model()),
                    None => false,
                },
                None => start_split == None && *l2 == *l1 && *q2 == *q1 && *v2 == *v1,
            });
            proof_assert! {
                lemma_bounded(*line, start_split, end, end_id, start_right);
                *l2 == bounded(*line, start_split, end, end_id, start_right)
                    && *q2 == bounded_end(line.len(), start_split, end)
            };
            proof_assert!(end_id == None ==> match start_right {
                Some(r) => l1[prefix_len@ - 1] == line[prefix_len@ - 1]
                    && split_shape(*old, *v2, line[prefix_len@ - 1], r.deep_model())
                    && *l2 == split_line(*line, prefix_len@ - 1, r.deep_model())
                    && forall<k> l2.contains(k) == (line.contains(k) || k == r.deep_model()),
                None => *v2 == *old && *l2 == *line,
            });
            proof! {
                let la = snapshot!(kept_line(*l2, *line, prefix_len@));
            }
            proof_assert! {
                lemma_kept_line(*l2, *line, prefix_len@);
                (prefix_len@ < line.len() ==> *la == *l2) && (line.len() <= prefix_len@ ==> *la == *line)
            };
            proof_assert!(forall<k> old.nodes.contains(&k) ==> linked(old.nodes, k));
            proof_assert!(end_id == None && start_right == None ==> line_of(v2.nodes, v2.active, *la));
            proof_assert!(end_id == None && start_right != None ==> 0 < prefix_len@
                && old.active.contains(&line[prefix_len@ - 1]) && old.nodes[line[prefix_len@ - 1]]@.active);
            proof_assert!(end_id == None && start_right != None && prefix_len@ < line.len()
                ==> old.nodes[line[prefix_len@]]@.from.contains(line[prefix_len@ - 1])
                    && linked(old.nodes, line[prefix_len@])
                    && old.nodes[line[prefix_len@ - 1]]@.to.contains(line[prefix_len@])
                    && old.active.contains(&line[prefix_len@]));
            proof_assert!(end_id == None && start_right != None && prefix_len@ < line.len()
                ==> has_child_in(old.nodes, old.active, line[prefix_len@ - 1]));
            proof_assert!(end_id == None && prefix_len@ < line.len() ==> match start_right {
                Some(r) => v2.active == old.active.add(r.deep_model()),
                None => true,
            });
            proof_assert!(end_id == None && start_right != None && prefix_len@ == line.len() ==> {
                lemma_end_childless(old.nodes, old.active, *line);
                !has_child_in(old.nodes, old.active, line[line.len() - 1])
            });
            proof_assert!(end_id == None && start_right != None && prefix_len@ == line.len()
                ==> v2.active == old.active);
            proof_assert!(end_id == None && prefix_len@ == line.len() ==> {
                lemma_full_prefix(*line);
                l2.subsequence(0, prefix_len@) == *line
            });
            proof_assert!(l2.subsequence(l2.len(), l2.len()).ext_eq(Seq::empty()));
            proof_assert! {
                l2.subsequence(0, prefix_len@).concat_empty();
                cut(*l2, prefix_len@, l2.len()) == l2.subsequence(0, prefix_len@)
            };
            proof_assert! {
                lemma_cut_line(v2.nodes, *l2, prefix_len@, l2.len());
                downward(v2.nodes, cut(*l2, prefix_len@, l2.len()))
                    && no_dup(cut(*l2, prefix_len@, l2.len()))
            };
            proof_assert!(end_id == None && prefix_len@ == line.len() ==> downward(v2.nodes, *line) && no_dup(*line));
            proof_assert!(end_id == None ==> forall<k> la.contains(k) == v2.active.contains(&k));
            proof_assert!(end_id == None ==> line_of(v2.nodes, v2.active, *la));
            proof_assert!(end_id == None ==> offset(old.nodes, *line, line.len()) == total.len());
            proof_assert!(end_id == None ==> prefix_len@ < l2.len());
            proof_assert!(end_id == None && prefix_len@ == line.len() ==> start_right != None);
            proof_assert!(prefix_len@ < replaced_end@ ==> prefix_len@ < *q2 && line[replaced_end@ - 1] == l2[*q2 - 1]);
            proof_assert!(replaced_end@ == prefix_len@ ==> match start_right {
                Some(r) => prefix_len@ < *q2 && r.deep_model() == l2[*q2 - 1],
                None => *q2 == prefix_len@,
            });

            (replaced_end, suffix_index, end_id, start_right)
        }
        /// Finds where `range` starts and ends in the active path, which `self.scratchpad` holds from
        /// its root: how many nodes come before the range, the index of the node the range starts
        /// inside of and how far into it, if it starts inside of one, and the index of the node it
        /// ends at or inside of, how far into it, and its identifier, if it ends before the end of
        /// the path. Returns `None` if nothing comes after the range's start.
        #[requires(ids(self.scratchpad@) == active_line(self.weave@))]
        #[requires(active_text(self.weave@).len() <= usize::MAX@)]
        #[requires(range.start@ < range.end@)]
        #[ensures(match result {
            None => active_text(self.weave@).len() <= range.start@,
            Some((prefix_len, start_split, end)) => located(
                self.weave@.nodes,
                ids(self.scratchpad@),
                self.scratchpad@,
                range.start@,
                range.end@,
                prefix_len@,
                start_split,
                end,
            ),
        })]
        #[allow(clippy::type_complexity, reason = "Each part is documented")]
        fn locate_range(
            &self,
            range: &Range<usize>,
        ) -> Option<(usize, Option<(usize, usize)>, Option<(usize, usize, K)>)> {
            proof! {
                let old = snapshot!(self.weave@);
                let line = snapshot!(ids(self.scratchpad@));
                let total = snapshot!(active_text(*old));
            }
            proof_assert! { lemma_active_line(*old); line_of(old.nodes, old.active, *line) };
            proof_assert!(forall<i> 0 <= i && i < line.len() ==> old.nodes.contains(&line[i]));
            proof_assert!(*total == text(old.nodes, *line));
            proof_assert! { lemma_full_prefix(*line); line.subsequence(0, line.len()) == *line };
            proof_assert!(forall<j> 0 <= j && j < line.len() ==> {
                lemma_text_step(old.nodes, *line, j);
                true
            } && offset(old.nodes, *line, j + 1)
                == offset(old.nodes, *line, j) + old.nodes[line[j]].contents.units().len());

            #[allow(clippy::branches_sharing_code, reason = "Variable scoping")]
            if range.start == 0 {
                let mut cursor: usize = 0;
                let mut prefix_len = 0;
                let mut end: Option<(usize, usize, K)> = None;

                #[invariant(self.weave@ == *old)]
                #[invariant(ids(self.scratchpad@) == *line)]
                #[invariant(cursor@ == offset(old.nodes, *line, produced.len()))]
                #[invariant(cursor@ <= range.end@)]
                #[invariant(end == None)]
                #[invariant(prefix_len@ <= produced.len())]
                #[invariant(offset(old.nodes, *line, prefix_len@) == 0)]
                #[invariant(cursor@ == 0 ==> prefix_len@ + 1 >= produced.len())]
                #[invariant(0 < cursor@ ==> prefix_len@ < produced.len()
                    && 0 < offset(old.nodes, *line, prefix_len@ + 1))]
                for index in 0..self.scratchpad.len() {
                    let id = &self.scratchpad[index];
                    proof! {
                        let j = snapshot!(index@);
                    }
                    proof_assert!(produced.len() == *j + 1);
                    proof_assert!(0 <= *j && *j < line.len() && id.deep_model() == line[*j]);
                    let length = self.weave.get_contents(id).unwrap().len();
                    proof_assert! {
                        lemma_text_step(old.nodes, *line, *j);
                        lemma_text_split(old.nodes, *line, *j + 1);
                        cursor@ + length@ == offset(old.nodes, *line, *j + 1)
                            && cursor@ + length@ <= total.len()
                    };
                    let next = cursor.strict_add(length);

                    if cursor == 0 {
                        prefix_len = index;
                    }

                    #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                    if cursor == range.end || next > range.end {
                        end = Some((index, range.end - cursor, *id));
                        break;
                    }

                    cursor = next;
                }
                proof_assert!(match end {
                    Some((e, at, l)) => e@ < line.len() && l == self.scratchpad@[e@]
                        && cursor@ == offset(old.nodes, *line, e@)
                        && at@ + cursor@ == range.end@
                        && (cursor@ == range.end@ || range.end@ < offset(old.nodes, *line, e@ + 1))
                        && prefix_len@ <= e@
                        && offset(old.nodes, *line, prefix_len@) == 0
                        && 0 < offset(old.nodes, *line, prefix_len@ + 1),
                    None => cursor@ == total.len()
                        && offset(old.nodes, *line, prefix_len@) == 0
                        && (0 < cursor@ ==> prefix_len@ < line.len()
                            && 0 < offset(old.nodes, *line, prefix_len@ + 1)),
                });

                if cursor == 0 && end.is_none() {
                    return None;
                }

                proof_assert! {
                    lemma_located(old.nodes, *line, self.scratchpad@, range.start@, range.end@, prefix_len@, None, end);
                    located(old.nodes, *line, self.scratchpad@, range.start@, range.end@, prefix_len@, None, end)
                };
                Some((prefix_len, None, end))
            } else {
                let mut cursor: usize = 0;
                let mut prefix_len = 0;
                let mut start_split: Option<(usize, usize)> = None;
                let mut end: Option<(usize, usize, K)> = None;

                #[invariant(self.weave@ == *old)]
                #[invariant(ids(self.scratchpad@) == *line)]
                #[invariant(cursor@ == offset(old.nodes, *line, produced.len()))]
                #[invariant(cursor@ <= range.end@)]
                #[invariant(end == None)]
                #[invariant(prefix_len@ <= produced.len())]
                #[invariant(prefix_len@ == 0 ==> cursor@ < range.start@ && start_split == None)]
                #[invariant(prefix_len@ != 0
                    ==> offset(old.nodes, *line, prefix_len@ - 1) < range.start@
                        && range.start@ <= offset(old.nodes, *line, prefix_len@)
                        && match start_split {
                            Some((l, a)) => l@ == prefix_len@ - 1
                                && a@ + offset(old.nodes, *line, prefix_len@ - 1) == range.start@
                                && range.start@ < offset(old.nodes, *line, prefix_len@),
                            None => range.start@ == offset(old.nodes, *line, prefix_len@),
                        })]
                for index in 0..self.scratchpad.len() {
                    let id = &self.scratchpad[index];
                    proof! {
                        let j = snapshot!(index@);
                    }
                    proof_assert!(produced.len() == *j + 1);
                    proof_assert!(0 <= *j && *j < line.len() && id.deep_model() == line[*j]);
                    let length = self.weave.get_contents(id).unwrap().len();
                    proof_assert! {
                        lemma_text_step(old.nodes, *line, *j);
                        lemma_text_split(old.nodes, *line, *j + 1);
                        cursor@ + length@ == offset(old.nodes, *line, *j + 1)
                            && cursor@ + length@ <= total.len()
                    };
                    let next = cursor.strict_add(length);

                    if prefix_len == 0 && next >= range.start {
                        prefix_len = index.strict_add(1);

                        #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                        let at = range.start - cursor;

                        if at != length {
                            start_split = Some((index, at));
                        }
                    }

                    #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                    if cursor == range.end || next > range.end {
                        end = Some((index, range.end - cursor, *id));
                        break;
                    }

                    cursor = next;
                }
                proof_assert!(match end {
                    Some((e, at, l)) => e@ < line.len() && l == self.scratchpad@[e@]
                        && cursor@ == offset(old.nodes, *line, e@)
                        && at@ + cursor@ == range.end@
                        && (cursor@ == range.end@ || range.end@ < offset(old.nodes, *line, e@ + 1))
                        && prefix_len@ != 0 && prefix_len@ - 1 <= e@,
                    None => cursor@ == total.len(),
                });

                if end.is_none()
                    && (range.start > cursor
                        || (range.start == cursor && prefix_len == self.scratchpad.len()))
                {
                    return None;
                }

                proof_assert! {
                    lemma_located(old.nodes, *line, self.scratchpad@, range.start@, range.end@, prefix_len@, start_split, end);
                    located(old.nodes, *line, self.scratchpad@, range.start@, range.end@, prefix_len@, start_split, end)
                };
                Some((prefix_len, start_split, end))
            }
        }
        /// Sets the active path to the nodes `active` lists, from the root, when they form a line.
        #[requires(forall<items: Seq<K>> iterates(active, items)
            ==> downward(weave@.nodes, ids(items)) && no_dup(ids(items)))]
        #[ensures(exists<items: Seq<K>> iterates(active, items) && active_line((^weave)@) == ids(items))]
        #[ensures(same_graph((^weave)@.nodes, weave@.nodes))]
        #[ensures(forall<k> weave@.nodes.contains(&k) ==> (^weave)@.nodes.contains(&k))]
        #[ensures((^weave).metadata == weave.metadata)]
        fn set_active_line<I>(weave: &mut $W, active: I)
        where
            I: VerifiableIterator<Item = K>,
        {
            proof! {
                let old = snapshot!(weave@);
                let path = snapshot!(active);
            }
            weave.set_active_path(active);
            proof_assert!(exists<listed: Seq<K>, requested: FSet<Id<K>>> iterates(*path, listed)
                && (forall<k> requested.contains(&k) == (ids(listed).contains(k) && old.nodes.contains(&k)))
                && longest_path_within(old.nodes, requested, weave@.active));
            proof_assert!(forall<listed: Seq<K>, requested: FSet<Id<K>>> iterates(*path, listed)
                && (forall<k> requested.contains(&k) == (ids(listed).contains(k) && old.nodes.contains(&k)))
                && longest_path_within(old.nodes, requested, weave@.active)
                ==> downward(old.nodes, ids(listed)) && no_dup(ids(listed))
                    && (forall<k> ids(listed).contains(k) ==> old.nodes.contains(&k))
                    && (forall<k> requested.contains(&k) == ids(listed).contains(k))
                    && {
                        lemma_longest_line(old.nodes, requested, weave@.active, ids(listed));
                        line_of(old.nodes, weave@.active, ids(listed))
                    }
                    && {
                        lemma_same_graph_line(weave@.nodes, old.nodes, ids(listed));
                        line_of(weave@.nodes, weave@.active, ids(listed))
                    }
                    && {
                        lemma_active_line(weave@);
                        active_line(weave@) == ids(listed)
                    });
        }
        /// Inserts `contents` as a new active node after `parent`, or as a new root if there is
        /// none. If `prefix_all` is true, the node takes all of `parent`'s children as its own.
        #[requires(generates(*generate_id, weave@.nodes))]
        #[requires(match parent {
            Some(p) => 0 < active_line(weave@).len()
                && p.deep_model() == active_line(weave@)[active_line(weave@).len() - 1],
            None => active_line(weave@).len() == 0,
        })]
        // The contents are appended to the active path's text, and no node is removed.
        #[ensures(active_text((^weave)@) == active_text(weave@).concat(contents.units()))]
        #[ensures(forall<k> weave@.nodes.contains(&k) ==> (^weave)@.nodes.contains(&k))]
        #[ensures((^weave).metadata == weave.metadata)]
        fn insert_tail_node<F>(
            weave: &mut $W,
            parent: Option<K>,
            contents: T,
            prefix_all: bool,
            generate_id: &mut F,
        ) where
            F: FnMut() -> K,
        {
            proof! {
                let old = snapshot!(weave@);
                let line = snapshot!(active_line(*old));
                let gen0 = snapshot!(*generate_id);
                let c0 = snapshot!(contents);
                let rank = snapshot!(rank_of(old.nodes));
            }
            snapshot! { lemma_collected::<K> };
            proof_assert! { lemma_active_line(*old); line_of(old.nodes, old.active, *line) };
            proof_assert!(forall<k> old.nodes.contains(&k) ==> linked(old.nodes, k));
            proof_assert!(ranked(old.nodes, *rank));

            let id = generate_id();
            proof_assert! {
                lemma_generates_call(*gen0, *generate_id, id, old.nodes);
                !old.nodes.contains(&id.deep_model())
            };
            proof! {
                let n = snapshot!(line.len());
                let to_model = snapshot!(match parent {
                    Some(p) => if prefix_all { old.nodes[p.deep_model()]@.to } else { Seq::empty() },
                    None => Seq::empty(),
                });
                let node = snapshot!(NodeView {
                    id: id.deep_model(),
                    from: match parent { Some(p) => Seq::singleton(p.deep_model()), None => Seq::empty() },
                    to: *to_model,
                    active: true,
                    bookmarked: false,
                });
            }
            proof_assert!(0 < *n ==> linked(old.nodes, line[*n - 1]));
            proof_assert!(0 < *n ==> forall<c> to_model.contains(c)
                ==> old.nodes.contains(&c) && old.nodes[c]@.from.contains(line[*n - 1])
                    && rank[line[*n - 1]] < rank[c]);
            proof_assert!(0 < *n ==> ranked_above(old.nodes, line[*n - 1], *to_model));
            proof_assert!(no_dup(*to_model));
            proof_assert!(0 < *n ==> {
                lemma_insertable_under(old.nodes, *node, line[*n - 1]);
                insertable(old.nodes, *node)
            });
            proof_assert!(*n == 0 ==> {
                lemma_insertable_root(old.nodes, *node);
                insertable(old.nodes, *node)
            });

            let to = if let Some(parent) = parent
                && prefix_all
            {
                Self::copied_ids(weave.get_children(&parent).unwrap())
            } else {
                <$N as Node<K, T>>::To::from_iter(iter::empty())
            };
            proof_assert!(to.model() == *to_model);
            let from = <$N as Node<K, T>>::From::from_iter(parent);
            proof_assert!(from.model() == node.from);

            assert!(
                weave.insert(<$N as BuildableNode<K, T>>::new(id, from, to, true, contents)),
                "Inserting node failed"
            );
            proof_assert!(inserted(*old, weave@, *node, *c0));
            proof_assert!(activated(weave@.nodes, old.active, id.deep_model(), weave@.active));
            proof! {
                let after = snapshot!(split_line(*line, *n - 1, id.deep_model()));
            }
            proof_assert! {
                lemma_insert_line(*old, weave@, *node, *c0, *line, *n - 1);
                downward(weave@.nodes, *after) && no_dup(*after)
                    && (0 < *n ==> downward(weave@.nodes, *line))
                    && text(weave@.nodes, *after)
                        == text(old.nodes, line.subsequence(0, *n - 1 + 1)).concat(c0.units())
                            .concat(text(old.nodes, line.subsequence(*n - 1 + 1, line.len())))
            };
            proof_assert! {
                lemma_full_prefix(*line);
                line.subsequence(line.len(), line.len()).ext_eq(Seq::empty());
                text(old.nodes, *line).concat(c0.units()).concat_empty();
                text(weave@.nodes, *after) == text(old.nodes, *line).concat(c0.units())
            };
            proof_assert!(weave@.nodes[id.deep_model()]@.to == *to_model);
            proof_assert!(0 < *n ==> forall<c> to_model.contains(c) ==> {
                lemma_snoc_contains(old.nodes[c]@.from, id.deep_model(), line[*n - 1]);
                true
            } && weave@.nodes[c]@.from.contains(line[*n - 1]));
            proof_assert!(ranked(weave@.nodes, rank_of(weave@.nodes)));
            proof_assert!(0 < *n ==> forall<c> to_model.contains(c)
                ==> rank_of(weave@.nodes)[line[*n - 1]] < rank_of(weave@.nodes)[c]);
            proof_assert!(below_end(weave@.nodes, *line, weave@.nodes[id.deep_model()]@.to));
            proof_assert!(weave@.nodes[id.deep_model()]@.from == node.from);
            proof_assert!(*n == 0 ==> line.len() == 0 && downward(weave@.nodes, *line));
            proof_assert!(line_of(weave@.nodes, old.active, *line));
            proof_assert!(!old.active.contains(&id.deep_model()));
            proof_assert!(*n == 0 ==> {
                lemma_activated_root(weave@, old.active, *line, id.deep_model(), weave@.active);
                after.ext_eq(Seq::singleton(id.deep_model()))
                    && line_of(weave@.nodes, weave@.active, Seq::singleton(id.deep_model()))
            } && line_of(weave@.nodes, weave@.active, *after));
            proof_assert!(0 < *n ==> {
                lemma_activated_after(weave@, old.active, *line, *n - 1, id.deep_model(), weave@.active);
                lemma_split_line_end(*line, id.deep_model());
                *after == line.subsequence(0, *n).snoc(id.deep_model())
                    && line_of(weave@.nodes, weave@.active, line.subsequence(0, *n).snoc(id.deep_model()))
            } && line_of(weave@.nodes, weave@.active, *after));
            proof_assert! {
                lemma_active_line(weave@);
                active_line(weave@) == *after
            };
        }
        /// Returns `T::default()`, which the wrapper's requirements say is empty.
        ///
        /// `Default::default` has no specification, so verification assumes that requirement.
        #[trusted]
        #[ensures(result.units().len() == 0)]
        fn default_contents() -> T {
            T::default()
        }
        /// Checks whether `parents` lists `id`.
        #[trusted]
        #[ensures(result == parents.model().contains(id.deep_model()))]
        fn lists(parents: &<$N as Node<K, T>>::From, id: &K) -> bool {
            !parents.into_iter().all(|parent| parent != id)
        }
        /// Appends `parents`, and then `last`, to `output`.
        #[trusted]
        #[ensures(ids((^output)@) == ids(output@).concat(parents.model()).snoc(last.deep_model()))]
        fn extend_with_parents(output: &mut Vec<K>, parents: &<$N as Node<K, T>>::From, last: K) {
            output.extend(parents.into_iter().copied().chain(iter::once(last)));
        }
        /// Copies `children` into a new set of identifiers.
        #[trusted]
        #[ensures(result.model() == children.model())]
        fn copied_ids(children: &<$N as Node<K, T>>::To) -> <$N as Node<K, T>>::To {
            <$N as Node<K, T>>::To::from_iter(children.into_iter().copied())
        }
    };
}

#[cfg(not(creusot))]
impl<W, K, N, T> PatchablePathWeave<W, K, N, T>
where
    W: DiscreteWeave<K, N, T> + ActivePathWeave<K, N, T> + IndependentWeave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: BuildableNode<K, T>,
    T: DiscreteContents + IndependentContents + Default,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    for<'a> &'a N::To: IntoIterator<Item = &'a K>,
    N::From: FromIterator<K>,
    N::To: FromIterator<K>,
{
    path_methods!(W, N);
}

#[cfg(creusot)]
impl<K, T, M, S>
    PatchablePathWeave<independent::IndependentWeave<K, T, M, S>, K, IndependentNode<K, T, S>, T>
where
    K: Hash + Copy + Eq + Ord + Verifiable,
    T: DiscreteContents + IndependentContents + Default,
    S: BuildHasher + Default + Clone,
{
    path_methods!(independent::IndependentWeave<K, T, M, S>, IndependentNode<K, T, S>);
}
