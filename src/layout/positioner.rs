use core::{
    hash::{BuildHasher, Hash},
    marker::PhantomData,
};

use alloc::vec::Vec;
use glam::Vec2;
use scratchpads::{Scratchpad, ScratchpadVec};

use crate::{
    IndependentContents, LayoutItem, Node, Weave, dependent::DependentWeave,
    independent::IndependentWeave, layout::Spacing,
};

#[derive(Debug, Clone)]
#[must_use]
pub struct Layout2D<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    _k: PhantomData<K>,
    _s: PhantomData<S>,
}

impl<K, S> Default for Layout2D<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    fn default() -> Self {
        Self {
            _k: PhantomData,
            _s: PhantomData,
        }
    }
}

enum Vertex<K> {
    Real(K),
    Segment { from: K, to: K },
}

impl<K, S> Layout2D<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    fn clear(&mut self) {}
    fn push_item(&mut self) {}
}

impl<K, S> Layout2D<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    pub fn layout_dependent<T, M, F>(
        &mut self,
        weave: &DependentWeave<K, T, M, S>,
        sizes: F,
        spacing: &Spacing,
    ) where
        F: FnMut(&K) -> Vec2,
    {
        todo!()
    }
    pub fn layout_independent<T, M, F>(
        &mut self,
        weave: &IndependentWeave<K, T, M, S>,
        sizes: F,
        spacing: &Spacing,
    ) where
        T: IndependentContents,
        F: FnMut(&K) -> Vec2,
    {
        todo!()
    }
    pub fn layout_topological<W, N, T, F>(
        &mut self,
        weave: &W,
        sizes: F,
        spacing: &Spacing,
        scratchpad: &mut Scratchpad,
        topological: &mut Vec<K>,
    ) where
        W: Weave<K, N, T>,
        K: Hash + Copy + Eq + Ord + 'static,
        N: Node<K, T>,
        F: FnMut(&K) -> Vec2,
    {
        let guard = scratchpad.guard();

        let mut parents: ScratchpadVec<'_, (K, usize, usize)> =
            guard.vec_with_capacity(topological.len()); // (id, index, rank)

        for id in topological.drain(..) {
            parents.clear();
        }

        todo!()
    }
    pub fn size(&self) -> Vec2 {
        todo!()
    }
    pub fn view<P, F>(&self, bounds: Vec2, callback: F)
    where
        P: Iterator<Item = Vec2>,
        F: FnMut(LayoutItem<K, Vec2, P>),
    {
        todo!()
    }
}
