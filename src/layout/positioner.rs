use core::{
    hash::{BuildHasher, Hash},
    marker::PhantomData,
};

use glam::Vec2;
use scratchpads::Scratchpad;

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

impl<K, S> Layout2D<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    pub fn layout_dependent<T, M>(
        &mut self,
        weave: &DependentWeave<K, T, M, S>,
        sizes: impl FnMut(&K) -> Vec2,
        spacing: &Spacing,
    ) {
        todo!()
    }
    pub fn layout_independent<T, M>(
        &mut self,
        weave: &IndependentWeave<K, T, M, S>,
        sizes: impl FnMut(&K) -> Vec2,
        spacing: &Spacing,
    ) where
        T: IndependentContents,
    {
        todo!()
    }
    pub fn layout_weave<W, N, T>(
        &mut self,
        weave: &W,
        sizes: impl FnMut(&K) -> Vec2,
        spacing: &Spacing,
        scratchpad: &mut Scratchpad,
    ) where
        W: Weave<K, N, T>,
        K: Hash + Copy + Eq + Ord + 'static,
        N: Node<K, T>,
    {
        todo!()
    }
    pub fn size(&self) -> Vec2 {
        todo!()
    }
    pub fn view<P>(&self, bounds: Vec2, callback: impl FnMut(LayoutItem<K, Vec2, P>))
    where
        P: Iterator<Item = Vec2>,
    {
        todo!()
    }
}
