use core::{
    hash::{BuildHasher, Hash},
    marker::PhantomData,
};

use glam::Vec2;

use crate::LayoutItem;

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
