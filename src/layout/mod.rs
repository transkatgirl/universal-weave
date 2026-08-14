//! [`Weave`] rendering helpers.
//!
//! This library provides 3 different 2D [`Layouter`] implementations with identical behavior:
//! - [`DependentLayouter`] - Takes a [`DependentWeave`] as an input.
//! - [`IndependentLayouter`] - Takes an [`IndependentWeave`] as an input.
//! - [`TopologicalLayouter`] - Takes any [`Weave`] as an input.

use core::hash::{BuildHasher, Hash};

use glam::Vec2;
use scratchpads::Scratchpad;

use crate::{
    IndependentContents, LayoutItem, Layouter, Node, Weave,
    dependent::{DependentNode, DependentWeave},
    independent::{IndependentNode, IndependentWeave},
    layout::positioner::Layout2D,
};

mod positioner;

/// Minimum gaps in a [`Weave`] layout.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
#[must_use]
pub struct Spacing {
    /// Gap between adjacent nodes.
    pub node: f32,
    /// Gap between layers of nodes.
    pub layer: f32,
    /// Gap between node connections.
    pub edge: f32,
}

impl Default for Spacing {
    fn default() -> Self {
        Self {
            node: 16.0,
            layer: 16.0,
            edge: 8.0,
        }
    }
}

/// A 2D [`Layouter`] which takes a [`DependentWeave`] as input.
///
/// This layout algorithm has identical behavior to [`TopologicalLayouter`].
#[derive(Default, Debug, Clone)]
#[must_use]
pub struct DependentLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// The [`Spacing`] used to arrange contents.
    pub spacing: Spacing,

    layout: Layout2D<K, S>,
}

impl<K, S> DependentLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// Creates a new [`DependentLayouter`] with the specified spacing.
    pub fn new(spacing: Spacing) -> Self {
        Self {
            spacing,
            layout: Layout2D::default(),
        }
    }
}

impl<K, T, M, S> Layouter<DependentWeave<K, T, M, S>, K, DependentNode<K, T, S>, T, Vec2>
    for DependentLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord + 'static,
    S: BuildHasher + Default + Clone + 'static,
{
    fn layout(&mut self, weave: &mut DependentWeave<K, T, M, S>, sizes: impl FnMut(&K) -> Vec2) {
        let scratchpad = &mut weave.scratchpad;

        todo!()
    }
    fn size(&self) -> Vec2 {
        self.layout.size()
    }
    fn view<P>(&self, bounds: Vec2, callback: impl FnMut(LayoutItem<K, Vec2, P>))
    where
        P: Iterator<Item = Vec2>,
    {
        self.layout.view(bounds, callback);
    }
}

/// A 2D [`Layouter`] which takes an [`IndependentWeave`] as input.
///
/// This layout algorithm has identical behavior to [`TopologicalLayouter`].
#[derive(Default, Debug, Clone)]
#[must_use]
pub struct IndependentLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// The [`Spacing`] used to arrange contents.
    pub spacing: Spacing,

    layout: Layout2D<K, S>,
}

impl<K, S> IndependentLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// Creates a new [`IndependentLayouter`] with the specified spacing.
    pub fn new(spacing: Spacing) -> Self {
        Self {
            spacing,
            layout: Layout2D::default(),
        }
    }
}

impl<K, T, M, S> Layouter<IndependentWeave<K, T, M, S>, K, IndependentNode<K, T, S>, T, Vec2>
    for IndependentLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord + 'static,
    T: IndependentContents,
    S: BuildHasher + Default + Clone + 'static,
{
    fn layout(&mut self, weave: &mut IndependentWeave<K, T, M, S>, sizes: impl FnMut(&K) -> Vec2) {
        let scratchpad = &mut weave.scratchpad;

        todo!()
    }
    fn size(&self) -> Vec2 {
        self.layout.size()
    }
    fn view<P>(&self, bounds: Vec2, callback: impl FnMut(LayoutItem<K, Vec2, P>))
    where
        P: Iterator<Item = Vec2>,
    {
        self.layout.view(bounds, callback);
    }
}

/// A 2D [`Layouter`] which orders nodes using [`Weave::get_ordered_identifiers()`].
///
/// However, this additional flexibility may result in worse performance and memory usage characteristics compared to [`DependentLayouter`] or [`IndependentLayouter`].
#[derive(Debug, Clone)]
#[must_use]
pub struct TopologicalLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// The [`Spacing`] used to arrange contents.
    pub spacing: Spacing,

    layout: Layout2D<K, S>,
    scratchpad: Scratchpad,
}

impl<K, S> Default for TopologicalLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    fn default() -> Self {
        Self {
            spacing: Spacing::default(),
            layout: Layout2D::default(),
            scratchpad: Scratchpad::new(),
        }
    }
}

impl<K, S> TopologicalLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// Creates a new [`TopologicalLayouter`] with the specified spacing.
    pub fn new(spacing: Spacing) -> Self {
        Self {
            spacing,
            layout: Layout2D::default(),
            scratchpad: Scratchpad::new(),
        }
    }
}

impl<W, K, N, T, S> Layouter<W, K, N, T, Vec2> for TopologicalLayouter<K, S>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord + 'static,
    N: Node<K, T>,
    S: BuildHasher + Default + Clone + 'static,
{
    fn layout(&mut self, weave: &mut W, sizes: impl FnMut(&K) -> Vec2) {
        todo!()
    }
    fn size(&self) -> Vec2 {
        self.layout.size()
    }
    fn view<P>(&self, bounds: Vec2, callback: impl FnMut(LayoutItem<K, Vec2, P>))
    where
        P: Iterator<Item = Vec2>,
    {
        self.layout.view(bounds, callback);
    }
}
