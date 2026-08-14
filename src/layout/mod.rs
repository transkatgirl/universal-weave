//! [`Weave`] rendering helpers.
//!
//! This library provides 3 different 2D [`Layouter`] implementations with identical behavior:
//! - [`DependentLayouter`] - Takes a [`DependentWeave`] as an input.
//! - [`IndependentLayouter`] - Takes an [`IndependentWeave`] as an input.
//! - [`TopologicalLayouter`] - Takes any [`Weave`] as an input.

use core::{
    hash::{BuildHasher, Hash},
    marker::PhantomData,
};

use glam::Vec2;
use scratchpads::Scratchpad;

use crate::{
    IndependentContents, Layouter, Node, Weave,
    dependent::{DependentNode, DependentWeave},
    independent::{IndependentNode, IndependentWeave},
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

/// A 2D arrangement of a [`Weave`]'s content.
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

/// An error which occured while attempting to build a [`Layout2D`].
pub enum LayoutError {}

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

impl<'a, K, T, M, S> Layouter<'a, DependentWeave<K, T, M, S>, K, DependentNode<K, T, S>, T>
    for DependentLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord + 'static,
    S: BuildHasher + Default + Clone + 'static,
{
    type Size = Vec2;
    type Layout = &'a Layout2D<K, S>;
    type Error = LayoutError;

    fn layout(
        &'a mut self,
        weave: &mut DependentWeave<K, T, M, S>,
        map: impl FnMut(&DependentNode<K, T, S>) -> Self::Size,
    ) -> Result<Self::Layout, Self::Error> {
        let scratchpad = &mut weave.scratchpad;

        todo!()
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

impl<'a, K, T, M, S> Layouter<'a, IndependentWeave<K, T, M, S>, K, IndependentNode<K, T, S>, T>
    for IndependentLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord + 'static,
    T: IndependentContents,
    S: BuildHasher + Default + Clone + 'static,
{
    type Size = Vec2;
    type Layout = &'a Layout2D<K, S>;
    type Error = LayoutError;

    fn layout(
        &'a mut self,
        weave: &mut IndependentWeave<K, T, M, S>,
        map: impl FnMut(&IndependentNode<K, T, S>) -> Self::Size,
    ) -> Result<Self::Layout, Self::Error> {
        let scratchpad = &mut weave.scratchpad;

        todo!()
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

impl<'a, W, K, N, T, S> Layouter<'a, W, K, N, T> for TopologicalLayouter<K, S>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord + 'static,
    N: Node<K, T>,
    S: BuildHasher + Default + Clone + 'static,
{
    type Size = Vec2;
    type Layout = &'a Layout2D<K, S>;
    type Error = LayoutError;

    fn layout(
        &'a mut self,
        weave: &mut W,
        map: impl FnMut(&N) -> Self::Size,
    ) -> Result<Self::Layout, Self::Error> {
        todo!()
    }
}
