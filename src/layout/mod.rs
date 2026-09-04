//! [`Weave`] rendering helpers.
//!
//! This library provides 3 different 2D [`Layouter`] implementations with identical behavior:
//! - [`DependentLayouter`] - Takes a [`DependentWeave`] as an input.
//! - [`IndependentLayouter`] - Takes an [`IndependentWeave`] as an input.
//! - [`TopologicalLayouter`] - Takes any [`Weave`] as an input.

use core::{
    hash::{BuildHasher, Hash},
    marker::PhantomData,
    num::FpCategory,
};

use alloc::vec::Vec;
use glam::Vec2;
use scratchpads::Scratchpad;
use tinyvec::ArrayVec;

use crate::{
    IndependentContents, LayoutItem, Layouter, Node, Weave,
    dependent::{DependentNode, DependentWeave},
    independent::{IndependentNode, IndependentWeave},
    layout::positioner::Layout2D,
};

mod positioner;

/// Minimum gaps in a [`Weave`] layout.
///
/// All values must be finite normal numbers >= 0.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
#[must_use]
pub struct Spacing {
    /// Gap between adjacent nodes.
    pub node: f32,
    /// Gap between layers of nodes.
    pub layer: f32,
    /// Reserved space for edges.
    pub corridor: f32,
    /// Gap between edges and adjacent items.
    pub edge: f32,
}

impl Default for Spacing {
    fn default() -> Self {
        Self {
            node: 16.0,
            layer: 16.0,
            corridor: 0.0,
            edge: 8.0,
        }
    }
}

impl Spacing {
    /// Validates that all spacing values are finite normal numbers >= 0.
    #[must_use]
    pub const fn validate(&self) -> bool {
        validate_float(self.node)
            && validate_float(self.layer)
            && validate_float(self.corridor)
            && validate_float(self.edge)
    }
}

/// A 2D [`Layouter`] which takes a [`DependentWeave`] as input.
///
/// This layout algorithm has identical behavior to [`TopologicalLayouter`].
#[derive(Default, Debug, Clone)]
#[must_use]
pub struct DependentLayouter<K>
where
    K: Hash + Copy + Eq + Ord,
{
    /// The [`Spacing`] used to arrange contents.
    pub spacing: Spacing,

    layout: Layout2D<K>,
}

impl<K> DependentLayouter<K>
where
    K: Hash + Copy + Eq + Ord,
{
    /// Creates a new [`DependentLayouter`] with the specified spacing.
    pub fn new(spacing: Spacing) -> Self {
        Self {
            spacing,
            layout: Layout2D::default(),
        }
    }
}

impl<K, T, M, S>
    Layouter<DependentWeave<K, T, M, S>, K, DependentNode<K, T, S>, T, Vec2, ArrayVec<[Vec2; 6]>>
    for DependentLayouter<K>
where
    K: Hash + Copy + Eq + Ord + 'static,
    S: BuildHasher + Default + Clone + 'static,
{
    fn layout(&mut self, weave: &mut DependentWeave<K, T, M, S>, sizes: impl FnMut(&K) -> Vec2) {
        self.layout.layout_dependent(weave, sizes, &self.spacing);
    }
    fn size(&self) -> Vec2 {
        self.layout.size()
    }
    fn view(
        &mut self,
        min: Vec2,
        max: Vec2,
        callback: impl FnMut(LayoutItem<K, Vec2, ArrayVec<[Vec2; 6]>>),
    ) {
        self.layout.view(min, max, callback);
    }
}

/// A 2D [`Layouter`] which takes an [`IndependentWeave`] as input.
///
/// This layout algorithm has identical behavior to [`TopologicalLayouter`].
#[derive(Default, Debug, Clone)]
#[must_use]
pub struct IndependentLayouter<K>
where
    K: Hash + Copy + Eq + Ord,
{
    /// The [`Spacing`] used to arrange contents.
    pub spacing: Spacing,

    layout: Layout2D<K>,
    topological: Vec<K>,
}

impl<K> IndependentLayouter<K>
where
    K: Hash + Copy + Eq + Ord,
{
    /// Creates a new [`IndependentLayouter`] with the specified spacing.
    pub fn new(spacing: Spacing) -> Self {
        Self {
            spacing,
            layout: Layout2D::default(),
            topological: Vec::new(),
        }
    }
}

impl<K, T, M, S>
    Layouter<
        IndependentWeave<K, T, M, S>,
        K,
        IndependentNode<K, T, S>,
        T,
        Vec2,
        ArrayVec<[Vec2; 6]>,
    > for IndependentLayouter<K>
where
    K: Hash + Copy + Eq + Ord + 'static,
    T: IndependentContents,
    S: BuildHasher + Default + Clone + 'static,
{
    fn layout(&mut self, weave: &mut IndependentWeave<K, T, M, S>, sizes: impl FnMut(&K) -> Vec2) {
        weave.get_ordered_identifiers(&mut self.topological);

        self.layout
            .layout_independent(weave, sizes, &self.spacing, &mut self.topological);
    }
    fn size(&self) -> Vec2 {
        self.layout.size()
    }
    fn view(
        &mut self,
        min: Vec2,
        max: Vec2,
        callback: impl FnMut(LayoutItem<K, Vec2, ArrayVec<[Vec2; 6]>>),
    ) {
        self.layout.view(min, max, callback);
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

    layout: Layout2D<K>,
    topological: Vec<K>,
    scratchpad: Scratchpad,
    _hasher: PhantomData<S>,
}

impl<K, S> Default for TopologicalLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    fn default() -> Self {
        Self::new(Spacing::default())
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
            topological: Vec::new(),
            scratchpad: Scratchpad::new(),
            _hasher: PhantomData,
        }
    }
}

impl<W, K, N, T, S> Layouter<W, K, N, T, Vec2, ArrayVec<[Vec2; 6]>> for TopologicalLayouter<K, S>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord + 'static,
    N: Node<K, T>,
    S: BuildHasher + Default + Clone + 'static,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
{
    fn layout(&mut self, weave: &mut W, sizes: impl FnMut(&K) -> Vec2) {
        weave.get_ordered_identifiers(&mut self.topological);

        assert_eq!(
            weave.len(),
            self.topological.len(),
            "Malformed topological order"
        );

        self.layout.layout_topological::<W, N, T, S, _>(
            weave,
            sizes,
            &self.spacing,
            &mut self.scratchpad,
            &mut self.topological,
        );
    }
    fn size(&self) -> Vec2 {
        self.layout.size()
    }
    fn view(
        &mut self,
        min: Vec2,
        max: Vec2,
        callback: impl FnMut(LayoutItem<K, Vec2, ArrayVec<[Vec2; 6]>>),
    ) {
        self.layout.view(min, max, callback);
    }
}

/// Smooths a polyline produced by this module's [`Layouter`] implementation into a chain of cubic Bézier segments.
///
/// This function may produce incorrect results if used to process polylines from other [`Layouter`] implementations.
#[allow(
    clippy::float_arithmetic,
    clippy::arithmetic_side_effects,
    reason = "Coordinate calculation"
)]
#[must_use]
pub fn smooth(points: ArrayVec<[Vec2; 6]>) -> ArrayVec<[[Vec2; 4]; 5]> {
    let mut segments = ArrayVec::new();

    for [start, end] in points.array_windows::<2>().copied() {
        let y_diff = end.y - start.y;

        segments.push(if y_diff == 0.0 {
            let x_diff = end.x - start.x;

            [
                start,
                start + Vec2::new(x_diff * (1.0 / 3.0), 0.0),
                start + Vec2::new(x_diff * (2.0 / 3.0), 0.0),
                end,
            ]
        } else {
            let arm = Vec2::new(0.0, y_diff * 0.5);

            [start, start + arm, end - arm, end]
        });
    }

    segments
}

#[must_use]
const fn validate_float(value: f32) -> bool {
    matches!(value.classify(), FpCategory::Normal | FpCategory::Zero) && value.is_sign_positive()
}

#[must_use]
const fn validate_vec2(value: Vec2) -> bool {
    validate_float(value.x) && validate_float(value.y)
}

#[must_use]
const fn validate_output_float(value: f32) -> bool {
    matches!(
        value.classify(),
        FpCategory::Normal | FpCategory::Zero | FpCategory::Subnormal
    ) && value.is_sign_positive()
}
