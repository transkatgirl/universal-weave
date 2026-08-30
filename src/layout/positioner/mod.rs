#![allow(clippy::too_many_lines, clippy::cognitive_complexity, reason = "WIP")]
#![allow(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    reason = "cfg gate ensures usize::MAX is always >= u32::MAX; assertions ensure input len < u32::MAX"
)]
#![allow(
    clippy::arithmetic_side_effects,
    reason = "Node and edge counts fit in u32"
)]
#![allow(clippy::impl_trait_in_params, reason = "Readability")]

// TODO: Substantial clean-up work

use core::hash::{BuildHasher, Hash};

use alloc::vec::Vec;
use glam::Vec2;
use scratchpads::{Scratchpad, ScratchpadGuard, ScratchpadVec};
use tinyvec::ArrayVec;

use crate::{
    IndependentContents, LayoutItem, Node, Weave,
    dependent::DependentWeave,
    independent::IndependentWeave,
    layout::{Spacing, positioner::slotset::SlotSet, validate_output_float, validate_vec2},
};

mod slotset;

#[derive(Debug, Clone)]
#[must_use]
pub struct Layout2D<K>
where
    K: Hash + Copy + Eq + Ord,
{
    keys: Vec<K>,
    sizes: Vec<Vec2>,
    height: u32,

    real_offsets: Vec<u32>,
    rank_half_width: Vec<f32>,
    x_coordinates: Vec<f32>,
    layer_ends: Vec<f32>,
    layer_gap: f32,
    size: Vec2,
    polylines: Vec<(u32, u32)>,
    polyline_source_x: Vec<f32>,
    polyline_segments: Vec<(u32, f32, u32)>,
    polyline_offsets: Vec<u32>,
    polyline_segment_offsets: Vec<u32>,
    polyline_bounds: Vec<(Vec2, Vec2)>,
    polyline_reach: Vec<(f32, f32)>,
    polyline_block_bounds: Vec<(Vec2, Vec2)>,
    reach_prefix: Vec<f32>,
}

impl<K> Default for Layout2D<K>
where
    K: Hash + Copy + Eq + Ord,
{
    fn default() -> Self {
        Self {
            keys: Vec::new(),
            sizes: Vec::new(),
            height: 0,
            real_offsets: Vec::new(),
            rank_half_width: Vec::new(),
            x_coordinates: Vec::new(),
            layer_ends: Vec::new(),
            layer_gap: 0.0,
            size: Vec2::ZERO,
            polylines: Vec::new(),
            polyline_source_x: Vec::new(),
            polyline_segments: Vec::new(),
            polyline_offsets: Vec::new(),
            polyline_segment_offsets: Vec::new(),
            polyline_bounds: Vec::new(),
            polyline_reach: Vec::new(),
            polyline_block_bounds: Vec::new(),
            reach_prefix: Vec::new(),
        }
    }
}

const SEG_BIT: u32 = 1 << 31_u32;
const RANK_MASK: u32 = SEG_BIT - 1;
const VIEW_BLOCK_SHIFT: u32 = 6;
const VIEW_BLOCK: usize = 1 << VIEW_BLOCK_SHIFT;

impl<K> Layout2D<K>
where
    K: Hash + Copy + Eq + Ord,
{
    fn clear(&mut self, reserved_nodes: usize) {
        self.keys.clear();
        self.keys.reserve(reserved_nodes);
        self.sizes.clear();
        self.sizes.reserve(reserved_nodes);
        self.height = 0;

        self.real_offsets.clear();

        self.rank_half_width.clear();

        self.x_coordinates.clear();
        self.layer_ends.clear();
        self.size = Vec2::ZERO;
        self.layer_gap = 0.0;
        self.polylines.clear();
        self.polyline_source_x.clear();
        self.polyline_segments.clear();
        self.polyline_offsets.clear();
        self.polyline_segment_offsets.clear();
        self.polyline_bounds.clear();
        self.polyline_reach.clear();
        self.polyline_block_bounds.clear();
        self.reach_prefix.clear();
    }
    fn push_real(
        &mut self,
        top: &mut ScratchpadVec<'_, u32>,
        bottom: &mut ScratchpadVec<'_, u32>,
        key: K,
        rank: u32,
        size: Vec2,
    ) -> u32 {
        assert!(validate_vec2(size), "Invalid size");

        let index = top.len() as u32;

        self.sizes.push(size);

        if !bottom.is_empty() {
            bottom.push(self.keys.len() as u32);
        }

        self.keys.push(key);
        top.push(rank);
        self.height = self.height.max(rank + 1);

        index
    }
    fn push_real_unsegmentable(
        &mut self,
        top: &mut ScratchpadVec<'_, u32>,
        key: K,
        rank: u32,
        size: Vec2,
    ) -> u32 {
        assert!(validate_vec2(size), "Invalid size");

        let index = top.len() as u32;

        self.sizes.push(size);

        self.keys.push(key);
        top.push(rank);
        self.height = self.height.max(rank + 1);

        index
    }
    fn push_segment(
        &mut self,
        top: &mut ScratchpadVec<'_, u32>,
        bottom: &mut ScratchpadVec<'_, u32>,
        first: u32,
        last: u32,
    ) -> u32 {
        let index = top.len() as u32;

        if bottom.is_empty() {
            bottom.reserve(top.capacity());
            bottom.extend(0..index);
        }

        top.push(first | SEG_BIT);
        bottom.push(last);
        self.height = self.height.max(last + 1);

        index
    }
}

impl<K> Layout2D<K>
where
    K: Hash + Copy + Eq + Ord,
{
    pub fn layout_dependent<T, M, S>(
        &mut self,
        weave: &mut DependentWeave<K, T, M, S>,
        mut sizes: impl FnMut(&K) -> Vec2,
        spacing: &Spacing,
    ) where
        S: BuildHasher + Default + Clone,
    {
        assert!(weave.nodes.len() < SEG_BIT as usize, "Too many nodes");

        self.clear(weave.nodes.len());

        let guard = weave.scratchpad.guard();

        let structure = {
            let mut top: ScratchpadVec<'_, u32> = guard.vec_with_capacity(weave.nodes.len());
            let mut edges: ScratchpadVec<'_, u32> =
                guard.vec_with_capacity(weave.nodes.len().strict_mul(2));
            let mut stack = guard.vec_with_capacity(weave.roots.len());

            stack.extend(weave.roots.iter().rev().map(|&id| (id, u32::MAX, 0)));

            while let Some((id, parent, rank)) = stack.pop() {
                let index = self.push_real_unsegmentable(&mut top, id, rank, sizes(&id));

                if parent != u32::MAX {
                    edges.extend([parent, index]);
                }

                let next_rank = rank + 1;

                stack.extend(
                    weave.nodes[&id]
                        .to
                        .iter()
                        .copied()
                        .rev()
                        .map(|child| (child, index, next_rank)),
                );
            }

            self.prepare_structure_inner::<false>(&guard, top, guard.vec(), edges)
        };

        self.assign_dag_coordinates_inner::<false>(&guard, &structure, spacing);
    }
    pub fn layout_independent<T, M, S>(
        &mut self,
        weave: &mut IndependentWeave<K, T, M, S>,
        mut sizes: impl FnMut(&K) -> Vec2,
        spacing: &Spacing,
        topological: &mut Vec<K>,
    ) where
        T: IndependentContents,
        S: BuildHasher + Default + Clone,
    {
        assert!(weave.nodes.len() < SEG_BIT as usize, "Too many nodes");

        self.clear(weave.nodes.len());

        let guard = weave.scratchpad.guard();

        let structure = {
            let edge_total = weave.nodes.values().map(|node| node.from.len()).sum();

            let mut top: ScratchpadVec<'_, u32> =
                guard.vec_with_capacity(weave.nodes.len().strict_add(edge_total));
            let mut bottom: ScratchpadVec<'_, u32> = guard.vec();
            let mut edges: ScratchpadVec<'_, u32> =
                guard.vec_with_capacity(edge_total.strict_mul(4));
            let mut indices = guard.map_with_capacity(weave.nodes.len(), S::default());
            let mut parents: ScratchpadVec<'_, (u32, u32)> = guard.vec();

            for id in topological.drain(..) {
                parents.extend(weave.nodes[&id].from.iter().map(|id| {
                    let index = indices[id];
                    (index, top[index as usize] & RANK_MASK)
                }));

                let rank = parents
                    .iter()
                    .map(|(_, top)| *top)
                    .max()
                    .map_or_default(|r| r + 1);

                let index = self.push_real(&mut top, &mut bottom, id, rank, sizes(&id));
                indices.insert(id, index);

                for (from_index, from_rank) in parents.drain(..) {
                    let next_from_rank = from_rank + 1;

                    if next_from_rank == rank {
                        edges.extend([from_index, index]);
                    } else {
                        let segment =
                            self.push_segment(&mut top, &mut bottom, next_from_rank, rank - 1);

                        edges.extend([from_index, segment, segment, index]);
                    }
                }
            }

            assert!(top.len() < u32::MAX as usize, "Too many vertices");

            debug_assert_eq!(
                weave.nodes.len(),
                indices.len(),
                "Malformed topological order"
            );

            self.prepare_structure(&guard, top, bottom, edges)
        };

        self.assign_dag_coordinates(&guard, &structure, spacing);
    }
    pub fn layout_topological<W, N, T, S, F>(
        &mut self,
        weave: &W,
        mut sizes: F,
        spacing: &Spacing,
        scratchpad: &mut Scratchpad,
        topological: &mut Vec<K>,
    ) where
        W: Weave<K, N, T>,
        K: Hash + Copy + Eq + Ord + 'static,
        N: Node<K, T>,
        S: BuildHasher + Default + Clone,
        F: FnMut(&K) -> Vec2,
        for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    {
        assert!(weave.len() < SEG_BIT as usize, "Too many nodes");

        self.clear(weave.len());

        let guard = scratchpad.guard();

        let structure = {
            let mut top: ScratchpadVec<'_, u32> =
                guard.vec_with_capacity(weave.len().strict_mul(2));
            let mut bottom: ScratchpadVec<'_, u32> = guard.vec();
            let mut edges: ScratchpadVec<'_, u32> =
                guard.vec_with_capacity(weave.len().strict_mul(4));
            let mut indices = guard.map_with_capacity(weave.len(), S::default());
            let mut parents: ScratchpadVec<'_, (u32, u32)> = guard.vec();

            for id in topological.drain(..) {
                parents.extend(weave.get_parents(&id).unwrap().into_iter().map(|id| {
                    let index = indices[id];
                    (index, top[index as usize] & RANK_MASK)
                }));

                let rank = parents
                    .iter()
                    .map(|(_, top)| *top)
                    .max()
                    .map_or_default(|r| r + 1);

                let index = self.push_real(&mut top, &mut bottom, id, rank, sizes(&id));
                indices.insert(id, index);

                for (from_index, from_rank) in parents.drain(..) {
                    let next_from_rank = from_rank + 1;

                    if next_from_rank == rank {
                        edges.extend([from_index, index]);
                    } else {
                        let segment =
                            self.push_segment(&mut top, &mut bottom, next_from_rank, rank - 1);

                        edges.extend([from_index, segment, segment, index]);
                    }
                }
            }

            assert!(top.len() < u32::MAX as usize, "Too many vertices");

            assert_eq!(weave.len(), indices.len(), "Malformed topological order");

            self.prepare_structure(&guard, top, bottom, edges)
        };

        self.assign_dag_coordinates(&guard, &structure, spacing);
    }
}

struct LayoutCSR<'g> {
    top: ScratchpadVec<'g, u32>,
    bottom: ScratchpadVec<'g, u32>,
    real_flat: ScratchpadVec<'g, u32>,
    down_offsets: ScratchpadVec<'g, u32>,
    down_flat: ScratchpadVec<'g, u32>,
    deepest: ScratchpadVec<'g, u32>,
    seg_top_offsets: ScratchpadVec<'g, u32>,
    seg_top_flat: ScratchpadVec<'g, u32>,
    seg_bottom_offsets: ScratchpadVec<'g, u32>,
    seg_bottom_flat: ScratchpadVec<'g, u32>,
    merged_top_offsets: ScratchpadVec<'g, u32>,
    merged_top_flat: ScratchpadVec<'g, u32>,
    merged_bottom_offsets: ScratchpadVec<'g, u32>,
    merged_bottom_flat: ScratchpadVec<'g, u32>,
    up_offsets: ScratchpadVec<'g, u32>,
    up_flat: ScratchpadVec<'g, u32>,
}

struct PassScratch<'a, 'g> {
    top: &'a [u32],
    bottom: &'a [u32],
    marked: &'a [bool],
    marked_up: &'a [bool],
    extent: &'a [f32],
    leftmost_at: &'a [u32],
    rightmost_at: &'a [u32],
    left_offsets: &'a [u32],
    left_runs: &'a [(u32, u32, u32)],
    right_offsets: &'a [u32],
    right_runs: &'a [(u32, u32, u32)],
    left_single: &'a [u32],
    right_single: &'a [u32],
    merged_top_flat: &'a [u32],
    merged_top_offsets: &'a [u32],
    merged_bottom_flat: &'a [u32],
    merged_bottom_offsets: &'a [u32],
    medians_down: &'a [Medians],
    medians_up: &'a [Medians],
    root: &'a mut ScratchpadVec<'g, u32>,
    align: &'a mut ScratchpadVec<'g, u32>,
    sink: &'a mut ScratchpadVec<'g, u32>,
    shift: &'a mut ScratchpadVec<'g, f32>,
    stack: &'a mut ScratchpadVec<'g, (u32, u32, u32)>,
}

impl<K> Layout2D<K>
where
    K: Hash + Copy + Eq + Ord,
{
    fn prepare_structure<'g>(
        &mut self,
        guard: &'g ScratchpadGuard<'_>,
        top: ScratchpadVec<'g, u32>,
        bottom: ScratchpadVec<'g, u32>,
        edges: ScratchpadVec<'g, u32>,
    ) -> LayoutCSR<'g> {
        if bottom.is_empty() {
            self.prepare_structure_inner::<false>(guard, top, bottom, edges)
        } else {
            self.prepare_structure_inner::<true>(guard, top, bottom, edges)
        }
    }
    fn prepare_structure_inner<'g, const HAS_SEGMENTS: bool>(
        &mut self,
        guard: &'g ScratchpadGuard<'_>,
        top: ScratchpadVec<'g, u32>,
        bottom: ScratchpadVec<'g, u32>,
        mut edges: ScratchpadVec<'g, u32>,
    ) -> LayoutCSR<'g> {
        let count = top.len();
        let edge_count = edges.len() >> 1_usize;
        let ranks = self.height as usize + 1;

        assert!(edge_count < u32::MAX as usize, "Too many edges");

        self.real_offsets.resize(ranks, 0);

        let mut seg_top_offsets = guard.vec();
        let mut seg_bottom_offsets = guard.vec();
        let mut merged_top_offsets = guard.vec();
        let mut merged_bottom_offsets = guard.vec();

        if HAS_SEGMENTS {
            seg_top_offsets.resize(ranks, 0);
            seg_bottom_offsets.resize(ranks, 0);

            for (top, bottom) in top.iter().copied().zip(bottom.iter().copied()) {
                let segment = top & SEG_BIT != 0;
                let top = (top & RANK_MASK) as usize;

                if segment {
                    seg_top_offsets[top] += 1;
                    seg_bottom_offsets[bottom as usize] += 1;
                } else {
                    self.real_offsets[top] += 1;
                }
            }

            merged_top_offsets.extend(
                self.real_offsets
                    .iter()
                    .copied()
                    .zip(seg_top_offsets.iter().copied())
                    .map(|(real, seg)| real + seg),
            );
            merged_bottom_offsets.extend(
                self.real_offsets
                    .iter()
                    .copied()
                    .zip(seg_bottom_offsets.iter().copied())
                    .map(|(real, seg)| real + seg),
            );
        } else {
            for top in top.iter().copied() {
                self.real_offsets[top as usize] += 1;
            }
        }

        let real_total = ends_from_counts(&mut self.real_offsets);
        let segment_total = ends_from_counts(&mut seg_top_offsets);
        ends_from_counts(&mut seg_bottom_offsets);
        ends_from_counts(&mut merged_top_offsets);
        ends_from_counts(&mut merged_bottom_offsets);

        let mut real_flat = guard.vec_with_capacity(real_total);
        let mut seg_top_flat = guard.vec_with_capacity(segment_total);
        let mut seg_bottom_flat = guard.vec_with_capacity(segment_total);
        let mut merged_top_flat = guard.vec();
        let mut merged_bottom_flat = guard.vec();

        real_flat.resize(real_total, 0);
        seg_top_flat.resize(segment_total, 0);
        seg_bottom_flat.resize(segment_total, 0);

        if HAS_SEGMENTS {
            merged_top_flat.resize(count, 0);
            merged_bottom_flat.resize(count, 0);

            for (index, (top, bottom)) in top
                .iter()
                .copied()
                .zip(bottom.iter().copied())
                .enumerate()
                .rev()
            {
                let index = index as u32;
                let segment = top & SEG_BIT != 0;
                let top = (top & RANK_MASK) as usize;
                let bottom = if segment { bottom as usize } else { top };

                if segment {
                    let cursor = &mut seg_top_offsets[top];

                    *cursor -= 1;
                    seg_top_flat[*cursor as usize] = index;

                    let cursor = &mut seg_bottom_offsets[bottom];

                    *cursor -= 1;
                    seg_bottom_flat[*cursor as usize] = index;
                } else {
                    let cursor = &mut self.real_offsets[top];

                    *cursor -= 1;
                    real_flat[*cursor as usize] = index;
                }

                let cursor = &mut merged_top_offsets[top];

                *cursor -= 1;
                merged_top_flat[*cursor as usize] = index;

                let cursor = &mut merged_bottom_offsets[bottom];

                *cursor -= 1;
                merged_bottom_flat[*cursor as usize] = index;
            }
        } else {
            for (index, top) in top.iter().copied().enumerate().rev() {
                let cursor = &mut self.real_offsets[top as usize];

                *cursor -= 1;
                real_flat[*cursor as usize] = index as u32;
            }
        }

        let pairs = edges.as_chunks::<2>().0;

        let mut up_offsets = guard.vec_with_capacity(count + 1);
        let mut down_offsets = guard.vec_with_capacity(count + 1);

        up_offsets.resize(count + 1, 0);
        down_offsets.resize(count + 1, 0);

        for [source, target] in pairs.iter().copied() {
            up_offsets[target as usize] += 1;
            down_offsets[source as usize] += 1;
        }

        ends_from_counts(&mut up_offsets);
        ends_from_counts(&mut down_offsets);

        let mut down_flat = guard.vec_with_capacity(edge_count);
        let mut deepest = guard.vec_with_capacity(self.height as usize);

        down_flat.resize(edge_count, 0);
        deepest.extend(0..self.height);

        for [source, target] in pairs.iter().copied().rev() {
            let source = source as usize;

            let cursor = &mut down_offsets[source];

            *cursor -= 1;
            down_flat[*cursor as usize] = target;

            let top_source_raw = top[source];

            if !HAS_SEGMENTS || top_source_raw & SEG_BIT == 0 {
                let target = target as usize;
                let top_target_raw = top[target];

                let child = if HAS_SEGMENTS && top_target_raw & SEG_BIT != 0 {
                    bottom[target] + 1
                } else {
                    top_target_raw
                };
                let rank = (top_source_raw & RANK_MASK) as usize;

                let deepest_rank = &mut deepest[rank];
                *deepest_rank = (*deepest_rank).max(child);
            }
        }

        edges.clear();
        let mut up_flat = edges;

        up_flat.resize(edge_count, 0);

        let ordered = if HAS_SEGMENTS {
            &merged_bottom_flat
        } else {
            &real_flat
        };

        for source in ordered.iter().copied().rev() {
            for target in bucket(&down_flat, &down_offsets, source as usize)
                .iter()
                .copied()
                .rev()
            {
                let target = target as usize;

                let cursor = &mut up_offsets[target];

                *cursor -= 1;
                up_flat[*cursor as usize] = source;
            }
        }

        let height_usize = self.height as usize;

        self.polyline_offsets.resize(height_usize + 1, 0);
        self.polyline_segment_offsets.resize(height_usize + 1, 0);
        self.polyline_bounds
            .resize(height_usize, (Vec2::INFINITY, Vec2::NEG_INFINITY));
        self.polyline_reach.resize(height_usize, (0.0_f32, 0.0_f32));

        LayoutCSR {
            top,
            bottom,
            real_flat,
            down_offsets,
            down_flat,
            deepest,
            seg_top_offsets,
            seg_top_flat,
            seg_bottom_offsets,
            seg_bottom_flat,
            merged_top_offsets,
            merged_top_flat,
            merged_bottom_offsets,
            merged_bottom_flat,
            up_offsets,
            up_flat,
        }
    }
    fn assign_dag_coordinates(
        &mut self,
        guard: &ScratchpadGuard<'_>,
        structure: &LayoutCSR<'_>,
        spacing: &Spacing,
    ) {
        if structure.bottom.is_empty() {
            self.assign_dag_coordinates_inner::<false>(guard, structure, spacing);
        } else {
            self.assign_dag_coordinates_inner::<true>(guard, structure, spacing);
        }
    }
    #[allow(clippy::float_arithmetic, reason = "Coordinate calculation")]
    fn assign_dag_coordinates_inner<const HAS_SEGMENTS: bool>(
        &mut self,
        guard: &ScratchpadGuard<'_>,
        structure: &LayoutCSR<'_>,
        spacing: &Spacing,
    ) {
        assert!(spacing.validate(), "Invalid spacing");

        self.layer_gap = spacing.layer;

        let count = structure.top.len();
        let height = self.height as usize;

        if count == 0 {
            return;
        }

        let mut extent = guard.vec_with_capacity(count);
        let mut rank_tallest = guard.vec_with_capacity(height);

        extent.resize(count, 0.0);
        rank_tallest.resize(height, 0.0_f32);

        self.rank_half_width.resize(height, 0.0);

        for ((top, extent), size) in structure
            .top
            .iter()
            .copied()
            .zip(extent.iter_mut())
            .filter_map(|(top, extent)| {
                if HAS_SEGMENTS && top & SEG_BIT != 0 {
                    *extent = -(spacing.corridor * 0.5);
                    None
                } else {
                    Some((top, extent))
                }
            })
            .zip(self.sizes.iter().copied())
        {
            let rank = top as usize;
            let half_width = size.x * 0.5_f32;

            *extent = half_width;

            let rank_tallest = &mut rank_tallest[rank];
            *rank_tallest = (*rank_tallest).max(size.y);

            let rank_half_width = &mut self.rank_half_width[rank];
            *rank_half_width = (*rank_half_width).max(half_width);
        }

        let (merged_top_flat, merged_top_offsets, merged_bottom_flat, merged_bottom_offsets) =
            if HAS_SEGMENTS {
                (
                    &*structure.merged_top_flat,
                    &*structure.merged_top_offsets,
                    &*structure.merged_bottom_flat,
                    &*structure.merged_bottom_offsets,
                )
            } else {
                (
                    &*structure.real_flat,
                    &*self.real_offsets,
                    &*structure.real_flat,
                    &*self.real_offsets,
                )
            };

        let mut marked = guard.vec();
        let mut marked_up = guard.vec();
        let mut leftmost_at = guard.vec_with_capacity(height);
        let mut rightmost_at = guard.vec_with_capacity(height);

        leftmost_at.resize(height, 0);
        rightmost_at.resize(height, 0);

        let mut closed_runs = if HAS_SEGMENTS {
            guard.vec_with_capacity(count.strict_mul(3))
        } else {
            guard.vec()
        };

        if HAS_SEGMENTS {
            marked.resize(structure.down_flat.len(), false);

            let mut any_marked = false;
            let mut active = SlotSet::new(guard);
            let mut spanning = SlotSet::new(guard);
            let mut open_run_start = guard.vec_with_capacity(count);

            active.rebuild(count);
            spanning.rebuild(count);
            open_run_start.resize(count, 0);

            for rank in 0..=height {
                if let Some(previous) = rank.checked_sub(1) {
                    let end = previous as u32;

                    for item in bucket(merged_bottom_flat, merged_bottom_offsets, previous)
                        .iter()
                        .copied()
                    {
                        let item = item as usize;
                        let before = active.predecessor(item);
                        let after = active.successor(item);

                        active.remove(item);

                        if let Some(left) = before {
                            let start = open_run_start[left];
                            let right = item;

                            if end >= start {
                                closed_runs.push((left as u32, right as u32, start, end));
                            }
                        }
                        if let Some(right) = after {
                            let left = item;
                            let start = open_run_start[item];

                            if end >= start {
                                closed_runs.push((left as u32, right as u32, start, end));
                            }
                        }
                        if let (Some(left), Some(_)) = (before, after) {
                            open_run_start[left] = rank as u32;
                        }
                    }
                }

                if rank == height {
                    continue;
                }

                if let Some(previous) = rank.checked_sub(1) {
                    let end = previous as u32;

                    for item in bucket(merged_top_flat, merged_top_offsets, rank)
                        .iter()
                        .copied()
                    {
                        let item = item as usize;
                        let rank = rank as u32;
                        let before = active.predecessor(item);
                        let after = active.successor(item);

                        active.insert(item);

                        if let (Some(left), Some(right)) = (before, after) {
                            let start = open_run_start[left];

                            if end >= start {
                                closed_runs.push((left as u32, right as u32, start, end));
                            }
                        }

                        if let Some(left) = before {
                            open_run_start[left] = rank;
                        }
                        if after.is_some() {
                            open_run_start[item] = rank;
                        }
                    }
                } else {
                    for item in bucket(merged_top_flat, merged_top_offsets, rank)
                        .iter()
                        .copied()
                    {
                        let item = item as usize;
                        let rank = rank as u32;

                        if let Some(left) = active.predecessor(item) {
                            open_run_start[left] = rank;
                        }
                        if active.successor(item).is_some() {
                            open_run_start[item] = rank;
                        }

                        active.insert(item);
                    }
                }

                leftmost_at[rank] = active.first().unwrap() as u32;
                rightmost_at[rank] = active.last().unwrap() as u32;

                if rank + 1 == height {
                    continue;
                }

                for segment in bucket(&structure.seg_top_flat, &structure.seg_top_offsets, rank)
                    .iter()
                    .copied()
                {
                    spanning.insert(segment as usize);
                }

                for segment in bucket(
                    &structure.seg_bottom_flat,
                    &structure.seg_bottom_offsets,
                    rank,
                )
                .iter()
                .copied()
                {
                    spanning.remove(segment as usize);
                }

                if spanning.is_empty() {
                    continue;
                }

                for source in bucket(merged_bottom_flat, merged_bottom_offsets, rank)
                    .iter()
                    .copied()
                {
                    let source = source as usize;
                    let base = structure.down_offsets[source] as usize;
                    let mut before = None;
                    let mut after = None;

                    for (offset, target) in
                        bucket(&structure.down_flat, &structure.down_offsets, source)
                            .iter()
                            .copied()
                            .enumerate()
                    {
                        let target = target as usize;
                        let crossed = if target > source {
                            after
                                .get_or_insert_with(|| spanning.successor(source))
                                .is_some_and(|found| found < target)
                        } else {
                            before
                                .get_or_insert_with(|| spanning.predecessor(source))
                                .is_some_and(|found| found > target)
                        };

                        if crossed {
                            marked[base + offset] = true;
                            any_marked = true;
                        }
                    }
                }
            }

            if any_marked {
                marked_up.resize(structure.down_flat.len(), false);

                let mut cursors = guard.vec_with_capacity(count);

                cursors.extend(structure.up_offsets[..count].iter().copied());

                for source in merged_bottom_flat.iter().copied() {
                    let source = source as usize;
                    let base = structure.down_offsets[source] as usize;

                    for (offset, target) in
                        bucket(&structure.down_flat, &structure.down_offsets, source)
                            .iter()
                            .copied()
                            .enumerate()
                    {
                        let cursor = &mut cursors[target as usize];

                        marked_up[*cursor as usize] = marked[base + offset];
                        *cursor += 1;
                    }
                }
            } else {
                marked.clear();
            }
        } else {
            for (rank, (leftmost, rightmost)) in leftmost_at
                .iter_mut()
                .zip(rightmost_at.iter_mut())
                .enumerate()
            {
                let layer = bucket(&structure.real_flat, &self.real_offsets, rank);

                *leftmost = layer[0];
                *rightmost = *layer.last().unwrap();
            }
        }

        let medians_down = build_medians(
            guard,
            &structure.up_offsets,
            &structure.up_flat,
            &structure.top,
        );
        let medians_up = build_medians(
            guard,
            &structure.down_offsets,
            &structure.down_flat,
            &structure.top,
        );

        let mut left_offsets = guard.vec();
        let mut right_offsets = guard.vec();
        let mut left_runs = guard.vec();
        let mut right_runs = guard.vec();
        let mut left_single = guard.vec();
        let mut right_single = guard.vec();

        if HAS_SEGMENTS {
            let total_runs = closed_runs.len();

            left_offsets.resize(count + 1, 0);
            right_offsets.resize(count + 1, 0);
            left_runs.resize(total_runs, (0, 0, 0));
            right_runs.resize(total_runs, (0, 0, 0));

            for (left, right, _, _) in closed_runs.iter().copied() {
                left_offsets[right as usize] += 1;
                right_offsets[left as usize] += 1;
            }

            ends_from_counts(&mut left_offsets);
            ends_from_counts(&mut right_offsets);

            for (left, right, start, end) in closed_runs.iter().copied().rev() {
                let cursor = &mut left_offsets[right as usize];

                *cursor -= 1;
                left_runs[*cursor as usize] = (left, start, end);

                let cursor = &mut right_offsets[left as usize];

                *cursor -= 1;
                right_runs[*cursor as usize] = (right, start, end);
            }
        } else {
            left_single.resize(count, u32::MAX);
            right_single.resize(count, u32::MAX);

            for rank in 0..height {
                let layer = bucket(&structure.real_flat, &self.real_offsets, rank);

                for (left, right) in layer.iter().copied().zip(layer.iter().copied().skip(1)) {
                    left_single[right as usize] = left;
                    right_single[left as usize] = right;
                }
            }
        }

        let mut candidates = [
            guard.vec_with_capacity(count),
            guard.vec_with_capacity(count),
            guard.vec_with_capacity(count),
            guard.vec_with_capacity(count),
        ];

        for candidate in &mut candidates {
            candidate.resize(count, f32::NAN);
        }

        let mut root = guard.vec_with_capacity(count);

        let mut scratch = PassScratch {
            top: &structure.top,
            bottom: &structure.bottom,
            marked: &marked,
            marked_up: &marked_up,
            extent: &extent,
            leftmost_at: &leftmost_at,
            rightmost_at: &rightmost_at,
            left_offsets: &left_offsets,
            left_runs: &left_runs,
            right_offsets: &right_offsets,
            right_runs: &right_runs,
            left_single: &left_single,
            right_single: &right_single,
            merged_top_flat,
            merged_top_offsets,
            merged_bottom_flat,
            merged_bottom_offsets,
            medians_down: &medians_down,
            medians_up: &medians_up,
            root: &mut root,
            align: &mut guard.vec_with_capacity(count),
            sink: &mut guard.vec_with_capacity(count),
            shift: &mut guard.vec_with_capacity(count),
            stack: &mut guard.vec(),
        };

        let extents = [
            self.coordinate_pass::<true, true, HAS_SEGMENTS>(
                &mut scratch,
                spacing,
                &mut candidates[0],
            ),
            self.coordinate_pass::<true, false, HAS_SEGMENTS>(
                &mut scratch,
                spacing,
                &mut candidates[1],
            ),
            self.coordinate_pass::<false, true, HAS_SEGMENTS>(
                &mut scratch,
                spacing,
                &mut candidates[2],
            ),
            self.coordinate_pass::<false, false, HAS_SEGMENTS>(
                &mut scratch,
                spacing,
                &mut candidates[3],
            ),
        ];

        let mut best = 0;

        for pass in 1..4 {
            let pass_extent = extents[pass];
            let best_extent = extents[best];

            if pass_extent.1 - pass_extent.0 < best_extent.1 - best_extent.0 {
                best = pass;
            }
        }

        let offsets = [
            extents[best].0 - extents[0].0,
            extents[best].1 - extents[1].1,
            extents[best].0 - extents[2].0,
            extents[best].1 - extents[3].1,
        ];

        let mut left = f32::INFINITY;
        let mut right = f32::NEG_INFINITY;

        let [first, second, third, mut fourth] = candidates;

        for ((((coordinate, a), b), c), extent) in fourth
            .iter_mut()
            .zip(first)
            .zip(second)
            .zip(third)
            .zip(extent.iter().copied())
        {
            let (a, b, c, d) = (
                a + offsets[0],
                b + offsets[1],
                c + offsets[2],
                *coordinate + offsets[3],
            );

            let combined = f32::midpoint(a.min(b).max(c.min(d)), a.max(b).min(c.max(d)));
            let extent = extent.abs();

            *coordinate = combined;
            left = left.min(combined - extent);
            right = right.max(combined + extent);
        }

        let mut cursor = rank_tallest[0];
        let mut valid = validate_output_float(cursor);

        self.layer_ends.clear();
        self.layer_ends.reserve(rank_tallest.len());
        self.layer_ends.push(cursor);
        self.layer_ends
            .extend(rank_tallest.into_iter().skip(1).map(|tallest| {
                cursor += spacing.layer + tallest;
                valid &= validate_output_float(cursor);

                cursor
            }));

        self.size = Vec2::new(right - left, cursor);
        valid &= validate_output_float(self.size.x);

        for coordinate in &mut fourth {
            *coordinate -= left;
            valid &= validate_output_float(*coordinate);
        }

        assert!(valid, "Output is not normal and positive");

        let mut reach = 0.0_f32;

        self.reach_prefix.clear();
        self.reach_prefix
            .extend(structure.deepest.iter().copied().map(|deep| {
                reach = reach.max(self.layer_ends[deep as usize]);
                reach
            }));

        self.x_coordinates.clear();
        self.x_coordinates.extend(
            structure
                .real_flat
                .iter()
                .copied()
                .map(|vertex| fourth[vertex as usize]),
        );

        {
            let ordinal_of = |vertex| {
                if HAS_SEGMENTS {
                    structure.bottom[vertex] as usize
                } else {
                    vertex
                }
            };

            let permuted_keys = guard.inner.alloc_iter_exact(
                structure
                    .real_flat
                    .iter()
                    .copied()
                    .map(|vertex| self.keys[ordinal_of(vertex as usize)]),
            );
            self.keys.copy_from_slice(&permuted_keys);

            let permuted_sizes = guard.inner.alloc_iter_exact(
                structure
                    .real_flat
                    .iter()
                    .copied()
                    .map(|vertex| self.sizes[ordinal_of(vertex as usize)]),
            );
            self.sizes.copy_from_slice(&permuted_sizes);
        }

        if !structure.down_flat.is_empty() {
            root.clear();

            let mut position_of = root;
            position_of.resize(count, 0);

            for (position, vertex) in structure.real_flat.iter().copied().enumerate() {
                position_of[vertex as usize] = position as u32;
            }

            self.build_polylines::<HAS_SEGMENTS>(structure, &position_of, &fourth);
        }

        self.polyline_block_bounds.clear();
        self.polyline_block_bounds
            .reserve(height.div_ceil(VIEW_BLOCK));

        self.polyline_block_bounds
            .extend(self.polyline_bounds.chunks(VIEW_BLOCK).map(|chunk| {
                chunk.iter().copied().fold(
                    (Vec2::INFINITY, Vec2::NEG_INFINITY),
                    |(low, high), (bound_min, bound_max)| (low.min(bound_min), high.max(bound_max)),
                )
            }));
    }
    #[allow(clippy::float_arithmetic, reason = "Coordinate calculation")]
    fn coordinate_pass<const DOWNWARD: bool, const LEFTWARD: bool, const HAS_SEGMENTS: bool>(
        &self,
        scratch: &mut PassScratch<'_, '_>,
        spacing: &Spacing,
        x: &mut [f32],
    ) -> (f32, f32) {
        let PassScratch {
            top,
            bottom,
            marked,
            marked_up,
            extent,
            leftmost_at,
            rightmost_at,
            left_offsets,
            left_runs,
            right_offsets,
            right_runs,
            left_single,
            right_single,
            merged_top_flat,
            merged_top_offsets,
            merged_bottom_flat,
            merged_bottom_offsets,
            medians_down,
            medians_up,
            root,
            align,
            sink,
            shift,
            stack,
        } = scratch;

        let height = self.height as usize;

        let (layer_flat, layer_offsets) = if DOWNWARD {
            (*merged_top_flat, *merged_top_offsets)
        } else {
            (*merged_bottom_flat, *merged_bottom_offsets)
        };
        let medians = if DOWNWARD { *medians_down } else { *medians_up };
        let edge_marked = if DOWNWARD { *marked_up } else { *marked };
        let (runs_flat, runs_offsets) = if LEFTWARD {
            (*left_runs, *left_offsets)
        } else {
            (*right_runs, *right_offsets)
        };
        let (across_flat, across_offsets) = if LEFTWARD {
            (*right_runs, *right_offsets)
        } else {
            (*left_runs, *left_offsets)
        };
        let (runs_single, across_single) = if LEFTWARD {
            (*left_single, *right_single)
        } else {
            (*right_single, *left_single)
        };

        let count = x.len() as u32;

        root.clear();
        root.extend(0..count);
        align.clear();
        align.extend_from_slice(root);
        sink.clear();
        sink.extend_from_slice(root);
        shift.clear();
        shift.resize(x.len(), f32::INFINITY);

        for layer in (0..height)
            .into_iter()
            .map(|i| bucket(layer_flat, layer_offsets, reflect(i, height, DOWNWARD)))
        {
            let mut last = None;

            let mut process = |vertex: usize| {
                let median = &medians[vertex];

                let entries: &[(u32, u32)] = match median.kind {
                    MedianKind::None => return,
                    MedianKind::Single => &[median.entries[0]],
                    MedianKind::Ordered if !LEFTWARD => &[median.entries[1], median.entries[0]],
                    MedianKind::Ordered | MedianKind::Fixed => &median.entries[..2],
                };

                for (neighbour, position) in entries.iter().copied() {
                    let neighbour = neighbour as usize;

                    if (!HAS_SEGMENTS || edge_marked.is_empty() || !edge_marked[position as usize])
                        && last.is_none_or(|last| {
                            if LEFTWARD {
                                last < neighbour
                            } else {
                                last > neighbour
                            }
                        })
                    {
                        align[neighbour] = vertex as u32;
                        root[vertex] = root[neighbour];
                        align[vertex] = root[vertex];

                        last = Some(neighbour);

                        break;
                    }
                }
            };

            if LEFTWARD {
                for vertex in layer.iter().copied() {
                    process(vertex as usize);
                }
            } else {
                for vertex in layer.iter().rev().copied() {
                    process(vertex as usize);
                }
            }
        }

        let separation = |a: usize, b: usize| {
            let (extent_a, extent_b) = (extent[a], extent[b]);

            extent_a.abs()
                + extent_b.abs()
                + if HAS_SEGMENTS {
                    if extent_a.is_sign_negative() | extent_b.is_sign_negative() {
                        spacing.edge
                    } else {
                        spacing.node
                    }
                } else {
                    spacing.node
                }
        };
        let vertex_runs = |vertex: usize| {
            if HAS_SEGMENTS {
                bucket(runs_flat, runs_offsets, vertex)
            } else {
                &[]
            }
        };
        let runs_len = |runs: &[(u32, u32, u32)], vertex: usize| {
            if HAS_SEGMENTS {
                runs.len()
            } else {
                usize::from(runs_single[vertex] != u32::MAX)
            }
        };
        let run_neighbour = |runs: &[(u32, u32, u32)], vertex: usize, run: usize| {
            if HAS_SEGMENTS {
                runs[run].0
            } else {
                runs_single[vertex]
            }
        };

        let mut place = |start: usize| {
            let x_start = &mut x[start];

            if root[start] as usize != start || !x_start.is_nan() {
                return;
            }

            *x_start = 0.0_f32;

            let start = start as u32;
            let mut frame = (start, start, 0);

            'outer: loop {
                let (root_val, member, applied) = frame;
                let mut applied = applied as usize;
                let (root_index, member_index) = (root_val as usize, member as usize);

                let member_runs = vertex_runs(member_index);
                let run_count = runs_len(member_runs, member_index);

                while applied < run_count {
                    let neighbour = run_neighbour(
                        member_runs,
                        member_index,
                        reflect(applied, run_count, DOWNWARD),
                    );
                    let neighbour_root = root[neighbour as usize];
                    let neighbour_x = &mut x[neighbour_root as usize];

                    if neighbour_x.is_nan() {
                        frame.2 = applied as u32;
                        stack.push(frame);
                        frame = (neighbour_root, neighbour_root, 0);

                        *neighbour_x = 0.0_f32;

                        continue 'outer;
                    }

                    let neighbour_x = *neighbour_x;
                    let neighbour_sink = sink[neighbour_root as usize];
                    let sink_root = &mut sink[root_index];
                    if sink_root == &root_val {
                        *sink_root = neighbour_sink;
                    }

                    if *sink_root == neighbour_sink {
                        let x_root = &mut x[root_index];

                        *x_root =
                            x_root.max(neighbour_x + separation(neighbour as usize, member_index));
                    }

                    applied += 1;
                }

                let next = align[member_index];

                if next == root_val {
                    let mut member = root_index;

                    while align[member] != root_val {
                        member = align[member] as usize;

                        x[member] = x[root_index];
                        sink[member] = sink[root_index];
                    }

                    if let Some(parent) = stack.pop() {
                        frame = parent;
                    } else {
                        break;
                    }
                } else {
                    frame = (root_val, next, 0);
                }
            }
        };

        for (layer_start, layer_end) in merged_top_offsets
            .iter()
            .copied()
            .zip(merged_top_offsets.iter().copied().skip(1))
        {
            let layer = &merged_top_flat[layer_start as usize..layer_end as usize];

            if LEFTWARD {
                for start in layer.iter().copied() {
                    place(start as usize);
                }
            } else {
                for start in layer.iter().rev().copied() {
                    place(start as usize);
                }
            }
        }

        for rank in (0..height)
            .into_iter()
            .map(|i| reflect(i, height, DOWNWARD))
        {
            let entry = if LEFTWARD {
                leftmost_at[rank]
            } else {
                rightmost_at[rank]
            } as usize;

            if sink[entry] as usize != entry {
                continue;
            }

            let top_entry_raw = top[entry];
            let entry_rank = if DOWNWARD {
                top_entry_raw & RANK_MASK
            } else if HAS_SEGMENTS && top_entry_raw & SEG_BIT != 0 {
                bottom[entry]
            } else {
                top_entry_raw
            } as usize;

            if rank != entry_rank {
                continue;
            }

            let shift_entry = &mut shift[entry];
            if !shift_entry.is_finite() {
                *shift_entry = 0.0_f32;
            }

            let mut vertex = entry;
            let mut from = rank;

            loop {
                if HAS_SEGMENTS {
                    let vertex_sink = sink[vertex] as usize;
                    let vertex_x = x[vertex];

                    for (neighbour, start, end) in
                        bucket(runs_flat, runs_offsets, vertex).iter().copied()
                    {
                        let forward = if DOWNWARD {
                            end as usize > from
                        } else {
                            (start as usize) < from
                        };

                        if !forward {
                            continue;
                        }

                        let neighbour = neighbour as usize;
                        let neighbour_sink = sink[neighbour] as usize;

                        shift[neighbour_sink] = shift[neighbour_sink].min(
                            shift[vertex_sink] + vertex_x
                                - (x[neighbour] + separation(neighbour, vertex)),
                        );
                    }
                }

                while align[vertex] != root[vertex] {
                    vertex = align[vertex] as usize;

                    let vertex_sink = sink[vertex] as usize;
                    let vertex_x = x[vertex];
                    let runs = vertex_runs(vertex);

                    for run in 0..runs_len(runs, vertex) {
                        let neighbour = run_neighbour(runs, vertex, run) as usize;
                        let neighbour_sink = sink[neighbour] as usize;

                        shift[neighbour_sink] = shift[neighbour_sink].min(
                            shift[vertex_sink] + vertex_x
                                - (x[neighbour] + separation(neighbour, vertex)),
                        );
                    }
                }

                let top_vertex_raw = top[vertex];

                let across = if DOWNWARD {
                    if HAS_SEGMENTS && top_vertex_raw & SEG_BIT != 0 {
                        bottom[vertex]
                    } else {
                        top_vertex_raw
                    }
                } else {
                    top_vertex_raw & RANK_MASK
                } as usize;

                let next = if HAS_SEGMENTS {
                    let runs = bucket(across_flat, across_offsets, vertex);

                    if DOWNWARD {
                        runs.last().and_then(|&(neighbour, _, end)| {
                            (end as usize == across).then_some(neighbour)
                        })
                    } else {
                        runs.first().and_then(|&(neighbour, start, _)| {
                            (start as usize == across).then_some(neighbour)
                        })
                    }
                } else {
                    let neighbour = across_single[vertex];

                    (neighbour != u32::MAX).then_some(neighbour)
                };

                let Some(next) = next else {
                    break;
                };
                let next = next as usize;

                if sink[next] != sink[vertex] {
                    break;
                }

                vertex = next;
                from = across;
            }
        }

        let mut minimum = f32::INFINITY;
        let mut maximum = f32::NEG_INFINITY;

        for ((x, sink), extent) in x
            .iter_mut()
            .zip(sink.iter().copied())
            .zip(extent.iter().copied())
        {
            *x += shift[sink as usize];

            if !LEFTWARD {
                *x = -*x;
            }

            let extent = extent.abs();

            minimum = minimum.min(*x - extent);
            maximum = maximum.max(*x + extent);
        }

        (minimum, maximum)
    }
}

impl<K> Layout2D<K>
where
    K: Hash + Copy + Eq + Ord,
{
    #[allow(clippy::float_arithmetic, reason = "Coordinate calculation")]
    #[inline]
    fn layer_start(&self, rank: usize) -> f32 {
        rank.checked_sub(1).map_or(0.0_f32, |previous| {
            self.layer_ends[previous] + self.layer_gap
        })
    }
    #[allow(clippy::float_arithmetic, reason = "Coordinate calculation")]
    #[inline]
    fn layer_center(&self, rank: usize) -> f32 {
        f32::midpoint(self.layer_start(rank), self.layer_ends[rank])
    }
    #[allow(clippy::float_arithmetic, reason = "Coordinate calculation")]
    #[inline]
    fn ranks_started_by(&self, y: f32) -> usize {
        let Some(interior) = self.layer_ends.len().checked_sub(1) else {
            return 0;
        };

        if 0.0_f32 <= y {
            1 + self.layer_ends[..interior].partition_point(|&end| end + self.layer_gap <= y)
        } else {
            0
        }
    }
    #[allow(clippy::float_arithmetic, reason = "Coordinate calculation")]
    fn build_polylines<const HAS_SEGMENTS: bool>(
        &mut self,
        structure: &LayoutCSR<'_>,
        position_of: &[u32],
        x_coordinates: &[f32],
    ) {
        let LayoutCSR {
            top,
            bottom,
            real_flat,
            down_offsets,
            down_flat,
            ..
        } = structure;

        let segment_count = top.len() - self.keys.len();
        let edge_count = down_flat.len() - segment_count;

        self.polylines.reserve(edge_count);
        self.polyline_source_x.reserve(edge_count);

        if HAS_SEGMENTS {
            self.polyline_segments.reserve(segment_count);
        }

        for rank in 0..self.height as usize {
            let mut rank_min = Vec2::INFINITY;
            let mut rank_max = Vec2::NEG_INFINITY;
            let mut left_reach = 0.0_f32;
            let mut right_reach = 0.0_f32;

            let rank_center = self.layer_center(rank);
            let next_rank = rank + 1;
            let next_center = if next_rank < self.layer_ends.len() {
                self.layer_center(next_rank)
            } else {
                0.0_f32
            };

            for source in bucket(real_flat, &self.real_offsets, rank).iter().copied() {
                let source = source as usize;
                let source_position = position_of[source] as usize;

                let source_x = x_coordinates[source];
                let source_border = rank_center + self.sizes[source_position].y * 0.5_f32;

                for target in bucket(down_flat, down_offsets, source).iter().copied() {
                    let target = target as usize;
                    let (real_target, segment) = if HAS_SEGMENTS && top[target] & SEG_BIT != 0 {
                        (
                            down_flat[down_offsets[target] as usize] as usize,
                            Some((x_coordinates[target], bottom[target])),
                        )
                    } else {
                        (target, None)
                    };
                    let target_position = position_of[real_target] as usize;
                    let target_top = (top[real_target] & RANK_MASK) as usize;

                    let target_center = if target_top == next_rank {
                        next_center
                    } else {
                        self.layer_center(target_top)
                    };

                    let target_x = x_coordinates[real_target];
                    let target_border = target_center - self.sizes[target_position].y * 0.5_f32;

                    let (min_x, max_x) = if let Some((seg_x, _)) = segment {
                        (
                            source_x.min(target_x).min(seg_x),
                            source_x.max(target_x).max(seg_x),
                        )
                    } else {
                        (source_x.min(target_x), source_x.max(target_x))
                    };

                    rank_min = rank_min.min(Vec2::new(min_x, source_border));
                    rank_max = rank_max.max(Vec2::new(max_x, target_border));
                    left_reach = left_reach.max(source_x - min_x);
                    right_reach = right_reach.max(max_x - source_x);

                    if let Some((seg_x, seg_last)) = segment {
                        self.polyline_segments
                            .push((self.polylines.len() as u32, seg_x, seg_last));
                    }

                    self.polylines
                        .push((source_position as u32, target_position as u32));
                    self.polyline_source_x.push(source_x);
                }
            }

            self.polyline_offsets[rank + 1] = self.polylines.len() as u32;
            self.polyline_segment_offsets[rank + 1] = self.polyline_segments.len() as u32;
            self.polyline_bounds[rank] = (rank_min, rank_max);
            self.polyline_reach[rank] = (left_reach, right_reach);
        }
    }
    #[must_use]
    pub const fn size(&self) -> Vec2 {
        self.size
    }
    #[allow(clippy::float_arithmetic, reason = "Coordinate calculation")]
    pub fn view(
        &self,
        min: Vec2,
        max: Vec2,
        mut callback: impl FnMut(LayoutItem<K, Vec2, ArrayVec<[Vec2; 6]>>),
    ) {
        if self.keys.is_empty() {
            return;
        }

        let first = self.layer_ends.partition_point(|&end| end < min.y);
        let first_reaching = self.reach_prefix.partition_point(|&reach| reach < min.y);
        let last = self.ranks_started_by(max.y);

        let mut next_rank = first_reaching;

        while next_rank < last {
            let block = next_rank >> VIEW_BLOCK_SHIFT;
            let block_end = ((block + 1) << VIEW_BLOCK_SHIFT).min(last);
            let (block_min, block_max) = self.polyline_block_bounds[block];

            if !(block_min.x <= max.x
                && block_max.x >= min.x
                && block_min.y <= max.y
                && block_max.y >= min.y)
            {
                next_rank = block_end;
                continue;
            }

            for rank in next_rank..block_end {
                let (rank_min, rank_max) = self.polyline_bounds[rank];

                if !(rank_min.x <= max.x
                    && rank_max.x >= min.x
                    && rank_min.y <= max.y
                    && rank_max.y >= min.y)
                {
                    continue;
                }

                let (range_start, range_end) =
                    (self.polyline_offsets[rank], self.polyline_offsets[rank + 1]);
                let (left_reach, right_reach) = self.polyline_reach[rank];

                let begin = range_start as usize
                    + self.polyline_source_x[range_start as usize..range_end as usize]
                        .partition_point(|&x| x < min.x - right_reach);

                let rank_center = self.layer_center(rank);
                let source_band_end = self.layer_ends[rank];
                let (next_center, next_band_start) = if rank + 1 < self.layer_ends.len() {
                    (self.layer_center(rank + 1), self.layer_start(rank + 1))
                } else {
                    (0.0_f32, 0.0_f32)
                };

                let segment_start = self.polyline_segment_offsets[rank] as usize;
                let segment_end = self.polyline_segment_offsets[rank + 1] as usize;
                let mut segment_cursor = segment_start
                    + self.polyline_segments[segment_start..segment_end]
                        .partition_point(|&(polyline, _, _)| (polyline as usize) < begin);

                for index in begin..range_end as usize {
                    let line = self.polylines[index];
                    let source_x = self.polyline_source_x[index];

                    if source_x - left_reach > max.x {
                        break;
                    }

                    let target_x = self.x_coordinates[line.1 as usize];
                    let segment_info = match self.polyline_segments.get(segment_cursor) {
                        Some(&(polyline, seg_x, seg_last)) if polyline as usize == index => {
                            segment_cursor += 1;

                            Some((seg_x, seg_last))
                        }
                        _ => None,
                    };

                    let (line_min_x, line_max_x) = if let Some((seg_x, _)) = segment_info {
                        (
                            source_x.min(target_x).min(seg_x),
                            source_x.max(target_x).max(seg_x),
                        )
                    } else {
                        (source_x.min(target_x), source_x.max(target_x))
                    };

                    let source_border = rank_center + self.sizes[line.0 as usize].y * 0.5_f32;
                    let (target_center, target_band_start) =
                        if let Some((_, seg_last)) = segment_info {
                            let target_rank = seg_last as usize + 1;

                            (
                                self.layer_center(target_rank),
                                self.layer_start(target_rank),
                            )
                        } else {
                            (next_center, next_band_start)
                        };
                    let target_border = target_center - self.sizes[line.1 as usize].y * 0.5_f32;

                    if line_min_x <= max.x
                        && line_max_x >= min.x
                        && source_border <= max.y
                        && target_border >= min.y
                    {
                        let segment = segment_info.map(|(seg_x, seg_last)| {
                            (seg_x, next_band_start, self.layer_ends[seg_last as usize])
                        });

                        callback(LayoutItem::Polyline {
                            from: self.keys[line.0 as usize],
                            to: self.keys[line.1 as usize],
                            points: line_points(
                                Vec2::new(source_x, source_border),
                                source_band_end,
                                segment,
                                Vec2::new(target_x, target_border),
                                target_band_start,
                            ),
                        });
                    }
                }
            }

            next_rank = block_end;
        }

        self.view_nodes(min, max, first, last, callback);
    }
    #[allow(clippy::float_arithmetic, reason = "Coordinate calculation")]
    fn view_nodes(
        &self,
        min: Vec2,
        max: Vec2,
        first: usize,
        last: usize,
        mut callback: impl FnMut(LayoutItem<K, Vec2, ArrayVec<[Vec2; 6]>>),
    ) {
        for (rank, ((start, end), half_width)) in self.real_offsets[first..last.max(first)]
            .iter()
            .copied()
            .zip(self.real_offsets[first + 1..].iter().copied())
            .zip(self.rank_half_width[first..].iter().copied())
            .enumerate()
        {
            let rank = rank + first;
            let (start, end) = (start as usize, end as usize);
            let y = self.layer_center(rank);

            let cutoff = min.x - half_width;
            let begin = start + self.x_coordinates[start..end].partition_point(|&x| x < cutoff);

            for ((x, size), id) in self.x_coordinates[begin..end]
                .iter()
                .copied()
                .zip(self.sizes[begin..end].iter().copied())
                .zip(self.keys[begin..end].iter().copied())
            {
                let center = Vec2::new(x, y);
                let half = size * 0.5_f32;

                if center.x - half.x > max.x {
                    break;
                }

                if center.x + half.x >= min.x
                    && center.y - half.y <= max.y
                    && center.y + half.y >= min.y
                {
                    callback(LayoutItem::Node { id, center, size });
                }
            }
        }
    }
}

#[inline]
fn line_points(
    first: Vec2,
    source_band_end: f32,
    segment: Option<(f32, f32, f32)>,
    last: Vec2,
    target_band_start: f32,
) -> ArrayVec<[Vec2; 6]> {
    let mut points = [Vec2::ZERO; 6];
    let mut len = 1;

    points[0] = first;

    if source_band_end > first.y {
        points[len] = Vec2::new(first.x, source_band_end);
        len += 1;
    }

    if let Some((x, span_start, span_end)) = segment {
        let point = Vec2::new(x, span_start);

        if points[len - 1] != point {
            points[len] = point;
            len += 1;
        }

        let point = Vec2::new(x, span_end);

        if points[len - 1] != point {
            points[len] = point;
            len += 1;
        }
    }

    if target_band_start < last.y {
        let point = Vec2::new(last.x, target_band_start);

        if points[len - 1] != point {
            points[len] = point;
            len += 1;
        }
    }

    if points[len - 1] != last || len == 1 {
        points[len] = last;
        len += 1;
    }

    ArrayVec::from_array_len(points, len)
}

#[inline]
const fn reflect(index: usize, len: usize, forward: bool) -> usize {
    if forward { index } else { len - 1 - index }
}

#[derive(Debug, Clone, Copy)]
enum MedianKind {
    None,
    Single,
    Ordered,
    Fixed,
}

#[derive(Debug, Clone, Copy)]
struct Medians {
    entries: [(u32, u32); 2],
    kind: MedianKind,
}

fn build_medians<'g>(
    guard: &'g ScratchpadGuard<'_>,
    offsets: &[u32],
    flat: &[u32],
    top: &[u32],
) -> ScratchpadVec<'g, Medians> {
    let mut medians = guard.vec_with_capacity(offsets.len().saturating_sub(1));

    medians.extend(
        offsets
            .iter()
            .copied()
            .zip(offsets.iter().copied().skip(1))
            .map(|(base, next)| {
                let degree = next - base;

                if degree == 0 {
                    Medians {
                        entries: [(0, 0); 2],
                        kind: MedianKind::None,
                    }
                } else {
                    let low_position = base + ((degree - 1) >> 1_u32);
                    let high_position = base + (degree >> 1_u32);
                    let low = (flat[low_position as usize], low_position);

                    if low_position == high_position {
                        Medians {
                            entries: [low, low],
                            kind: MedianKind::Single,
                        }
                    } else {
                        let high = (flat[high_position as usize], high_position);
                        let low_segment = top[low.0 as usize] & SEG_BIT != 0;
                        let high_segment = top[high.0 as usize] & SEG_BIT != 0;

                        if low_segment == high_segment {
                            Medians {
                                entries: [low, high],
                                kind: MedianKind::Ordered,
                            }
                        } else if low_segment {
                            Medians {
                                entries: [low, high],
                                kind: MedianKind::Fixed,
                            }
                        } else {
                            Medians {
                                entries: [high, low],
                                kind: MedianKind::Fixed,
                            }
                        }
                    }
                }
            }),
    );

    medians
}

fn ends_from_counts(offsets: &mut [u32]) -> usize {
    offsets.iter_mut().fold(0, |total, offset| {
        *offset += total;
        *offset
    }) as usize
}

#[allow(clippy::unreachable, reason = "Compiler limitation")]
#[inline]
fn bucket<'a, T>(flat: &'a [T], offsets: &[u32], index: usize) -> &'a [T] {
    let [left, right] = offsets[index..=index + 1] else {
        unreachable!()
    };
    &flat[left as usize..right as usize]
}
