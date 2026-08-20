#![allow(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::many_single_char_names,
    reason = "WIP"
)]
#![allow(clippy::as_conversions, reason = "usize::MAX is always >= u32::MAX")]
#![allow(
    clippy::arithmetic_side_effects,
    reason = "Node and edge counts fit in u32"
)]

// TODO: Substantial clean-up work, further optimizations

use core::{
    hash::{BuildHasher, Hash},
    mem,
};

use alloc::vec::Vec;
use glam::Vec2;
use scratchpads::{Scratchpad, ScratchpadVec};

use crate::{
    IndependentContents, LayoutItem, Node, Weave,
    dependent::DependentWeave,
    independent::IndependentWeave,
    layout::{Spacing, slotset::SlotSet, validate_float, validate_vec2},
};

#[derive(Debug, Clone)]
#[must_use]
pub struct Layout2D<K>
where
    K: Hash + Copy + Eq + Ord,
{
    keys: Vec<K>,
    is_segment: Vec<bool>,
    top: Vec<u32>,
    bottom: Vec<u32>,
    real_offsets: Vec<u32>,
    real_flat: Vec<u32>,
    seg_top_offsets: Vec<u32>,
    seg_top_flat: Vec<u32>,
    seg_bottom_offsets: Vec<u32>,
    seg_bottom_flat: Vec<u32>,
    merged_top_offsets: Vec<u32>,
    merged_top_flat: Vec<u32>,
    merged_bottom_offsets: Vec<u32>,
    merged_bottom_flat: Vec<u32>,
    up_offsets: Vec<u32>,
    up_flat: Vec<u32>,
    down_offsets: Vec<u32>,
    down_flat: Vec<u32>,
    height: u32,
    sizes: Vec<Vec2>,
    rank_half_width: Vec<f32>,
    x_coordinates: Vec<f32>,
    layer_bounds: Vec<(f32, f32)>,
    deepest: Vec<u32>,
    size: Vec2,
    polyline_points: Vec<Vec2>,
    polylines: Vec<Vec<Polyline<K>>>,
    polyline_bounds: Vec<(Vec2, Vec2)>,
    polyline_reach: Vec<(f32, f32)>,
    reach_prefix: Vec<f32>,
    rank_built: Vec<bool>,
}

impl<K> Default for Layout2D<K>
where
    K: Hash + Copy + Eq + Ord,
{
    fn default() -> Self {
        Self {
            keys: Vec::new(),
            is_segment: Vec::new(),
            top: Vec::new(),
            bottom: Vec::new(),
            real_offsets: Vec::new(),
            real_flat: Vec::new(),
            seg_top_offsets: Vec::new(),
            seg_top_flat: Vec::new(),
            seg_bottom_offsets: Vec::new(),
            seg_bottom_flat: Vec::new(),
            merged_top_offsets: Vec::new(),
            merged_top_flat: Vec::new(),
            merged_bottom_offsets: Vec::new(),
            merged_bottom_flat: Vec::new(),
            up_offsets: Vec::new(),
            up_flat: Vec::new(),
            down_offsets: Vec::new(),
            down_flat: Vec::new(),
            height: 0,
            sizes: Vec::new(),
            rank_half_width: Vec::new(),
            x_coordinates: Vec::new(),
            layer_bounds: Vec::new(),
            deepest: Vec::new(),
            size: Vec2::ZERO,
            polyline_points: Vec::new(),
            polylines: Vec::new(),
            polyline_bounds: Vec::new(),
            polyline_reach: Vec::new(),
            reach_prefix: Vec::new(),
            rank_built: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct Polyline<K> {
    from: K,
    to: K,
    start: u32,
    end: u32,
    source_x: f32,
    min: Vec2,
    max: Vec2,
}

impl<K> Layout2D<K>
where
    K: Hash + Copy + Eq + Ord,
{
    fn clear(&mut self, reserved_nodes: usize) {
        self.keys.clear();
        self.keys.reserve(reserved_nodes);
        self.is_segment.clear();
        self.is_segment.reserve(reserved_nodes);
        self.top.clear();
        self.top.reserve(reserved_nodes);
        self.bottom.clear();
        self.bottom.reserve(reserved_nodes);

        self.real_offsets.clear();
        self.real_flat.clear();
        self.seg_top_offsets.clear();
        self.seg_top_flat.clear();
        self.seg_bottom_offsets.clear();
        self.seg_bottom_flat.clear();
        self.merged_top_offsets.clear();
        self.merged_top_flat.clear();
        self.merged_bottom_offsets.clear();
        self.merged_bottom_flat.clear();

        self.up_offsets.clear();
        self.up_flat.clear();
        self.down_offsets.clear();
        self.down_flat.clear();

        self.height = 0;

        self.sizes.clear();
        self.sizes.reserve(reserved_nodes);
        self.rank_half_width.clear();

        self.x_coordinates.clear();
        self.layer_bounds.clear();
        self.deepest.clear();
        self.size = Vec2::ZERO;
        self.polyline_points.clear();
        for bucket in &mut self.polylines {
            bucket.clear();
        }
        self.polyline_bounds.clear();
        self.polyline_reach.clear();
        self.reach_prefix.clear();
        self.rank_built.clear();
    }
    fn push_item(&mut self, key: K, segment: bool, top: u32, bottom: u32, size: Vec2) -> u32 {
        assert!(
            self.keys.len() < usize::try_from(u32::MAX).unwrap(),
            "Too many vertices"
        );
        #[allow(clippy::cast_possible_truncation, reason = "Can never overflow")]
        let index = self.keys.len() as u32;

        self.keys.push(key);
        self.is_segment.push(segment);
        self.top.push(top);
        self.bottom.push(bottom);
        self.sizes.push(size);

        index
    }
    fn link(&mut self, from: u32, to: u32) {
        self.up_flat.push(from);
        self.up_flat.push(to);
    }
    fn prepare_structure(&mut self) {
        self.height = self.bottom.iter().copied().max().map_or(0, |rank| rank + 1);

        let height_usize = self.height as usize;

        if height_usize > self.polylines.len() {
            self.polylines.resize_with(height_usize, Vec::new);
        }

        self.rank_built.resize(height_usize, false);
        self.polyline_bounds
            .resize(height_usize, (Vec2::INFINITY, Vec2::NEG_INFINITY));
        self.polyline_reach.resize(height_usize, (0.0_f32, 0.0_f32));

        let count = self.keys.len();
        let ranks = self.height as usize + 1;

        self.real_offsets.resize(ranks, 0);
        self.seg_top_offsets.resize(ranks, 0);
        self.seg_bottom_offsets.resize(ranks, 0);
        self.merged_top_offsets.resize(ranks, 0);
        self.merged_bottom_offsets.resize(ranks, 0);

        for ((segment, top), bottom) in self
            .is_segment
            .iter()
            .copied()
            .zip(self.top.iter().copied())
            .zip(self.bottom.iter().copied())
        {
            let (top, bottom) = (top as usize, bottom as usize);

            if segment {
                self.seg_top_offsets[top] += 1;
                self.seg_bottom_offsets[bottom] += 1;
            } else {
                self.real_offsets[top] += 1;
            }

            self.merged_top_offsets[top] += 1;
            self.merged_bottom_offsets[bottom] += 1;
        }

        let real_total = exclusive_prefix_sum(&mut self.real_offsets);
        let segment_total = exclusive_prefix_sum(&mut self.seg_top_offsets);

        exclusive_prefix_sum(&mut self.seg_bottom_offsets);
        exclusive_prefix_sum(&mut self.merged_top_offsets);
        exclusive_prefix_sum(&mut self.merged_bottom_offsets);

        self.real_flat.resize(real_total, 0);
        self.seg_top_flat.resize(segment_total, 0);
        self.seg_bottom_flat.resize(segment_total, 0);
        self.merged_top_flat.resize(count, 0);
        self.merged_bottom_flat.resize(count, 0);

        for (index, ((segment, top), bottom)) in self
            .is_segment
            .iter()
            .copied()
            .zip(self.top.iter().copied())
            .zip(self.bottom.iter().copied())
            .enumerate()
        {
            #[allow(clippy::cast_possible_truncation, reason = "Can never overflow")]
            let narrowed = index as u32;
            let (top, bottom) = (top as usize, bottom as usize);

            if segment {
                let cursor = self.seg_top_offsets[top];

                self.seg_top_flat[cursor as usize] = narrowed;
                self.seg_top_offsets[top] = cursor + 1;

                let cursor = self.seg_bottom_offsets[bottom];

                self.seg_bottom_flat[cursor as usize] = narrowed;
                self.seg_bottom_offsets[bottom] = cursor + 1;
            } else {
                let cursor = self.real_offsets[top];

                self.real_flat[cursor as usize] = narrowed;
                self.real_offsets[top] = cursor + 1;
            }

            let cursor = self.merged_top_offsets[top];

            self.merged_top_flat[cursor as usize] = narrowed;
            self.merged_top_offsets[top] = cursor + 1;

            let cursor = self.merged_bottom_offsets[bottom];

            self.merged_bottom_flat[cursor as usize] = narrowed;
            self.merged_bottom_offsets[bottom] = cursor + 1;
        }

        self.real_offsets.copy_within(0..height_usize, 1);
        self.real_offsets[0] = 0;
        self.seg_top_offsets.copy_within(0..height_usize, 1);
        self.seg_top_offsets[0] = 0;
        self.seg_bottom_offsets.copy_within(0..height_usize, 1);
        self.seg_bottom_offsets[0] = 0;
        self.merged_top_offsets.copy_within(0..height_usize, 1);
        self.merged_top_offsets[0] = 0;
        self.merged_bottom_offsets.copy_within(0..height_usize, 1);
        self.merged_bottom_offsets[0] = 0;

        let edges = self.up_flat.len() >> 1_usize;

        assert!(edges < usize::try_from(u32::MAX).unwrap(), "Too many edges");

        self.down_offsets.resize(count + 1, 0);
        self.up_offsets.resize(count + 1, 0);

        let (pairs, _) = self.up_flat.as_chunks::<2>();

        for &[source, target] in pairs {
            let (source, target) = (source as usize, target as usize);

            self.down_offsets[source] += 1;
            self.up_offsets[target] += 1;
        }

        let mut down_total = 0_u32;
        let mut up_total = 0_u32;

        for (down_offset, up_offset) in self.down_offsets.iter_mut().zip(&mut self.up_offsets) {
            let down_len = *down_offset;
            let up_len = *up_offset;

            *down_offset = down_total;
            *up_offset = up_total;
            down_total += down_len;
            up_total += up_len;
        }

        self.down_flat.resize(edges, 0);

        let (pairs, _) = self.up_flat.as_chunks::<2>();

        for &[source, target] in pairs {
            let source = source as usize;
            let cursor = self.down_offsets[source];

            self.down_flat[cursor as usize] = target;
            self.down_offsets[source] = cursor + 1;
        }

        self.down_offsets.copy_within(0..count, 1);
        self.down_offsets[0] = 0;

        for source in self.merged_bottom_flat.iter().copied() {
            for target in bucket(&self.down_flat, &self.down_offsets, source as usize)
                .iter()
                .copied()
            {
                let target = target as usize;
                let cursor = self.up_offsets[target];

                self.up_flat[cursor as usize] = source;
                self.up_offsets[target] = cursor + 1;
            }
        }

        self.up_flat.truncate(edges);

        self.up_offsets.copy_within(0..count, 1);
        self.up_offsets[0] = 0;

        #[allow(clippy::cast_possible_truncation, reason = "Can never overflow")]
        self.deepest.extend(0..self.height);

        for ((start, end), deepest) in self
            .real_offsets
            .iter()
            .copied()
            .zip(self.real_offsets.iter().copied().skip(1))
            .zip(&mut self.deepest)
        {
            for source in self.real_flat[start as usize..end as usize].iter().copied() {
                for target in bucket(&self.down_flat, &self.down_offsets, source as usize)
                    .iter()
                    .copied()
                {
                    let target = target as usize;
                    let child = if self.is_segment[target] {
                        self.bottom[target] + 1
                    } else {
                        self.top[target]
                    };

                    *deepest = (*deepest).max(child);
                }
            }
        }
    }
}

impl<K> Layout2D<K>
where
    K: Hash + Copy + Eq + Ord,
{
    pub fn layout_dependent<T, M, S, F>(
        &mut self,
        weave: &mut DependentWeave<K, T, M, S>,
        sizes: F,
        spacing: &Spacing,
    ) where
        S: BuildHasher + Default + Clone,
        F: FnMut(&K) -> Vec2,
    {
        assert!(
            weave.nodes.len() < usize::try_from(u32::MAX).unwrap(),
            "Too many nodes"
        );

        self.clear(weave.nodes.len());

        let guard = weave.scratchpad.guard();

        todo!()
    }
    pub fn layout_independent<T, M, S, F>(
        &mut self,
        weave: &mut IndependentWeave<K, T, M, S>,
        mut sizes: F,
        spacing: &Spacing,
    ) where
        T: IndependentContents,
        S: BuildHasher + Default + Clone,
        F: FnMut(&K) -> Vec2,
    {
        assert!(
            weave.nodes.len() < usize::try_from(u32::MAX).unwrap(),
            "Too many nodes"
        );

        self.clear(weave.nodes.len());

        let mut processed = 0_usize;

        {
            let count = weave.nodes.len();
            let guard = weave.scratchpad.guard();

            let mut identifier_map = guard.map_with_capacity(count, S::default());
            let mut keys: ScratchpadVec<'_, K> = guard.vec_with_capacity(count);
            let mut remaining: ScratchpadVec<'_, u32> = guard.vec_with_capacity(count);
            let mut parent_offsets: ScratchpadVec<'_, u32> = guard.vec_with_capacity(count + 1);
            let mut child_offsets: ScratchpadVec<'_, u32> = guard.vec_with_capacity(count + 1);

            let mut parent_total = 0_u32;
            let mut child_total = 0_u32;

            parent_offsets.push(0);
            child_offsets.push(0);

            #[allow(clippy::cast_possible_truncation, reason = "Can never overflow")]
            for (dense, (&id, node)) in weave.nodes.iter().enumerate() {
                identifier_map.insert(id, dense as u32);
                keys.push(id);
                remaining.push(node.from.len() as u32);

                parent_total = parent_total.strict_add(node.from.len() as u32);
                child_total = child_total.strict_add(node.to.len() as u32);

                parent_offsets.push(parent_total);
                child_offsets.push(child_total);
            }

            let mut parent_flat: ScratchpadVec<'_, u32> =
                guard.vec_with_capacity(parent_total as usize);
            let mut child_flat: ScratchpadVec<'_, u32> =
                guard.vec_with_capacity(child_total as usize);

            for node in weave.nodes.values() {
                parent_flat.extend(node.from.iter().map(|id| identifier_map[id]));
                child_flat.extend(node.to.iter().map(|id| identifier_map[id]));
            }

            let mut vertex_of: ScratchpadVec<'_, u32> = guard.vec_with_capacity(count);
            let mut stack: ScratchpadVec<'_, u32> = guard.vec();
            let mut parents: ScratchpadVec<'_, (u32, u32)> = guard.vec_with_capacity(count);

            vertex_of.resize(count, u32::MAX);

            stack.extend(weave.roots.iter().rev().map(|id| identifier_map[id]));

            while let Some(dense) = stack.pop() {
                let dense = dense as usize;
                let mut rank = 0_u32;

                for parent in bucket(&parent_flat, &parent_offsets, dense).iter().copied() {
                    let index = vertex_of[parent as usize];
                    let top = self.top[index as usize];

                    rank = rank.max(top + 1);

                    parents.push((index, top));
                }

                let id = keys[dense];
                let size = sizes(&id);

                assert!(validate_vec2(size), "Invalid size");

                let index = self.push_item(id, false, rank, rank, size);

                vertex_of[dense] = index;
                processed += 1;

                for (from_index, from_rank) in parents.drain(..) {
                    if from_rank + 1 == rank {
                        self.link(from_index, index);
                    } else {
                        let segment = self.push_item(id, true, from_rank + 1, rank - 1, Vec2::ZERO);

                        self.link(from_index, segment);
                        self.link(segment, index);
                    }
                }

                for child in bucket(&child_flat, &child_offsets, dense)
                    .iter()
                    .rev()
                    .copied()
                {
                    let index = child as usize;

                    remaining[index] -= 1;

                    if remaining[index] == 0 {
                        stack.push(child);
                    }
                }
            }
        }

        debug_assert_eq!(weave.nodes.len(), processed, "Malformed weave");

        self.prepare_structure();
        self.assign_dag_coordinates(&mut weave.scratchpad, spacing);
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
        assert!(
            weave.len() < usize::try_from(u32::MAX).unwrap(),
            "Too many nodes"
        );

        self.clear(weave.len());

        {
            let guard = scratchpad.guard();

            let mut indices = guard.map_with_capacity(weave.len(), S::default());
            let mut parents: ScratchpadVec<'_, (u32, u32)> = guard.vec_with_capacity(weave.len());

            for id in topological.drain(..) {
                let mut rank = 0_u32;

                for parent in weave.get_parents(&id).unwrap() {
                    let index = indices[parent];
                    let top = self.top[index as usize];

                    rank = rank.max(top + 1);

                    parents.push((index, top));
                }

                let size = sizes(&id);

                assert!(validate_vec2(size), "Invalid size");

                let index = self.push_item(id, false, rank, rank, size);

                indices.insert(id, index);

                for (from_index, from_rank) in parents.drain(..) {
                    if from_rank + 1 == rank {
                        self.link(from_index, index);
                    } else {
                        let segment = self.push_item(id, true, from_rank + 1, rank - 1, Vec2::ZERO);

                        self.link(from_index, segment);
                        self.link(segment, index);
                    }
                }
            }

            assert_eq!(weave.len(), indices.len(), "Malformed topological order");
        }

        self.prepare_structure();
        self.assign_dag_coordinates(scratchpad, spacing);
    }
}

struct PassScratch<'a, 'g> {
    marked: &'a [bool],
    marked_up: &'a [bool],
    extent: &'a [(f32, bool)],
    leftmost_at: &'a [u32],
    rightmost_at: &'a [u32],
    left_offsets: &'a [u32],
    left_runs: &'a [(u32, u32, u32)],
    right_offsets: &'a [u32],
    right_runs: &'a [(u32, u32, u32)],
    root: &'a mut [u32],
    align: &'a mut [u32],
    sink: &'a mut [u32],
    shift: &'a mut [f32],
    stack: &'a mut ScratchpadVec<'g, (u32, u32, u32, bool)>,
}

impl<K> Layout2D<K>
where
    K: Hash + Copy + Eq + Ord,
{
    #[allow(clippy::float_arithmetic, reason = "Coordinate calculation")]
    fn assign_dag_coordinates(&mut self, scratchpad: &mut Scratchpad, spacing: &Spacing) {
        const PASSES: [(bool, bool); 4] =
            [(true, true), (true, false), (false, true), (false, false)];

        assert!(spacing.validate(), "Invalid spacing");

        let count = self.keys.len();

        if count == 0 {
            return;
        }

        let height_usize = self.height as usize;

        let mut fourth = mem::take(&mut self.x_coordinates);

        fourth.resize(count, 0.0_f32);
        self.rank_half_width.clear();
        self.rank_half_width.resize(height_usize, 0.0_f32);

        let guard = scratchpad.guard();

        let mut extent = guard.vec_with_capacity(count);
        let mut rank_tallest = guard.vec_with_capacity(height_usize);
        let mut candidates = [
            guard.vec_with_capacity(count),
            guard.vec_with_capacity(count),
            guard.vec_with_capacity(count),
        ];

        extent.resize(count, (0.0_f32, false));
        rank_tallest.resize(height_usize, 0.0_f32);
        for candidate in &mut candidates {
            candidate.resize(count, 0.0_f32);
        }

        for (((segment, size), rank), extent) in self
            .is_segment
            .iter()
            .copied()
            .zip(self.sizes.iter().copied())
            .zip(self.top.iter().copied())
            .zip(extent.iter_mut())
        {
            let rank = rank as usize;

            if segment {
                *extent = (spacing.corridor * 0.5_f32, true);
            } else {
                let half_width = size.x * 0.5_f32;

                *extent = (half_width, false);

                rank_tallest[rank] = rank_tallest[rank].max(size.y);
                self.rank_half_width[rank] = self.rank_half_width[rank].max(half_width);
            }
        }

        let mut marked = guard.vec_with_capacity(self.down_flat.len());
        let mut open_run_start = guard.vec_with_capacity(count);
        let mut leftmost_at = guard.vec_with_capacity(height_usize);
        let mut rightmost_at = guard.vec_with_capacity(height_usize);

        marked.resize(self.down_flat.len(), false);
        open_run_start.resize(count, 0_u32);
        leftmost_at.resize(height_usize, 0_u32);
        rightmost_at.resize(height_usize, 0_u32);

        let mut active = SlotSet::new(&guard);
        let mut spanning = SlotSet::new(&guard);

        active.rebuild(count);
        spanning.rebuild(count);

        let mut closed_runs = guard.vec_with_capacity(count.strict_mul(3));
        let mut any_marked = false;

        #[allow(clippy::cast_possible_truncation, reason = "Can never overflow")]
        for rank in 0..=height_usize {
            if let Some(previous) = rank.checked_sub(1) {
                for item in bucket(
                    &self.merged_bottom_flat,
                    &self.merged_bottom_offsets,
                    previous,
                )
                .iter()
                .copied()
                {
                    let item = item as usize;
                    let before = active.predecessor(item);
                    let after = active.successor(item);

                    active.remove(item);

                    let end = previous as u32;

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

            if rank >= height_usize {
                continue;
            }

            for item in bucket(&self.merged_top_flat, &self.merged_top_offsets, rank)
                .iter()
                .copied()
            {
                let item = item as usize;
                let after = active.successor(item);
                let before = active.predecessor(item);

                if let (Some(left), Some(right), Some(end)) = (before, after, rank.checked_sub(1)) {
                    let start = open_run_start[left];
                    let end = end as u32;

                    if end >= start {
                        closed_runs.push((left as u32, right as u32, start, end));
                    }
                }

                if let Some(left) = before {
                    open_run_start[left] = rank as u32;
                }
                if after.is_some() {
                    open_run_start[item] = rank as u32;
                }

                active.insert(item);
            }

            leftmost_at[rank] = active.first().unwrap() as u32;
            rightmost_at[rank] = active.last().unwrap() as u32;

            if rank + 1 >= height_usize {
                continue;
            }

            for segment in bucket(&self.seg_top_flat, &self.seg_top_offsets, rank)
                .iter()
                .copied()
            {
                spanning.insert(segment as usize);
            }
            for segment in bucket(&self.seg_bottom_flat, &self.seg_bottom_offsets, rank)
                .iter()
                .copied()
            {
                spanning.remove(segment as usize);
            }

            if spanning.is_empty() {
                continue;
            }

            for source in bucket(&self.merged_bottom_flat, &self.merged_bottom_offsets, rank)
                .iter()
                .copied()
            {
                let source = source as usize;
                let base = self.down_offsets[source] as usize;
                let mut before: Option<Option<usize>> = None;
                let mut after: Option<Option<usize>> = None;

                for (offset, target) in bucket(&self.down_flat, &self.down_offsets, source)
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

        let mut marked_up = guard.vec_with_capacity(self.down_flat.len());

        marked_up.resize(self.down_flat.len(), false);

        if any_marked {
            let mut cursors = guard.vec_with_capacity(count);

            cursors.extend(self.up_offsets[..count].iter().copied());

            for source in self.merged_bottom_flat.iter().copied() {
                let source = source as usize;
                let base = self.down_offsets[source] as usize;

                for (offset, target) in bucket(&self.down_flat, &self.down_offsets, source)
                    .iter()
                    .copied()
                    .enumerate()
                {
                    let cursor = cursors[target as usize];

                    marked_up[cursor as usize] = marked[base + offset];
                    cursors[target as usize] = cursor + 1;
                }
            }
        }

        let total_runs = closed_runs.len();

        let mut left_offsets = guard.vec_with_capacity(count + 1);
        let mut right_offsets = guard.vec_with_capacity(count + 1);

        left_offsets.resize(count + 1, 0_u32);
        right_offsets.resize(count + 1, 0_u32);

        for (left, right, _, _) in closed_runs.iter().copied() {
            let after_left = left as usize + 1;
            let after_right = right as usize + 1;

            right_offsets[after_left] += 1;
            left_offsets[after_right] += 1;
        }

        let mut left_total = 0_u32;
        let mut right_total = 0_u32;

        for (left_offset, right_offset) in left_offsets.iter_mut().zip(right_offsets.iter_mut()) {
            left_total += *left_offset;
            right_total += *right_offset;

            *left_offset = left_total;
            *right_offset = right_total;
        }

        let mut left_runs = guard.vec_with_capacity(total_runs);
        let mut right_runs = guard.vec_with_capacity(total_runs);

        left_runs.resize(total_runs, (0_u32, 0_u32, 0_u32));
        right_runs.resize(total_runs, (0_u32, 0_u32, 0_u32));

        for (left, right, start, end) in closed_runs.iter().copied() {
            let cursor = right_offsets[left as usize];

            right_runs[cursor as usize] = (right, start, end);
            right_offsets[left as usize] = cursor + 1;

            let cursor = left_offsets[right as usize];

            left_runs[cursor as usize] = (left, start, end);
            left_offsets[right as usize] = cursor + 1;
        }

        left_offsets.copy_within(0..count, 1);
        left_offsets[0] = 0;
        right_offsets.copy_within(0..count, 1);
        right_offsets[0] = 0;

        let mut root = guard.vec_with_capacity(count);
        let mut align = guard.vec_with_capacity(count);
        let mut sink = guard.vec_with_capacity(count);
        let mut shift = guard.vec_with_capacity(count);

        root.resize(count, 0_u32);
        align.resize(count, 0_u32);
        sink.resize(count, 0_u32);
        shift.resize(count, 0.0_f32);

        let mut stack = guard.vec();

        let mut scratch = PassScratch {
            marked: &marked,
            marked_up: &marked_up,
            extent: &extent,
            leftmost_at: &leftmost_at,
            rightmost_at: &rightmost_at,
            left_offsets: &left_offsets,
            left_runs: &left_runs,
            right_offsets: &right_offsets,
            right_runs: &right_runs,
            root: &mut root,
            align: &mut align,
            sink: &mut sink,
            shift: &mut shift,
            stack: &mut stack,
        };

        let [first, second, third] = &mut candidates;

        let extents = [
            self.coordinate_pass::<true, true>(&mut scratch, spacing, first),
            self.coordinate_pass::<true, false>(&mut scratch, spacing, second),
            self.coordinate_pass::<false, true>(&mut scratch, spacing, third),
            self.coordinate_pass::<false, false>(&mut scratch, spacing, &mut fourth),
        ];

        let mut best = 0_usize;

        for pass in 1..4_usize {
            if extents[pass].1 - extents[pass].0 < extents[best].1 - extents[best].0 {
                best = pass;
            }
        }

        let mut offsets = [0.0_f32; 4];

        for (pass, (_, leftward)) in PASSES.into_iter().enumerate() {
            offsets[pass] = if leftward {
                extents[best].0 - extents[pass].0
            } else {
                extents[best].1 - extents[pass].1
            };
        }

        let mut left = f32::INFINITY;
        let mut right = f32::NEG_INFINITY;

        let [first, second, third] = &candidates;

        for ((((coordinate, a), b), c), (extent, _)) in fourth
            .iter_mut()
            .zip(first.iter().copied())
            .zip(second.iter().copied())
            .zip(third.iter().copied())
            .zip(extent.iter().copied())
        {
            let (a, b, c, d) = (
                a + offsets[0],
                b + offsets[1],
                c + offsets[2],
                *coordinate + offsets[3],
            );

            let low = a.min(b).max(c.min(d));
            let high = a.max(b).min(c.max(d));
            let combined = (low + high) * 0.5_f32;

            *coordinate = combined;
            left = left.min(combined - extent);
            right = right.max(combined + extent);
        }

        self.x_coordinates = fourth;

        let mut valid = true;

        self.layer_bounds.clear();
        self.layer_bounds.resize(height_usize, (0.0_f32, 0.0_f32));

        let mut cursor = 0.0_f32;

        for (rank, (&tallest, layer_bounds)) in
            rank_tallest.iter().zip(&mut self.layer_bounds).enumerate()
        {
            let start = if rank == 0 {
                0.0_f32
            } else {
                cursor + spacing.layer
            };
            let end = start + tallest;

            *layer_bounds = (start, end);

            valid &= validate_float(start) && validate_float(end);

            cursor = end;
        }

        for coordinate in &mut self.x_coordinates {
            *coordinate -= left;
            valid &= validate_float(*coordinate);
        }

        self.reach_prefix.clear();
        self.reach_prefix.reserve(height_usize);

        let mut reach = 0.0_f32;

        for deep in self.deepest.iter().copied() {
            reach = reach.max(self.layer_bounds[deep as usize].1);
            self.reach_prefix.push(reach);
        }

        self.size = Vec2::new(right - left, cursor);

        valid &= validate_vec2(self.size);

        assert!(valid, "Output is not normal and positive");
    }
    #[allow(
        clippy::float_arithmetic,
        clippy::integer_division,
        clippy::integer_division_remainder_used,
        reason = "Coordinate calculation"
    )]
    #[allow(clippy::cast_possible_truncation, reason = "Can never overflow")]
    fn coordinate_pass<const DOWNWARD: bool, const LEFTWARD: bool>(
        &self,
        scratch: &mut PassScratch<'_, '_>,
        spacing: &Spacing,
        x: &mut [f32],
    ) -> (f32, f32) {
        let PassScratch {
            marked,
            marked_up,
            extent,
            leftmost_at,
            rightmost_at,
            left_offsets,
            left_runs,
            right_offsets,
            right_runs,
            root,
            align,
            sink,
            shift,
            stack,
        } = scratch;

        let height = self.height as usize;

        let separation = |a: usize, b: usize| {
            let (extent_a, segment_a) = extent[a];
            let (extent_b, segment_b) = extent[b];

            extent_a
                + extent_b
                + if segment_a || segment_b {
                    spacing.edge
                } else {
                    spacing.node
                }
        };

        let (layer_flat, layer_offsets) = if DOWNWARD {
            (&self.merged_top_flat, &self.merged_top_offsets)
        } else {
            (&self.merged_bottom_flat, &self.merged_bottom_offsets)
        };
        let neighbour_offsets = if DOWNWARD {
            &self.up_offsets
        } else {
            &self.down_offsets
        };
        let neighbour_at = |vertex: usize, index: usize| -> (u32, usize) {
            let position = neighbour_offsets[vertex] as usize + index;
            let neighbour = if DOWNWARD {
                self.up_flat[position]
            } else {
                self.down_flat[position]
            };

            (neighbour, position)
        };
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

        let runs_of = |vertex: usize| bucket(runs_flat, runs_offsets, vertex);
        let across_runs_of = |vertex: usize| bucket(across_flat, across_offsets, vertex);

        for (vertex, ((((root, align), sink), shift), x)) in root
            .iter_mut()
            .zip(align.iter_mut())
            .zip(sink.iter_mut())
            .zip(shift.iter_mut())
            .zip(x.iter_mut())
            .enumerate()
        {
            let vertex = vertex as u32;

            *root = vertex;
            *align = vertex;
            *sink = vertex;
            *shift = f32::INFINITY;
            *x = f32::NAN;
        }

        for step in 0..height {
            let rank = reflect(step, height, DOWNWARD);
            let layer = bucket(layer_flat, layer_offsets, rank);

            let mut last: Option<usize> = None;
            let mut process = |vertex: usize| {
                let degree = (neighbour_offsets[vertex + 1] - neighbour_offsets[vertex]) as usize;

                if degree == 0 {
                    return;
                }

                let mut medians = [
                    reflect((degree - 1) / 2, degree, LEFTWARD),
                    reflect(degree / 2, degree, LEFTWARD),
                ];

                let is_segment = |median: usize| extent[neighbour_at(vertex, median).0 as usize].1;

                let distinct = if medians[0] == medians[1] {
                    1
                } else {
                    if !is_segment(medians[0]) && is_segment(medians[1]) {
                        medians.swap(0, 1);
                    }

                    2
                };

                for median in medians[..distinct].iter().copied() {
                    if align[vertex] as usize != vertex {
                        continue;
                    }

                    let (neighbour, position) = neighbour_at(vertex, median);
                    let neighbour = neighbour as usize;

                    let admissible = last.is_none_or(|last| {
                        if LEFTWARD {
                            last < neighbour
                        } else {
                            last > neighbour
                        }
                    });

                    if !edge_marked[position] && admissible {
                        align[neighbour] = vertex as u32;
                        root[vertex] = root[neighbour];
                        align[vertex] = root[vertex];

                        last = Some(neighbour);
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

        stack.clear();

        let mut place = |start: usize| {
            if root[start] as usize != start || !x[start].is_nan() {
                return;
            }

            let narrowed = start as u32;

            stack.push((narrowed, narrowed, 0, false));

            while let Some((root_val, member, runs_applied, started)) = stack.last().copied() {
                let (root_index, member_index) = (root_val as usize, member as usize);

                if !started {
                    x[root_index] = 0.0_f32;
                    stack.last_mut().unwrap().3 = true;
                }

                let runs = runs_of(member_index);

                let mut applied = runs_applied as usize;
                let mut nested = false;

                while applied < runs.len() {
                    let run = reflect(applied, runs.len(), DOWNWARD);
                    let (neighbour, _, _) = runs[run];
                    let neighbour_root = root[neighbour as usize];

                    if x[neighbour_root as usize].is_nan() {
                        stack.last_mut().unwrap().2 = applied as u32;
                        stack.push((neighbour_root, neighbour_root, 0, false));

                        nested = true;
                        break;
                    }

                    if sink[root_index] == root_val {
                        sink[root_index] = sink[neighbour_root as usize];
                    }

                    if sink[root_index] == sink[neighbour_root as usize] {
                        x[root_index] = x[root_index].max(
                            x[neighbour_root as usize]
                                + separation(neighbour as usize, member_index),
                        );
                    }

                    applied += 1;
                }

                if nested {
                    continue;
                }

                let next = align[member_index];

                if next == root_val {
                    let mut member = root_index;

                    while align[member] != root_val {
                        member = align[member] as usize;

                        x[member] = x[root_index];
                        sink[member] = sink[root_index];
                    }

                    stack.pop();
                } else {
                    let frame = stack.last_mut().unwrap();

                    frame.1 = next;
                    frame.2 = 0;
                }
            }
        };

        for (layer_start, layer_end) in self
            .merged_top_offsets
            .iter()
            .copied()
            .zip(self.merged_top_offsets.iter().copied().skip(1))
        {
            let layer = &self.merged_top_flat[layer_start as usize..layer_end as usize];

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

        for step in 0..height {
            let rank = reflect(step, height, DOWNWARD);

            let entry = if LEFTWARD {
                leftmost_at[rank]
            } else {
                rightmost_at[rank]
            };
            let entry = entry as usize;

            if sink[entry] as usize != entry {
                continue;
            }

            let entry_rank = if DOWNWARD {
                self.top[entry]
            } else {
                self.bottom[entry]
            } as usize;

            if rank != entry_rank {
                continue;
            }

            if !shift[entry].is_finite() {
                shift[entry] = 0.0_f32;
            }

            let mut vertex = entry;
            let mut from = rank;

            loop {
                for (neighbour, start, end) in runs_of(vertex).iter().copied() {
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
                        shift[sink[vertex] as usize] + x[vertex]
                            - (x[neighbour] + separation(neighbour, vertex)),
                    );
                }

                while align[vertex] != root[vertex] {
                    vertex = align[vertex] as usize;

                    for (neighbour, _, _) in runs_of(vertex).iter().copied() {
                        let neighbour = neighbour as usize;
                        let neighbour_sink = sink[neighbour] as usize;

                        shift[neighbour_sink] = shift[neighbour_sink].min(
                            shift[sink[vertex] as usize] + x[vertex]
                                - (x[neighbour] + separation(neighbour, vertex)),
                        );
                    }
                }

                let across = if DOWNWARD {
                    self.bottom[vertex]
                } else {
                    self.top[vertex]
                } as usize;

                let runs = across_runs_of(vertex);
                let next = if DOWNWARD {
                    runs.last().and_then(|&(neighbour, _, end)| {
                        (end as usize == across).then_some(neighbour)
                    })
                } else {
                    runs.first().and_then(|&(neighbour, start, _)| {
                        (start as usize == across).then_some(neighbour)
                    })
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

        for ((x, sink), (extent, _)) in x
            .iter_mut()
            .zip(sink.iter().copied())
            .zip(extent.iter().copied())
        {
            *x += shift[sink as usize];

            if !LEFTWARD {
                *x = -*x;
            }

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
    fn layer_center(&self, rank: usize) -> f32 {
        let (start, end) = self.layer_bounds[rank];

        (start + end) * 0.5_f32
    }
    #[allow(clippy::float_arithmetic, reason = "Coordinate calculation")]
    fn build_polylines(&mut self, min: Vec2, max: Vec2) {
        let first = self.reach_prefix.partition_point(|&reach| reach < min.y);
        let last = self
            .layer_bounds
            .partition_point(|&(start, _)| start <= max.y);

        for rank in first..last {
            if self.rank_built[rank] || self.layer_bounds[self.deepest[rank] as usize].1 < min.y {
                continue;
            }

            self.rank_built[rank] = true;

            let mut rank_min = Vec2::INFINITY;
            let mut rank_max = Vec2::NEG_INFINITY;
            let mut left_reach = 0.0_f32;
            let mut right_reach = 0.0_f32;

            for source in bucket(&self.real_flat, &self.real_offsets, rank)
                .iter()
                .copied()
            {
                let source = source as usize;
                let from = self.keys[source];

                let source_x = self.x_coordinates[source];
                let source_border = self.layer_center(rank) + self.sizes[source].y * 0.5_f32;
                let source_band_end = self.layer_bounds[rank].1;

                for target in bucket(&self.down_flat, &self.down_offsets, source)
                    .iter()
                    .copied()
                {
                    let target = target as usize;
                    let to = self.keys[target];
                    let real_target = if self.is_segment[target] {
                        self.down_flat[self.down_offsets[target] as usize] as usize
                    } else {
                        target
                    };

                    let target_x = self.x_coordinates[real_target];
                    let target_border = self.layer_center(self.top[real_target] as usize)
                        - self.sizes[real_target].y * 0.5_f32;

                    let start = self.polyline_points.len();
                    let first = Vec2::new(source_x, source_border);
                    let mut line_min = first;
                    let mut line_max = first;

                    self.polyline_points.push(first);

                    if source_band_end > source_border {
                        let point = Vec2::new(source_x, source_band_end);

                        line_min = line_min.min(point);
                        line_max = line_max.max(point);
                        self.polyline_points.push(point);
                    }

                    if self.is_segment[target] {
                        let x = self.x_coordinates[target];

                        push_deduplicated(
                            &mut self.polyline_points,
                            Vec2::new(x, self.layer_bounds[self.top[target] as usize].0),
                            &mut line_min,
                            &mut line_max,
                        );
                        push_deduplicated(
                            &mut self.polyline_points,
                            Vec2::new(x, self.layer_bounds[self.bottom[target] as usize].1),
                            &mut line_min,
                            &mut line_max,
                        );
                    }

                    let target_band_start = self.layer_bounds[self.top[real_target] as usize].0;

                    if target_band_start < target_border {
                        push_deduplicated(
                            &mut self.polyline_points,
                            Vec2::new(target_x, target_band_start),
                            &mut line_min,
                            &mut line_max,
                        );
                    }

                    push_deduplicated(
                        &mut self.polyline_points,
                        Vec2::new(target_x, target_border),
                        &mut line_min,
                        &mut line_max,
                    );

                    if self.polyline_points.len() == start + 1 {
                        self.polyline_points
                            .push(Vec2::new(target_x, target_border));
                    }

                    rank_min = rank_min.min(line_min);
                    rank_max = rank_max.max(line_max);
                    left_reach = left_reach.max(source_x - line_min.x);
                    right_reach = right_reach.max(line_max.x - source_x);

                    #[allow(clippy::cast_possible_truncation, reason = "Can never overflow")]
                    self.polylines[rank].push(Polyline {
                        from,
                        to,
                        start: start as u32,
                        end: self.polyline_points.len() as u32,
                        source_x,
                        min: line_min,
                        max: line_max,
                    });
                }
            }

            self.polyline_bounds[rank] = (rank_min, rank_max);
            self.polyline_reach[rank] = (left_reach, right_reach);
        }
    }
    #[must_use]
    pub const fn size(&self) -> Vec2 {
        self.size
    }
    #[allow(clippy::float_arithmetic, reason = "Coordinate calculation")]
    pub fn view<'a, F>(&'a mut self, min: Vec2, max: Vec2, mut callback: F)
    where
        F: FnMut(LayoutItem<'a, K, Vec2>),
    {
        if !self.down_flat.is_empty() {
            self.build_polylines(min, max);
        }

        let first = self.layer_bounds.partition_point(|&(_, end)| end < min.y);
        let first_reaching = self.reach_prefix.partition_point(|&reach| reach < min.y);
        let last = self
            .layer_bounds
            .partition_point(|&(start, _)| start <= max.y);

        for ((lines, (rank_min, rank_max)), (left_reach, right_reach)) in self
            .polylines
            .iter()
            .zip(self.polyline_bounds.iter().copied())
            .zip(self.polyline_reach.iter().copied())
            .skip(first_reaching)
            .take(last.saturating_sub(first_reaching))
        {
            if !(rank_min.x <= max.x
                && rank_max.x >= min.x
                && rank_min.y <= max.y
                && rank_max.y >= min.y)
            {
                continue;
            }

            let begin = lines.partition_point(|line| line.source_x < min.x - right_reach);

            for line in &lines[begin..] {
                if line.source_x - left_reach > max.x {
                    break;
                }

                if line.min.x <= max.x
                    && line.max.x >= min.x
                    && line.min.y <= max.y
                    && line.max.y >= min.y
                {
                    callback(LayoutItem::Polyline {
                        from: line.from,
                        to: line.to,
                        points: &self.polyline_points[line.start as usize..line.end as usize],
                    });
                }
            }
        }

        for (((start, end), half_width), (band_start, band_end)) in self
            .real_offsets
            .iter()
            .copied()
            .zip(self.real_offsets.iter().copied().skip(1))
            .zip(self.rank_half_width.iter().copied())
            .zip(self.layer_bounds.iter().copied())
            .skip(first)
            .take(last.saturating_sub(first))
        {
            let reals = &self.real_flat[start as usize..end as usize];
            let y = (band_start + band_end) * 0.5_f32;

            let cutoff = min.x - half_width;
            let begin =
                reals.partition_point(|&vertex| self.x_coordinates[vertex as usize] < cutoff);

            for vertex in reals[begin..].iter().copied() {
                let vertex = vertex as usize;
                let id = self.keys[vertex];

                let center = Vec2::new(self.x_coordinates[vertex], y);
                let size = self.sizes[vertex];
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
const fn reflect(index: usize, len: usize, forward: bool) -> usize {
    if forward { index } else { len - 1 - index }
}

fn push_deduplicated(points: &mut Vec<Vec2>, point: Vec2, min: &mut Vec2, max: &mut Vec2) {
    *min = min.min(point);
    *max = max.max(point);

    if points.last().is_none_or(|&last| last != point) {
        points.push(point);
    }
}

fn exclusive_prefix_sum(offsets: &mut [u32]) -> usize {
    let mut total = 0_u32;

    for offset in offsets {
        let len = *offset;

        *offset = total;
        total += len;
    }

    total as usize
}

#[inline]
fn bucket<'a, T>(flat: &'a [T], offsets: &[u32], index: usize) -> &'a [T] {
    &flat[offsets[index] as usize..offsets[index + 1] as usize]
}
