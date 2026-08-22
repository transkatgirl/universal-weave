use std::{
    fmt::Debug,
    hash::{BuildHasher, Hash, RandomState},
};

use glam::Vec2;
use hashbrown::{HashMap, HashSet};
use rust_sugiyama_fork::{
    PairSeparation,
    algorithm::{Edge, Vertex, p3_calculate_coordinates},
    petgraph::stable_graph::{NodeIndex, StableDiGraph},
};

use indexmap::IndexSet;
use proptest::{collection::size_range, prelude::*, strategy::Strategy, test_runner::Config};
use proptest_derive::Arbitrary;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};
use scratchpads::Scratchpad;
use universal_weave::{
    ActivePathWeave, BookmarkableWeave, DiscreteContentResult, DiscreteContents, DiscreteWeave,
    IndependentContents, IndependentWeave as IndependentWeaveTrait, LayoutItem, Layouter,
    MetadataWeave, Node, SemiIndependentWeave, SortableBookmarkableWeave, SortableWeave, Weave,
    dependent::DependentWeave,
    independent::{IndependentNode, IndependentWeave},
    layout::{Spacing, TopologicalLayouter},
};

const CASES: u32 = 4096;
const MAX_TRANSITIONS: usize = 512;
const TOLERANCE: f32 = 1e-4;

prop_state_machine! {
    #![proptest_config(Config {
        cases: CASES,
        //verbose: 1,
        max_shrink_time: MAX_TRANSITIONS as u32 * 4000,
        max_shrink_iters: u32::MAX-1,
        //timeout: 1000,
        .. Config::default()
    })]

    #[test]
    fn run_state_machine(
        sequential
        1..MAX_TRANSITIONS
        =>
        WeaveWrapper
    );
}

struct ReferenceLayouter<K>
where
    K: Hash + Copy + Eq + Ord,
{
    spacing: Spacing,
    size: Vec2,
    nodes: Vec<(K, Vec2, Vec2)>,
    polylines: Vec<(K, K, Vec2, Vec2, Vec<Vec2>)>,
}

impl<K> ReferenceLayouter<K>
where
    K: Hash + Copy + Eq + Ord,
{
    fn new(spacing: Spacing) -> Self {
        Self {
            spacing,
            size: Vec2::ZERO,
            nodes: Vec::new(),
            polylines: Vec::new(),
        }
    }
}

struct GraphBuilder<K> {
    vertices: Vec<VertexInfo<K>>,
    layers: Vec<Vec<NodeIndex>>,
    graph: StableDiGraph<Vertex, Edge>,
}

struct VertexInfo<K> {
    id: Option<K>,
    rank: usize,
    next: Vec<usize>,
}

impl<K> GraphBuilder<K> {
    fn new() -> Self {
        Self {
            vertices: Vec::new(),
            layers: Vec::new(),
            graph: StableDiGraph::new(),
        }
    }
    fn push(&mut self, id: Option<K>, size: Vec2, rank: usize) -> usize {
        let mut node = Vertex::new(self.vertices.len(), (size.x as f64, size.y as f64));
        node.is_dummy = id.is_none();

        let index = self.graph.add_node(node);

        assert_eq!(node.id, index.index());

        if rank == self.layers.len() {
            self.layers.push(Vec::new());
        }

        self.layers[rank].push(index);
        self.vertices.push(VertexInfo {
            id,
            rank,
            next: Vec::new(),
        });

        index.index()
    }
    fn link(&mut self, from: usize, to: usize) {
        assert_eq!(self.vertices[from].rank + 1, self.vertices[to].rank);

        self.vertices[from].next.push(to);
        self.graph
            .add_edge(NodeIndex::new(from), NodeIndex::new(to), Edge::default());
    }
}

impl<W, K, N, T> Layouter<W, K, N, T, Vec2> for ReferenceLayouter<K>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
{
    fn layout(&mut self, weave: &mut W, mut sizes: impl FnMut(&K) -> Vec2) {
        assert!(self.spacing.validate());
        self.size = Vec2::ZERO;
        self.nodes.clear();
        self.polylines.clear();

        let mut order = Vec::with_capacity(weave.len());

        weave.get_ordered_identifiers(&mut order);

        assert_eq!(weave.len(), order.len());

        if order.is_empty() {
            return;
        }

        let mut builder = GraphBuilder::<K>::new();
        let mut indices: HashMap<K, usize> = HashMap::with_capacity(order.len());

        for id in order {
            let parents = weave.get_parents(&id).unwrap();

            let rank = parents
                .into_iter()
                .map(|id| builder.vertices[indices[id]].rank)
                .max()
                .map_or(0, |r| r + 1);

            let size = sizes(&id);

            assert!(
                matches!(
                    size.x.classify(),
                    core::num::FpCategory::Normal | core::num::FpCategory::Zero
                ) && size.x.is_sign_positive()
                    && matches!(
                        size.y.classify(),
                        core::num::FpCategory::Normal | core::num::FpCategory::Zero
                    )
                    && size.y.is_sign_positive()
            );

            let index = builder.push(Some(id), size, rank);

            assert!(indices.insert(id, index).is_none());

            for mut from in parents.into_iter().map(|id| indices[id]) {
                for dummy_rank in builder.vertices[from].rank + 1..rank {
                    let dummy =
                        builder.push(None, Vec2::new(self.spacing.corridor, 0.0), dummy_rank);

                    builder.link(from, dummy);
                    from = dummy;
                }

                builder.link(from, index);
            }
        }

        let mut layouts = p3_calculate_coordinates::create_layouts(
            &mut builder.graph,
            &mut builder.layers,
            Some(PairSeparation {
                vertex_gap: self.spacing.node as f64,
                edge_gap: self.spacing.edge as f64,
            }),
        );

        p3_calculate_coordinates::align_to_smallest_width_layout(&builder.graph, &mut layouts);

        let mut x = vec![0.0_f64; builder.vertices.len()];

        for (index, coordinate) in p3_calculate_coordinates::calculate_relative_coords(layouts) {
            x[index.index()] = coordinate;
        }

        let (left, right) = x.iter().copied().zip(builder.graph.node_weights()).fold(
            (f64::INFINITY, f64::NEG_INFINITY),
            |(left, right), (coordinate, vertex)| {
                (
                    left.min(coordinate - vertex.size.0 * 0.5),
                    right.max(coordinate + vertex.size.0 * 0.5),
                )
            },
        );

        let x: Vec<f32> = x
            .into_iter()
            .map(|coordinate| (coordinate - left) as f32)
            .collect();

        let mut bands = Vec::with_capacity(builder.layers.len());

        for members in &builder.layers {
            let start = bands
                .last()
                .map_or(0.0, |&(_, end)| end + self.spacing.layer as f64);

            bands.push((
                start,
                start
                    + members
                        .iter()
                        .copied()
                        .map(|index| builder.graph[index].size.1)
                        .fold(0.0_f64, f64::max),
            ));
        }

        self.size = Vec2::new((right - left) as f32, bands.last().unwrap().1 as f32);

        let center = |rank: usize| (bands[rank].0 + bands[rank].1) * 0.5;

        for (rank, members) in builder.layers.into_iter().enumerate() {
            let rank_center = center(rank);
            let rank_band_end = bands[rank].1;

            for source in members.into_iter().map(|index| index.index()) {
                let Some(source_id) = builder.vertices[source].id else {
                    continue;
                };

                let source_x = x[source];
                let source_size = builder.graph[NodeIndex::new(source)].size;

                self.nodes.push((
                    source_id,
                    Vec2::new(source_x, rank_center as f32),
                    Vec2::new(source_size.0 as f32, source_size.1 as f32),
                ));

                for target in builder.vertices[source].next.iter().copied() {
                    let mut final_target = target;

                    let target_key = loop {
                        match builder.vertices[final_target].id {
                            Some(key) => break key,
                            None => final_target = builder.vertices[final_target].next[0],
                        }
                    };

                    let target_rank = builder.vertices[final_target].rank;
                    let target_x = x[final_target];
                    let target_size = builder.graph[NodeIndex::new(final_target)].size;

                    let mut points = vec![
                        Vec2::new(source_x, (rank_center + source_size.1 * 0.5) as f32),
                        Vec2::new(source_x, rank_band_end as f32),
                    ];

                    if final_target != target {
                        let segment_x = x[target];

                        points.push(Vec2::new(segment_x, bands[rank + 1].0 as f32));
                        points.push(Vec2::new(segment_x, bands[target_rank - 1].1 as f32));
                    }

                    points.push(Vec2::new(target_x, bands[target_rank].0 as f32));
                    points.push(Vec2::new(
                        target_x,
                        (center(target_rank) - target_size.1 * 0.5) as f32,
                    ));

                    let (min, max) = points.iter().fold(
                        (Vec2::INFINITY, Vec2::NEG_INFINITY),
                        |(low, high), &point| (low.min(point), high.max(point)),
                    );

                    self.polylines
                        .push((source_id, target_key, min, max, points));
                }
            }
        }
    }
    fn size(&self) -> Vec2 {
        self.size
    }
    fn view<'a>(
        &'a mut self,
        min: Vec2,
        max: Vec2,
        mut callback: impl FnMut(LayoutItem<'a, K, Vec2>),
    ) {
        for (from, to, line_min, line_max, points) in &self.polylines {
            if line_min.x <= max.x
                && line_max.x >= min.x
                && line_min.y <= max.y
                && line_max.y >= min.y
            {
                callback(LayoutItem::Polyline {
                    from: *from,
                    to: *to,
                    points,
                })
            }
        }

        for (id, center, size) in self.nodes.iter().copied() {
            let half = size * 0.5;

            if center.x + half.x >= min.x
                && center.x - half.x <= max.x
                && center.y - half.y <= max.y
                && center.y + half.y >= min.y
            {
                callback(LayoutItem::Node { id, center, size });
            }
        }
    }
}

struct WeaveStateMachine;

impl ReferenceStateMachine for WeaveStateMachine {
    type State = (Vec<Self::Transition>, Spacing);
    type Transition = (WeaveTransition, u32, u8, (u32, u32), (u32, u32));

    fn init_state() -> BoxedStrategy<Self::State> {
        any::<(u8, u8, u8, u8)>()
            .prop_map(|values| {
                (
                    Vec::with_capacity(MAX_TRANSITIONS),
                    Spacing {
                        node: values.0 as f32 / 64.0,
                        layer: values.1 as f32 / 64.0,
                        corridor: values.2 as f32 / 64.0,
                        edge: values.3 as f32 / 64.0,
                    },
                )
            })
            .boxed()
    }
    fn transitions(_state: &Self::State) -> BoxedStrategy<Self::Transition> {
        any::<Self::Transition>().boxed()
    }
    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        state.0.push(transition.clone());
        state
    }
}

#[derive(Arbitrary, Debug, Clone)]
enum WeaveTransition {
    #[proptest(weight = 8)]
    Insert {
        #[proptest(strategy = "any_with::<Vec<u32>>((size_range(0..=3), ()))")]
        from_seeds: Vec<u32>,
        active: bool,
        bookmarked: bool,
        content_seed: u32,
        length: u32,
    },
    #[proptest(weight = 4)]
    InsertWithChildren {
        filter_cycles: bool,
        #[proptest(strategy = "any_with::<Vec<u32>>((size_range(0..=3), ()))")]
        to_seeds: Vec<u32>,
        #[proptest(strategy = "any_with::<Vec<u32>>((size_range(0..=3), ()))")]
        from_seeds: Vec<u32>,
        active: bool,
        bookmarked: bool,
        content_seed: u32,
        length: u32,
    },
    #[proptest(weight = 7)]
    SetActive {
        value: bool,
        id_seed: u32,
    },
    SetActiveDependentSemantics {
        value: bool,
        id_seed: u32,
    },
    SetBookmarked {
        value: bool,
        id_seed: u32,
    },
    #[proptest(weight = 3)]
    Remove {
        id_seed: u32,
    },
    #[proptest(weight = 3)]
    RemoveTracked {
        id_seed: u32,
    },
    Clear {
        apply_seed: u16,
    },
    MetadataMut {
        content_seed: u32,
    },
    SortChildrenBy {
        id_seed: u32,
        sort_seed: u32,
    },
    SortChildrenById {
        id_seed: u32,
        sort_seed: u32,
    },
    SortRootsBy {
        sort_seed: u32,
    },
    SortRootsById {
        sort_seed: u32,
    },
    SortBookmarksBy {
        sort_seed: u32,
    },
    SortBookmarksById {
        sort_seed: u32,
    },
    SetActivePath {
        #[proptest(strategy = "any_with::<Vec<u32>>((size_range(0..=16), ()))")]
        id_seeds: Vec<u32>,
    },
    #[proptest(weight = 3)]
    MoveTo {
        filter_cycles: bool,
        #[proptest(strategy = "any_with::<Vec<u32>>((size_range(0..=3), ()))")]
        new_parents_seeds: Vec<u32>,
        id_seed: u32,
    },
    GetContentsMut {
        id_seed: u32,
        content_seed: u32,
    },
    #[proptest(weight = 3)]
    Split {
        at_seed: u32,
        id_seed: u32,
    },
    #[proptest(weight = 3)]
    MergeWithParent {
        id_seed: u32,
    },
}

struct WeaveWrapper {
    weave: IndependentWeave<u32, WeaveContent, u32, RandomState>,
    sizes: HashMap<u32, Vec2>,
    layouter: TopologicalLayouter<u32, RandomState>,
    reference_layouter: ReferenceLayouter<u32>,
    counter: u32,
    scratchpad: Vec<u32>,
    scratchpad_set: HashSet<u32>,
    scratchpad_arena: Scratchpad,
    ordered_node_identifiers: Vec<u32>,
    ordered_node_identifiers_from: Vec<u32>,
    active_path: Vec<u32>,
    path_from: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WeaveContent {
    length: u32,
    content_seed: u32,
}

impl WeaveContent {
    fn size(&self) -> Vec2 {
        Vec2 {
            x: (self.content_seed as f32 / u32::MAX as f32) * 16.0,
            y: (self.length as f32 / u32::MAX as f32) * 16.0,
        }
    }
}

impl IndependentContents for WeaveContent {}

impl DiscreteContents for WeaveContent {
    fn split(self, at: usize) -> DiscreteContentResult<Self> {
        if at == 0 || at as u64 >= self.length as u64 {
            DiscreteContentResult::One(self)
        } else {
            let left = WeaveContent {
                length: at as u32,
                content_seed: self.content_seed,
            };
            let right = WeaveContent {
                length: self.length.saturating_sub(at as u32),
                content_seed: self.content_seed,
            };
            assert_eq!(left.length.saturating_add(right.length), self.length);
            assert_ne!(left.length, 0);
            assert_ne!(right.length, 0);

            DiscreteContentResult::Two(left, right)
        }
    }
    fn merge(self, value: Self) -> DiscreteContentResult<Self> {
        if self.content_seed == value.content_seed && !self.length.overflowing_add(value.length).1 {
            DiscreteContentResult::One(Self {
                length: self.length.saturating_add(value.length),
                content_seed: self.content_seed,
            })
        } else {
            DiscreteContentResult::Two(self, value)
        }
    }
}

impl StateMachineTest for WeaveWrapper {
    type SystemUnderTest = Self;
    type Reference = WeaveStateMachine;

    fn init_test(
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        WeaveWrapper {
            weave: IndependentWeave::with_capacity(ref_state.0.len(), ref_state.0.len() as u32),
            sizes: HashMap::with_capacity(ref_state.0.len()),
            layouter: TopologicalLayouter::new(ref_state.1),
            reference_layouter: ReferenceLayouter::new(ref_state.1),
            counter: 0,
            scratchpad: Vec::with_capacity(ref_state.0.len()),
            scratchpad_set: HashSet::with_capacity(ref_state.0.len()),
            scratchpad_arena: Scratchpad::new(),
            ordered_node_identifiers: Vec::with_capacity(ref_state.0.len()),
            ordered_node_identifiers_from: Vec::with_capacity(ref_state.0.len()),
            active_path: Vec::with_capacity(ref_state.0.len()),
            path_from: Vec::with_capacity(ref_state.0.len()),
        }
    }
    fn apply(
        mut state: Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        let s = RandomState::default();
        let hash_value = |value: u64| s.hash_one(value);
        let map_id = |seed: u32| seed % (state.counter + 2);
        let old_node_count = state.weave.nodes().len();
        let target = map_id(transition.1);

        match transition.0 {
            WeaveTransition::Insert {
                from_seeds,
                active,
                bookmarked,
                length,
                content_seed,
            } => {
                let node = IndependentNode {
                    id: state.counter,
                    from: IndexSet::from_iter(from_seeds.into_iter().map(&map_id)),
                    to: IndexSet::default(),
                    active,
                    bookmarked,
                    contents: WeaveContent {
                        length: length % 64,
                        content_seed: content_seed % 4,
                    },
                };
                state.weave.insert(node);
            }
            WeaveTransition::InsertWithChildren {
                filter_cycles,
                from_seeds,
                to_seeds,
                active,
                bookmarked,
                length,
                content_seed,
            } => {
                let mut node = IndependentNode {
                    id: state.counter,
                    from: from_seeds.into_iter().map(&map_id).collect(),
                    to: to_seeds.into_iter().map(&map_id).collect(),
                    active,
                    bookmarked,
                    contents: WeaveContent {
                        length: length % 64,
                        content_seed: content_seed % 4,
                    },
                };
                if filter_cycles {
                    state.scratchpad.clear();
                    state.scratchpad_set.clear();

                    for parent in node.from.iter().copied() {
                        ancestor_subgraph(
                            state.weave.nodes(),
                            parent,
                            &mut state.scratchpad,
                            &mut state.scratchpad_set,
                        );
                    }

                    state.scratchpad.clear();
                    state.scratchpad.extend(
                        node.to
                            .drain(..)
                            .filter(|id| !state.scratchpad_set.contains(id)),
                    );
                    node.to.extend(state.scratchpad.drain(..));
                }

                state.weave.insert(node);
            }
            WeaveTransition::SetActive { id_seed, value } => {
                state.weave.set_active(&map_id(id_seed), value);
            }
            WeaveTransition::SetActiveDependentSemantics { id_seed, value } => {
                state
                    .weave
                    .set_active_dependent_semantics(&map_id(id_seed), value);
            }
            WeaveTransition::SetBookmarked { id_seed, value } => {
                state.weave.set_bookmarked(&map_id(id_seed), value);
            }
            WeaveTransition::Remove { id_seed } => {
                state.weave.remove(&map_id(id_seed));
            }
            WeaveTransition::RemoveTracked { id_seed } => {
                state.weave.remove_tracked(&map_id(id_seed), |_r| {});
            }
            WeaveTransition::Clear { apply_seed } => {
                if apply_seed == 0 {
                    state.weave.clear();
                }
            }
            WeaveTransition::MetadataMut { content_seed } => {
                state.weave.metadata_mut(|m| *m = content_seed);
            }
            WeaveTransition::SortChildrenBy { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.weave.sort_children_by(&map_id(id_seed), |a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortChildrenById { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.weave.sort_children_by_id(&map_id(id_seed), |a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
            }
            WeaveTransition::SortRootsBy { sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.weave.sort_roots_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortRootsById { sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.weave.sort_roots_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
            }
            WeaveTransition::SortBookmarksBy { sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.weave.sort_bookmarks_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortBookmarksById { sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.weave.sort_bookmarks_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
            }
            WeaveTransition::SetActivePath { id_seeds } => {
                let active: Vec<u32> = id_seeds.into_iter().map(&map_id).collect();

                state.weave.set_active_path(active.into_iter());
            }
            WeaveTransition::MoveTo {
                filter_cycles,
                id_seed,
                new_parents_seeds,
            } => {
                let id = map_id(id_seed);
                let mut new_parents: Vec<u32> =
                    new_parents_seeds.into_iter().map(&map_id).collect();
                if filter_cycles && let Some(node) = state.weave.get(&id) {
                    state.scratchpad.clear();
                    state.scratchpad_set.clear();

                    for child in node.to().iter().copied() {
                        descendant_subgraph(
                            state.weave.nodes(),
                            child,
                            &mut state.scratchpad,
                            &mut state.scratchpad_set,
                        );
                    }

                    state.scratchpad.clear();
                    state.scratchpad.extend(
                        new_parents
                            .drain(..)
                            .filter(|id| !state.scratchpad_set.contains(id)),
                    );
                    new_parents.append(&mut state.scratchpad);
                }

                state.weave.move_to(&id, &new_parents);
            }
            WeaveTransition::GetContentsMut {
                id_seed,
                content_seed,
            } => {
                let _ = state
                    .weave
                    .get_contents_mut(&map_id(id_seed), |c| c.content_seed = content_seed % 4);
            }
            WeaveTransition::Split { id_seed, at_seed } => {
                let split_at = state
                    .weave
                    .get(&map_id(id_seed))
                    .map(|node| {
                        (at_seed
                            .checked_rem(node.contents.length)
                            .unwrap_or_default()) as usize
                    })
                    .unwrap_or_default();
                state.weave.split(&map_id(id_seed), split_at, state.counter);
            }
            WeaveTransition::MergeWithParent { id_seed } => {
                state.weave.merge_with_parent(&map_id(id_seed));
            }
        }
        if state.weave.nodes().len() > old_node_count {
            state.counter += 1;
        }

        if transition.2.is_multiple_of(4) {
            if let Ok(converted) = DependentWeave::try_from(state.weave.clone()) {
                assert_eq!(state.weave, IndependentWeave::from(converted));
            }

            state
                .weave
                .get_ordered_identifiers(&mut state.ordered_node_identifiers);
            state
                .weave
                .get_ordered_identifiers_from(&target, &mut state.ordered_node_identifiers_from);
            state.weave.get_active_path(&mut state.active_path);
            state.weave.get_path_from(&target, &mut state.path_from);
        }

        state.sizes.clear();
        state.sizes.extend(
            state
                .weave
                .nodes()
                .into_iter()
                .map(|(k, v)| (*k, v.contents.size())),
        );

        state
            .layouter
            .layout(&mut state.weave, |id| state.sizes[id]);
        state
            .reference_layouter
            .layout(&mut state.weave, |id| state.sizes[id]);

        assert!(
            (Layouter::<
                IndependentWeave<u32, WeaveContent, u32, RandomState>,
                u32,
                IndependentNode<u32, WeaveContent, RandomState>,
                WeaveContent,
                Vec2,
            >::size(&state.layouter)
                - Layouter::<
                    IndependentWeave<u32, WeaveContent, u32, RandomState>,
                    u32,
                    IndependentNode<u32, WeaveContent, RandomState>,
                    WeaveContent,
                    Vec2,
                >::size(&state.reference_layouter))
            .abs()
            .max_element()
                <= TOLERANCE
        );

        compare_layouter_views::<
            IndependentWeave<u32, WeaveContent, u32, RandomState>,
            u32,
            IndependentNode<u32, WeaveContent, RandomState>,
            WeaveContent,
        >(
            &mut state.scratchpad_arena,
            &mut state.layouter,
            &mut state.reference_layouter,
            Vec2::splat(-1.0e30),
            Vec2::splat(1.0e30),
        );

        let subview_min = Vec2 {
            x: ((transition.3.0 as f32 / u32::MAX as f32) - 0.5)
                * 2.0
                * (MAX_TRANSITIONS * 20) as f32
                * 3.0,
            y: ((transition.3.1 as f32 / u32::MAX as f32) - 0.5)
                * 2.0
                * (MAX_TRANSITIONS * 20) as f32
                * 3.0,
        };
        let subview_max = subview_min
            + Vec2 {
                x: ((transition.4.0 as f32 / u32::MAX as f32) - 0.5)
                    * 2.0
                    * (MAX_TRANSITIONS * 20) as f32,
                y: ((transition.4.1 as f32 / u32::MAX as f32) - 0.5)
                    * 2.0
                    * (MAX_TRANSITIONS * 20) as f32,
            };

        compare_layouter_views::<
            IndependentWeave<u32, WeaveContent, u32, RandomState>,
            u32,
            IndependentNode<u32, WeaveContent, RandomState>,
            WeaveContent,
        >(
            &mut state.scratchpad_arena,
            &mut state.layouter,
            &mut state.reference_layouter,
            subview_min,
            subview_max,
        );

        state
    }
    fn check_invariants(
        _state: &Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
    }
}

fn compare_layouter_views<W, K, N, T>(
    scratchpad: &mut Scratchpad,
    left: &mut impl Layouter<W, K, N, T, Vec2>,
    right: &mut impl Layouter<W, K, N, T, Vec2>,
    min: Vec2,
    max: Vec2,
) where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord + Debug,
    N: Node<K, T>,
{
    let lock = scratchpad.guard();

    let mut layouter_output = lock.vec();
    let mut reference_layouter_output = lock.vec();

    left.view(min, max, |item| {
        layouter_output.push(item);
    });
    right.view(min, max, |item| {
        reference_layouter_output.push(item);
    });

    assert_eq!(layouter_output.len(), reference_layouter_output.len());

    for (left, right) in layouter_output.into_iter().zip(reference_layouter_output) {
        match (left, right) {
            (
                LayoutItem::Node { id, center, size },
                LayoutItem::Node {
                    id: right_id,
                    center: right_center,
                    size: right_size,
                },
            ) => {
                assert_eq!(id, right_id);
                assert!((center - right_center).abs().max_element() <= TOLERANCE);
                assert!((size - right_size).abs().max_element() <= TOLERANCE);
            }
            (
                LayoutItem::Polyline { from, to, points },
                LayoutItem::Polyline {
                    from: right_from,
                    to: right_to,
                    points: right_points,
                },
            ) => {
                assert_eq!(from, right_from);
                assert_eq!(to, right_to);
                assert!(points.len() >= 2 && right_points.len() >= 2);
                assert!(
                    (points.first().unwrap() - right_points.first().unwrap())
                        .abs()
                        .max_element()
                        <= TOLERANCE
                );
                assert!(
                    (points.last().unwrap() - right_points.last().unwrap())
                        .abs()
                        .max_element()
                        <= TOLERANCE
                );
                assert!(
                    points
                        .windows(2)
                        .all(|window| { window[1].y >= window[0].y })
                );
                assert!(
                    right_points
                        .windows(2)
                        .all(|window| { window[1].y >= window[0].y })
                );
                for point in points {
                    assert!(
                        right_points
                            .iter()
                            .any(|p| (point - *p).abs().max_element() <= TOLERANCE)
                    )
                }
                for point in right_points {
                    assert!(
                        points
                            .iter()
                            .any(|p| (point - *p).abs().max_element() <= TOLERANCE)
                    )
                }
            }
            _ => panic!(),
        }
    }
}

// Copied from src/lib.rs
fn ancestor_subgraph<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    id: K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut HashSet<K>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T>,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push(id);

    while let Some(id) = scratchpad.pop() {
        if identifiers.insert(id)
            && let Some(node) = nodes.get(&id)
        {
            scratchpad.extend(node.from().into_iter().rev().copied());
        }
    }
}

// Copied from src/lib.rs
fn descendant_subgraph<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    id: K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut HashSet<K>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T>,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push(id);

    while let Some(id) = scratchpad.pop() {
        if identifiers.insert(id)
            && let Some(node) = nodes.get(&id)
        {
            scratchpad.extend(node.to().into_iter().rev().copied());
        }
    }
}
