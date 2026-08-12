use std::hash::{BuildHasher, RandomState};

use hashbrown::HashMap;
use indexmap::IndexSet;
use old_universal_weave::{
    ActiveSingularWeave as _, BookmarkableWeave as _,
    DiscreteContentResult as OldDiscreteContentResult, DiscreteContents as OldDiscreteContents,
    DiscreteWeave as _, IndependentContents as OldIndependentContents, MetadataWeave as _,
    SemiIndependentWeave as _, SortableBookmarkableWeave as _, SortableWeave as _, Weave as _,
    dependent::{DependentNode as OldDependentNode, DependentWeave as OldDependentWeave},
};
use proptest::{prelude::*, strategy::Strategy, test_runner::Config};
use proptest_derive::Arbitrary;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};
use universal_weave::{
    ActiveSingularWeave as _, BookmarkableWeave as _, DiscreteContentResult, DiscreteContents,
    DiscreteWeave as _, IndependentContents, MetadataWeave as _, SemiIndependentWeave as _,
    SortableBookmarkableWeave as _, SortableWeave as _, Weave as _,
    dependent::{DependentNode, DependentWeave},
};

const CASES: u32 = 5120;
const MAX_TRANSITIONS: usize = 512;

prop_state_machine! {
    #![proptest_config(Config {
        cases: CASES,
        //verbose: 1,
        max_shrink_time: MAX_TRANSITIONS as u32 * 2000,
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

struct WeaveStateMachine;

impl ReferenceStateMachine for WeaveStateMachine {
    type State = Vec<Self::Transition>;
    type Transition = (WeaveTransition, u32, u8);

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(Vec::with_capacity(MAX_TRANSITIONS)).boxed()
    }
    fn transitions(_state: &Self::State) -> BoxedStrategy<Self::Transition> {
        any::<Self::Transition>().boxed()
    }
    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        state.push(transition.clone());
        state
    }
}

#[derive(Arbitrary, Debug, Clone)]
enum WeaveTransition {
    #[proptest(weight = 8)]
    Insert {
        from_seed: Option<u32>,
        active: bool,
        bookmarked: bool,
        content_seed: u32,
        length: u32,
    },
    #[proptest(weight = 6)]
    SetActive {
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
    n_weave: DependentWeave<u32, WeaveContent, u32, RandomState>,
    o_weave: OldDependentWeave<u32, WeaveContent, u32, RandomState>,
    counter: u32,
    n_ordered_node_identifiers: Vec<u32>,
    n_ordered_node_identifiers_from: Vec<u32>,
    n_active_path: Vec<u32>,
    n_path_from: Vec<u32>,
    o_ordered_node_identifiers: Vec<u32>,
    o_ordered_node_identifiers_from: Vec<u32>,
    o_active_path: Vec<u32>,
    o_path_from: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WeaveContent {
    length: u32,
    content_seed: u32,
}

impl IndependentContents for WeaveContent {}

impl OldIndependentContents for WeaveContent {}

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

impl OldDiscreteContents for WeaveContent {
    fn split(self, at: usize) -> OldDiscreteContentResult<Self> {
        if at == 0 || at as u64 >= self.length as u64 {
            OldDiscreteContentResult::One(self)
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

            OldDiscreteContentResult::Two(left, right)
        }
    }
    fn merge(self, value: Self) -> OldDiscreteContentResult<Self> {
        if self.content_seed == value.content_seed && !self.length.overflowing_add(value.length).1 {
            OldDiscreteContentResult::One(Self {
                length: self.length.saturating_add(value.length),
                content_seed: self.content_seed,
            })
        } else {
            OldDiscreteContentResult::Two(self, value)
        }
    }
}

// Invariants are validated by the function's contracts
impl StateMachineTest for WeaveWrapper {
    type SystemUnderTest = Self;
    type Reference = WeaveStateMachine;

    fn init_test(
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        WeaveWrapper {
            n_weave: DependentWeave::with_capacity(ref_state.len(), ref_state.len() as u32),
            o_weave: OldDependentWeave::with_capacity(ref_state.len(), ref_state.len() as u32),
            counter: 0,
            n_ordered_node_identifiers: Vec::with_capacity(ref_state.len()),
            n_ordered_node_identifiers_from: Vec::with_capacity(ref_state.len()),
            n_active_path: Vec::with_capacity(ref_state.len()),
            n_path_from: Vec::with_capacity(ref_state.len()),
            o_ordered_node_identifiers: Vec::with_capacity(ref_state.len()),
            o_ordered_node_identifiers_from: Vec::with_capacity(ref_state.len()),
            o_active_path: Vec::with_capacity(ref_state.len()),
            o_path_from: Vec::with_capacity(ref_state.len()),
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
        let old_node_count = state.n_weave.nodes().len();
        let target = map_id(transition.1);

        match transition.0 {
            WeaveTransition::Insert {
                from_seed,
                active,
                bookmarked,
                length,
                content_seed,
            } => {
                assert_eq!(
                    state.n_weave.insert(DependentNode {
                        id: state.counter,
                        from: from_seed.map(map_id),
                        to: IndexSet::default(),
                        active,
                        bookmarked,
                        contents: WeaveContent {
                            length: length % 64,
                            content_seed: content_seed % 4,
                        },
                    }),
                    state.o_weave.insert(OldDependentNode {
                        id: state.counter,
                        from: from_seed.map(map_id),
                        to: IndexSet::default(),
                        active,
                        bookmarked,
                        contents: WeaveContent {
                            length: length % 64,
                            content_seed: content_seed % 4,
                        },
                    })
                );
            }
            WeaveTransition::SetActive { id_seed, value } => {
                assert_eq!(
                    state.n_weave.set_active(&map_id(id_seed), value),
                    state.o_weave.set_active(&map_id(id_seed), value),
                );
            }
            WeaveTransition::SetBookmarked { id_seed, value } => {
                assert_eq!(
                    state.n_weave.set_bookmarked(&map_id(id_seed), value),
                    state.o_weave.set_bookmarked(&map_id(id_seed), value)
                );
            }
            WeaveTransition::Remove { id_seed } => {
                assert_eq!(
                    state.n_weave.remove(&map_id(id_seed)).map(|node| node.id),
                    state.o_weave.remove(&map_id(id_seed)).map(|node| node.id)
                );
            }
            WeaveTransition::RemoveTracked { id_seed } => {
                assert_eq!(
                    state.n_weave.remove_tracked(&map_id(id_seed), |_r| {}),
                    state.o_weave.remove_tracked(&map_id(id_seed), |_r| {})
                );
            }
            WeaveTransition::Clear { apply_seed } => {
                if apply_seed == 0 {
                    state.n_weave.clear();
                    state.o_weave.clear();
                }
            }
            WeaveTransition::MetadataMut { content_seed } => {
                state.n_weave.metadata_mut(|m| *m = content_seed);
                state.o_weave.metadata_mut(|m| *m = content_seed);
            }
            WeaveTransition::SortChildrenBy { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                assert_eq!(
                    state.n_weave.sort_children_by(&map_id(id_seed), |a, b| {
                        hash_value(a.id as u64 + sort_seed)
                            .cmp(&hash_value(b.id as u64 + sort_seed))
                    }),
                    state.o_weave.sort_children_by(&map_id(id_seed), |a, b| {
                        hash_value(a.id as u64 + sort_seed)
                            .cmp(&hash_value(b.id as u64 + sort_seed))
                    })
                );
            }
            WeaveTransition::SortChildrenById { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                assert_eq!(
                    state.n_weave.sort_children_by_id(&map_id(id_seed), |a, b| {
                        hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                    }),
                    state.o_weave.sort_children_by_id(&map_id(id_seed), |a, b| {
                        hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                    })
                );
            }
            WeaveTransition::SortRootsBy { sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.n_weave.sort_roots_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
                state.o_weave.sort_roots_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortRootsById { sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.n_weave.sort_roots_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
                state.o_weave.sort_roots_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
            }
            WeaveTransition::SortBookmarksBy { sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.n_weave.sort_bookmarks_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
                state.o_weave.sort_bookmarks_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortBookmarksById { sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.n_weave.sort_bookmarks_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
                state.o_weave.sort_bookmarks_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
            }
            WeaveTransition::GetContentsMut {
                id_seed,
                content_seed,
            } => {
                assert_eq!(
                    state
                        .n_weave
                        .get_contents_mut(&map_id(id_seed), |c| c.content_seed = content_seed % 4),
                    state
                        .o_weave
                        .get_contents_mut(&map_id(id_seed), |c| c.content_seed = content_seed % 4)
                )
            }
            WeaveTransition::Split { id_seed, at_seed } => {
                assert_eq!(
                    state.n_weave.split(
                        &map_id(id_seed),
                        state
                            .n_weave
                            .get(&map_id(id_seed))
                            .map(|node| {
                                (at_seed
                                    .checked_rem(node.contents.length)
                                    .unwrap_or_default()) as usize
                            })
                            .unwrap_or_default(),
                        state.counter,
                    ),
                    state.o_weave.split(
                        &map_id(id_seed),
                        state
                            .o_weave
                            .get(&map_id(id_seed))
                            .map(|node| {
                                (at_seed
                                    .checked_rem(node.contents.length)
                                    .unwrap_or_default()) as usize
                            })
                            .unwrap_or_default(),
                        state.counter,
                    )
                );
            }
            WeaveTransition::MergeWithParent { id_seed } => {
                assert_eq!(
                    state.n_weave.merge_with_parent(&map_id(id_seed)),
                    state.o_weave.merge_with_parent(&map_id(id_seed))
                );
            }
        }
        assert_eq!(
            state.n_weave.nodes(),
            &HashMap::from_iter(state.o_weave.nodes().iter().map(|(id, node)| (
                *id,
                DependentNode {
                    id: node.id,
                    from: node.from,
                    to: node.to.clone(),
                    active: node.active,
                    bookmarked: node.bookmarked,
                    contents: node.contents.clone()
                }
            )))
        );
        assert_eq!(state.n_weave.roots().len(), state.o_weave.roots().len());
        assert!(
            state
                .n_weave
                .roots()
                .iter()
                .zip(state.o_weave.roots())
                .all(|(a, b)| a == b)
        );
        assert_eq!(state.n_weave.metadata(), state.o_weave.metadata());
        assert_eq!(
            state.n_weave.bookmarks().len(),
            state.o_weave.bookmarks().len()
        );
        assert!(
            state
                .n_weave
                .bookmarks()
                .iter()
                .zip(state.o_weave.bookmarks())
                .all(|(a, b)| a == b)
        );
        assert_eq!(state.n_weave.active(), state.o_weave.active());
        if state.n_weave.nodes().len() > old_node_count {
            state.counter += 1;
        }

        if transition.2.is_multiple_of(4) {
            state
                .n_weave
                .get_ordered_identifiers(&mut state.n_ordered_node_identifiers);
            state
                .o_weave
                .get_ordered_identifiers(&mut state.o_ordered_node_identifiers);
            state
                .n_weave
                .get_ordered_identifiers_from(&target, &mut state.n_ordered_node_identifiers_from);
            state
                .o_weave
                .get_ordered_identifiers_from(&target, &mut state.o_ordered_node_identifiers_from);
            state.n_weave.get_active_path(&mut state.n_active_path);
            state.o_weave.get_active_path(&mut state.o_active_path);
            state.n_weave.get_path_from(&target, &mut state.n_path_from);
            state.o_weave.get_path_from(&target, &mut state.o_path_from);

            assert_eq!(
                state.n_ordered_node_identifiers,
                state.o_ordered_node_identifiers
            );
            assert_eq!(
                state.n_ordered_node_identifiers_from,
                state.o_ordered_node_identifiers_from
            );
            assert_eq!(state.n_active_path, state.o_active_path);
            assert_eq!(state.n_path_from, state.o_path_from);
        }

        state
    }
    fn check_invariants(
        _state: &Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
    }
}
