use std::hash::{BuildHasher, RandomState};

use indexmap::IndexSet;
use proptest::{prelude::*, strategy::Strategy, test_runner::Config};
use proptest_derive::Arbitrary;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};
use universal_weave::{
    BookmarkableWeave, DiscreteContentResult, DiscreteContents, DiscreteWeave, IndependentContents,
    MetadataWeave, SemiIndependentWeave, SortableBookmarkableWeave, SortableWeave, Weave,
    dependent::{DependentNode, DependentWeave},
    independent::{IndependentNode, IndependentWeave},
};

const CASES: u32 = 2048;
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
    d_weave: DependentWeave<u32, WeaveContent, u32, RandomState>,
    i_weave: IndependentWeave<u32, WeaveContent, u32, RandomState>,
    counter: u32,
    d_ordered_node_identifiers: Vec<u32>,
    d_ordered_node_identifiers_from: Vec<u32>,
    d_active_path: Vec<u32>,
    d_path_from: Vec<u32>,
    i_ordered_node_identifiers: Vec<u32>,
    i_ordered_node_identifiers_from: Vec<u32>,
    i_active_path: Vec<u32>,
    i_path_from: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WeaveContent {
    length: u32,
    content_seed: u32,
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
            d_weave: DependentWeave::with_capacity(ref_state.len(), ref_state.len() as u32),
            i_weave: IndependentWeave::with_capacity(ref_state.len(), ref_state.len() as u32),
            counter: 0,
            d_ordered_node_identifiers: Vec::with_capacity(ref_state.len()),
            d_ordered_node_identifiers_from: Vec::with_capacity(ref_state.len()),
            d_active_path: Vec::with_capacity(ref_state.len()),
            d_path_from: Vec::with_capacity(ref_state.len()),
            i_ordered_node_identifiers: Vec::with_capacity(ref_state.len()),
            i_ordered_node_identifiers_from: Vec::with_capacity(ref_state.len()),
            i_active_path: Vec::with_capacity(ref_state.len()),
            i_path_from: Vec::with_capacity(ref_state.len()),
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
        let old_node_count = state.d_weave.nodes().len();
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
                    state.d_weave.insert(DependentNode {
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
                    state.i_weave.insert(IndependentNode {
                        id: state.counter,
                        from: IndexSet::from_iter(from_seed.into_iter().map(&map_id)),
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
                    state.d_weave.set_active(&map_id(id_seed), value),
                    state
                        .i_weave
                        .set_active_dependent_semantics(&map_id(id_seed), value)
                );
            }
            WeaveTransition::SetBookmarked { id_seed, value } => {
                assert_eq!(
                    state.d_weave.set_bookmarked(&map_id(id_seed), value),
                    state.i_weave.set_bookmarked(&map_id(id_seed), value)
                );
            }
            WeaveTransition::Remove { id_seed } => {
                assert_eq!(
                    state.d_weave.remove(&map_id(id_seed)).map(|node| node.id),
                    state.i_weave.remove(&map_id(id_seed)).map(|node| node.id)
                );
            }
            WeaveTransition::RemoveTracked { id_seed } => {
                assert_eq!(
                    state.d_weave.remove_tracked(&map_id(id_seed), |_r| {}),
                    state.i_weave.remove_tracked(&map_id(id_seed), |_r| {})
                );
            }
            WeaveTransition::Clear { apply_seed } => {
                if apply_seed == 0 {
                    state.d_weave.clear();
                    state.i_weave.clear();
                }
            }
            WeaveTransition::MetadataMut { content_seed } => {
                state.d_weave.metadata_mut(|m| *m = content_seed);
                state.i_weave.metadata_mut(|m| *m = content_seed);
            }
            WeaveTransition::SortChildrenBy { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                assert_eq!(
                    state.d_weave.sort_children_by(&map_id(id_seed), |a, b| {
                        hash_value(a.id as u64 + sort_seed)
                            .cmp(&hash_value(b.id as u64 + sort_seed))
                    }),
                    state.i_weave.sort_children_by(&map_id(id_seed), |a, b| {
                        hash_value(a.id as u64 + sort_seed)
                            .cmp(&hash_value(b.id as u64 + sort_seed))
                    })
                );
            }
            WeaveTransition::SortChildrenById { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                assert_eq!(
                    state.d_weave.sort_children_by_id(&map_id(id_seed), |a, b| {
                        hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                    }),
                    state.i_weave.sort_children_by_id(&map_id(id_seed), |a, b| {
                        hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                    })
                );
            }
            WeaveTransition::SortRootsBy { sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.d_weave.sort_roots_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
                state.i_weave.sort_roots_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortRootsById { sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.d_weave.sort_roots_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
                state.i_weave.sort_roots_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
            }
            WeaveTransition::SortBookmarksBy { sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.d_weave.sort_bookmarks_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
                state.i_weave.sort_bookmarks_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortBookmarksById { sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.d_weave.sort_bookmarks_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
                state.i_weave.sort_bookmarks_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
            }
            WeaveTransition::GetContentsMut {
                id_seed,
                content_seed,
            } => {
                assert_eq!(
                    state
                        .d_weave
                        .get_contents_mut(&map_id(id_seed), |c| c.content_seed = content_seed % 4),
                    state
                        .i_weave
                        .get_contents_mut(&map_id(id_seed), |c| c.content_seed = content_seed % 4)
                )
            }
            WeaveTransition::Split { id_seed, at_seed } => {
                assert_eq!(
                    state.d_weave.split(
                        &map_id(id_seed),
                        state
                            .d_weave
                            .get(&map_id(id_seed))
                            .map(|node| {
                                (at_seed
                                    .checked_rem(node.contents.length)
                                    .unwrap_or_default()) as usize
                            })
                            .unwrap_or_default(),
                        state.counter,
                    ),
                    state.i_weave.split(
                        &map_id(id_seed),
                        state
                            .i_weave
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
                    state.d_weave.merge_with_parent(&map_id(id_seed)),
                    state.i_weave.merge_with_parent(&map_id(id_seed))
                );
            }
        }
        let converted_d_weave = IndependentWeave::from(state.d_weave.clone());
        let converted_i_weave = DependentWeave::try_from(state.i_weave.clone()).unwrap();
        assert_eq!(converted_d_weave, state.i_weave);
        assert_eq!(state.d_weave, converted_i_weave);
        if state.d_weave.nodes().len() > old_node_count {
            state.counter += 1;
        }

        if transition.2.is_multiple_of(4) {
            state
                .d_weave
                .get_ordered_identifiers(&mut state.d_ordered_node_identifiers);
            state
                .i_weave
                .get_ordered_identifiers(&mut state.i_ordered_node_identifiers);
            state
                .d_weave
                .get_ordered_identifiers_from(&target, &mut state.d_ordered_node_identifiers_from);
            state
                .i_weave
                .get_ordered_identifiers_from(&target, &mut state.i_ordered_node_identifiers_from);
            state.d_weave.get_active_path(&mut state.d_active_path);
            state.i_weave.get_active_path(&mut state.i_active_path);
            state.d_weave.get_path_from(&target, &mut state.d_path_from);
            state.i_weave.get_path_from(&target, &mut state.i_path_from);

            assert_eq!(
                state.d_ordered_node_identifiers,
                state.i_ordered_node_identifiers
            );
            assert_eq!(
                state.d_ordered_node_identifiers_from,
                state.i_ordered_node_identifiers_from
            );
            assert_eq!(state.d_active_path, state.i_active_path);
            assert_eq!(state.d_path_from, state.i_path_from);
        }

        state
    }
    fn check_invariants(
        _state: &Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
    }
}

/*
#[test]
fn transition_set() {
    let items = vec![];

    let mut state = WeaveWrapper {
        weave: DependentWeave::with_capacity(items.len(), items.len() as u32),
        counter: 0,
        scratchpad: Vec::with_capacity(items.len()),
    };
    for item in items {
        state = WeaveWrapper::apply(state, &vec![], item);
    }
}
*/
