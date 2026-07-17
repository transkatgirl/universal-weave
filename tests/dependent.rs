use std::hash::{BuildHasher, RandomState};

use indexmap::IndexSet;
use proptest::{prelude::*, strategy::Strategy, test_runner::Config};
use proptest_derive::Arbitrary;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};
use universal_weave::{
    DiscreteContentResult, DiscreteContents, DiscreteWeave, IndependentContents, MetadataWeave,
    SemiIndependentWeave, SortableWeave, Weave,
    dependent::{DependentNode, DependentWeave},
};

const CASES: u32 = 16384;
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
    type State = Vec<WeaveTransition>;
    type Transition = WeaveTransition;

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
    GetOrderedNodeIdentifiers {
        reversed: bool,
    },
    GetOrderedNodeIdentifiersFrom {
        reversed: bool,
        id_seed: u32,
    },
    GetActiveThread,
    GetThreadFrom {
        id_seed: u32,
    },
    #[proptest(weight = 8)]
    AddNode {
        from_seed: Option<u32>,
        active: bool,
        bookmarked: bool,
        content_seed: u32,
        length: u32,
    },
    #[proptest(weight = 3)]
    SetNodeActiveStatus {
        alternate: bool,
        value: bool,
        id_seed: u32,
    },
    #[proptest(weight = 3)]
    SetNodeActiveStatusInPlace {
        value: bool,
        id_seed: u32,
    },
    SetNodeBookmarkedStatus {
        value: bool,
        id_seed: u32,
    },
    #[proptest(weight = 3)]
    RemoveNode {
        id_seed: u32,
    },
    #[proptest(weight = 3)]
    RemoveNodeTracked {
        id_seed: u32,
    },
    RemoveAllNodes {
        apply_seed: u16,
    },
    MetadataMut {
        content_seed: u32,
    },
    SortNodeChildrenBy {
        id_seed: u32,
        sort_seed: u32,
    },
    SortNodeChildrenById {
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
    SplitNode {
        at_seed: u32,
        id_seed: u32,
    },
    #[proptest(weight = 3)]
    MergeNodeWithParent {
        id_seed: u32,
    },
}

struct WeaveWrapper {
    weave: DependentWeave<u32, WeaveContent, u32, RandomState>,
    counter: u32,
    scratchpad: Vec<u32>,
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

// Invariants are validated by the function's contracts
impl StateMachineTest for WeaveWrapper {
    type SystemUnderTest = Self;
    type Reference = WeaveStateMachine;

    fn init_test(
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        WeaveWrapper {
            weave: DependentWeave::with_capacity(ref_state.len(), ref_state.len() as u32),
            counter: 0,
            scratchpad: Vec::with_capacity(ref_state.len()),
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

        match transition {
            WeaveTransition::GetOrderedNodeIdentifiers { reversed } => {
                if reversed {
                    state
                        .weave
                        .get_ordered_node_identifiers_reversed_children(&mut state.scratchpad);
                } else {
                    state
                        .weave
                        .get_ordered_node_identifiers(&mut state.scratchpad);
                }
            }
            WeaveTransition::GetOrderedNodeIdentifiersFrom { id_seed, reversed } => {
                if reversed {
                    state
                        .weave
                        .get_ordered_node_identifiers_from_reversed_children(
                            &map_id(id_seed),
                            &mut state.scratchpad,
                        );
                } else {
                    state
                        .weave
                        .get_ordered_node_identifiers_from(&map_id(id_seed), &mut state.scratchpad);
                }
            }
            WeaveTransition::GetActiveThread => {
                state.weave.get_active_thread(&mut state.scratchpad)
            }
            WeaveTransition::GetThreadFrom { id_seed } => state
                .weave
                .get_thread_from(&map_id(id_seed), &mut state.scratchpad),
            WeaveTransition::AddNode {
                from_seed,
                active,
                bookmarked,
                length,
                content_seed,
            } => {
                state.weave.add_node(DependentNode {
                    id: state.counter,
                    from: from_seed.map(map_id),
                    to: IndexSet::default(),
                    active,
                    bookmarked,
                    contents: WeaveContent {
                        length: length % 64,
                        content_seed: content_seed % 4,
                    },
                });
            }
            WeaveTransition::SetNodeActiveStatus {
                id_seed,
                value,
                alternate,
            } => {
                state
                    .weave
                    .set_node_active_status(&map_id(id_seed), value, alternate);
            }
            WeaveTransition::SetNodeActiveStatusInPlace { id_seed, value } => {
                state
                    .weave
                    .set_node_active_status_in_place(&map_id(id_seed), value);
            }
            WeaveTransition::SetNodeBookmarkedStatus { id_seed, value } => {
                state
                    .weave
                    .set_node_bookmarked_status(&map_id(id_seed), value);
            }
            WeaveTransition::RemoveNode { id_seed } => {
                state.weave.remove_node(&map_id(id_seed));
            }
            WeaveTransition::RemoveNodeTracked { id_seed } => {
                state.weave.remove_node_tracked(&map_id(id_seed), |_r| {});
            }
            WeaveTransition::RemoveAllNodes { apply_seed } => {
                if apply_seed == 0 {
                    state.weave.remove_all_nodes();
                }
            }
            WeaveTransition::MetadataMut { content_seed } => {
                state.weave.metadata_mut(|m| *m = content_seed)
            }
            WeaveTransition::SortNodeChildrenBy { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.weave.sort_node_children_by(&map_id(id_seed), |a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortNodeChildrenById { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                state
                    .weave
                    .sort_node_children_by_id(&map_id(id_seed), |a, b| {
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
            WeaveTransition::GetContentsMut {
                id_seed,
                content_seed,
            } => {
                state
                    .weave
                    .get_contents_mut(&map_id(id_seed), |c| c.content_seed = content_seed % 4);
            }
            WeaveTransition::SplitNode { id_seed, at_seed } => {
                state.weave.split_node(
                    &map_id(id_seed),
                    state
                        .weave
                        .get_node(&map_id(id_seed))
                        .map(|node| {
                            (at_seed
                                .checked_rem(node.contents.length)
                                .unwrap_or_default()) as usize
                        })
                        .unwrap_or_default(),
                    state.counter,
                );
            }
            WeaveTransition::MergeNodeWithParent { id_seed } => {
                state.weave.merge_with_parent(&map_id(id_seed));
            }
        }
        if state.weave.nodes().len() > old_node_count {
            state.counter += 1;
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
