use std::hash::{BuildHasher, RandomState};

use hashbrown::HashMap;
use indexmap::IndexSet;
use loro::{ExportMode, PeerID, VersionVector};
use proptest::{prelude::*, strategy::Strategy, test_runner::Config};
use proptest_derive::Arbitrary;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};
use rkyv::{Archive, Deserialize, Serialize};
use universal_weave::{
    BookmarkableWeave, IndependentContents, MetadataWeave, SemiIndependentWeave,
    SortableBookmarkableWeave, SortableWeave, Weave,
    dependent::{DependentNode, DependentWeave, loro::DependentLoroWeave},
};

const CASES: u32 = 1536;
const MAX_TRANSITIONS: usize = 512;

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
        VirtualPeers
    );
}

struct WeaveStateMachine;

impl ReferenceStateMachine for WeaveStateMachine {
    type State = Vec<Self::Transition>;
    type Transition = VirtualPeerTransition;

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
enum VirtualPeerTransition {
    #[proptest(weight = 8)]
    PeerA((WeaveTransition, u32, u8)),
    #[proptest(weight = 8)]
    PeerB((WeaveTransition, u32, u8)),
    SyncAtoB,
    SyncBtoA,
    SyncAB,
}

#[derive(Arbitrary, Debug, Clone)]
enum WeaveTransition {
    #[proptest(weight = 8)]
    AddNode {
        from_seed: Option<u32>,
        active: bool,
        bookmarked: bool,
        content_seed: u32,
    },
    #[proptest(weight = 6)]
    SetNodeActiveStatus {
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
        apply_seed: u8,
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
    Update,
    Commit,
}

#[derive(Default)]
struct VirtualPeers {
    counter: u32,
    a: WeaveWrapper,
    b: WeaveWrapper,
}

struct VirtualPeerMessage {
    id: PeerID,
    data: Vec<u8>,
    version: VersionVector,
}

struct WeaveWrapper {
    weave: DependentLoroWeave<u32, WeaveContent, u32, RandomState>,
    peers: HashMap<PeerID, VersionVector>,
    ordered_node_identifiers: Vec<u32>,
    ordered_node_identifiers_from: Vec<u32>,
    active_path: Vec<u32>,
    path_from: Vec<u32>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
struct WeaveContent(u32);

impl Default for WeaveWrapper {
    fn default() -> Self {
        Self {
            weave: DependentLoroWeave::try_from(DependentWeave::with_capacity(MAX_TRANSITIONS, 0))
                .unwrap(),
            peers: HashMap::new(),
            ordered_node_identifiers: Vec::with_capacity(MAX_TRANSITIONS),
            ordered_node_identifiers_from: Vec::with_capacity(MAX_TRANSITIONS),
            active_path: Vec::with_capacity(MAX_TRANSITIONS),
            path_from: Vec::with_capacity(MAX_TRANSITIONS),
        }
    }
}

impl WeaveWrapper {
    fn id(&self) -> PeerID {
        self.weave.peer_id()
    }
    fn apply(&mut self, counter: &mut u32, transition: (WeaveTransition, u32, u8)) {
        let s = RandomState::default();
        let hash_value = |value: u64| s.hash_one(value);
        let map_id = |seed: u32| seed % (*counter + 2);
        let old_node_count = self.weave.nodes().len();
        let target = map_id(transition.1);

        match transition.0 {
            WeaveTransition::AddNode {
                from_seed,
                active,
                bookmarked,
                content_seed,
            } => {
                self.weave.add_node(DependentNode {
                    id: *counter,
                    from: from_seed.map(map_id),
                    to: IndexSet::default(),
                    active,
                    bookmarked,
                    contents: WeaveContent(content_seed),
                });
            }
            WeaveTransition::SetNodeActiveStatus { id_seed, value } => {
                self.weave.set_node_active_status(&map_id(id_seed), value);
            }
            WeaveTransition::SetNodeBookmarkedStatus { id_seed, value } => {
                self.weave
                    .set_node_bookmarked_status(&map_id(id_seed), value);
            }
            WeaveTransition::RemoveNode { id_seed } => {
                self.weave.remove_node(&map_id(id_seed));
            }
            WeaveTransition::RemoveNodeTracked { id_seed } => {
                self.weave.remove_node_tracked(&map_id(id_seed), |_r| {});
            }
            WeaveTransition::RemoveAllNodes { apply_seed } => {
                if apply_seed == 0 {
                    self.weave.remove_all_nodes();
                }
            }
            WeaveTransition::MetadataMut { content_seed } => {
                self.weave.metadata_mut(|m| *m = content_seed)
            }
            WeaveTransition::SortNodeChildrenBy { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                self.weave.sort_node_children_by(&map_id(id_seed), |a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortNodeChildrenById { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                self.weave
                    .sort_node_children_by_id(&map_id(id_seed), |a, b| {
                        hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                    });
            }
            WeaveTransition::SortRootsBy { sort_seed } => {
                let sort_seed = sort_seed as u64;
                self.weave.sort_roots_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortRootsById { sort_seed } => {
                let sort_seed = sort_seed as u64;
                self.weave.sort_roots_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
            }
            WeaveTransition::SortBookmarksBy { sort_seed } => {
                let sort_seed = sort_seed as u64;
                self.weave.sort_bookmarks_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortBookmarksById { sort_seed } => {
                let sort_seed = sort_seed as u64;
                self.weave.sort_bookmarks_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
            }
            WeaveTransition::GetContentsMut {
                id_seed,
                content_seed,
            } => {
                let _ = self
                    .weave
                    .get_contents_mut(&map_id(id_seed), |c| c.0 = content_seed);
            }
            WeaveTransition::Update => {
                self.weave.update(|_doc| {}).unwrap();
            }
            WeaveTransition::Commit => {
                self.weave
                    .update(|doc| {
                        doc.commit();
                    })
                    .unwrap();
            }
        }
        assert!(self.weave.validate());
        if self.weave.nodes().len() > old_node_count {
            *counter += 1;
        }

        if transition.2.is_multiple_of(4) {
            self.weave
                .get_ordered_node_identifiers(&mut self.ordered_node_identifiers);
            self.weave.get_ordered_node_identifiers_from(
                &target,
                &mut self.ordered_node_identifiers_from,
            );
            self.weave.get_active_path(&mut self.active_path);
            self.weave.get_path_from(&target, &mut self.path_from);
        }
    }
    fn import(&mut self, message: VirtualPeerMessage) {
        self.weave
            .update(|doc| {
                doc.import(&message.data).unwrap();
            })
            .unwrap();

        self.peers.insert(message.id, message.version);

        assert!(self.weave.validate());
    }
    fn export(&mut self, peer: PeerID) -> VirtualPeerMessage {
        VirtualPeerMessage {
            id: self.weave.peer_id(),
            data: if let Some(version) = self.peers.get(&peer) {
                self.weave.export(ExportMode::updates(version))
            } else {
                self.weave.export(ExportMode::all_updates())
            }
            .unwrap(),
            version: self.weave.oplog_vv(),
        }
    }
}

impl IndependentContents for WeaveContent {}

// Invariants are validated by the function's contracts
impl StateMachineTest for VirtualPeers {
    type SystemUnderTest = Self;
    type Reference = WeaveStateMachine;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        VirtualPeers::default()
    }
    fn apply(
        mut state: Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        match transition {
            VirtualPeerTransition::PeerA(transition) => {
                state.a.apply(&mut state.counter, transition);
            }
            VirtualPeerTransition::PeerB(transition) => {
                state.b.apply(&mut state.counter, transition);
            }
            VirtualPeerTransition::SyncAtoB => {
                state.b.import(state.a.export(state.b.id()));
            }
            VirtualPeerTransition::SyncBtoA => {
                state.a.import(state.b.export(state.a.id()));
            }
            VirtualPeerTransition::SyncAB => {
                let a_export = state.a.export(state.b.id());
                let b_export = state.b.export(state.a.id());

                state.b.import(a_export);
                state.a.import(b_export);
                assert_eq!(state.a.weave.as_weave(), state.b.weave.as_weave());
            }
        }

        state
    }
    fn check_invariants(
        _state: &Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
    }
}
