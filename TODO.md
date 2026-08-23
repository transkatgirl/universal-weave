# TODO

## Correctness

- [ ] Improve test coverage
    - [ ] **DependentWeave unit tests**
    - [ ] **IndependentWeave unit tests**
    - [ ] DeduplicatedWeave unit tests
- [ ] **Turn function contracts into readable & executable documentation**
	- [ ] DependentWeave
	- [ ] IndependentWeave
- [ ] Improve documentation
    - [ ] Trait documentation improvements
    - [ ] DependentWeave-specific documentation
    - [ ] IndependentWeave-specific documentation
- [ ] Setup fuzzing w/ `cargo-fuzz`
	- [ ] DependentWeave (test validation of random weave + random weave action on success)
    - [ ] IndependentWeave (test validation of random weave + random weave action on success)
    - [ ] DependentLoroWeve import
- [ ] Full (library) code review

### API & Documentation Correctness

- [ ] Ensure crate is compliant with https://rust-lang.github.io/api-guidelines/checklist.html
    - [ ] Naming
    - [ ] Interoperability
    - [x] Macros
    - [ ] Documentation
    - [ ] Predictability
    - [ ] Flexibility
    - [ ] Type safety
    - [ ] Dependability
    - [ ] Debuggability
    - [ ] Future proofing
    - [x] Necessities
- [ ] Full documentation review (including README)
    - [ ] Add crate examples

### Future plans

- Formal verification using [Verus](https://github.com/verus-lang/verus) once it supports enough of the language features

## Features

- [ ] Loom UI building blocks using [egui](https://crates.io/crates/egui) (as separate library)
    - [ ] Implement graphing using [dagre](https://crates.io/crates/dagre) or by building our own fork to improve efficiency and add functionality?
- [ ] [micromap](https://crates.io/crates/micromap/0.3.0)-based Weave implementations (waiting on [rkyv support](https://github.com/yegor256/micromap/issues/414)) for performance-focused or memory-limited use cases where node edge limits are acceptable
    - Need to clearly state that this should only be used if nodes have <= ~64 edges

### Future plans

- [ ] Better CRDT support
    - [ ] Ordered set for bookmarks (waiting on Loro support)
    - [ ] DAG-based documents (waiting on Loro to implement [DAG CRDTs](https://dl.acm.org/doi/pdf/10.1145/3721473.3722141))