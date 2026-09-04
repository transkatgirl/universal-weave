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
- [ ] For node.from / node.to / weave.bookmarked, replace IndexMap with an IndexMap/micromap hybrid which switches between the two based on item count
    - Saves 22-40 bytes/element of memory for small collections
    - Vec::contains() roughly matches IndexSet::contains() for the following sizes:
        - &lt;16 = nohash
        - &lt;24 = foldhash
        - &lt;64 = `std` hasher

### Future plans

- [ ] Better CRDT support
    - [ ] Ordered set for bookmarks (waiting on Loro support)
    - [ ] DAG-based documents (waiting on Loro to implement [DAG CRDTs](https://dl.acm.org/doi/pdf/10.1145/3721473.3722141))