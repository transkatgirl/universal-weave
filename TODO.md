# TODO

## Correctness

- [ ] Improve test coverage
    - [ ] **DependentWeave unit tests**
    - [ ] **IndependentWeave unit tests**
- [ ] Review function contracts to ensure consistency with documentation & reasonable behavior
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

- [ ] Weave Deduplication wrapper
    - [ ] Remove DeduplicatableWeave trait
- [ ] Additional Weave traversal methods
    - [ ] Node siblings
    - [ ] Node siblings or roots
- [ ] ID reassignment module
- [ ] Rkyv convenience wrappers
- [ ] Loom UI building blocks using [egui](https://crates.io/crates/egui) (as separate library)
    - [ ] Implement graphing using [dagre](https://crates.io/crates/dagre) or by building our own fork to improve efficiency and add functionality?

### Future plans

- [ ] Better CRDT support
    - [ ] Ordered set for bookmarks (waiting on Loro support)
    - [ ] DAG-based documents (waiting on Loro to implement [DAG CRDTs](https://dl.acm.org/doi/pdf/10.1145/3721473.3722141))