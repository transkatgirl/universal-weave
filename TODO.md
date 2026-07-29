# TODO

## Release 0.1.0

- [ ] Review function contracts to ensure consistency with documentation & reasonable behavior
	- [ ] DependentWeave
	- [ ] IndependentWeave
- [ ] Publish to crates.io

## Release 0.1.1

- [ ] Improve test coverage
    - [ ] Property tests for DependentLoroWeave CRDT merging
    - [ ] Property tests for IndependentWeave::from(DependentWeave)
    - [ ] Property tests for IndependentWeave behavior parity with DependentWeave?
    - [ ] Property tests for Archived structs
    - [ ] Add DependentWeave fuzzing (test validation of random weave + random weave action on success)
    - [ ] Add IndependentWeave fuzzing (test validation of random weave + random weave action on success)
- [ ] Full (library) code review

## Release 0.2.0

- [ ] Separate bookmarking into a Weave wrapper?
- [ ] Add node layout calculation behind a feature flag?
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

## Future plans

- Formal verification using [Verus](https://github.com/verus-lang/verus) once it supports enough of the language features