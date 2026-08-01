# TODO

## Release 0.1.0

- [ ] Update all function contracts to use the new contracts macro
- [ ] Require all DependentWeave and IndependentWeave functions to clear scratchpads on end
    - [ ] Update validate()
    - [ ] Update property tests to check get_ordered_node_identifiers*, get_active_path() and get_path_from() after every transition
- [ ] Review function contracts to ensure consistency with documentation & reasonable behavior
	- [ ] DependentWeave
	- [ ] IndependentWeave
- [ ] Improve test coverage
    - [ ] Property tests for IndependentWeave behavior parity with DependentWeave?
        - [ ] Add IndependentWeave function which extends the semantics of DependentWeave::set_node_active_status() to a DAG
    - [ ] Property tests for IndependentWeave::from(DependentWeave)
    - [ ] DependentWeave unit tests
    - [ ] IndependentWeave unit tests
- [ ] Publish to crates.io

## Release 0.1.1

- [ ] Improve test coverage
	- [ ] Add DependentWeave fuzzing (test validation of random weave + random weave action on success)
    - [ ] Add IndependentWeave fuzzing (test validation of random weave + random weave action on success)
    - [ ] Add DependentLoroWeve import fuzzing
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