# TODO

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
    - [ ] Performance optimization
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