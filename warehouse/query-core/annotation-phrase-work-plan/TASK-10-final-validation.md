# Task 10 — End-to-End Validation, Compatibility Review, and Cleanup

## Objective

Perform final cross-cutting validation, close test gaps, document behavior, and leave a reviewable implementation with no plan-driven temporary code.

## Prerequisite

Tasks 01–09 validation gates passed.

## Read first

- Root plan `README.md` and all completion notes.
- Final diffs for every changed file.
- `src/main/java/datawave/query/transformer/annotation/AnnotationHitsTransformer.java`
- `src/main/java/datawave/query/transformer/annotation/AllHitsFactory.java`
- `src/main/java/datawave/query/tables/ShardQueryLogic.java`
- `src/main/java/datawave/query/config/annotation/AllHitsQueryConfig.java`
- All new parser/extractor/matcher tests.

## Review checklist

### Semantics

- [ ] Phrase is ordered distance 1.
- [ ] Adjacent is unordered window `n-1`.
- [ ] Within/slop is unordered configured window.
- [ ] Timestamp gaps do not affect distance.
- [ ] Flatten false/true semantics match the root plan.
- [ ] Unique value identity and per-constituent `minScore` are enforced.
- [ ] Positive-only extraction is correct.
- [ ] Field and `_ANYFIELD_` eligibility are correct; unfielded functions always included.
- [ ] Explicit keywords override rather than augment.
- [ ] Macro-expanded Lucene and generated alternatives are both represented.
- [ ] `content:scoredPhrase` is not accidentally supported.

### Compatibility

- [ ] Existing keyword formats still work.
- [ ] Existing standalone outputs remain unchanged.
- [ ] Existing `AllHitsFactory.create` API remains available.
- [ ] Configurable factory subclasses still intercept calls.
- [ ] `AllHits` JSON schema is unchanged.
- [ ] Defaults preserve old behavior when no phrases exist.
- [ ] Config copy/equality/serialization is coherent.

### Quality

- [ ] No repeated normalization per pattern.
- [ ] No unbounded Cartesian product in matchers.
- [ ] Deterministic ordering and canonical deduplication.
- [ ] No mutable criteria leaked from config/cache.
- [ ] Parse failures follow existing no-op/logging policy.
- [ ] Javadocs describe virtual same-boundary distance and annotation-vs-term-frequency limitations.
- [ ] Remove dead compatibility scaffolding only if safe; do not perform unrelated cleanup.

## Final tests

1. Run all focused tests from Tasks 01–09.
2. Run the complete query-core unit test suite if feasible:

```bash
mvn -pl warehouse/query-core -am -DskipITs \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

3. Run formatting/checkstyle/static analysis commands configured by the module, if they are not part of the normal lifecycle.
4. If integration-test infrastructure is available, run relevant query-core integration tests involving phrase/content functions and annotation hits. Do not claim unavailable external-service tests passed.

## Deliverable summary

Add final completion notes containing:

- Changed production files grouped by configuration, extraction, matching, and factory/integration.
- New/changed tests.
- Exact commands and results.
- Any unavailable validation and why.
- Any compatibility concern that remains.
- A concise behavioral example for:
  - Lucene phrase plus standalone term;
  - direct JEXL `content:within`;
  - keyword override with escaped whitespace;
  - flatten false versus true.

## Final validation gate

The work is complete only when:

- All feasible focused tests pass.
- Complete module tests pass, or environmental failures are precisely documented and unrelated to changes.
- Root checklist is fully checked.
- No task has an unresolved semantic deviation.
- The final diff contains implementation/tests only, with no generated artifacts, build outputs, or unrelated formatting changes.
