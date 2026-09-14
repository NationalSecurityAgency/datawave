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

## Completion notes

- Reviewed all Task 01–09 completion notes and final production/test diffs. The implementation preserves ordered phrase distance 1, unordered adjacent distance `n-1`, within/slop configured distance, ordinal boundary semantics (not timestamp gaps), both flatten modes, per-constituent score filtering, distinct value identities, positive-only and field eligibility rules, macro-expanded Lucene provenance, keyword escaping/override behavior, overlap and exact-identity deduplication, legacy factory interception, and the unchanged `AllHits` response model.
- Final production change: `warehouse/query-core/src/main/java/datawave/query/transformer/annotation/AnnotationHitsTransformer.java` now resets query-derived state on every `updateConfig()` and treats structured keyword values as a complete override. This prevents stale criteria across paging/config updates and prevents query criteria from being merged into a keyword override; the legacy no-parser keyword path remains unchanged.
- Configuration/extraction: `AllHitsQueryConfig`, `ShardQueryLogic`, and the structured parser/extractor classes were reviewed for copy/equality, configured parser use, macro-expanded Lucene plus generated-JEXL alternatives, positive-only extraction, and field eligibility. Matching: `AnnotationPositionView`, standalone, ordered, and unordered matchers were reviewed for normalized-value reuse, bounded candidate search, ordinal distance, score thresholds, flatten behavior, and deduplication. Factory/integration: `PhraseHit`, `AllHitsFactory`, and transformer wiring were reviewed for phrase-wide context, mixed hits, subclass interception, and JSON compatibility.
- Added `AnnotationHitsTransformerTest.structuredKeywordOverrideReplacesAndCanRestorePreparedCriteria`, covering prepared-query replacement, keyword override changes, and removal/restoration across repeated `updateConfig()` calls on the same transformer instance.
- Focused validation passed after the final change:
  `mvn -q -pl warehouse/query-core -am -DskipITs -Dtest=AllHitsQueryConfigTest,SearchExpressionsTest,DefaultKeywordSearchExpressionParserTest,JexlSearchExpressionExtractorTest,LuceneSearchExpressionExtractorTest,AnnotationValuePositionTest,StandaloneAnnotationMatcherTest,AnnotationHitsTransformerTest,AllHitsFactoryTest,AnnotationHitFactoryTest,OrderedAnnotationMatcherTest,UnorderedAnnotationMatcherTest,Task09IntegrationCoverageTest -Dsurefire.failIfNoSpecifiedTests=false test`.
- Full query-core validation was attempted with both `mvn -q -pl warehouse/query-core -am -DskipITs -Dsurefire.failIfNoSpecifiedTests=false test` and `mvn -q -pl warehouse/query-core -DskipITs -Dsurefire.failIfNoSpecifiedTests=false test`. It was unavailable as a clean gate because the environment cannot initialize Hadoop/Accumulo in-memory tests (`KerberosAuthException`, `UnixPrincipal` invalid null `name`, and `Unable to determine current user`); the direct query-core run also reported unrelated filesystem-backed sorted-map/set persistence failures and external-process initialization failures. The reactor run additionally stopped in `accumulo-utils` with the same Hadoop current-user failures. These failures were outside the annotation-focused tests.
- Formatting hygiene: `git diff --check` passed; no generated artifacts or unrelated formatting changes are present. No integration tests requiring external services were claimed as passed.
- Behavioral examples: Lucene `TEXT:"new york" OR TEXT:new` yields one phrase plus the explicit standalone `new`; direct JEXL `content:within(..., 3)` yields an unordered distance-3 criterion; keyword `new\\ york` remains one escaped-space standalone value while a multi-component keyword is a phrase; flatten false requires one component per boundary, whereas flatten true permits distinct same-boundary values with virtual unit spacing.
- Remaining compatibility risk: full in-memory query-logic and external-service integration validation could not run in this environment; no known semantic deviation remains from the plan.

## Final validation gate

The work is complete only when:

- All feasible focused tests pass.
- Complete module tests pass, or environmental failures are precisely documented and unrelated to changes.
- Root checklist is fully checked.
- No task has an unresolved semantic deviation.
- The final diff contains implementation/tests only, with no generated artifacts, build outputs, or unrelated formatting changes.
