# Task 05 — Annotation Position Model and Standalone Matcher

## Objective

Create a stable normalized annotation-position view and refactor standalone matching onto it without changing current behavior.

## Prerequisite

Tasks 01–04 validation gates passed.

## Read first

- `src/main/java/datawave/query/transformer/annotation/AnnotationHitsTransformer.java` — `sort`, `search`, `matchesSearchTerm`, `SegmentHit`
- `src/main/java/datawave/query/transformer/annotation/BoundaryComparator.java`
- `src/main/java/datawave/query/transformer/annotation/SegmentValueByScoreComparator.java`
- `src/main/java/datawave/query/transformer/annotation/AllHitsFactory.java` — value-index contract
- `src/test/java/datawave/query/transformer/annotation/AnnotationHitsTransformerTest.java` — segment fixtures and hit/context tests
- Annotation protobuf generated API usage in the same test; do not edit generated protobufs.

## Implement

1. Add an internal immutable `ValuePosition`/annotation view containing:
   - ordinal boundary index;
   - value index into the final score-sorted list consumed by `AllHitsFactory`;
   - boundary and value;
   - normalized value.
2. Normalize each annotation value once per annotation search.
3. Preserve the exact `valueHitIndex` contract with `AllHitsFactory`.
4. Keep semantic ordering separate from value-list score ordering. Value order must not drive phrase semantics.
5. Extract standalone matching into a focused matcher that:
   - checks `score >= minScore`;
   - applies full pattern matching to the normalized whole value;
   - emits one `SegmentHit` per matching value;
   - computes context exactly as current code does.
6. Integrate only the standalone matcher into the existing transformer or prove equivalence through unit-level tests. Do not enable phrases yet.

## Tests

- Port/add focused tests for exact, regex, multiple-value, score threshold, boundary ordering, and all existing context edge cases.
- Add a normalizer invocation test showing each value is normalized once per annotation view, not once per pattern.
- Verify sorting shuffled segments produces stable boundary/value indexes.

## Validation gate

Before Task 06:

- All existing `AnnotationHitsTransformerTest` tests pass unchanged.
- New annotation view/matcher tests pass.
- Existing `AllHitsFactoryTest` passes.
- Standalone output JSON is byte-for-byte/equality equivalent in representative tests.

Suggested command:

```bash
mvn -pl warehouse/query-core -am -DskipITs \
  -Dtest=AnnotationValuePositionTest,StandaloneAnnotationMatcherTest,AnnotationHitsTransformerTest,AllHitsFactoryTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```
