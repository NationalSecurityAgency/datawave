# Task 06 — Ordered Phrase Matcher

## Objective

Implement ordered, pairwise-distance-1 matching for `content:phrase`, exact Lucene phrases, and explicit multiword keyword phrases.

## Prerequisite

Tasks 01–05 validation gates passed.

## Read first

- Prior task position model and completion notes.
- `src/main/java/datawave/query/jexl/functions/ContentOrderedEvaluator.java` — conceptual ordered semantics; annotation positions do not include term-frequency skip metadata
- `src/main/java/datawave/query/transformer/annotation/AnnotationHitsTransformer.java` — context window behavior
- Task 06 semantics in root `README.md`.

## Implement

Create an ordered matcher returning constituent hit occurrences independently of response creation.

Rules:

- Match components in query order.
- Each component uses a distinct value identity meeting `minScore`.
- `flattenBoundary=false`: next component must be in the next boundary; at most one component per boundary; values are alternatives.
- `flattenBoundary=true`: successive components may use distinct values in the same boundary in any value-list order, or values in the next boundary. Both transitions have effective distance 1.
- Never tokenize annotation values.
- Preserve genuinely distinct and overlapping occurrences.
- Deduplicate exact constituent identity sequences and equivalent criteria.
- Repeated components require distinct values/positions.

Use bounded backtracking/dynamic programming and prune impossible candidates early. Do not materialize an unrestricted Cartesian product.

Represent raw matcher output with a small occurrence type if `PhraseHit` is not introduced until Task 08.

## Tests

Build explicit compact segments rather than relying only on the large transformer fixture. Cover:

- normal adjacent-boundary match and wrong order;
- actual boundary timestamp gaps ignored;
- multiple values as alternatives;
- flatten false rejects two components in one boundary;
- flatten true accepts multiple distinct same-boundary values;
- same-boundary list order irrelevant (`[new,york]` matches both query orders);
- one value cannot satisfy two components;
- repeated terms with one versus two matching values;
- phrase spanning same and subsequent boundaries;
- all constituents enforce `minScore`;
- overlapping/repeated occurrences retained;
- equivalent criteria and exact identities deduplicated.

## Validation gate

Before Task 07:

- Ordered matcher tests pass in both flatten modes.
- Existing standalone and transformer tests still pass.
- A stress-oriented test demonstrates pruning on several alternatives without timeout/explosion.

Suggested command:

```bash
mvn -pl warehouse/query-core -am -DskipITs \
  -Dtest=OrderedAnnotationMatcherTest,StandaloneAnnotationMatcherTest,AnnotationHitsTransformerTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

## Completion notes

- Added `OrderedAnnotationMatcher` with bounded, adjacent-boundary backtracking, whole-value regex matching, score filtering, flatten-boundary transitions, distinct value identity enforcement, and occurrence deduplication.
- Added immutable `AnnotationPhraseOccurrence` containing the selected `ValuePosition` constituents; no response/factory integration was added.
- Added focused tests covering order, ordinal distance versus timestamp gaps, alternatives, flattening and stored-order independence, score thresholds, repeated terms, overlap, identity deduplication, and existing standalone/transformer compatibility.
- Added validation that `ProximityExpression` inputs are ordered and have exact distance 1.
- Added direct `ProximityExpression` coverage for component order, flags, unsupported distances, and unordered expressions, plus explicit constituent-boundary identity assertions for overlapping occurrences.
- Validation passed: `mvn -pl warehouse/query-core -am -DskipITs -Dtest=AnnotationValuePositionTest,StandaloneAnnotationMatcherTest,AnnotationHitsTransformerTest,AllHitsFactoryTest,OrderedAnnotationMatcherTest -Dsurefire.failIfNoSpecifiedTests=false test` (113 tests, 0 failures, 0 errors). Checkstyle passed with only pre-existing repository import-control warnings.
