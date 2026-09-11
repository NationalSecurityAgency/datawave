# Task 07 — Unordered Adjacent and Within Matcher

## Objective

Implement DataWave-compatible unordered proximity matching for `content:adjacent`, `content:within`, and Lucene slop alternatives.

## Prerequisite

Tasks 01–06 validation gates passed.

## Read first

- Prior ordered matcher and position model.
- `src/main/java/datawave/query/jexl/functions/ContentUnorderedEvaluator.java`
- `src/main/java/datawave/query/jexl/functions/ContentFunctions.java` documentation
- `src/test/java/datawave/query/language/parser/jexl/TestLuceneToJexlQueryParser.java` slop-to-within assertions

## Implement

1. Match one distinct candidate value per component without requiring query order.
2. `adjacent` uses total window `componentCount - 1`.
3. `within` uses its configured total window.
4. Use sorted boundary ordinals, not boundary timestamp differences.
5. `flattenBoundary=false`: one component per boundary.
6. `flattenBoundary=true`: multiple distinct values may occupy one boundary; model each additional selected same-boundary value as requiring virtual unit spacing. Compute the minimum valid virtual span for an assignment without relying on stored value order.
7. Repeated terms require distinct matching values.
8. Canonicalize unordered occurrences by constituent boundary/value identity so permutations of the same occurrence are emitted once.
9. Preserve genuinely different and overlapping assignments.
10. Prune when the minimum possible window already exceeds the allowed distance.

Because annotation boundaries lack `TermWeightPosition.lowOffset`/skip metadata, document that matching mirrors ordinary offset-window behavior but cannot reproduce skip ranges.

## Tests

Cover:

- reversed terms satisfy adjacent/within;
- adjacent exact window pass/fail;
- within at boundary and one past boundary;
- timestamp gaps ignored;
- flatten false one component per boundary;
- flatten true same-boundary virtual spacing;
- mixed same-boundary/cross-boundary assignments;
- repeated terms and unique value identities;
- permutation deduplication;
- distinct overlapping occurrences retained;
- every constituent enforces `minScore`;
- Lucene generated distances already represented in criteria are honored.

Include a table-driven test that makes virtual-span calculations explicit, especially for three or more selected values sharing/mixing boundaries. If expected semantics conflict with root requirements, stop and record the exact case rather than inventing timestamp-based behavior.

## Validation gate

Before Task 08:

- Unordered matcher tests pass in both flatten modes.
- Ordered matcher regressions pass.
- Permutations do not duplicate occurrences.
- A stress test demonstrates bounded behavior.

Suggested command:

```bash
mvn -pl warehouse/query-core -am -DskipITs \
  -Dtest=UnorderedAnnotationMatcherTest,OrderedAnnotationMatcherTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```
