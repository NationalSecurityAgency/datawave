# Task 02 — Explicit Keyword Parser

## Objective

Implement the default Spring-injectable parser for `annotation.all.hits.keywords`. This task covers logical keyword values only; outer URL/JSON/semicolon decoding remains in the transformer until Task 09.

## Prerequisite

Task 01 validation gate passed.

## Read first

- Task 01 completion notes and its new model/interfaces.
- `src/main/java/datawave/query/transformer/annotation/AnnotationHitsTransformer.java` — current keyword decoding and normalization
- `src/test/java/datawave/query/transformer/annotation/AnnotationHitsTransformerTest.java` — `plaintextKeywordParameterTest`, `jsonKeywordParameterTest`, `urlEncodedJsonKeywordParameterTest`
- `src/main/java/datawave/data/normalizer/Normalizer.java` in the owning module, if needed for API details

## Implement

Create `DefaultKeywordSearchExpressionParser` with these rules per already-decoded logical value:

- Unescaped whitespace separates components.
- Escaped whitespace remains in one standalone keyword.
- `new york.*` becomes ordered exact phrase `[new, york.*]`.
- `new\ york` (one escaping backslash in the logical Java string) becomes one standalone keyword containing a space.
- A trailing unmatched backslash is literal.
- Empty expression: skip.
- One component: standalone expression.
- Multiple components: ordered, pairwise-distance-1 expression.
- Normalize each component independently with the configured normalizer.
- Preserve current regex interpretation; do not tokenize annotation values and do not introduce automatic quoting.

Implement escaping as a small deterministic scanner, not a whitespace split regex. Define escaped-backslash behavior, but do not expand the outer semicolon-delimited format beyond its existing contract.

## Tests

Use a real normalizer where practical. Cover logical values for:

- `city`
- `new york`
- repeated/multiple whitespace
- `new york.*`
- escaped space
- escaped backslash before whitespace
- trailing backslash
- empty/whitespace-only value
- one surviving component
- normalization per phrase component
- duplicate criteria deduplication

## Validation gate

Before Task 03:

- Parser tests pass.
- Existing keyword transformer tests still pass unchanged.
- Parsing does not mutate input collections.
- Malformed trailing escaping does not throw.

Suggested command:

```bash
mvn -pl warehouse/query-core -am -DskipITs \
  -Dtest=DefaultKeywordSearchExpressionParserTest,AnnotationHitsTransformerTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```
