# Task 01 — Core Search Models and Configuration

## Objective

Create immutable, serializable structured search models and add the `flattenBoundary` plus parser/extractor configuration surfaces. Do not integrate them into `AnnotationHitsTransformer` yet.

## Read first

- `src/main/java/datawave/query/config/annotation/AllHitsQueryConfig.java`
- `src/main/java/datawave/query/transformer/annotation/TermExtractor.java`
- `src/main/java/datawave/query/transformer/annotation/AnnotationHitsTransformer.java` — fields, constructor, `updateConfig`, lazy extraction only
- `src/test/java/datawave/query/transformer/annotation/TermExtractorTest.java`
- `src/test/java/datawave/query/config/ShardQueryConfigurationTest.java` — config copy/default conventions

Paths above are relative to `warehouse/query-core`.

## Implement

1. Add immutable structured criteria under `datawave.query.transformer.annotation` (or a focused subpackage):
   - standalone pattern expression;
   - proximity expression with ordered/unordered mode, ordered component list, and distance;
   - immutable aggregate collection.
2. Store normalized pattern source strings and flags/semantics needed to compile them; do not rely on `Pattern.equals()` for deduplication.
3. Define structural equality/hash code so equivalent alternatives deduplicate while component order remains significant for ordered phrases.
4. For unordered criteria, canonicalize equality without losing repeated components.
5. Add serializable interfaces for:
   - explicit keyword parsing;
   - syntax-aware query extraction.
6. Add to `AllHitsQueryConfig`:
   - `boolean flattenBoundary = false`;
   - keyword parser dependency;
   - query expression extractor dependency (retain `TermExtractor` for compatibility until integration).
7. Update copy constructor, accessors, `equals`, and `hashCode`.

Do not add compiled regex fields to serialized config state unless they are safely reconstructable.

## Tests

Add focused model/config tests for:

- default `flattenBoundary=false`;
- copy/equality/hash behavior for both boolean values and injected dependencies;
- ordered component order affects equality;
- equivalent unordered expressions deduplicate;
- repeated unordered components are preserved;
- aggregate collections are immutable from callers.

## Validation gate

Before Task 02:

- All new models and interfaces compile.
- Existing `TermExtractorTest` passes unchanged.
- New model/config tests pass.
- Existing `ShardQueryConfigurationTest` passes.
- No transformer behavior has changed.

Suggested command:

```bash
mvn -pl warehouse/query-core -am -DskipITs \
  -Dtest=TermExtractorTest,ShardQueryConfigurationTest,SearchExpressionsTest,AllHitsQueryConfigTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

## Completion notes

- Added immutable serializable `SearchExpression`, `StandalonePatternExpression`, `ProximityExpression`, and `SearchExpressions` models. Equality uses pattern source/flags, ordered component sequences, and multiplicity-preserving canonical unordered sequences; compiled `Pattern` instances are not used.
- Added serializable `KeywordParser` and `QueryExpressionExtractor` interfaces.
- Added `flattenBoundary`, parser, and query extractor configuration properties to `AllHitsQueryConfig`, including copy/equality/hash support. Existing `TermExtractor` remains unchanged for compatibility and `AnnotationHitsTransformer` was not integrated.
- Added focused tests in `SearchExpressionsTest` and `AllHitsQueryConfigTest`.
- Validation passed with the command above: 58 tests, 0 failures, 0 errors. Existing checkstyle output contained only pre-existing repository import-control warnings.
