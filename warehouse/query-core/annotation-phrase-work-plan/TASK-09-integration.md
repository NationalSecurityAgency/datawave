# Task 09 — Transformer and ShardQueryLogic Integration

## Objective

Wire parsing, macro-expanded Lucene extraction, structured matching, factory adaptation, configuration overrides, and paging-safe cache invalidation into production flow.

## Prerequisite

Tasks 01–08 validation gates passed.

## Read first

- All prior completion notes.
- `src/main/java/datawave/query/transformer/annotation/AnnotationHitsTransformer.java` completely
- `src/main/java/datawave/query/tables/ShardQueryLogic.java`:
  - copy constructor/config setup;
  - `getJexlQueryString`;
  - `initialize` around macro expansion/JEXL assignment;
  - `loadQueryParameters`;
  - `addConfigBasedTransformers`;
  - configured parser access.
- `src/main/java/datawave/query/config/ShardQueryConfiguration.java` — query/original JEXL/all-hits fields and copy/equality
- `src/test/java/datawave/query/transformer/annotation/AnnotationHitsTransformerTest.java`
- `src/test/java/datawave/query/tables/ShardQueryLogicTest.java` — annotation-hit sections

## Implement

### Query preparation

1. Expand query macros once in `ShardQueryLogic`.
2. Ensure the annotation extractor receives the same macro-expanded Lucene source used for conversion.
3. Use the exact configured `LuceneSyntaxQueryParser`; do not construct a default parser in the transformer.
4. Direct JEXL uses original pre-planning JEXL extraction.
5. Lucene merges raw provenance with generated original-JEXL content-function alternatives.
6. Prefer preparing immutable query-derived `SearchExpressions` in `ShardQueryLogic`, where syntax/parser/macro state is available, then pass them to the transformer. Avoid modifying user-visible `Query.getQuery()`.

### Transformer configuration

7. Add `annotation.all.hits.flattenBoundary`.
8. Config supplies default; nonblank query parameter overrides it.
9. Preserve keyword outer decoding (URL, JSON array, semicolon fallback), then delegate logical values to the injected keyword parser.
10. Nonblank keyword parameter remains a complete override.
11. Validate required normalizer/parser/extractor dependencies when enabled.

### Matching flow

12. Replace `Set<Pattern> searchHitTerms` with immutable structured criteria.
13. For each valid annotation:
    - sort/build normalized positions;
    - match standalone expressions;
    - match ordered expressions;
    - match unordered expressions;
    - deduplicate exact/equivalent occurrences;
    - create phrase-wide contexts;
    - deterministic sort;
    - call `AllHitsFactory.createFromHits`.
14. Preserve enrichment, final-document checks, target-field checks, cleanup, error handling, and no-op-on-empty behavior.

### Lifecycle/cache

15. Ensure later `updateConfig()` calls cannot reuse stale criteria. Cache identity must cover syntax, source query/prepared criteria, original JEXL, and keyword override.
16. Test keyword override add/change/removal, disable/re-enable, and flatten changes. Flatten changes should not require reparsing criteria.
17. Preserve constructor-before-original-JEXL lifecycle behavior or eliminate the race by passing prepared immutable criteria.

### Wiring

18. Pass `AllHitsQueryConfig.flattenBoundary`, keyword parser, and query extractor/prepared criteria through `ShardQueryLogic`.
19. Update test builders and any Spring bean definitions found in the repository. Production deployments may define beans externally; keep defaults/backward compatibility explicit.
20. Update config copy/equality only where new runtime/prepared fields require it.

## Tests

Extend transformer and logic tests for:

- direct JEXL standalone + phrase;
- Lucene phrase + standalone distinction;
- analyzer alternatives;
- macro-expanded phrase/term;
- keyword override in all three outer formats;
- escaped-space keyword;
- both flatten modes and parameter override in both directions;
- phrase, adjacent, and within hits;
- negation;
- score threshold;
- overlapping hits/context merge;
- no phrases leaves existing output unchanged;
- paging/updateConfig cache transitions;
- custom factory error path still works.

## Validation gate

Before Task 10:

- Full `AnnotationHitsTransformerTest`, `AllHitsFactoryTest`, `TermExtractorTest`, extractor/parser/matcher tests, and annotation sections of `ShardQueryLogicTest` pass.
- Existing standalone behavior is unchanged without phrases.
- Macro-expanded Lucene coverage proves the expanded source is used.
- No stale criteria remain across update scenarios.

Suggested command:

```bash
mvn -pl warehouse/query-core -am -DskipITs \
  -Dtest=AnnotationHitsTransformerTest,AllHitsFactoryTest,TermExtractorTest,DefaultKeywordSearchExpressionParserTest,JexlSearchExpressionExtractorTest,LuceneSearchExpressionExtractorTest,OrderedAnnotationMatcherTest,UnorderedAnnotationMatcherTest,ShardQueryLogicTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```
