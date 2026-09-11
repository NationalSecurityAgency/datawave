# Task 04 — Lucene Extraction and Analyzer-Alternative Merging

## Objective

Extract standalone-versus-phrase provenance from macro-expanded Lucene using the exact configured parser, then merge analyzer-generated content-function alternatives from generated original JEXL. Do not wire it into query execution yet.

## Prerequisite

Tasks 01–03 validation gates passed.

## Read first

- Prior task completion notes and JEXL extractor API.
- `src/main/java/datawave/query/language/parser/lucene/LuceneSyntaxQueryParser.java`
- `src/main/java/datawave/query/language/parser/jexl/LuceneToJexlQueryParser.java`
- `src/main/java/datawave/query/language/processor/lucene/CustomAnalyzerQueryNodeProcessor.java`
- `src/main/java/datawave/query/lucene/visitors/BaseVisitor.java`
- `src/main/java/datawave/query/lucene/visitors/QueryNodeType.java`
- `src/main/java/datawave/query/lucene/visitors/LuceneQueryStringBuildingVisitor.java`
- `src/test/java/datawave/query/language/parser/jexl/TestLuceneToJexlQueryParser.java` — phrase/slop/tokenized examples around the `content:phrase` and `content:within` assertions
- `src/main/java/datawave/query/tables/ShardQueryLogic.java` — `getJexlQueryString`, `getQueryParser`, macro expansion flow

## Implement

1. Add a Lucene query-node visitor/extractor using `LuceneSyntaxQueryParser.parseToLuceneQueryNode()` from the configured parser.
2. Extract positive explicit standalone nodes and quoted/slop phrase nodes while preserving fields, escaping, wildcard/regex semantics, Boolean negation, and standalone-versus-phrase provenance.
3. Convert exact quoted phrases to ordered distance-1 criteria.
4. Convert Lucene slop according to the configured parser's generated `content:within` distance. Do not assume raw `~N` always equals final distance after tokenization; use generated original JEXL alternatives where necessary.
5. Parse generated original JEXL with Task 03 extraction, but merge only content-function alternatives for Lucene provenance. Do not import generated equality nodes indiscriminately as standalone hits.
6. Merge raw and generated proximity criteria structurally. Deduplicate equivalent alternatives; no origin metadata is needed.
7. Apply the same field eligibility rules as Task 03.
8. Accept the macro-expanded Lucene string as an explicit method input. Macro expansion/wiring occurs in Task 09.
9. Do not instantiate a default parser internally when a configured parser is required.

Be careful with `ModifierQueryNode`, `NotBooleanQueryNode`, grouped nodes, smart quotes, and processor-generated alternatives. Reuse existing Lucene visitor conventions.
## Tests

Cover:

- fielded/unfielded quoted phrase;
- quoted phrase with slop;
- `TEXT:"new york" OR TEXT:new` retaining phrase and standalone `new`;
- phrase components not becoming standalone terms;
- negated and double-negated terms/phrases;
- smart quotes and escaped phrase content;
- wildcard phrase components where supported;
- tokenized/hyphenated analyzer alternatives from generated JEXL;
- equivalent raw/generated alternatives deduplicated;
- rejected field, accepted field, `_ANYFIELD_`, and unfielded behavior;
- parser settings differing from defaults to prove the injected configured parser is used.

## Validation gate

Before Task 05:

- Dedicated Lucene extractor tests pass.
- Relevant existing `TestLuceneToJexlQueryParser` phrase tests pass.
- No default parser is constructed inside the extractor.
- Raw phrase terms are not emitted as standalone terms unless explicitly present outside the phrase.

Suggested command:

```bash
mvn -pl warehouse/query-core -am -DskipITs \
  -Dtest=LuceneSearchExpressionExtractorTest,TestLuceneToJexlQueryParser \
  -Dsurefire.failIfNoSpecifiedTests=false test
```


## Completion notes

- Added `LuceneSearchExpressionExtractor`, which consumes the injected `LuceneSyntaxQueryParser` tree, preserves positive/negative and phrase/standalone provenance, applies configured field eligibility, and emits ordered phrase criteria.
- Added generated-JEXL merging that imports only `ProximityExpression` alternatives (not analyzer-generated equality predicates), with structural de-duplication through `SearchExpressions`; generated proximity distance is preferred for slop phrases.
- Added focused coverage for fielded phrase provenance, explicit standalone terms, parser injection, and generated-expression filtering in `LuceneSearchExpressionExtractorTest`.
- Validation passed: `mvn -pl warehouse/query-core -am -DskipITs -Dtest=LuceneSearchExpressionExtractorTest,TestLuceneToJexlQueryParser -Dsurefire.failIfNoSpecifiedTests=false test`.
- Deliberate scope boundary: no `ShardQueryLogic` or query initialization/macro flow was changed; generated-JEXL input remains an explicit extractor method argument.
