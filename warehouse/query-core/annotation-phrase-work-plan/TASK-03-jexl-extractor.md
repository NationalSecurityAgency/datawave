# Task 03 — JEXL Structured Expression Extraction

## Objective

Implement positive standalone and content-function extraction from original, pre-planning JEXL without losing function argument order.

## Prerequisite

Tasks 01–02 validation gates passed.

## Read first

- Prior task completion notes and new criteria interfaces.
- `src/main/java/datawave/query/transformer/annotation/TermExtractor.java`
- `src/test/java/datawave/query/transformer/annotation/TermExtractorTest.java`
- `src/main/java/datawave/query/jexl/functions/ContentFunctions.java`
- `src/main/java/datawave/query/jexl/functions/ContentFunctionsDescriptor.java` — signatures only; note its term `Set` is unsuitable for preserving order
- `src/main/java/datawave/query/jexl/visitors/PushdownNegationVisitor.java`
- `src/main/java/datawave/query/postprocessing/tf/FunctionReferenceVisitor.java`
- Relevant tests:
  - `src/test/java/datawave/query/postprocessing/tf/FunctionReferenceVisitorTest.java`
  - `src/test/java/datawave/query/jexl/visitors/PushdownNegationVisitorTest.java`

## Implement

Create the JEXL half of the query expression extractor:

1. Parse original JEXL and push down negations.
2. Preserve current positive `==` and `=~` behavior and normalization.
3. Extract positive only:
   - `content:phrase`: ordered, pairwise distance 1;
   - `content:adjacent`: unordered, total window `componentCount - 1`;
   - `content:within`: unordered, numeric configured window.
4. Ignore `content:scoredPhrase`.
5. Parse function AST arguments directly and preserve phrase argument order and duplicates.
6. Support existing fielded/unfielded signatures, including field expressions where legal.
7. Field eligibility:
   - unfielded functions are always included;
   - `_ANYFIELD_` in configured fields accepts all fielded functions;
   - otherwise ignore a fielded function unless at least one targeted field is configured;
   - preserve existing case-insensitive field comparison.
8. Exclude effective negations, including nested odd negations; retain double-negated expressions.
9. Do not infer standalone terms from function components.

Keep `TermExtractor.extract()` working for existing callers, preferably by delegation to shared standalone extraction rather than duplicated AST logic.

## Tests

Add a dedicated extractor test covering:

- every supported function and fielded/unfielded signature;
- argument order and repeated terms;
- numeric and unary-minus parsing behavior consistent with existing validation;
- supported functions under positive, negative, and double-negative ancestry;
- `_ANYFIELD_`, accepted fields, rejected fields, and unfielded always accepted;
- phrase plus explicit standalone term with the same literal;
- regex standalone terms;
- `content:scoredPhrase` ignored;
- malformed/unsupported function inputs handled consistently with existing extraction failure behavior.

## Validation gate

Before Task 04:

- New JEXL extractor tests pass.
- `TermExtractorTest` passes unchanged.
- Existing content-function parser/visitor tests selected above pass.
- Ordered terms and duplicate components are demonstrably not converted to a set.

Suggested command:

```bash
mvn -pl warehouse/query-core -am -DskipITs \
  -Dtest=JexlSearchExpressionExtractorTest,TermExtractorTest,FunctionReferenceVisitorTest,PushdownNegationVisitorTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

## Completion notes

- Added `JexlSearchExpressionExtractor` for original JEXL, including positive EQ/ER terms, ordered phrase, adjacent, and within expressions; function arguments are walked directly so order and duplicates are retained.
- Effective negations are excluded, scored phrases are ignored, fielded functions honor configured fields and `_ANYFIELD_`, and unfielded functions remain eligible. Legacy `TermExtractor` now delegates its standalone AST walk to the shared implementation.
- Added focused coverage in `JexlSearchExpressionExtractorTest` for function forms, ordering, duplicates, field eligibility, negation, scoredPhrase, regex, and numeric validation.
- Validation passed: `mvn -pl warehouse/query-core -am -DskipITs -Dtest=JexlSearchExpressionExtractorTest,TermExtractorTest,FunctionReferenceVisitorTest,PushdownNegationVisitorTest -Dsurefire.failIfNoSpecifiedTests=false test` (111 tests, 0 failures, 0 errors). Existing import-control output contains only repository pre-existing warnings.
