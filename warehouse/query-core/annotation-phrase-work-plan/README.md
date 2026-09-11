# Annotation Phrase Hits — Sequential Work Plan

This directory is the execution plan for a single coding subagent working sequentially. Complete tasks in order. Do not begin a task until the previous task's validation gate passes.

## Goal

Extend `AnnotationHitsTransformer` from standalone keyword matching to structured standalone, phrase, adjacent, and within matching while preserving the existing `AllHits` JSON model and keyword-only behavior.

## Non-negotiable semantics

- Sources:
  - Direct JEXL: original pre-planning JEXL.
  - Lucene: macro-expanded original Lucene plus generated `ShardQueryConfiguration.getOriginalJexlQuery()` alternatives.
  - Nonblank `annotation.all.hits.keywords`: complete override of query-derived expressions.
- Positive expressions only; ignore effective negations. Search every positive expression whether or not it caused the document match.
- Supported proximity forms:
  - `content:phrase`: ordered, pairwise distance 1.
  - exact Lucene quoted phrase: ordered, pairwise distance 1.
  - `content:adjacent`: unordered, total window `termCount - 1`.
  - `content:within`: unordered, configured total window.
  - Lucene phrase slop follows the generated `content:within` distance.
  - `content:scoredPhrase` is out of scope.
- Distance uses ordinal positions in the sorted boundary sequence, never timestamp/start/end gaps.
- Every constituent `SegmentValue` must meet `minScore`; one value can satisfy at most one component of one occurrence.
- `flattenBoundary=false` by default: one component per boundary; values are alternatives.
- `flattenBoundary=true`: distinct values in one boundary may satisfy successive components in any order, each same-boundary transition has virtual distance 1, and a value index cannot be reused.
- Context remains boundary-counted. A phrase gets up to `contextSize` boundaries outside its full span on both sides.
- Retain genuinely distinct/overlapping occurrences. Deduplicate exact constituent identities and equivalent analyzer alternatives.
- Preserve once-only serialized hit behavior when the same value is both standalone and a phrase constituent.
- Preserve `AllHits`, `AllHit`, `Term`, and `TermHit` JSON structure.

## Tracking

- [x] [Task 01 — Core models and configuration](TASK-01-core-models-config.md)
- [x] [Task 02 — Explicit keyword parser](TASK-02-keyword-parser.md)
- [x] [Task 03 — JEXL structured extraction](TASK-03-jexl-extractor.md)
- [x] [Task 04 — Lucene extraction and alternative merging](TASK-04-lucene-extractor.md)
- [x] [Task 05 — Annotation position model and standalone matcher](TASK-05-position-model.md)
- [x] [Task 06 — Ordered phrase matcher](TASK-06-ordered-matcher.md)
- [ ] [Task 07 — Unordered adjacent/within matcher](TASK-07-unordered-matcher.md)
- [ ] [Task 08 — PhraseHit and compatible AllHitsFactory extension](TASK-08-hit-factory.md)
- [ ] [Task 09 — Transformer and ShardQueryLogic integration](TASK-09-integration.md)
- [ ] [Task 10 — End-to-end validation and cleanup](TASK-10-final-validation.md)

## Working protocol

For every task:

1. Read only the context references listed in that task first; follow directly relevant types as needed.
2. Inspect current changes from prior tasks before editing.
3. Keep each intermediate state compiling.
4. Add focused tests in the same task as production code.
5. Run the task's validation commands. If repository build infrastructure prevents a command, record the exact command and failure; do not silently treat it as passing.
6. Update this checklist only after all validation criteria pass.
7. Add a short `## Completion notes` section to the task file with changed files, commands run, and any deliberate deviations.
8. Do not broaden scope to response-schema changes, `content:scoredPhrase`, or annotation-value tokenization.

## Build guidance

From repository root, focused tests are expected to use a command similar to:

```bash
mvn -pl warehouse/query-core -am -DskipITs -Dtest=TestClassName -Dsurefire.failIfNoSpecifiedTests=false test
```

If `-am` causes unrelated modules to reject `-Dtest`, retain `-Dsurefire.failIfNoSpecifiedTests=false`, or build prerequisites once and run from `warehouse/query-core`. Use the project's existing formatting/checkstyle conventions; do not reformat unrelated code.
