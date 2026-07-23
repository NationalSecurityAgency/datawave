# Unique / Grouping / Intermediate-Results Investigation

Investigation of bugs in the **Unique** function, the **Grouping** (`#GROUP_BY`) function, their
helper code, and the "multiple intermediate result pages returned at the same time" behavior.

Branch: `task/grouping-unique-intermediate-bugs` (based on `origin/integration`).
Executable reproductions: `warehouse/query-core/src/test/java/datawave/query/transformer/GroupingUniqueIntermediateResultBugTest.java`
(7 tests, all passing against current code — each asserts the *current buggy* behavior and documents the intended post-fix behavior inline).

All source line numbers below are relative to `origin/integration` at the time of writing.

---

## Summary of findings

| # | Severity | Area | Defect |
|---|----------|------|--------|
| 1 | **High** | `GroupingTransform` / `UniqueTransform` | Per-page timeout timer is never reset after emitting an intermediate result → a *flood* of intermediate (PARTIAL) pages. This is the "multiple intermediate result pages at the same time" symptom. |
| 2 | **High** | `UniqueTransform` (mostRecent) | mostRecent-unique **never** emits intermediate results, because `apply()` returns on the `map != null` path before the timeout check → no keep-alive pages for long-running mostRecent unique queries. |
| 3 | **Medium** | `GroupingIterator` | `hasNext()` is not idempotent and mutates state; calling it twice before `next()` silently discards a batch of grouped results. Also directly contradicts the class Javadoc's "there can be no saved state in this class." |
| 4 | **Low / needs confirmation** | `DocumentGrouper` | In `extractGroupsFromDocument()` the "unmapped" and "mapped" field-name variables are computed identically, defeating the stated purpose of the code and potentially breaking `#AVERAGE` re-aggregation under a query model. |
| 5 | **Doc-only** | multiple | Several Javadoc/behavior mismatches (details below). |
| 6 | **Medium** | `QueryOptionsFromQueryVisitor` | Multiple instances of the same aggregation function (`#SUM/#MAX/#MIN/#AVERAGE/#COUNT`) in one query silently overwrite each other — only the last survives. (issue #3411 / open PR #3412.) |

---

## Finding 1 (headline): intermediate-result timer is never reset → intermediate-page flood

### What the code does

Both transforms decide to emit an intermediate ("still working") result by comparing *now* against a
single page-start timestamp:

`GroupingTransform.apply()` (lines ~115-122):

```java
long elapsedExecutionTimeForCurrentPage = System.currentTimeMillis() - this.queryExecutionForPageStartTime;
if (elapsedExecutionTimeForCurrentPage > this.queryExecutionForPageTimeout) {
    Document intermediateResult = new Document();
    intermediateResult.setIntermediateResult(true);
    return Maps.immutableEntry(new Key(), intermediateResult);   // <-- queryExecutionForPageStartTime NOT reset
}
```

`UniqueTransform.apply()` (lines ~165-170) has the identical shape (emitted only when the current
document is a duplicate):

```java
long elapsedExecutionTimeForCurrentPage = System.currentTimeMillis() - this.queryExecutionForPageStartTime;
if (elapsedExecutionTimeForCurrentPage > this.queryExecutionForPageTimeout) {
    Document intermediateResult = new Document();
    intermediateResult.setIntermediateResult(true);
    return Maps.immutableEntry(keyDocumentEntry.getKey(), intermediateResult);   // <-- NOT reset
}
```

Neither transform ever resets `queryExecutionForPageStartTime` itself. The **only** thing that resets
it is an external call to `setQueryExecutionForPageStartTime(...)`, made once per page by
`RunningQuery.next()` → `ShardQueryLogic.setPageProcessingStartTime()`
(`RunningQuery.java:463`, `ShardQueryLogic.java:879-884`).

### Why this produces "multiple intermediate pages at the same time"

Grouping and unique are long-running query logics, so `RunningQuery` sets
`allowIntermediateEmptyPages = true` and forces `useResultsThread = true`
(`RunningQuery.java:207-211`). Results are then pulled by an **asynchronous results thread**
(`getResultsThread()`, `RunningQuery.java:250+`) into a size-1 hand-off queue, decoupled from page
boundaries.

The transform's timer is reset only at the *start* of a page (`next()`), but the results thread calls
`transform.apply()` continuously and *between* page requests. Sequence with, e.g., a 1 ms page timeout:

1. Page N `next()` resets the timer to `T`. The main loop blocks waiting on the queue.
2. The results thread accumulates until `now - T > timeout`, emits **intermediate #1**, offers it to the queue.
3. The main loop consumes it, `break`s, and returns a PARTIAL page to the client.
4. The results thread immediately loops, sees the queue is empty, and calls `apply()` again — **but the timer is still `T`** (page N+1 hasn't started yet, because the client hasn't called `next()` again). `now - T` is still `> timeout`, so it emits **intermediate #2 with zero accumulation**.
5. Repeat. Every hand-off yields another instant intermediate result.

The net effect: once the first timeout fires, the query returns a continuous burst of empty PARTIAL
pages with no real work performed between them, rather than one keep-alive page per timeout window.
The Javadoc's own description — *"If a full page is not collected before the timeout, **a** blank page
will be returned"* — implies **one** page per window, not a flood.

Even in the synchronous path, correctness depends entirely on the external reset landing before the
next `apply()` call; the transform makes no effort to pace itself.

### Fix

Reset the timer at the moment the intermediate result is produced, so the next intermediate cannot be
emitted until a fresh timeout window has actually elapsed:

```java
if (elapsedExecutionTimeForCurrentPage > this.queryExecutionForPageTimeout) {
    this.queryExecutionForPageStartTime = System.currentTimeMillis(); // pace intermediates; do real work between them
    Document intermediateResult = new Document();
    intermediateResult.setIntermediateResult(true);
    return Maps.immutableEntry(..., intermediateResult);
}
```

Apply the same change in both `GroupingTransform` and `UniqueTransform`. This makes intermediate
emission self-paced and independent of the racy external reset.

### Reproduction

`groupingTransform_emitsIntermediateFlood_becauseTimerIsNeverReset` and
`uniqueTransform_emitsIntermediateFlood_onDuplicatesBecauseTimerIsNeverReset`: with a 1 ms timeout and
a page-start time set in the past, **every** `apply()` call returns an intermediate result (5/5 and
4/4 respectively). After the fix, only the first call in each window should.

---

## Finding 2: mostRecent-unique never emits intermediate results

### What the code does

`UniqueTransform.apply()` (lines ~151-174):

```java
try {
    if (map != null) {                    // mostRecent path
        byte[] signature = getBytes(keyDocumentEntry.getValue());
        synchronized (map) {
            this.map.put(signature, keyDocumentEntry.getValue());
        }
        return null;                      // <-- always returns here; timeout check below is unreachable
    } else if (!isDuplicate(keyDocumentEntry.getValue())) {
        return keyDocumentEntry;
    }
} catch (IOException ioe) { ... }

// timeout / intermediate-result check -- only reachable on the bloom (non-mostRecent) path
long elapsedExecutionTimeForCurrentPage = System.currentTimeMillis() - this.queryExecutionForPageStartTime;
if (elapsedExecutionTimeForCurrentPage > this.queryExecutionForPageTimeout) { ... }
```

For a `_MOST_RECENT_` unique query the map is non-null, so `apply()` **always** returns `null` during
accumulation and never reaches the intermediate-result branch. mostRecent-unique also accumulates the
entire result set before `flush()` produces anything (like grouping), so it is exactly the case that
needs keep-alive pages.

### Consequence

A long-running mostRecent-unique query produces no intermediate/keep-alive pages during accumulation,
so `RunningQuery` can hit its `next()`-call timeout and fail the query
(`DatawaveErrorCode.QUERY_TIMEOUT`) instead of returning empty PARTIAL pages. This contradicts the
class Javadoc, which advertises intermediate results as a property of the transform without carving
out mostRecent.

### Fix

Perform the elapsed-time / intermediate-result check on the mostRecent path as well (e.g. before the
`return null` inside the `map != null` branch, or by restructuring so the timeout check runs for both
paths). Combine with the Finding 1 reset.

> Not covered by an executable test here because a mostRecent `UniqueTransform` requires the
> ivarator / HDFS-backed-map setup used by `UniqueTransformMostRecentTest`. The defect is evident by
> inspection of the control flow above.

---

## Finding 3: `GroupingIterator.hasNext()` is not idempotent (and the class is not stateless)

### Doc/behavior discrepancy

The class Javadoc states:

> *"Because the t-server may tear down and start a new iterator at any time after a next() call, there
> can be no saved state in this class."*

But the class keeps mutable instance state across calls: `groups`, `mostRecentKey`, `documentCount`,
and `next` (`GroupingIterator.java:41-62`). The comment is describing an aspiration the code does not
honor.

### The bug

`hasNext()` (lines 74-126) consumes up to `groupFieldsBatchSize` entries from the source, builds a
flattened document into `next`, then **clears `groups`**. It also sets `next = null` at the top of
every call. Consequently, calling `hasNext()` twice before `next()`:

1. First call: reads batch #1, prepares its flattened document into `next`.
2. Second call: sets `next = null`, reads batch #2 into the freshly-cleared `groups`, and overwrites
   `next` with batch #2's document. **Batch #1's result is silently lost.**

`java.util.Iterator` requires `hasNext()` to be idempotent and side-effect-free with respect to
iteration position. Any wrapper or defensive caller that calls `hasNext()` more than once per element
will drop data.

### Fix

Cache the computed `next` and make `hasNext()` a no-op when a result is already prepared (the standard
"compute-ahead iterator" pattern, as used by `DatawaveTransformIterator`), or move the batch-advancing
work into `next()`. Also correct or remove the misleading "no saved state" Javadoc.

### Reproduction

`groupingIterator_doubleHasNext_silentlyDiscardsAGroup` (two distinct groups, batch size 1): after two
`hasNext()` calls only the second group (`beta`) is retrievable; `alpha` is lost.
`groupingIterator_singleHasNextPerNext_returnsAllGroups` is the control showing correct usage returns
both groups.

---

## Finding 4 (needs confirmation): identical "unmapped" vs "mapped" names in `DocumentGrouper`

`DocumentGrouper.extractGroupsFromDocument()` (lines ~276-285), handling the `#AVERAGE` numerator when
re-aggregating already-grouped documents:

```java
} else if (field.getBase().endsWith(FIELD_AVERAGE_NUMERATOR_SUFFIX)) {
    String unmappedFieldName = removeSuffix(field.getBase(), FIELD_AVERAGE_NUMERATOR_SUFFIX);
    String fieldName = removeSuffix(field.getBase(), FIELD_AVERAGE_NUMERATOR_SUFFIX);   // identical to unmappedFieldName
    // ... comment: "It's possible that the divisor will be stored under a previously unmapped field
    //     name ... Use the original field name (e.g. ETA) to ensure we find the corresponding divisor"
    String divisorField = unmappedFieldName + FIELD_AVERAGE_DIVISOR_SUFFIX + "." + field.getInstance();
    TypeAttribute<BigDecimal> divisorAttribute = (TypeAttribute<BigDecimal>) document.get(divisorField);
    ...
    fieldAggregator.mergeAggregator(AverageAggregator.of(fieldName, numeratorAttribute, divisorAttribute));
}
```

The comment describes an intended distinction between the *unmapped* original field name (for finding
the divisor entry in the document) and the *mapped* model name (for the aggregator). Both variables are
computed identically from `field.getBase()`, so the distinction is not implemented. Whether this is an
active bug depends on whether `field.getBase()` is model-mapped for aggregation-suffixed fields (it is
mapped in `parseField()` via `getMappedFieldName()`, but only if the suffixed key is itself a model
key, which it normally isn't). Under a query model that maps the aggregated field (e.g. `ETA -> AG`),
the divisor lookup and the aggregator's field name can disagree, corrupting the re-aggregated
`#AVERAGE`.

**Recommended:** add a `DocumentGrouperTest` case that runs `#AVERAGE` with a query model applied and a
document already flattened by `GroupingIterator` (i.e. the `isDocumentAlreadyGrouped()` path), and
confirm the average survives re-aggregation. If it does not, one of the two names must be derived from
the *unmapped* field.

---

## Finding 6: duplicate aggregation functions overwrite each other (issue #3411 / PR #3412)

### What the code does

`QueryOptionsFromQueryVisitor.visit(ASTFunctionNode, ...)` translates each grouping function in the
query into an entry in the options map. The grouping/uniqueness functions route through **merge**
helpers, so repeating them accumulates fields:

- `GROUPBY` → `updateGroupByFieldsOption(...)`
- `UNIQUE` / `unique_by_*` → `updateUniqueFieldsOption(...)`
- `EXCERPT` / `LENIENT` / `STRICT` / `RENAME` / `NO_EXPANSION` → `updateFieldsOption(...)`

But the five aggregation functions overwrite instead (lines ~351-380):

```java
case QueryFunctions.SUM: {
    List<String> options = new ArrayList<>();
    this.visit(node, options);
    optionsMap.put(QueryParameters.SUM_FIELDS, JOINER.join(options));   // <-- overwrites any prior #SUM
    return null;
}
// ... identical shape for MAX, MIN, AVERAGE, COUNT
```

Because it is a plain `Map.put`, a second occurrence of the same aggregation function replaces the
first. For the query from issue #3411:

```
UUID =~ '^[CS].*' && f:groupby('GENDER') && f:average('AGE', VALUE) && f:max('AGE') && f:average('VALUE')
```

`AVERAGE_FIELDS` ends up as just `VALUE` — the `AGE` average requested by the first `f:average(...)` is
silently dropped, so the results are missing an entire requested aggregation with no error.

### Consequence

Users combining multiple instances of the same aggregation function get silently incomplete results.
This is inconsistent with `#GROUPBY` and `#UNIQUE`, which merge, and violates the least-surprise
expectation that all requested aggregations are honored.

### Fix

Route the aggregation cases through the same merge helper the other functions use, e.g.
`updateFieldsOption(optionsMap, QueryParameters.SUM_FIELDS, options)` (this is exactly what the open
**PR #3412** does for all five functions).

### Reproduction

Added to `GroupingUniqueIntermediateResultBugTest`, driving `QueryOptionsFromQueryVisitor.collect(...)`
directly (no Accumulo):

- `multipleAverageFunctions_dropAllButTheLast_becauseOptionsMapIsOverwritten` — the issue #3411 query;
  `AVERAGE_FIELDS == "VALUE"` (last wins) while `GROUP_FIELDS`/`MAX_FIELDS` (single instances) are correct.
- `multipleAggregationFunctionsOfSameType_onlyLastInstanceSurvives` — same overwrite for SUM/MIN/MAX/COUNT/AVERAGE.
- `multipleGroupByAndUniqueFunctions_mergeCorrectly_control` — control proving `#GROUPBY`/`#UNIQUE` do merge,
  isolating the defect to the aggregation functions.

---

## Finding 5: other documentation / behavior discrepancies

- **`UniqueTransform.getIterator` Javadoc** (line ~114) reads *"Add phrase excerpts to the documents
  from the given iterator."* — copy/paste from an excerpt transform; it has nothing to do with unique.
- **`UniqueTransform` class Javadoc** claims the transform emits intermediate results generally; per
  Finding 2 this is false for mostRecent, and per Finding 1 the pacing is broken for all modes.
- **`GroupingTransform.apply()`** runs its timeout/intermediate check even when `keyDocumentEntry ==
  null` (the check sits outside the null guard, unlike `UniqueTransform`). Harmless in the normal flow
  (entries are non-null) but inconsistent between the two transforms and worth aligning.
- **`GroupingUtils.createDocument(Group, List<Key>, ...)`** is `@Deprecated` with a "Do not use this
  method" note but is retained; confirm no remaining callers and remove.

---

## Test coverage added

`warehouse/query-core/src/test/java/datawave/query/transformer/GroupingUniqueIntermediateResultBugTest.java`
(JUnit 5, no Accumulo required, `Tests run: 4, Failures: 0`):

| Test | Finding |
|------|---------|
| `groupingTransform_emitsIntermediateFlood_becauseTimerIsNeverReset` | 1 |
| `uniqueTransform_emitsIntermediateFlood_onDuplicatesBecauseTimerIsNeverReset` | 1 |
| `groupingIterator_doubleHasNext_silentlyDiscardsAGroup` | 3 |
| `groupingIterator_singleHasNextPerNext_returnsAllGroups` | 3 (control) |
| `multipleAverageFunctions_dropAllButTheLast_becauseOptionsMapIsOverwritten` | 6 |
| `multipleAggregationFunctionsOfSameType_onlyLastInstanceSurvives` | 6 |
| `multipleGroupByAndUniqueFunctions_mergeCorrectly_control` | 6 (control) |

Each intermediate-result test asserts the *current* (buggy) behavior and documents the intended
post-fix assertion inline, so the same file becomes the regression test once the fixes land.

### End-to-end harness already present

`warehouse/query-core/src/test/java/datawave/query/IntermediateResultsQueryTest.java` (uses
`ShapesIngest` + a `DelayIterator` + a 1 ms page timeout, driving a real `RunningQuery`) is the natural
home for an end-to-end assertion on Finding 1. It already counts `intermediatePageCount`, but currently
only asserts `>= expectedMinimumIntermediatePages` (default 0), so a flood passes unnoticed. A targeted
follow-up test could assert an *upper bound* on intermediate pages for a grouping query to lock in the
Finding 1 fix at the integration level.

---

## Note on building in this environment

The change is test-only in `warehouse/query-core` and was verified with
`mvn -o -pl warehouse/query-core -Dtest=GroupingUniqueIntermediateResultBugTest test` (passes). A full
`-am install` currently fails to *compile* the unrelated upstream module `web-services/security`
(`package java.security.acl does not exist` — that package was removed in modern JDKs); this is a
pre-existing environment/JDK issue, not a product of this change.
