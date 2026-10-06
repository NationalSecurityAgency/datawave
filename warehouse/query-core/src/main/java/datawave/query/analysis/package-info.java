/**
 * Offline query workload analysis using DataWave's Lucene and JEXL parsers.
 *
 * <pre>
 * QueryAnalyzer.AnalysisReport analysis = new QueryAnalyzer().analyze(queries, QueryAnalyzer.Syntax.JEXL);
 * QueryClusterer.Result groups = new QueryClusterer().cluster(analysis);
 * QueryWorkloadSelector.Cursor cursor = new QueryWorkloadSelector().cursor(groups);
 * List&lt;QueryWorkloadSelector.Selection&gt; firstBatch = cursor.take(100);
 * // Later calls continue the same sequence until all distinct text/syntax pairs have been emitted.
 * </pre>
 *
 * Existing exact signatures and clusters remain available on the analysis report. Enriched version-2 fingerprints add regex access classes, Boolean structure,
 * field/operator bindings, counts, markers, and selected function controls. Function-specific enrichment covers filter:compare,
 * filter:includeRegex/excludeRegex/getAllMatches/matchesAtLeastCountOf, and content:within/phrase/adjacent. Other functions retain their name, ordered argument
 * shapes, and arity; their literal parameters receive no additional semantic interpretation. These features do not establish executability or identical plans:
 * field indexing, data distributions, normalization, and planner configuration are not available here.
 *
 * <p>
 * Similarity combines field bindings (35%), context-sensitive operation counts (30%), topology (20%), and normalized complexity measurements (15%). The default
 * threshold is 0.70. Protected profiles must match regardless of score. Group representatives remain fixed; similarity to every other member is not guaranteed.
 * Bounded candidate discovery can produce additional groups. Identical enriched fingerprints always receive the same assignment within a run.
 *
 * <p>
 * Selection emits a representative from each group first, then continues in balanced rounds. A window of 16 eligible groups and up to eight members per group
 * competes against the first four and latest four selections, plus each previously visited group's representative. Unvisited enriched fingerprints precede
 * further literal variants. Ranking favors protected differences, but makes no claim of globally optimal diversity. Exact query text and syntax are
 * deduplicated for selection only; each selection retains all input indexes and its occurrence count.
 *
 * <p>
 * Limits are configurable through immutable options. Grouping performs at most {@code uniqueFingerprints * candidateLimit} similarity comparisons. Selection
 * performs at most {@code distinctQueries * groupWindow * memberWindow * (firstLandmarks + recentLandmarks + 1)} comparisons. Feature extraction and comparison
 * costs also depend on query size. Neither operation allocates a dense pairwise distance matrix. Input permutations produce the same group identities and
 * selected query identities, with indexes remapped to the current input. A cursor is mutable, in-memory, and not thread safe; a new cursor deterministically
 * replays the sequence.
 */
package datawave.query.analysis;
