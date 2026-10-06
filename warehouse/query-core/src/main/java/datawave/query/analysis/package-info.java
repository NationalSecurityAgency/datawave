/**
 * Offline query workload analysis using DataWave's Lucene and JEXL parsers.
 *
 * <pre>
 * QueryAnalyzer.AnalysisReport analysis = new QueryAnalyzer().analyze(queries, QueryAnalyzer.Syntax.JEXL);
 * QueryClusterer.Result groups = new QueryClusterer().cluster(analysis);
 * QueryWorkloadSelector.Cursor cursor = new QueryWorkloadSelector().cursor(groups);
 * QueryMinimizationReport clusteringSummary = new QueryAnalyzer().summarizeMinimization(groups);
 * List&lt;QueryWorkloadSelector.Selection&gt; firstBatch = cursor.take(100);
 * QueryMinimizationReport progress = new QueryAnalyzer().summarizeMinimization(cursor);
 * System.out.println(progress.describe());
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
 *
 * <p>
 * Minimization reports expose immutable structured clustering statistics and, for the cursor overload, a snapshot of selection progress. Reporting does not
 * advance the cursor, rerun analysis, or perform similarity comparisons. Distinct queries are exact original text/syntax identities; duplicate occurrences are
 * successful inputs beyond those identities. Exact structural families refer to the legacy signatures, while enriched fingerprints retain additional
 * planning-sensitive features. Invalid and unsupported inputs are counted separately and excluded from reduction and coverage denominators.
 *
 * <p>
 * Potential reduction is {@code 1 - groups / successfulOccurrences}, or separately {@code 1 - groups / distinctQueries}, when keeping one representative per
 * group. Size distributions use nearest-rank p50 and p95. Representative similarity counts each distinct fingerprint once, including a representative score of
 * one; group means and the overall mean use fingerprint weighting, so duplicates and literal variants do not inflate similarity. Complete group and
 * protected-profile statistics are available through the structured API. The description highlights up to five largest groups and five weakest groups with
 * multiple fingerprints, deduplicated by stable ID. It describes representative structure without printing original query text.
 *
 * <p>
 * Limit diagnostics count actual feature/candidate truncations and posting evictions observed during grouping. Group creation distinguishes new profiles from
 * cases where no considered candidate qualifies. Best rejected scores cover only the latter cases, using the best score among candidates actually compared.
 * These measurements do not prove that a compatible group was missed by bounded discovery. Threshold guidance respects protected-profile boundaries; larger
 * discovery limits, selection windows, and retained landmark counts can increase comparison work. No parameter sweep or automatic numeric recommendation is
 * performed.
 *
 * <p>
 * Selection coverage counts emitted groups, enriched fingerprints, and protected profiles against the original clustering result. Occurrence coverage counts
 * only input identities actually emitted and their exact duplicates against successful inputs; it does not attribute an entire group to its representative.
 * Diversity statistics summarize the distance observed when each query was selected, excluding the first selection because no previous landmarks exist.
 * Snapshots retain no selection history. Empty distributions and zero-denominator rates use {@code OptionalDouble.empty()} and render as {@code n/a}.
 * Descriptions are deterministic and use locale-independent numeric formatting.
 */
package datawave.query.analysis;
