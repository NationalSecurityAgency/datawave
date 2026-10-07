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
 * Similarity defaults to field bindings (35%), context-sensitive operation counts (30%), topology (20%), and normalized complexity measurements (15%).
 * Immutable weights can change these relative contributions. The default threshold is 0.70. Protected profiles must match regardless of score. Group
 * representatives remain fixed; similarity to every other member is not guaranteed. Bounded candidate discovery can produce additional groups. Identical
 * enriched fingerprints always receive the same assignment within a run.
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
 * discovery limits, selection windows, and retained landmark counts can increase comparison work. Reporting itself performs no parameter sweep.
 *
 * <p>
 * {@link datawave.query.analysis.QueryAutoTuner} tunes a threshold and similarity weights from 1-9999 labeled raw queries through a supplied analyzer. Desired
 * bucket labels guide a reusable grouping policy rather than persistent named classification. Each distinct successful fingerprint has equal mass in the
 * B-cubed F1 objective; contradictory labels share that fingerprint's mass equally and remain in training. Duplicate occurrences and literal variants do not
 * add voting weight. Invalid/unsupported samples are excluded with diagnostics, and an entirely excluded workload fails. Protected profiles and configured
 * discovery limits remain unchanged. The default deterministic grid/refinement search evaluates at most 128 distinct settings and makes no optimality claim.
 *
 * <p>
 * Holdout validation groups complete fingerprints and requires at least two labels with four conflict-free fingerprints each. Each eligible label reserves
 * approximately 20%, with at least two holdout and two training fingerprints; conflicts and ineligible labels remain in training. After selection, the holdout
 * is clustered separately with fixed settings and never causes retuning. Scores cover eligible labels rather than every future distribution. Results expose
 * baseline/selected fit, split details, search work, and exclusion/conflict/enrichment diagnostics.
 *
 * <p>
 * {@link datawave.query.analysis.QueryTuningJson} reads strict version-1 sample JSON and reads/writes immutable configurations tied to fingerprint version 2.
 * Draft-2020-12 schemas and examples reside under {@code datawave/query/analysis} in main resources. Unknown properties, duplicate keys, trailing content,
 * incompatible versions, wrong scalar types, blank fields, duplicate supplied IDs, and unsafe numeric options are rejected. Raw text and labels are preserved.
 * Stream overloads leave caller-owned streams open; path overloads close their streams. Reuse the same deployment-configured analyzer/parser when applying
 * settings to future queries: parser configuration is not serialized. Applications invoke these APIs directly; this package supplies no CLI.
 *
 * <p>
 * Selection coverage counts emitted groups, enriched fingerprints, and protected profiles against the original clustering result. Occurrence coverage counts
 * only input identities actually emitted and their exact duplicates against successful inputs; it does not attribute an entire group to its representative.
 * Diversity statistics summarize the distance observed when each query was selected, excluding the first selection because no previous landmarks exist.
 * Snapshots retain no selection history. Empty distributions and zero-denominator rates use {@code OptionalDouble.empty()} and render as {@code n/a}.
 * Descriptions are deterministic and use locale-independent numeric formatting.
 */
package datawave.query.analysis;
