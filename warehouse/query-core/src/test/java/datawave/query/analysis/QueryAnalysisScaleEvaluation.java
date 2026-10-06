package datawave.query.analysis;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalDouble;
import java.util.Set;

import datawave.query.analysis.QueryAnalyzer.AnalysisReport;
import datawave.query.analysis.QueryAnalyzer.Syntax;
import datawave.query.analysis.QueryWorkloadSelector.Cursor;
import datawave.query.analysis.QueryWorkloadSelector.Selection;

/**
 * Opt-in scale evaluation, intentionally outside normal test discovery. Run this main class on the query-core test classpath with an optional query count
 * (default 100000). It exercises actual parsing, grouping, snapshot reporting, and full draining and checks comparison bounds. Heap figures are observations,
 * not performance guarantees. Set a fixed heap, for example -Xmx2g, when comparing runs.
 */
public final class QueryAnalysisScaleEvaluation {
    private QueryAnalysisScaleEvaluation() {}

    public static void main(String[] args) {
        int size = args.length == 0 ? 100000 : Integer.parseInt(args[0]);
        if (size < 1) {
            throw new IllegalArgumentException("Query count must be positive");
        }
        List<String> queries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            // Mostly unique fingerprints; occasional exact duplicates and literal variants exercise aggregation and later rounds.
            int identity = i % 10 == 9 ? i - 1 : i;
            String field = "FIELD_" + identity;
            String query = identity % 3 == 0 ? field + " =~ 'prefix.*'" : identity % 3 == 1 ? field + " == 'value'" : field + " >= 10";
            if (i % 10 == 8) {
                query = "SHARED == 'value_" + i + "'";
            } else if (i % 10 == 9) {
                query = queries.get(i - 1);
            }
            queries.add(query);
        }
        long start = System.nanoTime();
        QueryAnalyzer analyzer = new QueryAnalyzer();
        AnalysisReport report = analyzer.analyze(queries, Syntax.JEXL);
        long parsed = System.nanoTime();
        QueryClusterer.Result groups = new QueryClusterer().cluster(report);
        long clustered = System.nanoTime();
        if (groups.getSuccessfulCount() != size) {
            throw new AssertionError("Unexpected analysis failures");
        }
        if (groups.getComparisonCount() > (long) groups.getFingerprintCount() * groups.getOptions().getCandidateLimit()) {
            throw new AssertionError("Unbounded clustering comparisons");
        }
        Cursor cursor = new QueryWorkloadSelector().cursor(groups);
        int distinctCount = cursor.getRemainingCount();
        int checkpoint = Math.max(1, distinctCount / 2);
        QueryMinimizationReport initial = snapshotWithoutAdvancing(analyzer, cursor);
        QueryMinimizationReport partial = null;
        Set<String> selected = new HashSet<>();
        Set<String> roundGroups = new HashSet<>();
        int lastRound = 1;
        int occurrences = 0;
        while (cursor.hasNext()) {
            Selection selection = cursor.next();
            if (lastRound != selection.getRound()) {
                if (selection.getRound() != lastRound + 1) {
                    throw new AssertionError("Skipped selection round");
                }
                roundGroups.clear();
                lastRound = selection.getRound();
            }
            if (!roundGroups.add(selection.getGroupId()) || !selected.add(selection.getQuery().getInput().getQuery())) {
                throw new AssertionError("Repeated group in round or repeated query");
            }
            occurrences += selection.getOccurrenceCount();
            if (cursor.getEmittedCount() == checkpoint) {
                partial = snapshotWithoutAdvancing(analyzer, cursor);
            }
        }
        QueryMinimizationReport complete = snapshotWithoutAdvancing(analyzer, cursor);
        long drained = System.nanoTime();
        if (occurrences != size || selected.size() != new HashSet<>(queries).size()) {
            throw new AssertionError("Incomplete draining");
        }
        QueryWorkloadSelector.Options options = cursor.getOptions();
        long bound = (long) selected.size() * options.getGroupWindow() * options.getMemberWindow()
                        * (options.getFirstLandmarks() + options.getRecentLandmarks() + 1);
        if (cursor.getComparisonCount() > bound) {
            throw new AssertionError("Unbounded selection comparisons");
        }
        if (initial.getSelection().get().getEmittedCount() != 0 || initial.getSelection().get().getRemainingCount() != distinctCount
                        || initial.getSelection().get().getComparisonCount() != 0 || partial.getSelection().get().getEmittedCount() != checkpoint
                        || partial.getSelection().get().getRemainingCount() != distinctCount - checkpoint) {
            throw new AssertionError("Snapshot changed after cursor advancement");
        }
        if (complete.getSelection().get().getRepresentedOccurrenceCount() != size || complete.getSelection().get().getRemainingCount() != 0
                        || complete.getSelection().get().getCoveredGroupCount() != groups.getGroups().size()
                        || complete.getSelection().get().getCoveredFingerprintCount() != groups.getFingerprintCount()
                        || complete.getClustering().getComparisonCount() != groups.getComparisonCount()
                        || complete.getSelection().get().getComparisonCount() != cursor.getComparisonCount()) {
            throw new AssertionError("Incomplete or inconsistent minimization report");
        }
        long peakHeap = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                peakHeap += pool.getPeakUsage().getUsed();
            }
        }
        System.out.printf("queries=%d distinct=%d fingerprints=%d groups=%d rounds=%d%n", size, selected.size(), groups.getFingerprintCount(),
                        groups.getGroups().size(), lastRound);
        System.out.printf("parseSeconds=%.3f clusterSeconds=%.3f drainSeconds=%.3f%n", seconds(parsed - start), seconds(clustered - parsed),
                        seconds(drained - clustered));
        System.out.printf("clusterComparisons=%d selectionComparisons=%d heapPoolPeakSumMiB=%.1f%n", groups.getComparisonCount(), cursor.getComparisonCount(),
                        peakHeap / (1024.0 * 1024));
        System.out.println(complete.describe());
    }

    private static QueryMinimizationReport snapshotWithoutAdvancing(QueryAnalyzer analyzer, Cursor cursor) {
        int emitted = cursor.getEmittedCount();
        int remaining = cursor.getRemainingCount();
        int round = cursor.getCurrentRound();
        int groups = cursor.getCoveredGroupCount();
        int fingerprints = cursor.getCoveredFingerprintCount();
        int profiles = cursor.getCoveredProfileCount();
        int occurrences = cursor.getRepresentedOccurrenceCount();
        long comparisons = cursor.getComparisonCount();
        OptionalDouble minimumDistance = cursor.getMinimumDiversityDistance();
        OptionalDouble meanDistance = cursor.getMeanDiversityDistance();
        QueryMinimizationReport snapshot = analyzer.summarizeMinimization(cursor);
        snapshot.describe();
        if (cursor.getEmittedCount() != emitted || cursor.getRemainingCount() != remaining || cursor.getCurrentRound() != round
                        || cursor.getCoveredGroupCount() != groups || cursor.getCoveredFingerprintCount() != fingerprints
                        || cursor.getCoveredProfileCount() != profiles || cursor.getRepresentedOccurrenceCount() != occurrences
                        || cursor.getComparisonCount() != comparisons || !cursor.getMinimumDiversityDistance().equals(minimumDistance)
                        || !cursor.getMeanDiversityDistance().equals(meanDistance)) {
            throw new AssertionError("Reporting advanced or changed the selection cursor");
        }
        if (snapshot.getSelection().get().getEmittedCount() != emitted || snapshot.getSelection().get().getRemainingCount() != remaining
                        || snapshot.getSelection().get().getComparisonCount() != comparisons) {
            throw new AssertionError("Snapshot differs from the selection cursor");
        }
        return snapshot;
    }

    private static double seconds(long nanos) {
        return nanos / 1_000_000_000.0;
    }
}
