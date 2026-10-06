package datawave.query.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.junit.Test;

import datawave.query.analysis.QueryAnalyzer.QueryInput;
import datawave.query.analysis.QueryAnalyzer.Syntax;
import datawave.query.analysis.QueryClusterer.Group;
import datawave.query.analysis.QueryClusterer.Result;
import datawave.query.analysis.QueryWorkloadSelector.Cursor;
import datawave.query.analysis.QueryWorkloadSelector.Options;
import datawave.query.analysis.QueryWorkloadSelector.Selection;

public class QueryWorkloadSelectorTest {
    @Test
    public void drainsEveryDistinctQueryOnceWithRoundFairnessAndBoundedWork() {
        List<String> queries = new ArrayList<>();
        for (int group = 0; group < 40; group++) {
            for (int member = 0; member <= group % 5; member++) {
                queries.add("FIELD_" + group + " == '" + member + "'");
            }
        }
        queries.add(queries.get(0));
        queries.add("FIELD_ ==");
        Result groups = cluster(queries);
        Options options = new Options(3, 2, 1, 1);
        Cursor cursor = new QueryWorkloadSelector().cursor(groups, options);
        List<Selection> selections = cursor.take(Integer.MAX_VALUE);
        assertEquals(120, selections.size());
        assertEquals(120, cursor.getEmittedCount());
        assertEquals(0, cursor.getRemainingCount());
        assertFalse(cursor.hasNext());
        assertThrows(NoSuchElementException.class, cursor::next);
        assertEquals(120, selections.stream().map(selection -> QueryFingerprint.inputKey(selection.getQuery())).distinct().count());
        assertEquals(121, selections.stream().mapToInt(Selection::getOccurrenceCount).sum());
        assertFairRounds(groups, selections);
        assertTrue(cursor.getComparisonCount() <= 120L * 3 * 2 * 3);
        assertThrows(UnsupportedOperationException.class, selections::clear);
        assertThrows(UnsupportedOperationException.class, () -> selections.get(0).getInputIndexes().clear());
    }

    @Test
    public void deduplicatesTextAndSyntaxAndReportsAllOriginalIndexes() {
        List<QueryInput> inputs = List.of(new QueryInput("A == 'x'", Syntax.JEXL), new QueryInput("A:x", Syntax.LUCENE),
                        new QueryInput("A == 'x'", Syntax.JEXL), new QueryInput("A == 'y'", Syntax.JEXL));
        Result groups = new QueryClusterer().cluster(new QueryAnalyzer().analyze(inputs));
        assertEquals(1, groups.getGroups().size());
        List<Selection> selections = new QueryWorkloadSelector().cursor(groups).take(100);
        assertEquals(3, selections.size());
        Selection duplicate = selections.stream().filter(selection -> selection.getOccurrenceCount() == 2).findFirst().get();
        assertEquals(List.of(0, 2), duplicate.getInputIndexes());
        assertEquals("A == 'x'", duplicate.getQuery().getInput().getQuery());
        assertEquals(List.of(1, 2, 3), selections.stream().map(Selection::getRound).collect(Collectors.toList()));
    }

    @Test
    public void supportsIncrementalPullAndReproducibleReplay() {
        List<String> queries = new ArrayList<>(List.of("A == 1", "A == 2", "B == 1", "A =~ 'a.*'", "A =~ 'b.*'", "A =~ '.*a.*'", "A >= 2", "A =="));
        Result groups = cluster(queries);
        Cursor cursor = new QueryWorkloadSelector().cursor(groups);
        assertTrue(cursor.take(0).isEmpty());
        List<Selection> incremental = new ArrayList<>(cursor.take(2));
        incremental.add(cursor.next());
        incremental.addAll(cursor.take(100));
        List<Selection> replay = new QueryWorkloadSelector().cursor(groups).take(100);
        assertEquals(identities(replay), identities(incremental));
        Collections.shuffle(queries, new Random(43));
        List<Selection> shuffled = new QueryWorkloadSelector().cursor(cluster(queries)).take(100);
        assertEquals(identities(replay), identities(shuffled));
        for (Selection selection : shuffled) {
            for (int index : selection.getInputIndexes()) {
                assertEquals(selection.getQuery().getInput().getQuery(), queries.get(index));
            }
        }
    }

    @Test
    public void coversProtectedDifferencesEarlyAndPrefersUnvisitedFingerprints() {
        Result groups = cluster(List.of("A == 1", "B == 1", "C == 1", "A =~ 'a.*'", "A =~ '.*a.*'"));
        List<Selection> early = new QueryWorkloadSelector().cursor(groups).take(2);
        assertNotEquals(early.get(0).getQuery().getFingerprint().getProtectedProfile(), early.get(1).getQuery().getFingerprint().getProtectedProfile());
        assertTrue(early.get(1).getDiversityDistance() >= 0.6);

        Result near = cluster(List.of("A == 1 && B == 2", "A == 4 && B == 5", "A == 1 && B == 2 && C == 3", "A == 4 && B == 5 && C == 6"));
        assertEquals(1, near.getGroups().size());
        List<Selection> selections = new QueryWorkloadSelector().cursor(near).take(4);
        assertNotEquals(selections.get(0).getQuery().getFingerprint().getKey(), selections.get(1).getQuery().getFingerprint().getKey());
        assertEquals(QueryFingerprint.inputKey(near.getGroups().get(0).getRepresentative()), QueryFingerprint.inputKey(selections.get(0).getQuery()));
    }

    @Test
    public void snapshotsTrackOnlyEmittedQueriesWithoutAdvancingOrChangingSelection() {
        Result groups = cluster(List.of("A == 1 && B == 2", "A == 1 && B == 2", "A == 4 && B == 5", "A == 1 && B == 2 && C == 3", "A == 4 && B == 5 && C == 6",
                        "A =~ 'a.*'", "A =~ 'b.*'", "A =="));
        Options options = new Options(2, 2, 1, 2);
        Cursor cursor = new QueryWorkloadSelector().cursor(groups, options);
        Cursor replay = new QueryWorkloadSelector().cursor(groups, options);
        QueryAnalyzer analyzer = new QueryAnalyzer();
        List<Selection> selected = new ArrayList<>();
        QueryMinimizationReport initial = analyzer.summarizeMinimization(cursor);
        assertSame(groups, cursor.getClusteringResult());
        assertCursorDiagnostics(cursor, selected);
        QueryMinimizationReport partial = null;
        QueryMinimizationReport first = null;
        while (cursor.hasNext()) {
            Selection actual = cursor.next();
            Selection expected = replay.next();
            selected.add(actual);
            assertEquals(identities(List.of(expected)), identities(List.of(actual)));
            assertEquals(expected.getDiversityDistance(), actual.getDiversityDistance(), 0);
            assertEquals(replay.getComparisonCount(), cursor.getComparisonCount());
            assertCursorDiagnostics(cursor, selected);
            for (int snapshot = 0; snapshot < 3; snapshot++) {
                QueryMinimizationReport report = analyzer.summarizeMinimization(cursor);
                report.describe();
                assertSnapshotDiagnostics(cursor, report);
                assertEquals(replay.getEmittedCount(), cursor.getEmittedCount());
                assertEquals(replay.getRemainingCount(), cursor.getRemainingCount());
                assertEquals(replay.getComparisonCount(), cursor.getComparisonCount());
            }
            if (selected.size() == 1) {
                first = analyzer.summarizeMinimization(cursor);
                assertTrue(cursor.getRepresentedOccurrenceCount() < groups.getGroups().stream().filter(group -> group.getId().equals(actual.getGroupId()))
                                .findFirst().get().getOccurrenceCount());
            } else if (selected.size() == 2) {
                partial = analyzer.summarizeMinimization(cursor);
            }
        }
        assertFalse(replay.hasNext());
        assertEquals(6, cursor.getEmittedCount());
        assertEquals(7, cursor.getRepresentedOccurrenceCount());
        assertEquals(groups.getGroups().size(), cursor.getCoveredGroupCount());
        assertEquals(groups.getFingerprintCount(), cursor.getCoveredFingerprintCount());
        assertEquals(0, initial.getSelection().get().getEmittedCount());
        assertEquals(6, initial.getSelection().get().getRemainingCount());
        assertEquals(0, initial.getSelection().get().getRepresentedOccurrenceCount());
        assertFalse(initial.getSelection().get().getMinimumDiversityDistance().isPresent());
        assertEquals(1, first.getSelection().get().getEmittedCount());
        assertFalse(first.getSelection().get().getMeanDiversityDistance().isPresent());
        assertEquals(2, partial.getSelection().get().getEmittedCount());
        assertEquals(4, partial.getSelection().get().getRemainingCount());
        assertEquals(selected.get(1).getDiversityDistance(), partial.getSelection().get().getMinimumDiversityDistance().getAsDouble(), 0);
        assertEquals(selected.get(1).getDiversityDistance(), partial.getSelection().get().getMeanDiversityDistance().getAsDouble(), 0);
        assertSnapshotDiagnostics(cursor, analyzer.summarizeMinimization(cursor));
    }

    private void assertCursorDiagnostics(Cursor cursor, List<Selection> selected) {
        assertEquals(selected.size(), cursor.getEmittedCount());
        assertEquals(selected.isEmpty() ? 0 : selected.get(selected.size() - 1).getRound(), cursor.getCurrentRound());
        assertEquals(selected.stream().map(Selection::getGroupId).distinct().count(), cursor.getCoveredGroupCount());
        assertEquals(selected.stream().map(selection -> selection.getQuery().getFingerprint().getKey()).distinct().count(),
                        cursor.getCoveredFingerprintCount());
        assertEquals(selected.stream().map(selection -> selection.getQuery().getFingerprint().getProtectedProfile()).distinct().count(),
                        cursor.getCoveredProfileCount());
        assertEquals(selected.stream().mapToInt(Selection::getOccurrenceCount).sum(), cursor.getRepresentedOccurrenceCount());
        if (selected.size() < 2) {
            assertFalse(cursor.getMinimumDiversityDistance().isPresent());
            assertFalse(cursor.getMeanDiversityDistance().isPresent());
        } else {
            assertEquals(selected.stream().skip(1).mapToDouble(Selection::getDiversityDistance).min().getAsDouble(),
                            cursor.getMinimumDiversityDistance().getAsDouble(), 0);
            assertEquals(selected.stream().skip(1).mapToDouble(Selection::getDiversityDistance).average().getAsDouble(),
                            cursor.getMeanDiversityDistance().getAsDouble(), 1e-12);
        }
    }

    private void assertSnapshotDiagnostics(Cursor cursor, QueryMinimizationReport report) {
        assertEquals(cursor.getEmittedCount(), report.getSelection().get().getEmittedCount());
        assertEquals(cursor.getRemainingCount(), report.getSelection().get().getRemainingCount());
        assertEquals(cursor.getCurrentRound(), report.getSelection().get().getCurrentRound());
        assertEquals(cursor.getCoveredGroupCount(), report.getSelection().get().getCoveredGroupCount());
        assertEquals(cursor.getCoveredFingerprintCount(), report.getSelection().get().getCoveredFingerprintCount());
        assertEquals(cursor.getCoveredProfileCount(), report.getSelection().get().getCoveredProfileCount());
        assertEquals(cursor.getRepresentedOccurrenceCount(), report.getSelection().get().getRepresentedOccurrenceCount());
        assertEquals(cursor.getComparisonCount(), report.getSelection().get().getComparisonCount());
        assertSame(cursor.getOptions(), report.getSelection().get().getOptions());
        assertEquals(cursor.getMinimumDiversityDistance(), report.getSelection().get().getMinimumDiversityDistance());
        assertEquals(cursor.getMeanDiversityDistance(), report.getSelection().get().getMeanDiversityDistance());
    }

    @Test
    public void handlesEmptyAndInvalidOptions() {
        Cursor empty = new QueryWorkloadSelector().cursor(cluster(List.of()));
        assertFalse(empty.hasNext());
        assertCursorDiagnostics(empty, List.of());
        assertSnapshotDiagnostics(empty, new QueryAnalyzer().summarizeMinimization(empty));
        assertTrue(empty.take(10).isEmpty());
        assertThrows(NoSuchElementException.class, empty::next);
        assertThrows(IllegalArgumentException.class, () -> empty.take(-1));
        assertThrows(IllegalArgumentException.class, () -> new Options(0, 8, 4, 4));
        assertThrows(IllegalArgumentException.class, () -> new Options(16, 0, 4, 4));
        assertThrows(IllegalArgumentException.class, () -> new Options(16, 8, 0, 4));
        assertThrows(IllegalArgumentException.class, () -> new Options(16, 8, 4, 0));
    }

    static void assertFairRounds(Result groups, List<Selection> selections) {
        Map<String,Integer> remaining = new TreeMap<>();
        for (Group group : groups.getGroups()) {
            remaining.put(group.getId(), group.getDistinctQueryCount());
        }
        int round = 1;
        Set<String> visited = new HashSet<>();
        for (Selection selection : selections) {
            if (selection.getRound() != round) {
                assertEquals(round + 1, selection.getRound());
                for (Map.Entry<String,Integer> entry : remaining.entrySet()) {
                    assertTrue("Skipped active group " + entry.getKey(), entry.getValue() == 0 || visited.contains(entry.getKey()));
                }
                visited.clear();
                round++;
            }
            assertTrue("Group visited twice in round " + round, visited.add(selection.getGroupId()));
            assertTrue(remaining.get(selection.getGroupId()) > 0);
            remaining.compute(selection.getGroupId(), (key, count) -> count - 1);
        }
        assertTrue(remaining.values().stream().allMatch(count -> count == 0));
    }

    private Result cluster(List<String> queries) {
        return new QueryClusterer().cluster(new QueryAnalyzer().analyze(queries, Syntax.JEXL));
    }

    private List<String> identities(List<Selection> selections) {
        return selections.stream().map(selection -> selection.getGroupId() + ":" + selection.getRound() + ":" + QueryFingerprint.inputKey(selection.getQuery()))
                        .collect(Collectors.toList());
    }
}
