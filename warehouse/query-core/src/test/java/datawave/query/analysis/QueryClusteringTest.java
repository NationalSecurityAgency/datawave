package datawave.query.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.junit.Test;

import datawave.query.analysis.QueryAnalyzer.AnalysisReport;
import datawave.query.analysis.QueryAnalyzer.QueryAnalysis;
import datawave.query.analysis.QueryAnalyzer.Syntax;
import datawave.query.analysis.QueryClusterer.Group;
import datawave.query.analysis.QueryClusterer.Options;
import datawave.query.analysis.QueryClusterer.Result;

public class QueryClusteringTest {
    @Test
    public void approximateGroupingKeepsLegacyExactClusters() {
        AnalysisReport report = new QueryAnalyzer().analyze(List.of("A == 1 && B == 2", "A == 1 && B == 2 && C == 3", "A == 1 && B == 2 && C == 3",
                        "A == 1 && B =~ 'x.*'", "A == 1 && B =~ '.*x.*'", "A =="), Syntax.JEXL);
        assertEquals(3, report.getClusters().size());
        Result result = new QueryClusterer().cluster(report);
        assertEquals(3, result.getGroups().size());
        assertEquals(6, result.getInputCount());
        assertEquals(5, result.getSuccessfulCount());
        assertEquals(1, result.getExcludedCount());
        assertEquals(4, result.getFingerprintCount());
        assertTrue(result.getGroups().stream().anyMatch(group -> group.getOccurrenceCount() == 3 && group.getDistinctQueryCount() == 2));
        assertMembership(result);
        assertEquals(3, report.getClusters().size());
        assertThrows(UnsupportedOperationException.class, () -> result.getGroups().clear());
        assertThrows(UnsupportedOperationException.class, () -> result.getGroups().get(0).getMembers().clear());
    }

    @Test
    public void doesNotDriftThroughSimilarityChains() {
        // A~B and B~C, but A and C have no bindings in common. Regardless of seed order, each member must qualify against the fixed representative.
        List<String> queries = List.of("A == 1 && B == 2", "B == 2 && C == 3", "C == 3 && D == 4");
        AnalysisReport report = new QueryAnalyzer().analyze(queries, Syntax.JEXL);
        List<QueryAnalysis> analyzed = report.getQueries();
        assertTrue(QuerySimilarity.compare(analyzed.get(0).getFingerprint(), analyzed.get(1).getFingerprint()).getScore() >= 0.70);
        assertTrue(QuerySimilarity.compare(analyzed.get(1).getFingerprint(), analyzed.get(2).getFingerprint()).getScore() >= 0.70);
        assertTrue(QuerySimilarity.compare(analyzed.get(0).getFingerprint(), analyzed.get(2).getFingerprint()).getScore() < 0.70);
        Result result = new QueryClusterer().cluster(report);
        assertMembership(result);
        // A group seeded with the middle query may legitimately contain both ends; pairwise closeness is deliberately not promised.
        for (Group group : result.getGroups()) {
            assertTrue(group.getMembers().contains(group.getRepresentative()));
        }
    }

    @Test
    public void boundsCandidateWorkAndPreservesIdenticalFingerprintAssignments() {
        List<String> queries = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            queries.add("FIELD_" + i + " == 'x'");
            queries.add("FIELD_" + i + " == 'y'");
        }
        Result result = new QueryClusterer().cluster(new QueryAnalyzer().analyze(queries, Syntax.JEXL), new Options(0.70, 2, 3, 4));
        assertEquals(500, result.getFingerprintCount());
        assertEquals(500, result.getGroups().size());
        assertTrue(result.getComparisonCount() <= 500L * 4);
        assertTrue(result.getGroups().stream().allMatch(group -> group.getDistinctQueryCount() == 2));
        assertMembership(result);
    }

    @Test
    public void inputPermutationDoesNotChangeGroupsOrRepresentatives() {
        List<String> queries = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            queries.add("A == '" + i + "' && FIELD_" + i + " == 'x'");
        }
        queries.add(queries.get(0));
        QueryAnalyzer analyzer = new QueryAnalyzer();
        Result first = new QueryClusterer().cluster(analyzer.analyze(queries, Syntax.JEXL));
        Collections.shuffle(queries, new Random(42));
        Result second = new QueryClusterer().cluster(analyzer.analyze(queries, Syntax.JEXL));
        assertEquals(identities(first), identities(second));
        assertEquals(first.getComparisonCount(), second.getComparisonCount());
    }

    @Test
    public void handlesEmptyInvalidAndOptionLimits() {
        assertTrue(new QueryClusterer().cluster(new QueryAnalyzer().analyze(List.of(), Syntax.JEXL)).getGroups().isEmpty());
        Result invalid = new QueryClusterer().cluster(new QueryAnalyzer().analyze(List.of("A ==", "A = 1"), Syntax.JEXL));
        assertTrue(invalid.getGroups().isEmpty());
        assertEquals(2, invalid.getExcludedCount());
        for (double threshold : new double[] {-1, 2, Double.NaN, Double.POSITIVE_INFINITY}) {
            assertThrows(IllegalArgumentException.class, () -> new Options(threshold, 4, 32, 64));
        }
        assertThrows(IllegalArgumentException.class, () -> new Options(0.7, 0, 32, 64));
        AnalysisReport report = new QueryAnalyzer().analyze(List.of("A == 1 && B == 2", "A == 1 && B == 2 && C == 3"), Syntax.JEXL);
        assertEquals(2, new QueryClusterer().cluster(report, new Options(1, 4, 32, 64)).getGroups().size());
        assertFalse(new QueryClusterer().cluster(report).getGroups().isEmpty());
    }

    static void assertMembership(Result result) {
        for (Group group : result.getGroups()) {
            assertTrue(group.getMinimumSimilarity() >= result.getOptions().getThreshold());
            for (QueryAnalysis member : group.getMembers()) {
                QuerySimilarity.Result similarity = QuerySimilarity.compare(member.getFingerprint(), group.getRepresentative().getFingerprint());
                assertTrue(similarity.isCompatible());
                assertTrue(similarity.getScore() + 1e-12 >= result.getOptions().getThreshold());
            }
        }
    }

    private Map<String,List<String>> identities(Result result) {
        Map<String,List<String>> identities = new TreeMap<>();
        for (Group group : result.getGroups()) {
            identities.put(group.getId() + QueryFingerprint.inputKey(group.getRepresentative()),
                            group.getMembers().stream().map(QueryFingerprint::inputKey).collect(Collectors.toList()));
        }
        return identities;
    }
}
