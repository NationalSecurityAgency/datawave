package datawave.query.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.junit.Test;

import datawave.query.analysis.QueryAnalyzer.QueryInput;
import datawave.query.analysis.QueryAnalyzer.Syntax;
import datawave.query.analysis.QueryClusterer.Options;
import datawave.query.analysis.QueryClusterer.Result;
import datawave.query.analysis.QueryMinimizationReport.ClusteringStats;
import datawave.query.analysis.QueryMinimizationReport.GroupStats;
import datawave.query.analysis.QueryMinimizationReport.ProfileStats;
import datawave.query.analysis.QueryWorkloadSelector.Cursor;

public class QueryMinimizationReportTest {
    private final QueryAnalyzer analyzer = new QueryAnalyzer();

    @Test
    public void separatesOccurrencesIdentitiesFamiliesAndExcludedInputs() {
        List<QueryInput> inputs = List.of(new QueryInput("A == 'secret-literal'", Syntax.JEXL), new QueryInput("A == 'secret-literal'", Syntax.JEXL),
                        new QueryInput("A == 'other'", Syntax.JEXL), new QueryInput("A:other", Syntax.LUCENE), new QueryInput("B == 1", Syntax.JEXL),
                        new QueryInput("A =~ 'x.*'", Syntax.JEXL), new QueryInput("A ==", Syntax.JEXL), new QueryInput("A.size() > 0", Syntax.JEXL));
        Result result = new QueryClusterer().cluster(analyzer.analyze(inputs), new Options(1, 100, 100, 100));
        QueryMinimizationReport report = analyzer.summarizeMinimization(result);
        ClusteringStats stats = report.getClustering();
        assertEquals(8, stats.getInputCount());
        assertEquals(6, stats.getSuccessfulCount());
        assertEquals(1, stats.getInvalidCount());
        assertEquals(1, stats.getUnsupportedCount());
        assertEquals(2, stats.getExcludedCount());
        assertEquals(5, stats.getDistinctQueryCount());
        assertEquals(1, stats.getDuplicateOccurrenceCount());
        assertEquals(3, stats.getExactStructuralFamilyCount());
        assertEquals(3, stats.getFingerprintCount());
        assertEquals(3, stats.getGroupCount());
        assertEquals(2, stats.getProfileCount());
        assertEquals(0.5, stats.getOccurrenceReduction().getAsDouble(), 0);
        assertEquals(0.4, stats.getDistinctQueryReduction().getAsDouble(), 1e-15);
        assertEquals(6, stats.getProfiles().stream().mapToInt(ProfileStats::getOccurrenceCount).sum());
        assertEquals(5, stats.getProfiles().stream().mapToInt(ProfileStats::getDistinctQueryCount).sum());
        assertEquals(3, stats.getProfiles().stream().mapToInt(ProfileStats::getFingerprintCount).sum());
        assertEquals(3, stats.getProfiles().stream().mapToInt(ProfileStats::getGroupCount).sum());
        assertFalse(report.getSelection().isPresent());
        assertFalse(report.describe().contains("secret-literal"));
        assertTrue(report.describe().contains("excluded: 2"));
    }

    @Test
    public void usesNearestRankPercentilesAndDeterministicLargestHighlights() {
        List<String> queries = new ArrayList<>();
        for (int group = 1; group <= 7; group++) {
            for (int member = 0; member < group; member++) {
                queries.add("FIELD_" + group + " == " + member);
            }
            queries.add("FIELD_" + group + " == 0");
        }
        ClusteringStats stats = analyzer.summarizeMinimization(cluster(queries, new Options(1, 100, 100, 100))).getClustering();
        assertEquals(2, stats.getOccurrenceDistribution().getMinimum().getAsDouble(), 0);
        assertEquals(5, stats.getOccurrenceDistribution().getMean().getAsDouble(), 0);
        assertEquals(5, stats.getOccurrenceDistribution().getP50().getAsDouble(), 0);
        assertEquals(8, stats.getOccurrenceDistribution().getP95().getAsDouble(), 0);
        assertEquals(8, stats.getOccurrenceDistribution().getMaximum().getAsDouble(), 0);
        assertEquals(0, stats.getOccurrenceDistribution().getSingletonCount());
        assertEquals(1, stats.getDistinctQueryDistribution().getMinimum().getAsDouble(), 0);
        assertEquals(4, stats.getDistinctQueryDistribution().getMean().getAsDouble(), 0);
        assertEquals(4, stats.getDistinctQueryDistribution().getP50().getAsDouble(), 0);
        assertEquals(7, stats.getDistinctQueryDistribution().getP95().getAsDouble(), 0);
        assertEquals(7, stats.getDistinctQueryDistribution().getMaximum().getAsDouble(), 0);
        assertEquals(1, stats.getDistinctQueryDistribution().getSingletonCount());
        assertEquals(List.of(8, 7, 6, 5, 4), stats.getHighlightedGroups().stream().map(GroupStats::getOccurrenceCount).collect(Collectors.toList()));
        assertEquals(stats.getGroups().stream().map(GroupStats::getId).sorted().collect(Collectors.toList()),
                        stats.getGroups().stream().map(GroupStats::getId).collect(Collectors.toList()));
    }

    @Test
    public void similarityAveragesWeightFingerprintsAndWeakHighlightsExcludeLiteralOnlyGroups() {
        List<String> queries = new ArrayList<>(List.of("A == 1 && B == 2", "A == 1 && B == 2 && C == 3", "A =~ 'x.*'"));
        for (int i = 0; i < 30; i++) {
            queries.add("A == " + i + " && B == 2");
        }
        Result result = cluster(queries, new Options(0, 100, 100, 100));
        ClusteringStats stats = analyzer.summarizeMinimization(result).getClustering();
        double sum = 0;
        int fingerprints = 0;
        for (QueryClusterer.Group group : result.getGroups()) {
            TreeMap<String,QueryFingerprint> distinct = new TreeMap<>();
            group.getMembers().forEach(member -> distinct.put(member.getFingerprint().getKey(), member.getFingerprint()));
            double expectedMean = distinct.values().stream().mapToDouble(fp -> QuerySimilarity.score(fp, group.getRepresentative().getFingerprint())).average()
                            .getAsDouble();
            GroupStats summary = stats.getGroups().stream().filter(g -> g.getId().equals(group.getId())).findFirst().get();
            assertEquals(expectedMean, summary.getMeanSimilarity(), 1e-15);
            assertEquals(distinct.size(), summary.getFingerprintCount());
            sum += expectedMean * distinct.size();
            fingerprints += distinct.size();
        }
        assertEquals(sum / fingerprints, stats.getMeanSimilarity().getAsDouble(), 1e-15);
        assertEquals(stats.getHighlightedGroups().size(), stats.getHighlightedGroups().stream().map(GroupStats::getId).distinct().count());
        assertTrue(stats.getGroups().stream().anyMatch(g -> g.getFingerprintCount() > 1));

        List<String> tied = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            tied.add("F_" + i + " == 1 && G_" + i + " == 2");
            tied.add("F_" + i + " == 1 && G_" + i + " == 2 && H_" + i + " == 3");
        }
        ClusteringStats highlightStats = analyzer.summarizeMinimization(cluster(tied, new Options(0.7, 100, 100, 100))).getClustering();
        List<String> expected = new ArrayList<>();
        highlightStats.getGroups().stream().sorted(Comparator.comparingInt(GroupStats::getOccurrenceCount).reversed().thenComparing(GroupStats::getId)).limit(5)
                        .forEach(g -> expected.add(g.getId()));
        highlightStats.getGroups().stream().filter(g -> g.getFingerprintCount() > 1)
                        .sorted(Comparator.comparingDouble(GroupStats::getMinimumSimilarity).thenComparing(GroupStats::getId)).limit(5).forEach(g -> {
                            if (!expected.contains(g.getId()))
                                expected.add(g.getId());
                        });
        assertEquals(expected, highlightStats.getHighlightedGroups().stream().map(GroupStats::getId).collect(Collectors.toList()));
    }

    @Test
    public void handlesEmptyAndAllFailedReportsAndRejectsNullArguments() {
        for (List<String> queries : List.of(Collections.<String> emptyList(), List.of("A ==", "A.size() > 0"))) {
            Result result = cluster(queries, new Options());
            QueryMinimizationReport report = analyzer.summarizeMinimization(new QueryWorkloadSelector().cursor(result));
            ClusteringStats stats = report.getClustering();
            assertEquals(0, stats.getSuccessfulCount());
            assertEquals(0, stats.getDuplicateOccurrenceCount());
            assertFalse(stats.getOccurrenceReduction().isPresent());
            assertFalse(stats.getDistinctQueryReduction().isPresent());
            assertFalse(stats.getOccurrenceDistribution().getMinimum().isPresent());
            assertFalse(stats.getOccurrenceDistribution().getMean().isPresent());
            assertFalse(stats.getOccurrenceDistribution().getP50().isPresent());
            assertFalse(stats.getOccurrenceDistribution().getP95().isPresent());
            assertFalse(stats.getOccurrenceDistribution().getMaximum().isPresent());
            assertEquals(0, stats.getOccurrenceDistribution().getSingletonCount());
            assertFalse(stats.getMinimumSimilarity().isPresent());
            assertFalse(stats.getMeanSimilarity().isPresent());
            assertFalse(stats.getMeanBestRejectedScore().isPresent());
            assertTrue(stats.getHighlightedGroups().isEmpty());
            assertFalse(report.getSelection().get().getOccurrenceCoverage().isPresent());
            assertFalse(report.getSelection().get().getGroupCoverage().isPresent());
            assertFalse(report.getSelection().get().getFingerprintCoverage().isPresent());
            assertFalse(report.getSelection().get().getProfileCoverage().isPresent());
            assertFalse(report.getSelection().get().getDistinctQueryCoverage().isPresent());
            assertFalse(report.getSelection().get().getMeanDiversityDistance().isPresent());
            assertTrue(report.describe().contains("n/a"));
        }
        assertThrows(NullPointerException.class, () -> analyzer.summarizeMinimization((Result) null));
        assertThrows(NullPointerException.class, () -> analyzer.summarizeMinimization((Cursor) null));
    }

    @Test
    public void exposesImmutableCollectionsAndLocaleIndependentPermutationStableDescriptions() {
        List<String> queries = new ArrayList<>(List.of("A == 1", "A == 2", "B == 1", "A =~ 'x.*'", "A =~ '.*x.*'", "A =="));
        QueryMinimizationReport report = analyzer.summarizeMinimization(cluster(queries, new Options()));
        ClusteringStats stats = report.getClustering();
        assertThrows(UnsupportedOperationException.class, () -> stats.getGroups().clear());
        assertThrows(UnsupportedOperationException.class, () -> stats.getProfiles().clear());
        assertThrows(UnsupportedOperationException.class, () -> stats.getHighlightedGroups().clear());
        GroupStats group = stats.getGroups().get(0);
        assertThrows(UnsupportedOperationException.class, () -> group.getCategories().clear());
        assertThrows(UnsupportedOperationException.class, () -> group.getFields().clear());
        assertThrows(UnsupportedOperationException.class, () -> group.getFunctions().clear());
        assertThrows(UnsupportedOperationException.class, () -> group.getProtections().clear());
        Collections.shuffle(queries, new Random(67));
        assertEquals(report.describe(), analyzer.summarizeMinimization(cluster(queries, new Options())).describe());
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(Locale.GERMANY);
            assertEquals(report.describe(), analyzer.summarizeMinimization(cluster(queries, new Options())).describe());
            assertTrue(report.describe().contains("threshold 0.700"));
        } finally {
            Locale.setDefault(original);
        }
    }

    @Test
    public void describesObservedRestrictionsAndConsideredRejectedScoresConditionally() {
        List<String> queries = List.of("FIELD_A == 1", "FIELD_B == 2", "FIELD_C == 3");
        QueryMinimizationReport unrestricted = analyzer.summarizeMinimization(cluster(queries, new Options(1, 100, 100, 100)));
        assertTrue(unrestricted.describe().contains("No feature/candidate truncation or posting eviction was observed"));
        assertTrue(unrestricted.describe().contains("Best rejected scores describe considered candidates only"));
        QueryMinimizationReport restricted = analyzer.summarizeMinimization(cluster(queries, new Options(1, 1, 1, 1)));
        assertTrue(restricted.describe().contains("Observed limits restricted candidate discovery"));
        assertTrue(restricted.describe().contains("do not establish that a compatible group was missed"));
        ClusteringStats stats = restricted.getClustering();
        assertTrue(stats.getFeatureTruncatedFingerprintCount() > 0);
        assertTrue(stats.getFeaturePostingEvictionCount() > 0);
        assertTrue(stats.getFallbackPostingEvictionCount() > 0);
        QueryMinimizationReport singleton = analyzer.summarizeMinimization(cluster(List.of("A == 1"), new Options()));
        assertFalse(singleton.describe().contains("Best rejected scores describe considered candidates only"));
    }

    private Result cluster(List<String> queries, Options options) {
        return new QueryClusterer().cluster(analyzer.analyze(queries, Syntax.JEXL), options);
    }
}
