package datawave.query.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import datawave.query.analysis.QueryAnalyzer.AnalysisReport;
import datawave.query.analysis.QueryAnalyzer.Status;
import datawave.query.analysis.QueryAnalyzer.Syntax;
import datawave.query.analysis.QuerySimilarity.Weights;
import datawave.query.analysis.QueryWorkloadSelector.Cursor;
import datawave.query.analysis.QueryWorkloadSelector.Selection;

class QuerySimilarityWeightsTest {
    @Test
    void preservesHistoricalDefaultsExactly() {
        Weights defaults = new Weights();
        assertWeights(defaults, 0.35, 0.30, 0.20, 0.15);
        assertSame(Weights.DEFAULT, new QueryClusterer.Options().getWeights());
        assertSame(Weights.DEFAULT, new QueryClusterer.Options(0.8, 4, 32, 64).getWeights());
        List<QueryFingerprint> fingerprints = fingerprints("A == 1 && B == 2", "A == 3 && C == 4 && D == 5");
        QueryFingerprint left = fingerprints.get(0);
        QueryFingerprint right = fingerprints.get(1);
        Map<String,Double> components = QuerySimilarity.compare(left, right).getComponents();
        double historicalScore = 0.35 * components.get("bindings") + 0.30 * components.get("counts") + 0.20 * components.get("topology")
                        + 0.15 * components.get("complexity");
        assertEquals(historicalScore, QuerySimilarity.compare(left, right).getScore(), 0);
        assertEquals(historicalScore, QuerySimilarity.score(left, right), 0);
        assertEquals(historicalScore, QuerySimilarity.compare(left, right, defaults).getScore(), 0);
        assertEquals(0.4 * (1 - historicalScore), QuerySimilarity.distance(left, right), 0);
    }

    @Test
    void normalizesRelativeWeightsIncludingExtremeFiniteValues() {
        assertWeights(new Weights(7, 6, 4, 3), 0.35, 0.30, 0.20, 0.15);
        assertWeights(new Weights(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE), 0.25, 0.25, 0.25, 0.25);
        assertWeights(new Weights(Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE), 0.25, 0.25, 0.25, 0.25);
        assertWeights(new Weights(Double.MAX_VALUE, 0, 0, 0), 1, 0, 0, 0);
        assertWeights(new Weights(0, 0, 0, Double.MIN_VALUE), 0, 0, 0, 1);
    }

    @Test
    void normalizedWeightsRetainTheirExactValuesAcrossRepeatedConstruction() {
        assertStableRoundTrip(new Weights(0.6180339887498949, 0.2718281828459045, 0.1414213562373095, 0.5772156649015329));
        Random random = new Random(42);
        for (int iteration = 0; iteration < 100000; iteration++) {
            assertStableRoundTrip(new Weights(random.nextDouble(), random.nextDouble(), random.nextDouble(), random.nextDouble()));
        }
        assertStableRoundTrip(new Weights(Double.MAX_VALUE, Double.MAX_VALUE / 3, Double.MIN_VALUE, 0));
        assertStableRoundTrip(new Weights(Double.MIN_VALUE, 2 * Double.MIN_VALUE, 3 * Double.MIN_VALUE, 4 * Double.MIN_VALUE));
    }

    @Test
    void identicalFingerprintsHaveExactlyUnitSimilarityForEveryWeightVector() {
        List<QueryFingerprint> fingerprints = fingerprints("A == 1 && B == 2", "B == 9 && A == 8");
        QueryFingerprint left = fingerprints.get(0);
        QueryFingerprint sameKey = fingerprints.get(1);
        assertEquals(left.getKey(), sameKey.getKey());
        Random random = new Random(42);
        for (int iteration = 0; iteration < 1000; iteration++) {
            Weights weights = new Weights(random.nextDouble(), random.nextDouble(), random.nextDouble(), random.nextDouble());
            assertEquals(1, QuerySimilarity.compare(left, left, weights).getScore(), 0);
            assertEquals(1, QuerySimilarity.compare(left, sameKey, weights).getScore(), 0);
            assertEquals(1, QuerySimilarity.score(left, sameKey, weights), 0);
            assertEquals(0, QuerySimilarity.distance(left, sameKey, weights), 0);
        }
    }

    @Test
    void rejectsInvalidWeightsAndNullScoringConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> new Weights(0, 0, 0, 0));
        double[] invalid = {-1, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
        for (double value : invalid) {
            assertThrows(IllegalArgumentException.class, () -> new Weights(value, 1, 1, 1));
            assertThrows(IllegalArgumentException.class, () -> new Weights(1, value, 1, 1));
            assertThrows(IllegalArgumentException.class, () -> new Weights(1, 1, value, 1));
            assertThrows(IllegalArgumentException.class, () -> new Weights(1, 1, 1, value));
        }
        QueryFingerprint fingerprint = fingerprints("A == 1").get(0);
        assertThrows(NullPointerException.class, () -> QuerySimilarity.compare(fingerprint, fingerprint, null));
        assertThrows(NullPointerException.class, () -> QuerySimilarity.distance(fingerprint, fingerprint, null));
        assertThrows(NullPointerException.class, () -> new QueryClusterer.Options(0.7, 4, 32, 64, null));
    }

    @Test
    void scoresSelectedComponentsWithoutChangingCompatibility() {
        List<QueryFingerprint> fingerprints = fingerprints("A == 1 && B == 2", "A == 3 && C == 4 && D == 5");
        QueryFingerprint left = fingerprints.get(0);
        QueryFingerprint right = fingerprints.get(1);
        Map<String,Double> components = QuerySimilarity.compare(left, right).getComponents();
        Weights[] weights = {new Weights(1, 0, 0, 0), new Weights(0, 1, 0, 0), new Weights(0, 0, 1, 0), new Weights(0, 0, 0, 1)};
        String[] names = {"bindings", "counts", "topology", "complexity"};
        for (int index = 0; index < weights.length; index++) {
            QuerySimilarity.Result result = QuerySimilarity.compare(left, right, weights[index]);
            assertTrue(result.isCompatible());
            assertEquals(components, result.getComponents());
            assertEquals(components.get(names[index]), result.getScore(), 0);
            assertEquals(result.getScore(), QuerySimilarity.score(left, right, weights[index]), 0);
            assertEquals(0.4 * (1 - result.getScore()), QuerySimilarity.distance(left, right, weights[index]), 0);
        }
        List<QueryFingerprint> regexes = fingerprints("A =~ 'x.*'", "A =~ '.*x'");
        QuerySimilarity.Result incompatible = QuerySimilarity.compare(regexes.get(0), regexes.get(1), weights[1]);
        assertFalse(incompatible.isCompatible());
        assertFalse(incompatible.getProtectedDifferences().isEmpty());
        assertTrue(QuerySimilarity.distance(regexes.get(0), regexes.get(1), weights[1]) >= 0.6);
        AnalysisReport regexReport = new QueryAnalyzer().analyze(List.of("A =~ 'x.*'", "A =~ '.*x'"), Syntax.JEXL);
        assertEquals(2, new QueryClusterer().cluster(regexReport, new QueryClusterer.Options(0, 4, 32, 64, weights[1])).getGroups().size());
    }

    @Test
    void clusteringAndReportsUseConfiguredSimilarityWeights() {
        QueryAnalyzer analyzer = new QueryAnalyzer();
        AnalysisReport analysis = analyzer.analyze(List.of("A == 1", "B == 2"), Syntax.JEXL);
        assertEquals(2, new QueryClusterer().cluster(analysis).getGroups().size());
        Weights weights = new Weights(0, 1, 0, 0);
        QueryClusterer.Options options = new QueryClusterer.Options(0.7, 4, 32, 64, weights);
        QueryClusterer.Result result = new QueryClusterer().cluster(analysis, options);
        assertEquals(1, result.getGroups().size());
        assertSame(weights, result.getOptions().getWeights());
        QueryClusterer.Group group = result.getGroups().get(0);
        assertEquals(1, group.getMinimumSimilarity(), 0);
        assertEquals(1, group.getMeanSimilarity(), 0);
        for (QueryAnalyzer.QueryAnalysis member : group.getMembers()) {
            assertTrue(QuerySimilarity.compare(member.getFingerprint(), group.getRepresentative().getFingerprint(), weights).getScore() >= options
                            .getThreshold());
        }
        QueryMinimizationReport report = analyzer.summarizeMinimization(result);
        assertSame(weights, report.getClustering().getOptions().getWeights());
        assertTrue(report.describe().contains("Similarity weights: bindings 0.000; counts 1.000; topology 0.000; complexity 0.000."));
    }

    @Test
    void selectorInheritsClusteringWeightsForLandmarksAndRepresentative() {
        QueryAnalyzer analyzer = new QueryAnalyzer();
        Weights weights = new Weights(1, 0, 0, 0);
        QueryClusterer.Result result = new QueryClusterer().cluster(analyzer.analyze(List.of("A == 1", "B == 2", "C == 3"), Syntax.JEXL),
                        new QueryClusterer.Options(0, 4, 32, 64, weights));
        assertEquals(1, result.getGroups().size());
        Cursor cursor = new QueryWorkloadSelector().cursor(result);
        List<Selection> selections = cursor.take(3);
        assertEquals(3, selections.size());
        assertEquals(1, selections.get(0).getDiversityDistance(), 0);
        assertEquals(0.4, selections.get(1).getDiversityDistance(), 0);
        assertEquals(0.4, selections.get(2).getDiversityDistance(), 0);
        QueryMinimizationReport report = analyzer.summarizeMinimization(cursor);
        assertEquals(0.4, report.getSelection().orElseThrow().getMinimumDiversityDistance().orElseThrow(), 0);
        assertEquals(0.4, report.getSelection().orElseThrow().getMeanDiversityDistance().orElseThrow(), 0);
    }

    private static List<QueryFingerprint> fingerprints(String... queries) {
        AnalysisReport report = new QueryAnalyzer().analyze(List.of(queries), Syntax.JEXL);
        for (QueryAnalyzer.QueryAnalysis query : report.getQueries()) {
            assertEquals(Status.SUCCESS, query.getStatus(), query.getError());
        }
        return report.getQueries().stream().map(QueryAnalyzer.QueryAnalysis::getFingerprint).collect(Collectors.toList());
    }

    private static void assertWeights(Weights weights, double bindings, double counts, double topology, double complexity) {
        assertEquals(bindings, weights.getBindings(), 0);
        assertEquals(counts, weights.getCounts(), 0);
        assertEquals(topology, weights.getTopology(), 0);
        assertEquals(complexity, weights.getComplexity(), 0);
    }

    private static void assertStableRoundTrip(Weights original) {
        Weights reloaded = new Weights(original.getBindings(), original.getCounts(), original.getTopology(), original.getComplexity());
        assertWeights(reloaded, original.getBindings(), original.getCounts(), original.getTopology(), original.getComplexity());
        double sum = original.getBindings() + original.getCounts() + original.getTopology() + original.getComplexity();
        assertEquals(1, sum, 2 * Math.ulp(1.0));
    }
}
