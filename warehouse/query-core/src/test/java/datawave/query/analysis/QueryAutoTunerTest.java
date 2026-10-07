package datawave.query.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import datawave.query.analysis.QueryAnalyzer.AnalysisReport;
import datawave.query.analysis.QueryAnalyzer.Syntax;
import datawave.query.analysis.QueryAutoTuner.Result;
import datawave.query.analysis.QuerySimilarity.Weights;
import datawave.query.language.parser.ParseException;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.language.tree.QueryNode;

public class QueryAutoTunerTest {
    @Test
    public void learnsGroupingAndReusesConfigurationForFreshQueries() {
        Result tuned = new QueryAutoTuner().tune(List.of(sample("A == 1", "same"), sample("B == 2", "same")));
        assertTrue(tuned.getTrainingFit().getF1() > tuned.getBaselineTrainingFit().getF1());
        assertEquals(1, tuned.getTrainingFit().getF1(), 0);
        AnalysisReport fresh = new QueryAnalyzer().analyze(List.of("C == 3", "D == 4"), Syntax.JEXL);
        assertEquals(1, new QueryClusterer().cluster(fresh, tuned.getConfiguration().toClusteringOptions()).getGroups().size());
        assertFalse(tuned.getHoldoutFit().isPresent());
        assertEquals(2, tuned.getSplit().getTrainingFingerprintCount());
        assertTrue(tuned.getSearch().getEvaluationCount() <= 128);
        assertTrue(tuned.getSearch().getComparisonCount() <= 2L * 64 * tuned.getSearch().getEvaluationCount());
    }

    @Test
    public void changesWeightsWhenThresholdAloneCannotExpressLabels() {
        List<QueryTuningSample> samples = List.of(sample("A == 1 && B == 2", "two"), sample("C == 1 && D == 2", "two"),
                        sample("A == 1 && B == 2 && E == 3", "three"), sample("C == 1 && D == 2 && E == 3", "three"));
        Result tuned = new QueryAutoTuner().tune(samples);
        assertEquals(1, tuned.getTrainingFit().getF1(), 1e-12);
        assertTrue(tuned.getTrainingFit().getF1() > tuned.getBaselineTrainingFit().getF1());
        assertTrue(Math.abs(tuned.getConfiguration().toClusteringOptions().getWeights().getBindings() - Weights.DEFAULT.getBindings()) > 1e-9);
    }

    @Test
    public void preservesBaselineAndDiscoveryLimitsWithOneEvaluation() {
        Weights weights = new Weights(2, 3, 4, 5);
        QueryClusterer.Options baseline = new QueryClusterer.Options(.421, 7, 11, 13, weights);
        Result result = new QueryAutoTuner().tune(List.of(sample("A == 1", "a"), sample("B == 1", "b")), new QueryAutoTuner.Options(baseline, 1));
        QueryClusterer.Options selected = result.getConfiguration().toClusteringOptions();
        assertEquals(.421, selected.getThreshold(), 0);
        assertEquals(7, selected.getFeatureLimit());
        assertEquals(11, selected.getPostingLimit());
        assertEquals(13, selected.getCandidateLimit());
        assertEquals(weights.getBindings(), selected.getWeights().getBindings(), 0);
        assertEquals(weights.getComplexity(), selected.getWeights().getComplexity(), 0);
        assertEquals(1, result.getSearch().getEvaluationCount());
        assertTrue(result.getSearch().isBudgetExhausted());
        assertEquals(result.getBaselineTrainingFit().getF1(), result.getTrainingFit().getF1(), 0);
    }

    @Test
    public void fractionalConflictsIgnoreOccurrenceFrequencyAndReportOriginalIndexes() {
        List<QueryTuningSample> samples = new ArrayList<>();
        samples.add(new QueryTuningSample("first", "A == 1", Syntax.JEXL, "x"));
        samples.add(new QueryTuningSample("second", "A == 2", Syntax.JEXL, "y"));
        samples.add(new QueryTuningSample("bad", "A ==", Syntax.JEXL, "x"));
        Result initial = new QueryAutoTuner().tune(samples);
        assertEquals(.5, initial.getTrainingFit().getPrecision(), 0);
        assertEquals(1, initial.getTrainingFit().getRecall(), 0);
        assertEquals(2.0 / 3, initial.getTrainingFit().getF1(), 1e-12);
        assertEquals(1, initial.getTrainingFit().getFingerprintCount());
        assertEquals(List.of(0, 1), initial.getDiagnostics().getLabelConflicts().get(0).getSampleIndexes());
        assertEquals(List.of("first", "second"), initial.getDiagnostics().getLabelConflicts().get(0).getSampleIds());
        assertEquals(2, initial.getDiagnostics().getExcludedSamples().get(0).getIndex());
        assertEquals("bad", initial.getDiagnostics().getExcludedSamples().get(0).getId());
        assertEquals(QueryAnalyzer.Status.INVALID, initial.getDiagnostics().getExcludedSamples().get(0).getStatus());
        for (int i = 0; i < 30; i++) {
            samples.add(sample("A == 1", "x"));
        }
        Result repeated = new QueryAutoTuner().tune(samples);
        assertEquals(initial.getTrainingFit().getF1(), repeated.getTrainingFit().getF1(), 0);
        assertSameConfiguration(initial, repeated);
        assertThrows(UnsupportedOperationException.class, () -> initial.getDiagnostics().getLabelConflicts().clear());
    }

    @Test
    public void protectedProfilesRemainHardBoundariesEvenAtZeroThreshold() {
        Result result = new QueryAutoTuner().tune(List.of(sample("A =~ 'x.*'", "together"), sample("A =~ '.*x.*'", "together")));
        assertEquals(2, result.getTrainingFit().getGroupCount());
        assertEquals(1, result.getTrainingFit().getPrecision(), 0);
        assertEquals(.5, result.getTrainingFit().getRecall(), 0);
        assertEquals(1, result.getDiagnostics().getProtectedProfileConflicts().size());
        assertEquals("together", result.getDiagnostics().getProtectedProfileConflicts().get(0).getBucket());
        assertEquals(List.of(0, 1), result.getDiagnostics().getProtectedProfileConflicts().get(0).getSampleIndexes());
    }

    @Test
    public void reportsSuccessfulButUncertainFingerprintEnrichment() {
        Result result = new QueryAutoTuner().tune(List.of(new QueryTuningSample("uncertain", "A =~ '['", Syntax.JEXL, "regex"), sample("B == 1", "equal")));
        assertTrue(result.getDiagnostics().getExcludedSamples().isEmpty());
        assertEquals(1, result.getDiagnostics().getFingerprintDiagnostics().size());
        QueryAutoTuner.FingerprintDiagnostic diagnostic = result.getDiagnostics().getFingerprintDiagnostics().get(0);
        assertEquals(List.of(0), diagnostic.getSampleIndexes());
        assertEquals(List.of("uncertain"), diagnostic.getSampleIds());
        assertTrue(diagnostic.getMessages().get(0).contains("regex"));
        assertThrows(UnsupportedOperationException.class, () -> diagnostic.getMessages().clear());
    }

    @Test
    public void validationIsFingerprintDisjointAndPermutationInvariant() {
        List<QueryTuningSample> samples = validationSamples();
        Result first = new QueryAutoTuner().tune(samples);
        assertEquals(4, first.getSplit().getHoldoutFingerprintCount());
        assertEquals(4, first.getSplit().getTrainingFingerprintCount());
        assertTrue(first.getHoldoutFit().isPresent());
        assertEquals(4, first.getHoldoutFit().get().getFingerprintCount());
        assertEquals(8, first.getFullFit().getFingerprintCount());
        assertEquals(List.of("equal", "regex"), first.getSplit().getValidatedBuckets());

        Collections.shuffle(samples, new Random(37));
        Result shuffled = new QueryAutoTuner().tune(samples);
        assertSameConfiguration(first, shuffled);
        assertEquals(first.getSplit().getHoldoutFingerprintKeys(), shuffled.getSplit().getHoldoutFingerprintKeys());
        assertEquals(first.getTrainingFit().getF1(), shuffled.getTrainingFit().getF1(), 0);
        assertEquals(first.getHoldoutFit().get().getF1(), shuffled.getHoldoutFit().get().getF1(), 0);
        assertEquals(first.getSearch().getEvaluationCount(), shuffled.getSearch().getEvaluationCount());

        // Literal variants and exact duplicates stay with their shared fingerprint and cannot add validation evidence.
        samples.add(sample("E0 == 99", "equal"));
        samples.add(sample("E0 == 99", "equal"));
        Result repeated = new QueryAutoTuner().tune(samples);
        assertEquals(first.getSplit().getHoldoutFingerprintKeys(), repeated.getSplit().getHoldoutFingerprintKeys());
        assertSameConfiguration(first, repeated);
        assertEquals(first.getHoldoutFit().get().getF1(), repeated.getHoldoutFit().get().getF1(), 0);
    }

    @Test
    public void holdoutDoesNotInfluenceTrainingSelection() {
        List<QueryTuningSample> samples = validationSamples();
        Result complete = new QueryAutoTuner().tune(samples);
        AnalysisReport report = new QueryAnalyzer()
                        .analyze(samples.stream().map(QueryTuningSample::toQueryInput).collect(java.util.stream.Collectors.toList()));
        List<QueryTuningSample> trainingOnly = new ArrayList<>();
        for (QueryAnalyzer.QueryAnalysis query : report.getQueries()) {
            if (!complete.getSplit().getHoldoutFingerprintKeys().contains(query.getFingerprint().getKey())) {
                trainingOnly.add(samples.get(query.getIndex()));
            }
        }
        Result training = new QueryAutoTuner().tune(trainingOnly);
        assertFalse(training.getHoldoutFit().isPresent());
        assertSameConfiguration(complete, training);
        assertEquals(complete.getTrainingFit().getF1(), training.getTrainingFit().getF1(), 0);
        assertEquals(complete.getSearch().getComparisonCount(), training.getSearch().getComparisonCount());
    }

    @Test
    public void parsesOnceAcrossSearchHoldoutAndFullDataEvaluation() {
        CountingParser parser = new CountingParser();
        List<QueryTuningSample> samples = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            samples.add(new QueryTuningSample("E" + i + ":value", Syntax.LUCENE, "equal"));
            samples.add(new QueryTuningSample("R" + i + ":prefix*", Syntax.LUCENE, "regex"));
        }
        Result result = new QueryAutoTuner(new QueryAnalyzer(parser)).tune(samples);
        assertTrue(result.getHoldoutFit().isPresent());
        assertTrue(result.getSearch().getEvaluationCount() > 1);
        assertEquals(samples.size(), parser.calls);
    }

    @Test
    public void insufficientValidationAndExcludedSamplesAreExplicit() {
        List<QueryTuningSample> samples = validationSamples().subList(0, 4);
        Result oneBucket = new QueryAutoTuner().tune(samples);
        assertFalse(oneBucket.getSplit().isValidationAvailable());
        assertEquals(List.of("equal"), oneBucket.getSplit().getUnvalidatedBuckets());
        Result invalid = new QueryAutoTuner().tune(List.of(sample("A == 1", "ok"), sample("A = 'x'", "unsupported")));
        assertEquals(QueryAnalyzer.Status.UNSUPPORTED, invalid.getDiagnostics().getExcludedSamples().get(0).getStatus());
        assertThrows(IllegalArgumentException.class, () -> new QueryAutoTuner().tune(List.of(sample("A ==", "invalid"))));
        assertThrows(IllegalArgumentException.class, () -> new QueryAutoTuner().tune(Collections.emptyList()));
        assertThrows(IllegalArgumentException.class, () -> new QueryAutoTuner().tune(Collections.nCopies(10000, sample("A == 1", "x"))));
        assertThrows(IllegalArgumentException.class, () -> new QueryAutoTuner()
                        .tune(List.of(new QueryTuningSample("id", "A == 1", Syntax.JEXL, "x"), new QueryTuningSample("id", "B == 1", Syntax.JEXL, "x"))));
        assertThrows(IllegalArgumentException.class, () -> new QueryAutoTuner.Options(new QueryClusterer.Options(), 0));
    }

    @Test
    public void singletonLabelsHaveDefinedPerfectFitAndSmallBudgetsAreBounded() {
        List<QueryTuningSample> samples = List.of(sample("A == 1", "a"), sample("B == 1", "b"));
        for (int budget : new int[] {1, 2, 3, 4, 7, 16}) {
            Result result = new QueryAutoTuner().tune(samples, new QueryAutoTuner.Options(new QueryClusterer.Options(), budget));
            assertEquals(1, result.getTrainingFit().getPrecision(), 0);
            assertEquals(1, result.getTrainingFit().getRecall(), 0);
            assertTrue(result.getSearch().getEvaluationCount() >= 1);
            assertTrue(result.getSearch().getEvaluationCount() <= budget);
        }
    }

    private static List<QueryTuningSample> validationSamples() {
        List<QueryTuningSample> samples = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            samples.add(sample("E" + i + " == 1", "equal"));
        }
        for (int i = 0; i < 4; i++) {
            samples.add(sample("R" + i + " =~ 'prefix.*'", "regex"));
        }
        return samples;
    }

    private static QueryTuningSample sample(String query, String bucket) {
        return new QueryTuningSample(query, Syntax.JEXL, bucket);
    }

    private static final class CountingParser extends LuceneToJexlQueryParser {
        private int calls;

        @Override
        public QueryNode parse(String query) throws ParseException {
            calls++;
            return super.parse(query);
        }
    }

    private static void assertSameConfiguration(Result left, Result right) {
        QueryClusterer.Options l = left.getConfiguration().toClusteringOptions();
        QueryClusterer.Options r = right.getConfiguration().toClusteringOptions();
        assertEquals(l.getThreshold(), r.getThreshold(), 0);
        assertEquals(l.getWeights().getBindings(), r.getWeights().getBindings(), 0);
        assertEquals(l.getWeights().getCounts(), r.getWeights().getCounts(), 0);
        assertEquals(l.getWeights().getTopology(), r.getWeights().getTopology(), 0);
        assertEquals(l.getWeights().getComplexity(), r.getWeights().getComplexity(), 0);
    }
}
