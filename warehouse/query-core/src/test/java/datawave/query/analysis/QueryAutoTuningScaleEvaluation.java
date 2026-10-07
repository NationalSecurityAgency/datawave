package datawave.query.analysis;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.ArrayList;
import java.util.List;

import datawave.query.analysis.QueryAnalyzer.Syntax;
import datawave.query.analysis.QueryAutoTuner.Result;

/** Opt-in full tuning evaluation, outside normal test discovery. Timings and heap figures are observations, not capacity guarantees. */
public final class QueryAutoTuningScaleEvaluation {
    private QueryAutoTuningScaleEvaluation() {}

    public static void main(String[] args) {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 9999;
        int budget = args.length > 1 ? Integer.parseInt(args[1]) : 128;
        if (count < 1 || count > QueryAutoTuner.MAX_SAMPLES) {
            throw new IllegalArgumentException("Query count must be between 1 and " + QueryAutoTuner.MAX_SAMPLES);
        }
        List<QueryTuningSample> samples = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String field = "FIELD_" + i;
            // Unique fingerprints, multiple protected profiles, and enough same-profile labels to exercise candidate discovery and fit.
            String query = i % 3 == 0 ? field + " == 'value'" : i % 3 == 1 ? field + " =~ 'prefix.*'" : field + " >= 10";
            samples.add(new QueryTuningSample("sample-" + i, query, Syntax.JEXL, "bucket-" + (i % 30)));
        }
        long start = System.nanoTime();
        Result result = new QueryAutoTuner().tune(samples, new QueryAutoTuner.Options(new QueryClusterer.Options(), budget));
        long elapsed = System.nanoTime() - start;
        long bound = (long) result.getSplit().getTrainingFingerprintCount() * result.getSearch().getEvaluationCount()
                        * result.getConfiguration().toClusteringOptions().getCandidateLimit();
        if (result.getSearch().getEvaluationCount() > budget || result.getSearch().getComparisonCount() > bound) {
            throw new AssertionError("Tuning exceeded its evaluation or comparison bound");
        }
        long peakHeap = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                peakHeap += pool.getPeakUsage().getUsed();
            }
        }
        System.out.printf("samples=%d training=%d holdout=%d evaluations=%d searchComparisons=%d%n", count, result.getSplit().getTrainingFingerprintCount(),
                        result.getSplit().getHoldoutFingerprintCount(), result.getSearch().getEvaluationCount(), result.getSearch().getComparisonCount());
        System.out.printf("tuneSeconds=%.3f heapPoolPeakSumMiB=%.1f baselineTrainingF1=%.6f trainingF1=%.6f%n", elapsed / 1_000_000_000.0,
                        peakHeap / (1024.0 * 1024), result.getBaselineTrainingFit().getF1(), result.getTrainingFit().getF1());
        result.getHoldoutFit().ifPresent(fit -> System.out.printf("holdoutF1=%.6f%n", fit.getF1()));
    }
}
