package datawave.query.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import datawave.query.analysis.QueryAnalyzer.AnalysisReport;
import datawave.query.analysis.QueryAnalyzer.QueryAnalysis;
import datawave.query.analysis.QueryAnalyzer.Status;
import datawave.query.analysis.QuerySimilarity.Weights;

/**
 * Bounded, deterministic supervised tuning of an offline grouping policy. Labels describe a partition, not persistent group names. Protected profiles and
 * discovery limits remain unchanged. Holdout scores never influence parameter selection; neither training nor holdout scores guarantee future performance.
 * Analysis is performed once. Search evaluates the baseline first, then baseline/uniform/component-only weight seeds on a 0.1 threshold grid. Best-improvement
 * refinement uses threshold/weight steps of 0.05/0.1 and then 0.01/0.025, reserving the final quarter of the evaluation budget for the latter stage.
 *
 * <p>
 * Validation is available only when two labels each contain at least four conflict-free fingerprints. Fingerprints are held out together, using a fixed hash
 * ordering. Conflicts and small labels stay in training. Validation therefore describes eligible conflict-free labels, not every supplied sample.
 */
public final class QueryAutoTuner {
    public static final int SEARCH_VERSION = 1;
    public static final int MAX_SAMPLES = 9999;
    private final QueryAnalyzer analyzer;

    public QueryAutoTuner() {
        this(new QueryAnalyzer());
    }

    public QueryAutoTuner(QueryAnalyzer analyzer) {
        this.analyzer = Objects.requireNonNull(analyzer, "analyzer");
    }

    public Result tune(List<QueryTuningSample> samples) {
        return tune(samples, new Options());
    }

    public Result tune(List<QueryTuningSample> samples, Options options) {
        Objects.requireNonNull(options, "options");
        validate(samples);
        List<QueryAnalyzer.QueryInput> inputs = new ArrayList<>();
        for (QueryTuningSample sample : samples) {
            inputs.add(sample.toQueryInput());
        }
        AnalysisReport analysis = analyzer.analyze(inputs);
        Map<String,FingerprintData> data = new TreeMap<>();
        List<ExcludedSample> excluded = new ArrayList<>();
        for (QueryAnalysis query : analysis.getQueries()) {
            QueryTuningSample sample = samples.get(query.getIndex());
            if (query.getStatus() != Status.SUCCESS) {
                excluded.add(new ExcludedSample(query.getIndex(), sample.getId(), query.getStatus(), query.getError()));
            } else {
                FingerprintData fingerprint = data.computeIfAbsent(query.getFingerprint().getKey(), key -> new FingerprintData(key));
                fingerprint.queries.add(query);
                fingerprint.labels.add(sample.getBucket());
            }
        }
        if (data.isEmpty()) {
            throw new IllegalArgumentException("At least one successfully analyzed sample is required");
        }
        Split split = split(data);
        Set<String> holdoutKeys = new HashSet<>(split.holdoutFingerprintKeys);
        List<QueryAnalysis> training = new ArrayList<>();
        List<QueryAnalysis> holdout = new ArrayList<>();
        for (QueryAnalysis query : analysis.getQueries()) {
            if (query.getStatus() == Status.SUCCESS) {
                (holdoutKeys.contains(query.getFingerprint().getKey()) ? holdout : training).add(query);
            }
        }
        SearchState search = new SearchState(AnalysisReport.fromAnalyses(training), data, options);
        search.run();
        Candidate selected = search.best;
        Fit baselineHoldout = null;
        Fit selectedHoldout = null;
        if (!holdout.isEmpty()) {
            AnalysisReport holdoutReport = AnalysisReport.fromAnalyses(holdout);
            baselineHoldout = fit(new QueryClusterer().cluster(holdoutReport, options.baseline), data);
            selectedHoldout = sameOptions(options.baseline, selected.options) ? baselineHoldout
                            : fit(new QueryClusterer().cluster(holdoutReport, selected.options), data);
        }
        AnalysisReport fullReport = AnalysisReport.fromAnalyses(successful(analysis));
        Fit baselineFull;
        Fit selectedFull;
        if (holdout.isEmpty()) {
            baselineFull = search.baseline.fit;
            selectedFull = selected.fit;
        } else {
            baselineFull = fit(new QueryClusterer().cluster(fullReport, options.baseline), data);
            selectedFull = sameOptions(options.baseline, selected.options) ? baselineFull
                            : fit(new QueryClusterer().cluster(fullReport, selected.options), data);
        }
        return new Result(new QueryTuningConfiguration(selected.options), search.baseline.fit, selected.fit, baselineHoldout, selectedHoldout, baselineFull,
                        selectedFull, split, diagnostics(data, excluded, samples),
                        new Search(options.maxEvaluations, search.evaluations, search.comparisons, search.evaluations >= options.maxEvaluations));
    }

    private static void validate(List<QueryTuningSample> samples) {
        Objects.requireNonNull(samples, "samples");
        if (samples.isEmpty() || samples.size() > MAX_SAMPLES) {
            throw new IllegalArgumentException("Sample count must be between 1 and " + MAX_SAMPLES);
        }
        Set<String> ids = new HashSet<>();
        for (QueryTuningSample sample : samples) {
            if (sample == null || sample.getQuery() == null || sample.getSyntax() == null || sample.getBucket() == null
                            || sample.getBucket().trim().isEmpty()) {
                throw new IllegalArgumentException("Each sample requires query text, explicit syntax, and a nonblank bucket");
            }
            if (sample.getId() != null && (sample.getId().trim().isEmpty() || !ids.add(sample.getId()))) {
                throw new IllegalArgumentException("Provided sample IDs must be nonblank and unique");
            }
        }
    }

    private static List<QueryAnalysis> successful(AnalysisReport report) {
        List<QueryAnalysis> queries = new ArrayList<>();
        for (QueryAnalysis query : report.getQueries()) {
            if (query.getStatus() == Status.SUCCESS) {
                queries.add(query);
            }
        }
        return queries;
    }

    private static Split split(Map<String,FingerprintData> data) {
        Map<String,List<String>> buckets = new TreeMap<>();
        int conflicts = 0;
        for (FingerprintData fingerprint : data.values()) {
            if (fingerprint.labels.size() == 1) {
                buckets.computeIfAbsent(fingerprint.labels.iterator().next(), key -> new ArrayList<>()).add(fingerprint.key);
            } else {
                conflicts++;
            }
        }
        List<String> eligible = new ArrayList<>();
        List<String> unvalidated = new ArrayList<>();
        Set<String> allLabels = new TreeSet<>();
        data.values().forEach(fp -> allLabels.addAll(fp.labels));
        for (String label : allLabels) {
            if (buckets.getOrDefault(label, Collections.emptyList()).size() >= 4) {
                eligible.add(label);
            } else {
                unvalidated.add(label);
            }
        }
        List<String> holdout = new ArrayList<>();
        if (eligible.size() >= 2) {
            for (String bucket : eligible) {
                List<String> keys = buckets.get(bucket);
                keys.sort(Comparator.comparing((String key) -> QueryFingerprint.digest("query-auto-tune-holdout-v1:" + key)).thenComparing(key -> key));
                int count = Math.min(keys.size() - 2, Math.max(2, keys.size() / 5));
                holdout.addAll(keys.subList(0, count));
            }
        } else {
            unvalidated = new ArrayList<>(allLabels);
        }
        Collections.sort(holdout);
        return new Split(data.size() - holdout.size(), holdout, eligible.size() >= 2 ? eligible : Collections.emptyList(), unvalidated, conflicts);
    }

    /** Sparse weighted B-cubed: one unit per fingerprint, divided equally between distinct conflicting labels. */
    private static Fit fit(QueryClusterer.Result result, Map<String,FingerprintData> data) {
        Map<String,Double> labelMasses = new TreeMap<>();
        List<Map<String,Double>> cells = new ArrayList<>();
        List<Integer> groupSizes = new ArrayList<>();
        int count = 0;
        for (QueryClusterer.Group group : result.getGroups()) {
            Set<String> keys = new TreeSet<>();
            group.getMembers().forEach(query -> keys.add(query.getFingerprint().getKey()));
            Map<String,Double> masses = new TreeMap<>();
            for (String key : keys) {
                Set<String> labels = data.get(key).labels;
                double mass = 1.0 / labels.size();
                for (String label : labels) {
                    masses.merge(label, mass, Double::sum);
                    labelMasses.merge(label, mass, Double::sum);
                }
            }
            cells.add(masses);
            groupSizes.add(keys.size());
            count += keys.size();
        }
        double precision = 0;
        double recall = 0;
        for (int i = 0; i < cells.size(); i++) {
            for (Map.Entry<String,Double> cell : cells.get(i).entrySet()) {
                double square = cell.getValue() * cell.getValue();
                precision += square / groupSizes.get(i);
                recall += square / labelMasses.get(cell.getKey());
            }
        }
        precision = Math.min(1, precision / count);
        recall = Math.min(1, recall / count);
        return new Fit(precision, recall, count, result.getGroups().size(), result.getComparisonCount());
    }

    private static Diagnostics diagnostics(Map<String,FingerprintData> data, List<ExcludedSample> excluded, List<QueryTuningSample> samples) {
        List<LabelConflict> conflicts = new ArrayList<>();
        List<FingerprintDiagnostic> fingerprintDiagnostics = new ArrayList<>();
        Map<String,Set<String>> profiles = new TreeMap<>();
        Map<String,List<Integer>> indexes = new TreeMap<>();
        for (FingerprintData fp : data.values()) {
            List<String> messages = fp.queries.get(0).getFingerprint().getDiagnostics();
            if (!messages.isEmpty()) {
                List<Integer> originalIndexes = new ArrayList<>();
                List<String> ids = new ArrayList<>();
                for (QueryAnalysis query : fp.queries) {
                    originalIndexes.add(query.getIndex());
                    if (samples.get(query.getIndex()).getId() != null) {
                        ids.add(samples.get(query.getIndex()).getId());
                    }
                }
                fingerprintDiagnostics.add(new FingerprintDiagnostic(fp.key, originalIndexes, ids, messages));
            }
            if (fp.labels.size() > 1) {
                List<Integer> originalIndexes = new ArrayList<>();
                List<String> ids = new ArrayList<>();
                for (QueryAnalysis query : fp.queries) {
                    originalIndexes.add(query.getIndex());
                    if (samples.get(query.getIndex()).getId() != null) {
                        ids.add(samples.get(query.getIndex()).getId());
                    }
                }
                conflicts.add(new LabelConflict(fp.key, new ArrayList<>(fp.labels), originalIndexes, ids));
            }
            for (QueryAnalysis query : fp.queries) {
                String label = samples.get(query.getIndex()).getBucket();
                profiles.computeIfAbsent(label, key -> new TreeSet<>()).add(query.getFingerprint().getProtectedProfile());
                indexes.computeIfAbsent(label, key -> new ArrayList<>()).add(query.getIndex());
            }
        }
        List<ProtectedProfileConflict> profileConflicts = new ArrayList<>();
        for (Map.Entry<String,Set<String>> bucket : profiles.entrySet()) {
            if (bucket.getValue().size() > 1) {
                Collections.sort(indexes.get(bucket.getKey()));
                profileConflicts.add(new ProtectedProfileConflict(bucket.getKey(), new ArrayList<>(bucket.getValue()), indexes.get(bucket.getKey())));
            }
        }
        return new Diagnostics(excluded, conflicts, profileConflicts, fingerprintDiagnostics);
    }

    private static final class FingerprintData {
        private final String key;
        private final Set<String> labels = new TreeSet<>();
        private final List<QueryAnalysis> queries = new ArrayList<>();

        private FingerprintData(String key) {
            this.key = key;
        }
    }

    private static final class Candidate {
        private final QueryClusterer.Options options;
        private final Fit fit;
        private final double baselineDistance;

        private Candidate(QueryClusterer.Options options, Fit fit, QueryClusterer.Options baseline) {
            this.options = options;
            this.fit = fit;
            double[] weights = components(options.getWeights());
            double[] original = components(baseline.getWeights());
            double distance = Math.abs(options.getThreshold() - baseline.getThreshold());
            for (int i = 0; i < weights.length; i++) {
                distance += Math.abs(weights[i] - original[i]);
            }
            baselineDistance = distance;
        }
    }

    private static final class SearchState {
        private final AnalysisReport training;
        private final Map<String,FingerprintData> data;
        private final Options options;
        private final Set<String> seen = new HashSet<>();
        private Candidate baseline;
        private Candidate best;
        private int evaluations;
        private long comparisons;

        private SearchState(AnalysisReport training, Map<String,FingerprintData> data, Options options) {
            this.training = training;
            this.data = data;
            this.options = options;
        }

        private void run() {
            evaluate(options.baseline, options.maxEvaluations);
            baseline = best;
            int coarseLimit = Math.max(1, options.maxEvaluations - options.maxEvaluations / 4);
            List<Weights> seeds = List.of(options.baseline.getWeights(), new Weights(1, 1, 1, 1), new Weights(1, 0, 0, 0), new Weights(0, 1, 0, 0),
                            new Weights(0, 0, 1, 0), new Weights(0, 0, 0, 1));
            for (Weights seed : seeds) {
                for (int tick = 0; tick <= 10; tick++) {
                    evaluate(configuration(tick / 10.0, seed), coarseLimit);
                }
                evaluate(configuration(options.baseline.getThreshold(), seed), coarseLimit);
            }
            refine(.05, .10, coarseLimit);
            refine(.01, .025, options.maxEvaluations);
        }

        private void refine(double thresholdStep, double weightStep, int limit) {
            while (evaluations < limit) {
                Candidate previous = best;
                evaluate(configuration(round(Math.max(0, previous.options.getThreshold() - thresholdStep)), previous.options.getWeights()), limit);
                evaluate(configuration(round(Math.min(1, previous.options.getThreshold() + thresholdStep)), previous.options.getWeights()), limit);
                double[] weights = components(previous.options.getWeights());
                for (int from = 0; from < 4; from++) {
                    if (weights[from] + 1e-12 < weightStep) {
                        continue;
                    }
                    for (int to = 0; to < 4; to++) {
                        if (from != to) {
                            double[] moved = weights.clone();
                            moved[from] = Math.max(0, round(moved[from] - weightStep));
                            moved[to] = round(moved[to] + weightStep);
                            evaluate(configuration(previous.options.getThreshold(), new Weights(moved[0], moved[1], moved[2], moved[3])), limit);
                        }
                    }
                }
                if (compare(best, previous) >= 0) {
                    break;
                }
            }
        }

        private QueryClusterer.Options configuration(double threshold, Weights weights) {
            return new QueryClusterer.Options(threshold, options.baseline.getFeatureLimit(), options.baseline.getPostingLimit(),
                            options.baseline.getCandidateLimit(), weights);
        }

        private void evaluate(QueryClusterer.Options candidate, int limit) {
            if (evaluations >= limit || !seen.add(key(candidate))) {
                return;
            }
            QueryClusterer.Result grouped = new QueryClusterer().cluster(training, candidate);
            Candidate evaluated = new Candidate(candidate, fit(grouped, data), options.baseline);
            comparisons += grouped.getComparisonCount();
            evaluations++;
            if (best == null || compare(evaluated, best) < 0) {
                best = evaluated;
            }
        }
    }

    private static int compare(Candidate left, Candidate right) {
        int order = Double.compare(right.fit.f1, left.fit.f1);
        if (order == 0) {
            order = Double.compare(left.baselineDistance, right.baselineDistance);
        }
        if (order == 0) {
            order = Long.compare(left.fit.comparisonCount, right.fit.comparisonCount);
        }
        if (order == 0) {
            order = Double.compare(left.options.getThreshold(), right.options.getThreshold());
        }
        double[] l = components(left.options.getWeights());
        double[] r = components(right.options.getWeights());
        for (int i = 0; order == 0 && i < l.length; i++) {
            order = Double.compare(l[i], r[i]);
        }
        return order;
    }

    private static double[] components(Weights weights) {
        return new double[] {weights.getBindings(), weights.getCounts(), weights.getTopology(), weights.getComplexity()};
    }

    private static double round(double value) {
        return Math.round(value * 1_000_000_000.0) / 1_000_000_000.0;
    }

    private static String key(QueryClusterer.Options options) {
        StringBuilder key = new StringBuilder().append(round(options.getThreshold()));
        for (double weight : components(options.getWeights())) {
            key.append(':').append(round(weight));
        }
        return key.toString();
    }

    private static boolean sameOptions(QueryClusterer.Options left, QueryClusterer.Options right) {
        return key(left).equals(key(right));
    }

    private static <T> List<T> immutable(List<T> values) {
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    /** The baseline supplies fixed discovery limits. The positive evaluation budget bounds training configurations only. */
    public static final class Options {
        private final QueryClusterer.Options baseline;
        private final int maxEvaluations;

        public Options() {
            this(new QueryClusterer.Options(), 128);
        }

        public Options(QueryClusterer.Options baseline, int maxEvaluations) {
            this.baseline = Objects.requireNonNull(baseline, "baseline");
            if (maxEvaluations < 1) {
                throw new IllegalArgumentException("Maximum evaluations must be positive");
            }
            this.maxEvaluations = maxEvaluations;
        }

        public QueryClusterer.Options getBaseline() {
            return baseline;
        }

        public int getMaxEvaluations() {
            return maxEvaluations;
        }
    }

    /** Weighted B-cubed metrics: one vote per fingerprint, divided equally among its distinct labels when those labels conflict. */
    public static final class Fit {
        private final double precision;
        private final double recall;
        private final double f1;
        private final int fingerprintCount;
        private final int groupCount;
        private final long comparisonCount;

        private Fit(double precision, double recall, int fingerprintCount, int groupCount, long comparisonCount) {
            this.precision = precision;
            this.recall = recall;
            this.f1 = precision + recall == 0 ? 0 : 2 * precision * recall / (precision + recall);
            this.fingerprintCount = fingerprintCount;
            this.groupCount = groupCount;
            this.comparisonCount = comparisonCount;
        }

        public double getPrecision() {
            return precision;
        }

        public double getRecall() {
            return recall;
        }

        public double getF1() {
            return f1;
        }

        public int getFingerprintCount() {
            return fingerprintCount;
        }

        public int getGroupCount() {
            return groupCount;
        }

        public long getComparisonCount() {
            return comparisonCount;
        }
    }

    /** Immutable split coverage. Unavailable validation means fewer than two eligible labels; unvalidated labels and conflicts remain in training. */
    public static final class Split {
        private final int trainingFingerprintCount;
        private final List<String> holdoutFingerprintKeys;
        private final List<String> validatedBuckets;
        private final List<String> unvalidatedBuckets;
        private final int conflictingTrainingFingerprintCount;

        private Split(int trainingFingerprintCount, List<String> holdoutFingerprintKeys, List<String> validatedBuckets, List<String> unvalidatedBuckets,
                        int conflictingTrainingFingerprintCount) {
            this.trainingFingerprintCount = trainingFingerprintCount;
            this.holdoutFingerprintKeys = immutable(holdoutFingerprintKeys);
            this.validatedBuckets = immutable(validatedBuckets);
            this.unvalidatedBuckets = immutable(unvalidatedBuckets);
            this.conflictingTrainingFingerprintCount = conflictingTrainingFingerprintCount;
        }

        public int getTrainingFingerprintCount() {
            return trainingFingerprintCount;
        }

        public int getHoldoutFingerprintCount() {
            return holdoutFingerprintKeys.size();
        }

        public List<String> getHoldoutFingerprintKeys() {
            return holdoutFingerprintKeys;
        }

        public List<String> getValidatedBuckets() {
            return validatedBuckets;
        }

        public List<String> getUnvalidatedBuckets() {
            return unvalidatedBuckets;
        }

        public int getConflictingTrainingFingerprintCount() {
            return conflictingTrainingFingerprintCount;
        }

        public boolean isValidationAvailable() {
            return !holdoutFingerprintKeys.isEmpty();
        }
    }

    public static final class ExcludedSample {
        private final int index;
        private final String id;
        private final Status status;
        private final String error;

        private ExcludedSample(int index, String id, Status status, String error) {
            this.index = index;
            this.id = id;
            this.status = status;
            this.error = error;
        }

        public int getIndex() {
            return index;
        }

        public String getId() {
            return id;
        }

        public Status getStatus() {
            return status;
        }

        public String getError() {
            return error;
        }
    }

    public static final class LabelConflict {
        private final String fingerprintKey;
        private final List<String> buckets;
        private final List<Integer> sampleIndexes;
        private final List<String> sampleIds;

        private LabelConflict(String fingerprintKey, List<String> buckets, List<Integer> sampleIndexes, List<String> sampleIds) {
            this.fingerprintKey = fingerprintKey;
            this.buckets = immutable(buckets);
            this.sampleIndexes = immutable(sampleIndexes);
            this.sampleIds = immutable(sampleIds);
        }

        public String getFingerprintKey() {
            return fingerprintKey;
        }

        public List<String> getBuckets() {
            return buckets;
        }

        public List<Integer> getSampleIndexes() {
            return sampleIndexes;
        }

        public List<String> getSampleIds() {
            return sampleIds;
        }
    }

    public static final class ProtectedProfileConflict {
        private final String bucket;
        private final List<String> profiles;
        private final List<Integer> sampleIndexes;

        private ProtectedProfileConflict(String bucket, List<String> profiles, List<Integer> sampleIndexes) {
            this.bucket = bucket;
            this.profiles = immutable(profiles);
            this.sampleIndexes = immutable(sampleIndexes);
        }

        public String getBucket() {
            return bucket;
        }

        public List<String> getProfiles() {
            return profiles;
        }

        public List<Integer> getSampleIndexes() {
            return sampleIndexes;
        }
    }

    public static final class Diagnostics {
        private final List<ExcludedSample> excludedSamples;
        private final List<LabelConflict> labelConflicts;
        private final List<ProtectedProfileConflict> protectedProfileConflicts;
        private final List<FingerprintDiagnostic> fingerprintDiagnostics;

        private Diagnostics(List<ExcludedSample> excludedSamples, List<LabelConflict> labelConflicts, List<ProtectedProfileConflict> protectedProfileConflicts,
                        List<FingerprintDiagnostic> fingerprintDiagnostics) {
            this.excludedSamples = immutable(excludedSamples);
            this.labelConflicts = immutable(labelConflicts);
            this.protectedProfileConflicts = immutable(protectedProfileConflicts);
            this.fingerprintDiagnostics = immutable(fingerprintDiagnostics);
        }

        public List<ExcludedSample> getExcludedSamples() {
            return excludedSamples;
        }

        public List<LabelConflict> getLabelConflicts() {
            return labelConflicts;
        }

        public List<ProtectedProfileConflict> getProtectedProfileConflicts() {
            return protectedProfileConflicts;
        }

        /** Successful entries with uncertain enrichment; distinct from parsing/unsupported exclusions. */
        public List<FingerprintDiagnostic> getFingerprintDiagnostics() {
            return fingerprintDiagnostics;
        }
    }

    public static final class FingerprintDiagnostic {
        private final String fingerprintKey;
        private final List<Integer> sampleIndexes;
        private final List<String> sampleIds;
        private final List<String> messages;

        private FingerprintDiagnostic(String fingerprintKey, List<Integer> sampleIndexes, List<String> sampleIds, List<String> messages) {
            this.fingerprintKey = fingerprintKey;
            this.sampleIndexes = immutable(sampleIndexes);
            this.sampleIds = immutable(sampleIds);
            this.messages = immutable(messages);
        }

        public String getFingerprintKey() {
            return fingerprintKey;
        }

        public List<Integer> getSampleIndexes() {
            return sampleIndexes;
        }

        public List<String> getSampleIds() {
            return sampleIds;
        }

        public List<String> getMessages() {
            return messages;
        }
    }

    public static final class Search {
        private final int maxEvaluations;
        private final int evaluationCount;
        private final long comparisonCount;
        private final boolean budgetExhausted;

        private Search(int maxEvaluations, int evaluationCount, long comparisonCount, boolean budgetExhausted) {
            this.maxEvaluations = maxEvaluations;
            this.evaluationCount = evaluationCount;
            this.comparisonCount = comparisonCount;
            this.budgetExhausted = budgetExhausted;
        }

        public int getVersion() {
            return SEARCH_VERSION;
        }

        public int getMaxEvaluations() {
            return maxEvaluations;
        }

        public int getEvaluationCount() {
            return evaluationCount;
        }

        /** Training search only; independent holdout and descriptive full-data runs are excluded. */
        public long getComparisonCount() {
            return comparisonCount;
        }

        public boolean isBudgetExhausted() {
            return budgetExhausted;
        }
    }

    public static final class Result {
        private final QueryTuningConfiguration configuration;
        private final Fit baselineTrainingFit;
        private final Fit trainingFit;
        private final Fit baselineHoldoutFit;
        private final Fit holdoutFit;
        private final Fit baselineFullFit;
        private final Fit fullFit;
        private final Split split;
        private final Diagnostics diagnostics;
        private final Search search;

        private Result(QueryTuningConfiguration configuration, Fit baselineTrainingFit, Fit trainingFit, Fit baselineHoldoutFit, Fit holdoutFit,
                        Fit baselineFullFit, Fit fullFit, Split split, Diagnostics diagnostics, Search search) {
            this.configuration = configuration;
            this.baselineTrainingFit = baselineTrainingFit;
            this.trainingFit = trainingFit;
            this.baselineHoldoutFit = baselineHoldoutFit;
            this.holdoutFit = holdoutFit;
            this.baselineFullFit = baselineFullFit;
            this.fullFit = fullFit;
            this.split = split;
            this.diagnostics = diagnostics;
            this.search = search;
        }

        public QueryTuningConfiguration getConfiguration() {
            return configuration;
        }

        public Fit getBaselineTrainingFit() {
            return baselineTrainingFit;
        }

        public Fit getTrainingFit() {
            return trainingFit;
        }

        public Optional<Fit> getBaselineHoldoutFit() {
            return Optional.ofNullable(baselineHoldoutFit);
        }

        public Optional<Fit> getHoldoutFit() {
            return Optional.ofNullable(holdoutFit);
        }

        public Fit getBaselineFullFit() {
            return baselineFullFit;
        }

        public Fit getFullFit() {
            return fullFit;
        }

        public Split getSplit() {
            return split;
        }

        public Diagnostics getDiagnostics() {
            return diagnostics;
        }

        public Search getSearch() {
            return search;
        }

        public int getFingerprintVersion() {
            return QueryFingerprint.VERSION;
        }
    }
}
