package datawave.query.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import datawave.query.analysis.QueryAnalyzer.AnalysisReport;
import datawave.query.analysis.QueryAnalyzer.QueryAnalysis;
import datawave.query.analysis.QueryAnalyzer.Status;

/**
 * Bounded deterministic grouping around fixed representatives. Every member meets the configured threshold against its representative. Candidate limits may
 * split otherwise compatible groups; they never relax the threshold or protected profile. No all-pairs similarity matrix is constructed.
 */
public final class QueryClusterer {
    static final Comparator<QueryAnalysis> QUERY_ORDER = Comparator.comparing(QueryFingerprint::inputKey).thenComparingInt(QueryAnalysis::getIndex);

    public Result cluster(AnalysisReport report) {
        return cluster(report, new Options());
    }

    public Result cluster(AnalysisReport report, Options options) {
        Objects.requireNonNull(report, "report");
        Objects.requireNonNull(options, "options");
        Map<String,List<QueryAnalysis>> fingerprints = new TreeMap<>();
        int successful = 0;
        for (QueryAnalysis query : report.getQueries()) {
            if (query.getStatus() == Status.SUCCESS) {
                fingerprints.computeIfAbsent(query.getFingerprint().getKey(), key -> new ArrayList<>()).add(query);
                successful++;
            }
        }
        Map<String,Integer> frequencies = new HashMap<>();
        for (List<QueryAnalysis> queries : fingerprints.values()) {
            queries.sort(QUERY_ORDER);
            for (String feature : features(queries.get(0).getFingerprint())) {
                frequencies.merge(feature, 1, Integer::sum);
            }
        }
        Map<String,Partition> partitions = new HashMap<>();
        List<GroupBuilder> builders = new ArrayList<>();
        long comparisons = 0;
        for (List<QueryAnalysis> queries : fingerprints.values()) {
            QueryAnalysis query = queries.get(0);
            QueryFingerprint fingerprint = query.getFingerprint();
            Partition partition = partitions.computeIfAbsent(fingerprint.getProtectedProfile(), key -> new Partition(options));
            List<String> features = features(fingerprint);
            features.sort(Comparator.<String> comparingInt(frequencies::get).thenComparing(Comparator.naturalOrder()));
            GroupBuilder best = null;
            double bestScore = -1;
            for (GroupBuilder candidate : partition.candidates(features)) {
                comparisons++;
                double score = QuerySimilarity.score(fingerprint, candidate.representative.getFingerprint());
                if (score >= options.threshold && (score > bestScore || (score == bestScore && candidate.id.compareTo(best.id) < 0))) {
                    best = candidate;
                    bestScore = score;
                }
            }
            if (best == null) {
                best = new GroupBuilder(query);
                bestScore = 1;
                builders.add(best);
                partition.add(best, features);
            }
            best.members.addAll(queries);
            best.minimumSimilarity = Math.min(best.minimumSimilarity, bestScore);
        }
        builders.sort(Comparator.comparing(group -> group.id));
        List<Group> groups = builders.stream().map(Group::new).collect(Collectors.toList());
        return new Result(groups, options, comparisons, fingerprints.size(), report.getQueries().size(), successful);
    }

    private static List<String> features(QueryFingerprint fingerprint) {
        List<String> features = new ArrayList<>();
        for (String binding : fingerprint.getBindings()) {
            features.add("B:" + binding);
        }
        for (String token : fingerprint.getTopology()) {
            features.add("T:" + token);
        }
        return features;
    }

    private static final class Partition {
        private final Options options;
        private final Map<String,NavigableSet<GroupBuilder>> postings = new HashMap<>();
        private final NavigableSet<GroupBuilder> fallback = new TreeSet<>(Comparator.comparing(group -> group.id));

        private Partition(Options options) {
            this.options = options;
        }

        private void add(GroupBuilder group, List<String> features) {
            retain(fallback, group);
            for (String feature : features) {
                retain(postings.computeIfAbsent(feature, key -> new TreeSet<>(Comparator.comparing(candidate -> candidate.id))), group);
            }
        }

        private void retain(NavigableSet<GroupBuilder> posting, GroupBuilder group) {
            posting.add(group);
            if (posting.size() > options.postingLimit) {
                posting.pollLast();
            }
        }

        private List<GroupBuilder> candidates(List<String> features) {
            Map<GroupBuilder,Integer> hits = new HashMap<>();
            for (GroupBuilder group : fallback) {
                hits.put(group, 0);
            }
            for (int i = 0; i < Math.min(options.featureLimit, features.size()); i++) {
                NavigableSet<GroupBuilder> posting = postings.get(features.get(i));
                if (posting != null) {
                    for (GroupBuilder group : posting) {
                        hits.merge(group, 1, Integer::sum);
                    }
                }
            }
            return hits.keySet().stream().sorted(Comparator.<GroupBuilder> comparingInt(hits::get).reversed().thenComparing(group -> group.id))
                            .limit(options.candidateLimit).collect(Collectors.toList());
        }
    }

    private static final class GroupBuilder {
        private final String id;
        private final QueryAnalysis representative;
        private final List<QueryAnalysis> members = new ArrayList<>();
        private double minimumSimilarity = 1;

        private GroupBuilder(QueryAnalysis representative) {
            this.representative = representative;
            id = "group:" + representative.getFingerprint().getKey();
        }
    }

    public static final class Options {
        private final double threshold;
        private final int featureLimit;
        private final int postingLimit;
        private final int candidateLimit;

        public Options() {
            this(0.70, 4, 32, 64);
        }

        public Options(double threshold, int featureLimit, int postingLimit, int candidateLimit) {
            if (!Double.isFinite(threshold) || threshold < 0 || threshold > 1 || featureLimit < 1 || postingLimit < 1 || candidateLimit < 1) {
                throw new IllegalArgumentException("Threshold must be in [0,1] and comparison limits must be positive");
            }
            this.threshold = threshold;
            this.featureLimit = featureLimit;
            this.postingLimit = postingLimit;
            this.candidateLimit = candidateLimit;
        }

        public double getThreshold() {
            return threshold;
        }

        public int getFeatureLimit() {
            return featureLimit;
        }

        public int getPostingLimit() {
            return postingLimit;
        }

        public int getCandidateLimit() {
            return candidateLimit;
        }
    }

    public static final class Group {
        private final String id;
        private final QueryAnalysis representative;
        private final List<QueryAnalysis> members;
        private final int distinctQueryCount;
        private final double minimumSimilarity;

        private Group(GroupBuilder builder) {
            id = builder.id;
            representative = builder.representative;
            List<QueryAnalysis> sorted = new ArrayList<>(builder.members);
            sorted.sort(QUERY_ORDER);
            members = Collections.unmodifiableList(sorted);
            distinctQueryCount = (int) members.stream().map(QueryFingerprint::inputKey).distinct().count();
            minimumSimilarity = builder.minimumSimilarity;
        }

        public String getId() {
            return id;
        }

        public QueryAnalysis getRepresentative() {
            return representative;
        }

        /** Includes duplicate input occurrences. Selection collapses exact text/syntax duplicates. */
        public List<QueryAnalysis> getMembers() {
            return members;
        }

        public int getOccurrenceCount() {
            return members.size();
        }

        public int getDistinctQueryCount() {
            return distinctQueryCount;
        }

        public double getMinimumSimilarity() {
            return minimumSimilarity;
        }
    }

    public static final class Result {
        private final List<Group> groups;
        private final Options options;
        private final long comparisons;
        private final int fingerprintCount;
        private final int inputCount;
        private final int successfulCount;

        private Result(List<Group> groups, Options options, long comparisons, int fingerprintCount, int inputCount, int successfulCount) {
            this.groups = Collections.unmodifiableList(groups);
            this.options = options;
            this.comparisons = comparisons;
            this.fingerprintCount = fingerprintCount;
            this.inputCount = inputCount;
            this.successfulCount = successfulCount;
        }

        public List<Group> getGroups() {
            return groups;
        }

        public Options getOptions() {
            return options;
        }

        public long getComparisonCount() {
            return comparisons;
        }

        public int getFingerprintCount() {
            return fingerprintCount;
        }

        public int getInputCount() {
            return inputCount;
        }

        public int getSuccessfulCount() {
            return successfulCount;
        }

        public int getExcludedCount() {
            return inputCount - successfulCount;
        }
    }
}
