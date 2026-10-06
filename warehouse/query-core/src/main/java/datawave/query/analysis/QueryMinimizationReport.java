package datawave.query.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.TreeMap;

import datawave.query.analysis.QueryAnalyzer.Category;
import datawave.query.analysis.QueryAnalyzer.QueryAnalysis;

/**
 * Immutable diagnostics for an existing minimization run. Statistics describe syntactic grouping and observed selection, not execution equivalence or the
 * globally optimal workload. Construct reports through {@link QueryAnalyzer#summarizeMinimization(QueryClusterer.Result)} or its cursor overload.
 */
public final class QueryMinimizationReport {
    private final ClusteringStats clustering;
    private final Optional<SelectionStats> selection;

    private QueryMinimizationReport(ClusteringStats clustering, SelectionStats selection) {
        this.clustering = clustering;
        this.selection = Optional.ofNullable(selection);
    }

    static QueryMinimizationReport from(QueryClusterer.Result result) {
        return new QueryMinimizationReport(new ClusteringStats(result), null);
    }

    static QueryMinimizationReport from(QueryWorkloadSelector.Cursor cursor) {
        ClusteringStats clustering = new ClusteringStats(cursor.getClusteringResult());
        return new QueryMinimizationReport(clustering, new SelectionStats(cursor, clustering));
    }

    public ClusteringStats getClustering() {
        return clustering;
    }

    public Optional<SelectionStats> getSelection() {
        return selection;
    }

    /** Deterministic, locale-independent summary. Original query text remains available only through the existing group API. */
    public String describe() {
        StringBuilder text = new StringBuilder();
        ClusteringStats c = clustering;
        text.append(String.format(Locale.ROOT, "Inputs: %d; successful: %d; invalid: %d; unsupported: %d; excluded: %d.%n"
                        + "Distinct text/syntax queries: %d; duplicate occurrences: %d; exact structural families: %d; enriched fingerprints: %d.%n"
                        + "Groups: %d; protected profiles: %d.%n", c.inputCount, c.successfulCount, c.invalidCount, c.unsupportedCount, c.getExcludedCount(),
                        c.distinctQueryCount, c.getDuplicateOccurrenceCount(), c.exactStructuralFamilyCount, c.fingerprintCount, c.getGroupCount(),
                        c.getProfileCount()));
        text.append("Potential reduction with one representative per group: successful occurrences ").append(percent(c.getOccurrenceReduction()))
                        .append("; distinct queries ").append(percent(c.getDistinctQueryReduction()))
                        .append(". Excluded inputs are outside both denominators.\n");
        appendDistribution(text, "Group occurrences", c.occurrenceDistribution);
        appendDistribution(text, "Group distinct queries", c.distinctQueryDistribution);
        text.append("Representative similarity (one score per fingerprint, including representative 1): min ").append(number(c.minimumSimilarity))
                        .append(", mean ").append(number(c.meanSimilarity)).append(".\n");
        text.append(String.format(Locale.ROOT, "Clustering: threshold %.3f; feature limit %d; posting limit %d; candidate limit %d; comparisons %d.%n",
                        c.options.getThreshold(), c.options.getFeatureLimit(), c.options.getPostingLimit(), c.options.getCandidateLimit(), c.comparisonCount));
        text.append(String.format(Locale.ROOT,
                        "Restricted searches: feature-truncated fingerprints %d; candidate-truncated fingerprints %d; feature-posting evictions %d; fallback-posting evictions %d.%n"
                                        + "New groups: new protected profile %d; no considered candidate met threshold %d; best rejected scores min/mean/max %s/%s/%s.%n",
                        c.featureTruncatedFingerprintCount, c.candidateTruncatedFingerprintCount, c.featurePostingEvictionCount, c.fallbackPostingEvictionCount,
                        c.newProfileGroupCount, c.noQualifyingCandidateGroupCount, number(c.minimumBestRejectedScore), number(c.meanBestRejectedScore),
                        number(c.maximumBestRejectedScore)));
        text.append("Protected-profile boundaries always apply. A higher threshold requires closer representative similarity; lowering it can merge more compatible fingerprints.\n");
        if (c.featureTruncatedFingerprintCount > 0 || c.candidateTruncatedFingerprintCount > 0 || c.featurePostingEvictionCount > 0
                        || c.fallbackPostingEvictionCount > 0) {
            text.append("Observed limits restricted candidate discovery. Increasing feature, posting, or candidate limits can broaden the search and increase work; these counters do not establish that a compatible group was missed.\n");
        } else {
            text.append("No feature/candidate truncation or posting eviction was observed. Candidate limits bound search work.\n");
        }
        if (c.noQualifyingCandidateGroupCount > 0) {
            text.append("Best rejected scores describe considered candidates only; inspect them with the threshold when tuning grouping strictness.\n");
        }
        if (!c.highlightedGroups.isEmpty()) {
            text.append("Highlighted groups (largest by occurrences, then weakest multi-fingerprint groups; each shown once):\n");
            for (GroupStats group : c.highlightedGroups) {
                text.append(String.format(Locale.ROOT,
                                "  %s: occurrences %d; distinct %d; fingerprints %d; similarity min/mean %.3f/%.3f; categories %s; fields %s; functions %s; protections %s; profile %s%n",
                                group.id, group.occurrenceCount, group.distinctQueryCount, group.fingerprintCount, group.minimumSimilarity,
                                group.meanSimilarity, group.categories, group.fields, group.functions, group.protections, group.protectedProfile));
            }
        }
        selection.ifPresent(s -> {
            text.append(String.format(Locale.ROOT, "Selection: emitted %d; remaining %d; round %d; comparisons %d.%n", s.emittedCount, s.remainingCount,
                            s.currentRound, s.comparisonCount));
            text.append(String.format(Locale.ROOT, "Coverage: groups %d (%s); fingerprints %d (%s); protected profiles %d (%s); distinct queries %s.%n",
                            s.coveredGroupCount, percent(s.groupCoverage), s.coveredFingerprintCount, percent(s.fingerprintCoverage), s.coveredProfileCount,
                            percent(s.profileCoverage), percent(s.distinctQueryCoverage)));
            text.append(String.format(Locale.ROOT,
                            "Original occurrences represented by emitted identities and their exact duplicates: %d (%s of successful inputs).%n",
                            s.representedOccurrenceCount, percent(s.occurrenceCoverage)));
            text.append("Observed diversity distance (excluding first selection): min ").append(number(s.minimumDiversityDistance)).append(", mean ")
                            .append(number(s.meanDiversityDistance)).append(".\n");
            text.append(String.format(Locale.ROOT, "Selection limits: group window %d; member window %d; first landmarks %d; recent landmarks %d.%n",
                            s.options.getGroupWindow(), s.options.getMemberWindow(), s.options.getFirstLandmarks(), s.options.getRecentLandmarks()));
            text.append("Larger selection windows consider more candidates; more landmarks compare diversity against more retained selections. Both increase comparison work. Observed distances use the configured bounded search.\n");
        });
        return text.toString();
    }

    private static void appendDistribution(StringBuilder text, String label, Distribution distribution) {
        text.append(label).append(": min ").append(number(distribution.minimum)).append(", mean ").append(number(distribution.mean)).append(", p50 ")
                        .append(number(distribution.p50)).append(", p95 ").append(number(distribution.p95)).append(", max ")
                        .append(number(distribution.maximum)).append("; singletons ").append(distribution.singletonCount).append(".\n");
    }

    private static String number(OptionalDouble value) {
        return value.isPresent() ? String.format(Locale.ROOT, "%.3f", value.getAsDouble()) : "n/a";
    }

    private static String percent(OptionalDouble value) {
        return value.isPresent() ? String.format(Locale.ROOT, "%.1f%%", 100 * value.getAsDouble()) : "n/a";
    }

    private static OptionalDouble fraction(int numerator, int denominator) {
        return denominator == 0 ? OptionalDouble.empty() : OptionalDouble.of((double) numerator / denominator);
    }

    private static OptionalDouble reduction(int groups, int denominator) {
        return denominator == 0 ? OptionalDouble.empty() : OptionalDouble.of(1 - (double) groups / denominator);
    }

    /** Group-size distribution using nearest-rank percentiles. Empty inputs leave all numeric summaries absent. */
    public static final class Distribution {
        private final OptionalDouble minimum;
        private final OptionalDouble mean;
        private final OptionalDouble p50;
        private final OptionalDouble p95;
        private final OptionalDouble maximum;
        private final int singletonCount;

        private Distribution(int[] sizes) {
            java.util.Arrays.sort(sizes);
            minimum = sizes.length == 0 ? OptionalDouble.empty() : OptionalDouble.of(sizes[0]);
            maximum = sizes.length == 0 ? OptionalDouble.empty() : OptionalDouble.of(sizes[sizes.length - 1]);
            mean = java.util.Arrays.stream(sizes).average();
            p50 = percentile(sizes, 0.50);
            p95 = percentile(sizes, 0.95);
            singletonCount = (int) java.util.Arrays.stream(sizes).filter(size -> size == 1).count();
        }

        private static OptionalDouble percentile(int[] sizes, double percentile) {
            return sizes.length == 0 ? OptionalDouble.empty() : OptionalDouble.of(sizes[(int) Math.ceil(percentile * sizes.length) - 1]);
        }

        public OptionalDouble getMinimum() {
            return minimum;
        }

        public OptionalDouble getMean() {
            return mean;
        }

        public OptionalDouble getP50() {
            return p50;
        }

        public OptionalDouble getP95() {
            return p95;
        }

        public OptionalDouble getMaximum() {
            return maximum;
        }

        public int getSingletonCount() {
            return singletonCount;
        }
    }

    /** Complete group statistics, ordered by stable group ID in {@link ClusteringStats#getGroups()}. */
    public static final class GroupStats {
        private final String id;
        private final String protectedProfile;
        private final int occurrenceCount;
        private final int distinctQueryCount;
        private final int fingerprintCount;
        private final Set<Category> categories;
        private final Set<String> fields;
        private final Set<String> functions;
        private final Set<String> protections;
        private final double minimumSimilarity;
        private final double meanSimilarity;

        private GroupStats(QueryClusterer.Group group) {
            QueryAnalysis representative = group.getRepresentative();
            id = group.getId();
            protectedProfile = representative.getFingerprint().getProtectedProfile();
            occurrenceCount = group.getOccurrenceCount();
            distinctQueryCount = group.getDistinctQueryCount();
            fingerprintCount = group.getFingerprintCount();
            // These sets are already immutable snapshots on the immutable analysis/fingerprint objects.
            categories = representative.getCategories();
            fields = representative.getFields();
            functions = representative.getFunctions();
            protections = representative.getFingerprint().getProtections();
            minimumSimilarity = group.getMinimumSimilarity();
            meanSimilarity = group.getMeanSimilarity();
        }

        public String getId() {
            return id;
        }

        public String getProtectedProfile() {
            return protectedProfile;
        }

        public int getOccurrenceCount() {
            return occurrenceCount;
        }

        public int getDistinctQueryCount() {
            return distinctQueryCount;
        }

        public int getFingerprintCount() {
            return fingerprintCount;
        }

        public Set<Category> getCategories() {
            return categories;
        }

        public Set<String> getFields() {
            return fields;
        }

        public Set<String> getFunctions() {
            return functions;
        }

        public Set<String> getProtections() {
            return protections;
        }

        public double getMinimumSimilarity() {
            return minimumSimilarity;
        }

        public double getMeanSimilarity() {
            return meanSimilarity;
        }
    }

    /** Aggregated groups within one protected profile. */
    public static final class ProfileStats {
        private final String protectedProfile;
        private final int groupCount;
        private final int fingerprintCount;
        private final int distinctQueryCount;
        private final int occurrenceCount;

        private ProfileStats(String profile, List<GroupStats> groups) {
            protectedProfile = profile;
            groupCount = groups.size();
            fingerprintCount = groups.stream().mapToInt(GroupStats::getFingerprintCount).sum();
            distinctQueryCount = groups.stream().mapToInt(GroupStats::getDistinctQueryCount).sum();
            occurrenceCount = groups.stream().mapToInt(GroupStats::getOccurrenceCount).sum();
        }

        public String getProtectedProfile() {
            return protectedProfile;
        }

        public int getGroupCount() {
            return groupCount;
        }

        public int getFingerprintCount() {
            return fingerprintCount;
        }

        public int getDistinctQueryCount() {
            return distinctQueryCount;
        }

        public int getOccurrenceCount() {
            return occurrenceCount;
        }
    }

    /** Successful-input statistics and counters captured during the original clustering run. */
    public static final class ClusteringStats {
        private final QueryClusterer.Options options;
        private final int inputCount;
        private final int successfulCount;
        private final int invalidCount;
        private final int unsupportedCount;
        private final int distinctQueryCount;
        private final int exactStructuralFamilyCount;
        private final int fingerprintCount;
        private final List<GroupStats> groups;
        private final List<ProfileStats> profiles;
        private final List<GroupStats> highlightedGroups;
        private final Distribution occurrenceDistribution;
        private final Distribution distinctQueryDistribution;
        private final OptionalDouble minimumSimilarity;
        private final OptionalDouble meanSimilarity;
        private final long comparisonCount;
        private final int featureTruncatedFingerprintCount;
        private final int candidateTruncatedFingerprintCount;
        private final long featurePostingEvictionCount;
        private final long fallbackPostingEvictionCount;
        private final int newProfileGroupCount;
        private final int noQualifyingCandidateGroupCount;
        private final OptionalDouble minimumBestRejectedScore;
        private final OptionalDouble meanBestRejectedScore;
        private final OptionalDouble maximumBestRejectedScore;

        private ClusteringStats(QueryClusterer.Result result) {
            options = result.getOptions();
            inputCount = result.getInputCount();
            successfulCount = result.getSuccessfulCount();
            invalidCount = result.getInvalidCount();
            unsupportedCount = result.getUnsupportedCount();
            exactStructuralFamilyCount = result.getExactStructuralFamilyCount();
            fingerprintCount = result.getFingerprintCount();
            List<GroupStats> summaries = new ArrayList<>();
            for (QueryClusterer.Group group : result.getGroups()) {
                summaries.add(new GroupStats(group));
            }
            summaries.sort(Comparator.comparing(GroupStats::getId));
            groups = Collections.unmodifiableList(summaries);
            distinctQueryCount = groups.stream().mapToInt(GroupStats::getDistinctQueryCount).sum();
            occurrenceDistribution = new Distribution(groups.stream().mapToInt(GroupStats::getOccurrenceCount).toArray());
            distinctQueryDistribution = new Distribution(groups.stream().mapToInt(GroupStats::getDistinctQueryCount).toArray());
            minimumSimilarity = groups.stream().mapToDouble(GroupStats::getMinimumSimilarity).min();
            meanSimilarity = fingerprintCount == 0 ? OptionalDouble.empty()
                            : OptionalDouble.of(groups.stream().mapToDouble(g -> g.meanSimilarity * g.fingerprintCount).sum() / fingerprintCount);
            Map<String,List<GroupStats>> byProfile = new TreeMap<>();
            for (GroupStats group : groups) {
                byProfile.computeIfAbsent(group.protectedProfile, key -> new ArrayList<>()).add(group);
            }
            List<ProfileStats> totals = new ArrayList<>();
            byProfile.forEach((profile, members) -> totals.add(new ProfileStats(profile, members)));
            profiles = Collections.unmodifiableList(totals);
            Map<String,GroupStats> highlights = new LinkedHashMap<>();
            groups.stream().sorted(Comparator.comparingInt(GroupStats::getOccurrenceCount).reversed().thenComparing(GroupStats::getId)).limit(5)
                            .forEach(group -> highlights.put(group.id, group));
            groups.stream().filter(group -> group.fingerprintCount > 1)
                            .sorted(Comparator.comparingDouble(GroupStats::getMinimumSimilarity).thenComparing(GroupStats::getId)).limit(5)
                            .forEach(group -> highlights.putIfAbsent(group.id, group));
            highlightedGroups = Collections.unmodifiableList(new ArrayList<>(highlights.values()));
            comparisonCount = result.getComparisonCount();
            featureTruncatedFingerprintCount = result.getFeatureTruncatedFingerprintCount();
            candidateTruncatedFingerprintCount = result.getCandidateTruncatedFingerprintCount();
            featurePostingEvictionCount = result.getFeaturePostingEvictionCount();
            fallbackPostingEvictionCount = result.getFallbackPostingEvictionCount();
            newProfileGroupCount = result.getNewProfileGroupCount();
            noQualifyingCandidateGroupCount = result.getNoQualifyingCandidateGroupCount();
            minimumBestRejectedScore = result.getMinimumBestRejectedScore();
            meanBestRejectedScore = result.getMeanBestRejectedScore();
            maximumBestRejectedScore = result.getMaximumBestRejectedScore();
        }

        public QueryClusterer.Options getOptions() {
            return options;
        }

        public int getInputCount() {
            return inputCount;
        }

        public int getSuccessfulCount() {
            return successfulCount;
        }

        public int getInvalidCount() {
            return invalidCount;
        }

        public int getUnsupportedCount() {
            return unsupportedCount;
        }

        public int getExcludedCount() {
            return invalidCount + unsupportedCount;
        }

        public int getDistinctQueryCount() {
            return distinctQueryCount;
        }

        public int getDuplicateOccurrenceCount() {
            return successfulCount - distinctQueryCount;
        }

        public int getExactStructuralFamilyCount() {
            return exactStructuralFamilyCount;
        }

        public int getFingerprintCount() {
            return fingerprintCount;
        }

        public int getGroupCount() {
            return groups.size();
        }

        public int getProfileCount() {
            return profiles.size();
        }

        public OptionalDouble getOccurrenceReduction() {
            return reduction(groups.size(), successfulCount);
        }

        public OptionalDouble getDistinctQueryReduction() {
            return reduction(groups.size(), distinctQueryCount);
        }

        public List<GroupStats> getGroups() {
            return groups;
        }

        public List<ProfileStats> getProfiles() {
            return profiles;
        }

        public List<GroupStats> getHighlightedGroups() {
            return highlightedGroups;
        }

        public Distribution getOccurrenceDistribution() {
            return occurrenceDistribution;
        }

        public Distribution getDistinctQueryDistribution() {
            return distinctQueryDistribution;
        }

        public OptionalDouble getMinimumSimilarity() {
            return minimumSimilarity;
        }

        public OptionalDouble getMeanSimilarity() {
            return meanSimilarity;
        }

        public long getComparisonCount() {
            return comparisonCount;
        }

        public int getFeatureTruncatedFingerprintCount() {
            return featureTruncatedFingerprintCount;
        }

        public int getCandidateTruncatedFingerprintCount() {
            return candidateTruncatedFingerprintCount;
        }

        public long getFeaturePostingEvictionCount() {
            return featurePostingEvictionCount;
        }

        public long getFallbackPostingEvictionCount() {
            return fallbackPostingEvictionCount;
        }

        public int getNewProfileGroupCount() {
            return newProfileGroupCount;
        }

        public int getNoQualifyingCandidateGroupCount() {
            return noQualifyingCandidateGroupCount;
        }

        public OptionalDouble getMinimumBestRejectedScore() {
            return minimumBestRejectedScore;
        }

        public OptionalDouble getMeanBestRejectedScore() {
            return meanBestRejectedScore;
        }

        public OptionalDouble getMaximumBestRejectedScore() {
            return maximumBestRejectedScore;
        }
    }

    /** Selection progress captured at report creation; later cursor operations do not change this object. */
    public static final class SelectionStats {
        private final QueryWorkloadSelector.Options options;
        private final int emittedCount;
        private final int remainingCount;
        private final int currentRound;
        private final long comparisonCount;
        private final int coveredGroupCount;
        private final int coveredFingerprintCount;
        private final int coveredProfileCount;
        private final int representedOccurrenceCount;
        private final OptionalDouble groupCoverage;
        private final OptionalDouble fingerprintCoverage;
        private final OptionalDouble profileCoverage;
        private final OptionalDouble occurrenceCoverage;
        private final OptionalDouble distinctQueryCoverage;
        private final OptionalDouble minimumDiversityDistance;
        private final OptionalDouble meanDiversityDistance;

        private SelectionStats(QueryWorkloadSelector.Cursor cursor, ClusteringStats clustering) {
            options = cursor.getOptions();
            emittedCount = cursor.getEmittedCount();
            remainingCount = cursor.getRemainingCount();
            currentRound = cursor.getCurrentRound();
            comparisonCount = cursor.getComparisonCount();
            coveredGroupCount = cursor.getCoveredGroupCount();
            coveredFingerprintCount = cursor.getCoveredFingerprintCount();
            coveredProfileCount = cursor.getCoveredProfileCount();
            representedOccurrenceCount = cursor.getRepresentedOccurrenceCount();
            groupCoverage = fraction(coveredGroupCount, clustering.getGroupCount());
            fingerprintCoverage = fraction(coveredFingerprintCount, clustering.fingerprintCount);
            profileCoverage = fraction(coveredProfileCount, clustering.getProfileCount());
            occurrenceCoverage = fraction(representedOccurrenceCount, clustering.successfulCount);
            distinctQueryCoverage = fraction(emittedCount, clustering.distinctQueryCount);
            minimumDiversityDistance = cursor.getMinimumDiversityDistance();
            meanDiversityDistance = cursor.getMeanDiversityDistance();
        }

        public QueryWorkloadSelector.Options getOptions() {
            return options;
        }

        public int getEmittedCount() {
            return emittedCount;
        }

        public int getRemainingCount() {
            return remainingCount;
        }

        public int getCurrentRound() {
            return currentRound;
        }

        public long getComparisonCount() {
            return comparisonCount;
        }

        public int getCoveredGroupCount() {
            return coveredGroupCount;
        }

        public int getCoveredFingerprintCount() {
            return coveredFingerprintCount;
        }

        public int getCoveredProfileCount() {
            return coveredProfileCount;
        }

        public int getRepresentedOccurrenceCount() {
            return representedOccurrenceCount;
        }

        public OptionalDouble getGroupCoverage() {
            return groupCoverage;
        }

        public OptionalDouble getFingerprintCoverage() {
            return fingerprintCoverage;
        }

        public OptionalDouble getProfileCoverage() {
            return profileCoverage;
        }

        public OptionalDouble getOccurrenceCoverage() {
            return occurrenceCoverage;
        }

        public OptionalDouble getDistinctQueryCoverage() {
            return distinctQueryCoverage;
        }

        public OptionalDouble getMinimumDiversityDistance() {
            return minimumDiversityDistance;
        }

        public OptionalDouble getMeanDiversityDistance() {
            return meanDiversityDistance;
        }
    }
}
