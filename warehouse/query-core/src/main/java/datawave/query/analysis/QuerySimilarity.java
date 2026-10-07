package datawave.query.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/** Explainable similarity using structure and selected literal characteristics, without data statistics. */
public final class QuerySimilarity {
    private QuerySimilarity() {}

    public static Result compare(QueryFingerprint left, QueryFingerprint right) {
        return compare(left, right, Weights.DEFAULT);
    }

    public static Result compare(QueryFingerprint left, QueryFingerprint right, Weights weights) {
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
        Objects.requireNonNull(weights, "weights");
        Map<String,Double> components = new LinkedHashMap<>();
        components.put("bindings", overlap(left.getBindings(), right.getBindings()));
        components.put("counts", countedOverlap(left.getCounts(), right.getCounts()));
        components.put("topology", overlap(left.getTopology(), right.getTopology()));
        components.put("complexity", closeness(left.getMeasurements(), right.getMeasurements()));
        boolean compatible = left.getProtectedProfile().equals(right.getProtectedProfile());
        List<String> differences = new ArrayList<>();
        if (!compatible) {
            Set<String> different = new TreeSet<>(left.getProtections());
            different.addAll(right.getProtections());
            different.removeIf(token -> left.getProtections().contains(token) && right.getProtections().contains(token));
            differences.addAll(different);
            if (differences.isEmpty()) {
                differences.add("Protected Boolean structure, argument roles, or uncertain controls differ");
            }
        }
        double score = left == right || left.getKey().equals(right.getKey()) ? 1
                        : score(components.get("bindings"), components.get("counts"), components.get("topology"), components.get("complexity"), weights);
        return new Result(score, compatible, components, differences);
    }

    // The grouping and selection hot paths do not allocate explanation objects or temporary sets.
    static double score(QueryFingerprint left, QueryFingerprint right) {
        return score(left, right, Weights.DEFAULT);
    }

    static double score(QueryFingerprint left, QueryFingerprint right, Weights weights) {
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
        Objects.requireNonNull(weights, "weights");
        if (left == right || left.getKey().equals(right.getKey())) {
            return 1;
        }
        return score(overlap(left.getBindings(), right.getBindings()), countedOverlap(left.getCounts(), right.getCounts()),
                        overlap(left.getTopology(), right.getTopology()), closeness(left.getMeasurements(), right.getMeasurements()), weights);
    }

    private static double score(double bindings, double counts, double topology, double complexity, Weights weights) {
        return Math.min(1, Math.max(0, weights.bindings * bindings + weights.counts * counts + weights.topology * topology + weights.complexity * complexity));
    }

    public static double distance(QueryFingerprint left, QueryFingerprint right) {
        return distance(left, right, Weights.DEFAULT);
    }

    public static double distance(QueryFingerprint left, QueryFingerprint right, Weights weights) {
        double distance = 0.4 * (1 - score(left, right, weights));
        return left.getProtectedProfile().equals(right.getProtectedProfile()) ? distance : 0.6 + distance;
    }

    /** Immutable relative component weights, normalized to sum to one. Protected-profile compatibility remains independent of these weights. */
    public static final class Weights {
        public static final Weights DEFAULT = new Weights();

        private final double bindings;
        private final double counts;
        private final double topology;
        private final double complexity;

        public Weights() {
            this(0.35, 0.30, 0.20, 0.15);
        }

        public Weights(double bindings, double counts, double topology, double complexity) {
            double[] values = {bindings, counts, topology, complexity};
            double maximum = 0;
            double sum = 0;
            double correction = 0;
            for (double value : values) {
                if (!Double.isFinite(value) || value < 0) {
                    throw new IllegalArgumentException("Similarity weights must be finite, nonnegative, and have a positive sum");
                }
                maximum = Math.max(maximum, value);
                // Compensated summation keeps the historical default weights unchanged.
                double adjusted = value - correction;
                double next = sum + adjusted;
                correction = (next - sum) - adjusted;
                sum = next;
            }
            if (maximum == 0) {
                throw new IllegalArgumentException("Similarity weights must have a positive sum");
            }
            if (!Double.isFinite(sum)) {
                // Scale before summing when large finite weights would overflow.
                bindings /= maximum;
                counts /= maximum;
                topology /= maximum;
                complexity /= maximum;
                sum = bindings + counts + topology + complexity;
            }
            // Division can leave a rounded total a few ulps from one. Preserve those normalized values on JSON reload rather than changing their bits again.
            if (Math.abs(sum - 1) <= 2 * Math.ulp(1.0)) {
                sum = 1;
            }
            this.bindings = bindings / sum;
            this.counts = counts / sum;
            this.topology = topology / sum;
            this.complexity = complexity / sum;
        }

        public double getBindings() {
            return bindings;
        }

        public double getCounts() {
            return counts;
        }

        public double getTopology() {
            return topology;
        }

        public double getComplexity() {
            return complexity;
        }
    }

    private static double overlap(Set<String> left, Set<String> right) {
        Set<String> smaller = left.size() <= right.size() ? left : right;
        Set<String> larger = left.size() <= right.size() ? right : left;
        int intersection = 0;
        for (String token : smaller) {
            if (larger.contains(token)) {
                intersection++;
            }
        }
        int union = left.size() + right.size() - intersection;
        return union == 0 ? 1 : (double) intersection / union;
    }

    private static double countedOverlap(Map<String,Integer> left, Map<String,Integer> right) {
        long intersection = 0;
        long union = 0;
        for (Map.Entry<String,Integer> entry : left.entrySet()) {
            int other = right.getOrDefault(entry.getKey(), 0);
            intersection += Math.min(entry.getValue(), other);
            union += Math.max(entry.getValue(), other);
        }
        for (Map.Entry<String,Integer> entry : right.entrySet()) {
            if (!left.containsKey(entry.getKey())) {
                union += entry.getValue();
            }
        }
        return union == 0 ? 1 : (double) intersection / union;
    }

    private static double closeness(Map<String,Double> left, Map<String,Double> right) {
        double sum = 0;
        int count = left.size();
        for (Map.Entry<String,Double> entry : left.entrySet()) {
            double a = entry.getValue();
            double b = right.getOrDefault(entry.getKey(), 0.0);
            double maximum = Math.max(a, b);
            sum += maximum == 0 ? 1 : Math.min(a, b) / maximum;
        }
        for (Map.Entry<String,Double> entry : right.entrySet()) {
            if (!left.containsKey(entry.getKey())) {
                count++;
                sum += entry.getValue() == 0 ? 1 : 0;
            }
        }
        return count == 0 ? 1 : sum / count;
    }

    public static final class Result {
        private final double score;
        private final boolean compatible;
        private final Map<String,Double> components;
        private final List<String> protectedDifferences;

        private Result(double score, boolean compatible, Map<String,Double> components, List<String> protectedDifferences) {
            this.score = score;
            this.compatible = compatible;
            this.components = Collections.unmodifiableMap(components);
            this.protectedDifferences = Collections.unmodifiableList(protectedDifferences);
        }

        public double getScore() {
            return score;
        }

        public boolean isCompatible() {
            return compatible;
        }

        public Map<String,Double> getComponents() {
            return components;
        }

        public List<String> getProtectedDifferences() {
            return protectedDifferences;
        }
    }
}
