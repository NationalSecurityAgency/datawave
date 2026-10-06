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
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
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
        return new Result(score(components.get("bindings"), components.get("counts"), components.get("topology"), components.get("complexity")), compatible,
                        components, differences);
    }

    // The grouping and selection hot paths do not allocate explanation objects or temporary sets.
    static double score(QueryFingerprint left, QueryFingerprint right) {
        if (left == right || left.getKey().equals(right.getKey())) {
            return 1;
        }
        return score(overlap(left.getBindings(), right.getBindings()), countedOverlap(left.getCounts(), right.getCounts()),
                        overlap(left.getTopology(), right.getTopology()), closeness(left.getMeasurements(), right.getMeasurements()));
    }

    private static double score(double bindings, double counts, double topology, double complexity) {
        return Math.min(1, Math.max(0, 0.35 * bindings + 0.30 * counts + 0.20 * topology + 0.15 * complexity));
    }

    public static double distance(QueryFingerprint left, QueryFingerprint right) {
        double distance = 0.4 * (1 - score(left, right));
        return left.getProtectedProfile().equals(right.getProtectedProfile()) ? distance : 0.6 + distance;
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
