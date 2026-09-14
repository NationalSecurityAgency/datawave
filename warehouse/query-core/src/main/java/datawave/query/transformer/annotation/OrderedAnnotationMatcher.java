package datawave.query.transformer.annotation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Matches an ordered, exact (effective distance one) phrase against annotation values. Values are treated as whole strings; this matcher deliberately does not
 * tokenize them.
 */
public final class OrderedAnnotationMatcher {
    private final List<Pattern> components;

    /** Accepts either compiled patterns or {@link StandalonePatternExpression} components. */
    public OrderedAnnotationMatcher(List<?> components) {
        if (components == null || components.isEmpty()) {
            throw new IllegalArgumentException("components must not be empty");
        }
        List<Pattern> patterns = new ArrayList<>();
        for (Object component : components) {
            if (component instanceof Pattern) {
                patterns.add((Pattern) component);
            } else if (component instanceof StandalonePatternExpression) {
                StandalonePatternExpression expression = (StandalonePatternExpression) component;
                patterns.add(Pattern.compile(expression.getPatternSource(), expression.getFlags()));
            } else {
                throw new IllegalArgumentException("components must be Pattern or StandalonePatternExpression");
            }
        }
        this.components = Collections.unmodifiableList(patterns);
    }

    public OrderedAnnotationMatcher(ProximityExpression expression) {
        if (expression == null || !expression.isOrdered() || expression.getDistance() != 1) {
            throw new IllegalArgumentException("an ordered distance-1 proximity expression is required");
        }
        List<Pattern> patterns = new ArrayList<>();
        for (StandalonePatternExpression component : expression.getComponents()) {
            patterns.add(Pattern.compile(component.getPatternSource(), component.getFlags()));
        }
        this.components = Collections.unmodifiableList(patterns);
    }

    /** Match using the default non-flattened boundary semantics. */
    public List<AnnotationPhraseOccurrence> match(AnnotationPositionView view, float minScore) {
        return match(view, minScore, false);
    }

    /**
     * Returns each distinct constituent identity sequence. Backtracking advances only to the current or immediately following boundary, so it never creates an
     * unrestricted product.
     */
    public List<AnnotationPhraseOccurrence> match(AnnotationPositionView view, float minScore, boolean flattenBoundary) {
        if (view == null) {
            throw new IllegalArgumentException("view must not be null");
        }
        List<List<ValuePosition>> candidates = candidates(view, minScore);
        List<AnnotationPhraseOccurrence> results = new ArrayList<>();
        Set<AnnotationPhraseOccurrence> seen = new HashSet<>();
        if (components.size() == 1) {
            for (ValuePosition position : candidates.get(0)) {
                AnnotationPhraseOccurrence occurrence = new AnnotationPhraseOccurrence(Collections.singletonList(position));
                if (seen.add(occurrence)) {
                    results.add(occurrence);
                }
            }
            return results;
        }
        for (ValuePosition first : candidates.get(0)) {
            ArrayList<ValuePosition> path = new ArrayList<>();
            path.add(first);
            search(1, first, flattenBoundary, candidates, path, results, seen);
        }
        return results;
    }

    public List<AnnotationPhraseOccurrence> match(AnnotationPositionView view, boolean flattenBoundary, float minScore) {
        return match(view, minScore, flattenBoundary);
    }

    private void search(int component, ValuePosition previous, boolean flattenBoundary, List<List<ValuePosition>> candidates, List<ValuePosition> path,
                    List<AnnotationPhraseOccurrence> results, Set<AnnotationPhraseOccurrence> seen) {
        if (component == candidates.size()) {
            AnnotationPhraseOccurrence occurrence = new AnnotationPhraseOccurrence(path);
            if (seen.add(occurrence)) {
                results.add(occurrence);
            }
            return;
        }
        for (ValuePosition candidate : candidates.get(component)) {
            int boundaryDelta = candidate.getBoundaryIndex() - previous.getBoundaryIndex();
            boolean permitted = flattenBoundary ? (boundaryDelta == 0 || boundaryDelta == 1) : boundaryDelta == 1;
            if (!permitted || sameIdentity(previous, candidate) || containsIdentity(path, candidate)) {
                continue;
            }
            path.add(candidate);
            search(component + 1, candidate, flattenBoundary, candidates, path, results, seen);
            path.remove(path.size() - 1);
        }
    }

    private List<List<ValuePosition>> candidates(AnnotationPositionView view, float minScore) {
        List<List<ValuePosition>> candidates = new ArrayList<>();
        for (Pattern component : components) {
            List<ValuePosition> matching = new ArrayList<>();
            for (int boundary = 0; boundary < view.getBoundaries().size(); boundary++) {
                for (ValuePosition position : view.getValues(boundary)) {
                    if (position.getValue().getScore() >= minScore && component.matcher(position.getNormalizedValue()).matches()) {
                        matching.add(position);
                    }
                }
            }
            candidates.add(matching);
        }
        return candidates;
    }

    private static boolean containsIdentity(List<ValuePosition> path, ValuePosition candidate) {
        for (ValuePosition position : path) {
            if (sameIdentity(position, candidate)) {
                return true;
            }
        }
        return false;
    }

    private static boolean sameIdentity(ValuePosition left, ValuePosition right) {
        return left.getBoundaryIndex() == right.getBoundaryIndex() && left.getValueIndex() == right.getValueIndex();
    }
}
