package datawave.query.transformer.annotation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Matches an unordered proximity expression. The distance is a span in boundary ordinals, rather than a timestamp or character distance. In flattened mode,
 * values sharing a boundary occupy consecutive virtual positions; their stored value order is intentionally not used.
 */
public final class UnorderedAnnotationMatcher {
    private static final Comparator<ValuePosition> IDENTITY_ORDER = Comparator.comparingInt(ValuePosition::getBoundaryIndex)
                    .thenComparingInt(ValuePosition::getValueIndex);

    private final List<Pattern> components;
    private final int distance;

    public UnorderedAnnotationMatcher(ProximityExpression expression) {
        if (expression == null || expression.isOrdered()) {
            throw new IllegalArgumentException("an unordered proximity expression is required");
        }
        List<Pattern> patterns = new ArrayList<>();
        for (StandalonePatternExpression component : expression.getComponents()) {
            patterns.add(Pattern.compile(component.getPatternSource(), component.getFlags()));
        }
        components = Collections.unmodifiableList(patterns);
        distance = expression.getDistance();
    }

    /** Accepts compiled patterns for callers that build criteria outside the search-expression model. */
    public UnorderedAnnotationMatcher(List<?> components, int distance) {
        if (components == null || components.isEmpty() || distance < 0) {
            throw new IllegalArgumentException("components must not be empty and distance must not be negative");
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
        this.distance = distance;
    }

    /** Match using non-flattened boundary semantics. */
    public List<AnnotationPhraseOccurrence> match(AnnotationPositionView view, float minScore) {
        return match(view, minScore, false);
    }

    public List<AnnotationPhraseOccurrence> match(AnnotationPositionView view, float minScore, boolean flattenBoundary) {
        if (view == null) {
            throw new IllegalArgumentException("view must not be null");
        }
        if (components.size() > distance + 1) {
            return Collections.emptyList();
        }

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
            if (matching.isEmpty()) {
                return Collections.emptyList();
            }
            candidates.add(matching);
        }

        // Choosing the most selective components first bounds the backtracking tree.
        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < components.size(); i++) {
            order.add(i);
        }
        order.sort(Comparator.comparingInt(i -> candidates.get(i).size()));

        List<ValuePosition> selected = new ArrayList<>();
        Set<AnnotationPhraseOccurrence> results = new HashSet<>();
        search(0, order, candidates, selected, flattenBoundary, results);
        return new ArrayList<>(results);
    }

    public List<AnnotationPhraseOccurrence> match(AnnotationPositionView view, boolean flattenBoundary, float minScore) {
        return match(view, minScore, flattenBoundary);
    }

    private void search(int depth, List<Integer> order, List<List<ValuePosition>> candidates, List<ValuePosition> selected, boolean flattenBoundary,
                    Set<AnnotationPhraseOccurrence> results) {
        if (depth == order.size()) {
            List<ValuePosition> canonical = new ArrayList<>(selected);
            canonical.sort(IDENTITY_ORDER);
            results.add(new AnnotationPhraseOccurrence(canonical));
            return;
        }

        for (ValuePosition candidate : candidates.get(order.get(depth))) {
            if (containsIdentity(selected, candidate)) {
                continue;
            }
            selected.add(candidate);
            if (validPartialSpan(selected, flattenBoundary)) {
                search(depth + 1, order, candidates, selected, flattenBoundary, results);
            }
            selected.remove(selected.size() - 1);
        }
    }

    private boolean validPartialSpan(List<ValuePosition> selected, boolean flattenBoundary) {
        if (!flattenBoundary) {
            int first = Integer.MAX_VALUE;
            int last = Integer.MIN_VALUE;
            for (ValuePosition position : selected) {
                // One component per boundary in the non-flattened model.
                for (ValuePosition other : selected) {
                    if (other != position && other.getBoundaryIndex() == position.getBoundaryIndex()) {
                        return false;
                    }
                }
                first = Math.min(first, position.getBoundaryIndex());
                last = Math.max(last, position.getBoundaryIndex());
            }
            return last - first <= distance;
        }

        int first = Integer.MAX_VALUE;
        int last = Integer.MIN_VALUE;
        int previousBoundary = Integer.MIN_VALUE;
        int countAtBoundary = 0;
        int virtualSpan = 0;
        List<Integer> boundaries = new ArrayList<>();
        for (ValuePosition position : selected) {
            boundaries.add(position.getBoundaryIndex());
        }
        Collections.sort(boundaries);
        for (int boundary : boundaries) {
            if (boundary != previousBoundary) {
                if (previousBoundary != Integer.MIN_VALUE) {
                    virtualSpan += countAtBoundary - 1;
                }
                if (first == Integer.MAX_VALUE) {
                    first = boundary;
                }
                last = boundary;
                previousBoundary = boundary;
                countAtBoundary = 1;
            } else {
                countAtBoundary++;
            }
        }
        virtualSpan += countAtBoundary - 1;
        virtualSpan += last - first;
        return virtualSpan <= distance;
    }

    private static boolean containsIdentity(List<ValuePosition> selected, ValuePosition candidate) {
        for (ValuePosition position : selected) {
            if (position.getBoundaryIndex() == candidate.getBoundaryIndex() && position.getValueIndex() == candidate.getValueIndex()) {
                return true;
            }
        }
        return false;
    }
}
