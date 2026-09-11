package datawave.query.transformer.annotation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** A structural proximity criterion made up of pattern components. */
public final class ProximityExpression implements SearchExpression {
    private static final long serialVersionUID = 1L;
    private static final Comparator<StandalonePatternExpression> COMPONENT_ORDER = Comparator.comparing(StandalonePatternExpression::getPatternSource)
                    .thenComparingInt(StandalonePatternExpression::getFlags);

    private final boolean ordered;
    private final List<StandalonePatternExpression> components;
    private final int distance;
    private final List<StandalonePatternExpression> equalityComponents;

    public ProximityExpression(boolean ordered, List<StandalonePatternExpression> components, int distance) {
        if (components == null || components.isEmpty()) {
            throw new IllegalArgumentException("components must not be empty");
        }
        if (distance < 0) {
            throw new IllegalArgumentException("distance must not be negative");
        }
        this.ordered = ordered;
        this.components = Collections.unmodifiableList(new ArrayList<>(components));
        this.distance = distance;
        List<StandalonePatternExpression> canonical = new ArrayList<>(components);
        if (!ordered) {
            canonical.sort(COMPONENT_ORDER);
        }
        this.equalityComponents = Collections.unmodifiableList(canonical);
    }

    public ProximityExpression(List<StandalonePatternExpression> components, boolean ordered, int distance) {
        this(ordered, components, distance);
    }

    public boolean isOrdered() {
        return ordered;
    }

    public List<StandalonePatternExpression> getComponents() {
        return components;
    }

    public int getDistance() {
        return distance;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ProximityExpression)) {
            return false;
        }
        ProximityExpression that = (ProximityExpression) other;
        return ordered == that.ordered && distance == that.distance && equalityComponents.equals(that.equalityComponents);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ordered, distance, equalityComponents);
    }
}
