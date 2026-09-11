package datawave.query.transformer.annotation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** The constituent values selected for one ordered annotation phrase occurrence. */
public final class AnnotationPhraseOccurrence {
    private final List<ValuePosition> constituents;

    public AnnotationPhraseOccurrence(List<ValuePosition> constituents) {
        if (constituents == null || constituents.isEmpty()) {
            throw new IllegalArgumentException("constituents must not be empty");
        }
        this.constituents = Collections.unmodifiableList(new ArrayList<>(constituents));
    }

    public List<ValuePosition> getConstituents() {
        return constituents;
    }

    /** Alias for callers which use the term positions vocabulary. */
    public List<ValuePosition> getPositions() {
        return constituents;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof AnnotationPhraseOccurrence)) {
            return false;
        }
        AnnotationPhraseOccurrence that = (AnnotationPhraseOccurrence) other;
        if (constituents.size() != that.constituents.size()) {
            return false;
        }
        for (int i = 0; i < constituents.size(); i++) {
            if (!sameIdentity(constituents.get(i), that.constituents.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (ValuePosition position : constituents) {
            result = 31 * result + Objects.hash(position.getBoundaryIndex(), position.getValueIndex());
        }
        return result;
    }

    private static boolean sameIdentity(ValuePosition left, ValuePosition right) {
        return left.getBoundaryIndex() == right.getBoundaryIndex() && left.getValueIndex() == right.getValueIndex();
    }
}
