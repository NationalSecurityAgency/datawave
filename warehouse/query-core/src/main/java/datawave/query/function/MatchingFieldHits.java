package datawave.query.function;

import java.util.HashSet;
import java.util.Set;

public class MatchingFieldHits {
    private final Set<String> hitTermValues;

    public MatchingFieldHits() {
        this.hitTermValues = new HashSet<>();
    }

    public void addHitTermValue(String value) {
        hitTermValues.add(value);
    }

    public boolean containsHitTermValue(String value) {
        return hitTermValues.contains(value);
    }
}
