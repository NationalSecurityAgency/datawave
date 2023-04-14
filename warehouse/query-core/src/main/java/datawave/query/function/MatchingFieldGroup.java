package datawave.query.function;

import java.util.HashSet;
import java.util.Set;

public class MatchingFieldGroup {
    private final Set<String> matchingFieldSet;
    private final Set<String> hitTermValues;
    
    public MatchingFieldGroup(Set<String> matchingFieldSet) {
        this.matchingFieldSet = matchingFieldSet;
        this.hitTermValues = new HashSet<>();
    }
    
    public boolean containsField(String fieldNoGrouping) {
        return matchingFieldSet.contains(fieldNoGrouping);
    }
    
    public void addHitTermValue(String value) {
        hitTermValues.add(value);
    }
    
    public boolean containsHitTermValue(String value) {
        return hitTermValues.contains(value);
    }
}
