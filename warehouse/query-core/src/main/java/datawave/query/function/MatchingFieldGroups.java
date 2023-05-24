package datawave.query.function;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.attributes.Attribute;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MatchingFieldGroups {
    
    private final Multimap<String,MatchingFieldHits> matchingFieldGroups;
    private final Set<String> matchingGroups;
    private final Multimap<String,String[]> potentialMatches;
    
    public MatchingFieldGroups(Set<Set<String>> matchingFieldSets) {
        matchingFieldGroups = HashMultimap.create();
        if (matchingFieldSets != null) {
            for (Set<String> matchingFieldSet : matchingFieldSets) {
                MatchingFieldHits matchingFieldGroup = new MatchingFieldHits();
                for (String field : matchingFieldSet) {
                    matchingFieldGroups.put(field, matchingFieldGroup);
                }
            }
        }
        matchingGroups = new HashSet<>();
        potentialMatches = ArrayListMultimap.create();
    }
    
    public void addHit(String keyNoGrouping, Attribute attr) {
        if (matchingFieldGroups.containsKey(keyNoGrouping)) {
            for (MatchingFieldHits matchingFieldGroup : matchingFieldGroups.get(keyNoGrouping)) {
                matchingFieldGroup.addHitTermValue(getStringValue(attr));
            }
        }
    }
    
    public void addPotential(String keyNoGrouping, String keyWithGrouping, Attribute attr) {
        if (matchingFieldGroups.containsKey(keyNoGrouping)) {
            String group = getGroup(keyWithGrouping);
            if (group != null) {
                potentialMatches.put(keyNoGrouping, new String[] {group, getStringValue(attr)});
            }
        }
    }
    
    public void processMatches() {
        for (Map.Entry<String,String[]> potentialEntry : potentialMatches.entries()) {
            String keyNoGrouping = potentialEntry.getKey();
            String group = potentialEntry.getValue()[0];
            String value = potentialEntry.getValue()[1];
            if (!matchingGroups.contains(group)) {
                for (MatchingFieldHits matchingFieldGroup : matchingFieldGroups.get(keyNoGrouping)) {
                    if (matchingFieldGroup.containsHitTermValue(value)) {
                        matchingGroups.add(group);
                        break;
                    }
                }
            }
        }
    }
    
    public boolean hasMatches() {
        return !matchingGroups.isEmpty();
    }
    
    public boolean isMatchingGroup(String keyWithGrouping) {
        String group = getGroup(keyWithGrouping);
        if (group != null) {
            return matchingGroups.contains(group);
        }
        return false;
    }
    
    static String getStringValue(Attribute attr) {
        return String.valueOf(attr.getData());
    }
    
    static String getGroup(String keyWithGrouping) {
        String[] keyTokens = LimitFields.getCommonalityAndGroupingContext(keyWithGrouping);
        if (keyTokens != null) {
            return Joiner.on('.').join(keyTokens);
        }
        return null;
    }
    
}
