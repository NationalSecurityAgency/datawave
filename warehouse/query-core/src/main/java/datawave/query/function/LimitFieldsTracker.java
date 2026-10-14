package datawave.query.function;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.query.attributes.Attribute;

/**
 * Provides useful tracking of a number of different properties for use with {@link LimitFields}.
 */
public class LimitFieldsTracker {

    private final Multimap<String,MatchingFieldHits> matchingFieldGroups;
    private final Set<FieldName.GroupAndInstance> matchingGroups;
    private final Multimap<String,PotentialMatch> potentialMatches;
    private final CountMap fieldCounts = new CountMap();
    private final CountMap hitCounts = new CountMap();
    private final CountMap nonHitCounts = new CountMap();
    private int attributesToDrop = 0;

    /**
     * Return the data of the given attribute as a string.
     *
     * @param attr
     *            the attribute
     * @return the attribute's data as a string
     */
    private static String getStringValue(Attribute<?> attr) {
        return String.valueOf(attr.getData());
    }

    public LimitFieldsTracker(Set<Set<String>> matchingFieldSets) {
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

    /**
     * Add a hit for the given field and attribute.
     *
     * @param fieldName
     *            the field name
     * @param attr
     *            the attribute
     */
    public void addHit(FieldName fieldName, Attribute<?> attr) {
        String fieldNoGrouping = fieldName.getBaseName();
        if (matchingFieldGroups.containsKey(fieldNoGrouping)) {
            for (MatchingFieldHits matchingFieldGroup : matchingFieldGroups.get(fieldNoGrouping)) {
                matchingFieldGroup.addHitTermValue(getStringValue(attr));
            }
        }
    }

    /**
     * Add a potential match for the given field and attribute.
     *
     * @param fieldName
     *            the field name
     * @param attr
     *            the attribute
     */
    public void addPotential(FieldName fieldName, Attribute<?> attr) {
        if (matchingFieldGroups.containsKey(fieldName.getBaseName()) && fieldName.isGrouped()) {
            potentialMatches.put(fieldName.getBaseName(), new PotentialMatch(fieldName.getGroupAndInstance(), getStringValue(attr)));
        }
    }

    /**
     * Identifies any groups from potential matches where the field without its grouping context contains a matching hit term value, and adds them to the sets
     * of matching groups.
     */
    public void processMatches() {
        for (Map.Entry<String,PotentialMatch> potentialEntry : potentialMatches.entries()) {
            String fieldNoGrouping = potentialEntry.getKey();
            PotentialMatch potential = potentialEntry.getValue();
            if (!matchingGroups.contains(potential.groupAndInstance)) {
                for (MatchingFieldHits matchingFieldGroup : matchingFieldGroups.get(fieldNoGrouping)) {
                    if (matchingFieldGroup.containsHitTermValue(potential.value)) {
                        matchingGroups.add(potential.groupAndInstance);
                        break;
                    }
                }
            }
        }
    }

    /**
     * Return whether any matches were found.
     *
     * @return true if matches were found, or false otherwise
     */
    public boolean hasMatches() {
        return !matchingGroups.isEmpty();
    }

    /**
     * Return whether the given field belongs to a matching group.
     *
     * @param fieldName
     *            the field name
     * @return true if there is a matching group, or false otherwise
     */
    public boolean isMatchingGroup(FieldName fieldName) {
        return fieldName.isGrouped() && matchingGroups.contains(fieldName.getGroupAndInstance());
    }

    /**
     * Increment the number of hits seen for the given field.
     *
     * @param fieldNoGrouping
     *            the field without its grouping context
     */
    public void incrementHit(String fieldNoGrouping) {
        hitCounts.increment(fieldNoGrouping);
    }

    /**
     * Return the total hits seen for the given field
     *
     * @param fieldNoGrouping
     *            the field without its grouping context
     * @return the total hits
     */
    public int getTotalHits(String fieldNoGrouping) {
        return hitCounts.get(fieldNoGrouping);
    }

    /**
     * Increment the non-hit count for the given field by 1.
     *
     * @param field
     *            the field without its grouping context
     */
    public void incrementNonHit(String field) {
        nonHitCounts.increment(field);
    }

    /**
     * Decrement the non-hit count for the given field by 1.
     *
     * @param field
     *            the field without its grouping context
     */
    public void decrementNonHit(String field) {
        nonHitCounts.decrement(field);
    }

    /**
     * Return the total non-hit count for the given field.
     *
     * @param field
     *            the field without its grouping context
     * @return the count
     */
    public int getTotalNonHits(String field) {
        return nonHitCounts.get(field);
    }

    /**
     * Increment the number of times the given field has been seen by 1.
     *
     * @param field
     *            the field without its grouping context.
     */
    public void incrementFieldCount(String field) {
        fieldCounts.increment(field);
    }

    /**
     * Return the number of times the given field has been seen.
     *
     * @param field
     *            the field without its grouping context
     * @return the total
     */
    public int getFieldCount(String field) {
        return fieldCounts.get(field);
    }

    /**
     * Return the distinct set of fields seen.
     *
     * @return the fields
     */
    public Set<String> getFields() {
        return fieldCounts.keySet();
    }

    /**
     * Increment the number of attributes to drop by 1.
     */
    public void incrementAttributesToDrop() {
        attributesToDrop++;
    }

    /**
     * Decrement the number of attributes to drop by 1.
     */
    public void decrementAttributesToDrop() {
        attributesToDrop--;
    }

    /**
     * Get the total attributes to drop.
     *
     * @return the total attributes to drop
     */
    public int getAttributesToDrop() {
        return attributesToDrop;
    }

    /**
     * A non-hit value for a limited field, remembered with its group and instance so the group can be retained if the value later proves to match a hit term
     * value from one of the matching field sets.
     */
    private static class PotentialMatch {
        private final FieldName.GroupAndInstance groupAndInstance;
        private final String value;

        PotentialMatch(FieldName.GroupAndInstance groupAndInstance, String value) {
            this.groupAndInstance = groupAndInstance;
            this.value = value;
        }
    }
}
