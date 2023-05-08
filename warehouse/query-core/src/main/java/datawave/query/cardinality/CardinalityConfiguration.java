package datawave.query.cardinality;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.model.QueryModel;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class CardinalityConfiguration {

    private Set<String> cardinalityFields = null;
    private Map<String, String> cardinalityFieldReverseMapping = null;
    private String cardinalityUidField = null;
    private String outputFileDirectory = null;
    private int flushThreshold = 50000;

    private String[] nonDocumentFields = {"QUERY_USER", "QUERY_SYSTEM_FROM", "QUERY_LOGIC_NAME", "RESULT_DATA_AGE", "RESULT_DATATYPE"};

    public Set<String> getCardinalityFields() {
        return cardinalityFields;
    }

    public void setCardinalityFields(Set<String> cardinalityFields) {
        this.cardinalityFields = cardinalityFields;
    }

    public String getCardinalityUidField() {
        return cardinalityUidField;
    }

    public void setCardinalityUidField(String cardinalityUidField) {
        this.cardinalityUidField = cardinalityUidField;
    }

    public String getOutputFileDirectory() {
        return outputFileDirectory;
    }

    public void setOutputFileDirectory(String outputFileDirectory) {
        this.outputFileDirectory = outputFileDirectory;
    }

    public void setFlushThreshold(int flushThreshold) {
        this.flushThreshold = flushThreshold;
    }

    public int getFlushThreshold() {
        return flushThreshold;
    }

    public void setCardinalityFieldReverseMapping(Map<String, String> cardinalityFieldReverseMapping) {
        this.cardinalityFieldReverseMapping = cardinalityFieldReverseMapping;
    }

    public Map<String, String> getCardinalityFieldReverseMapping() {
        return cardinalityFieldReverseMapping;
    }

    public Set<String> getAllFieldNames() {

        Set<String> configuredFields = new TreeSet<>();
        for (String field : this.cardinalityFields) {
            Iterable<String> fieldSplit = Splitter.on("|").split(field);
            for (String s : fieldSplit) {
                configuredFields.add(s);
            }
        }
        return configuredFields;
    }

    private Set<String> getStoredProjectionFields() {
        Set<String> initialProjectionFields = new HashSet<>();
        Set<String> finalProjectionFields = new HashSet<>();
        initialProjectionFields.addAll(getAllFieldNames());
        if (cardinalityUidField != null) {
            initialProjectionFields.add(cardinalityUidField);
        }

        // remove fields that will not be stored with the event
        initialProjectionFields.removeAll(Arrays.asList(nonDocumentFields));

        // map local model names to the stored names that the tserver will find
        Multimap<String, String> forwardMap = HashMultimap.create();
        for (Map.Entry<String, String> entry : cardinalityFieldReverseMapping.entrySet()) {
            forwardMap.put(entry.getValue(), entry.getKey());
        }
        for (String f : initialProjectionFields) {
            if (forwardMap.containsKey(f)) {
                finalProjectionFields.addAll(forwardMap.get(f));
            } else {
                finalProjectionFields.add(f);
            }
        }
        return finalProjectionFields;
    }

    public Set<String> getRevisedDisallowlistFields(QueryModel queryModel, Set<String> originalDisallowlistedFields) {
        Set<String> revisedDisallowlistFields = new HashSet<>(originalDisallowlistedFields);
        if (!originalDisallowlistedFields.isEmpty()) {
            Collection<String> storedDisallowlistedFieldsToRemove = getStoredDisallowlistedFieldsToRemove(queryModel, originalDisallowlistedFields);
            if (queryModel != null) {
                // the disallowlisted fields will be mapped to their stored values in the DefaultQueryPlanner, so we will remove
                // both the stored version and the model version of the must return field

                Multimap<String, String> queryMapping = invertMultimap(queryModel.getForwardQueryMapping());
                for (String bl : storedDisallowlistedFieldsToRemove) {
                    if (queryMapping.containsKey(bl)) {
                        revisedDisallowlistFields.removeAll(queryMapping.get(bl));
                    }
                    if (cardinalityFieldReverseMapping.containsKey(bl)) {
                        revisedDisallowlistFields.remove(cardinalityFieldReverseMapping.get(bl));
                    }
                    revisedDisallowlistFields.remove(bl);
                }
            } else {
                // if a disallowlist is being used with no model, then the disallowlist fields will contain the stored names
                revisedDisallowlistFields.removeAll(storedDisallowlistedFieldsToRemove);
            }
        }
        return revisedDisallowlistFields;
    }

    public Set<String> getStoredDisallowlistedFieldsToRemove(QueryModel queryModel, Set<String> originalDisallowlistedFields) {

        if (!originalDisallowlistedFields.isEmpty()) {
            Set<String> disallowlistedFieldsToRemove = getStoredProjectionFields();
            if (queryModel != null) {
                // using the stored version of the cardinality fields, find out what they would be called in the model that's being used
                Collection<String> storedOriginaldisallowlistedFields = queryModel.remapParameter(originalDisallowlistedFields, queryModel.getForwardQueryMapping());
                // retain all fields that are both in the disallowlisted fields and being used for cardinalities
                disallowlistedFieldsToRemove.retainAll(storedOriginaldisallowlistedFields);
            } else {
                // if disallowlistedFields is being used with no model, then the disallowlistedFields will contain the stored names
                disallowlistedFieldsToRemove.retainAll(originalDisallowlistedFields);
            }
            return disallowlistedFieldsToRemove;
        } else {
            return Collections.emptySet();
        }
    }

    public Set<String> getRevisedProjectFields(QueryModel queryModel, Set<String> originalProjectFields) {
        Set<String> revisedProjectFields = new HashSet<>(originalProjectFields);
        if (!originalProjectFields.isEmpty()) {
            Set<String> storedProjectFieldsToAdd = getStoredProjectFieldsToAdd(queryModel, originalProjectFields);
            if (queryModel != null) {

                // if the DefaultQueryPlanner is fixed to use the forwardMapping instead of the inverse reverseMapping,
                // then we should change this to use the inverseForward mapping to catch all possible model fields for that on-disk field
                Map<String, String> reverseQueryMapping = queryModel.getReverseQueryMapping();
                for (String pf : storedProjectFieldsToAdd) {
                    if (reverseQueryMapping.containsKey(pf)) {
                        revisedProjectFields.add(reverseQueryMapping.get(pf));
                    } else {
                        // shouldn't happen, but if pf not found in the reverse model, then we can just add the on-disk field
                        revisedProjectFields.add(pf);
                    }
                }
            } else {
                // if projectFields is being used with no model, then the projectFields will contain the stored names
                revisedProjectFields.addAll(storedProjectFieldsToAdd);
            }
        }
        return revisedProjectFields;
    }

    public Set<String> getStoredProjectFieldsToAdd(QueryModel queryModel, Set<String> originalProjectFields) {

        if (!originalProjectFields.isEmpty()) {
            Set<String> projectFieldsToAdd = getStoredProjectionFields();
            if (queryModel != null) {
                // using the stored version of the cardinality fields, find out what they would be called in the model that's being used
                Collection<String> storedOriginalProjectFields = queryModel.remapParameter(originalProjectFields, queryModel.getForwardQueryMapping());
                // retain all fields that are both in the project fields and being used for cardinalities
                projectFieldsToAdd.removeAll(storedOriginalProjectFields);
            } else {
                // if disallowlistedFields is being used with no model, then the disallowlistedFields will contain the stored names
                projectFieldsToAdd.removeAll(originalProjectFields);
            }
            return projectFieldsToAdd;
        } else {
            return Collections.emptySet();
        }
    }

    private Multimap<String, String> invertMap(Map<String, String> map) {
        Multimap<String, String> inverse = HashMultimap.create();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            inverse.put(entry.getValue(), entry.getKey());
        }
        return inverse;
    }

    private Multimap<String, String> invertMultimap(Multimap<String, String> multi) {
        Multimap<String, String> inverse = HashMultimap.create();
        for (Map.Entry<String, String> entry : multi.entries()) {
            inverse.put(entry.getValue(), entry.getKey());
        }
        return inverse;
    }

    private Multimap<String, String> toMultiMap(Map<String, String> map) {
        Multimap<String, String> mmap = HashMultimap.create();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            mmap.put(entry.getKey(), entry.getValue());
        }
        return mmap;
    }
}
