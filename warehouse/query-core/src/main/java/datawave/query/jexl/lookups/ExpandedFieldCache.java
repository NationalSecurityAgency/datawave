package datawave.query.jexl.lookups;

import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class ExpandedFieldCache {
    private Multimap<String,ValueSet> previouslyExpandedFieldCache = HashMultimap.create();

    private boolean containsExpansionsFor(IndexLookupMap fieldstoterms) {
        if (!fieldstoterms.isKeyThresholdExceeded()) { // see why this will not catch some of the exceeded thresholds being able to get into the values
            for (Map.Entry<String,ValueSet> fieldTermPair : fieldstoterms.entrySet()) {
                if (previouslyExpandedFieldCache.containsEntry(fieldTermPair.getKey(), fieldTermPair.getValue())) {
                    return true;
                } else {
                    return false;
                }
            }
        }
        return false;
    }

    public boolean containsExpansionsFor(String fieldName, String literal) {
        if (previouslyExpandedFieldCache.containsKey(fieldName)) {
            for (ValueSet value : previouslyExpandedFieldCache.get(fieldName)) {
                if (value.contains(literal)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void addExpansion(IndexLookupMap fieldstoterms) {
        if (!containsExpansionsFor(fieldstoterms)) {
            addExpansionToCache(fieldstoterms);
        }
    }

    private void addExpansionToCache(IndexLookupMap fieldstoterms) {
        if (!fieldstoterms.isKeyThresholdExceeded()) {
            for (Map.Entry<String,ValueSet> fieldTermPair : fieldstoterms.entrySet()) {
                previouslyExpandedFieldCache.put(fieldTermPair.getKey(), fieldTermPair.getValue());
            }
        }
    }
}
