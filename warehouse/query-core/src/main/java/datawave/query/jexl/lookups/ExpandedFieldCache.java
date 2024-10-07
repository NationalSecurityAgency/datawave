package datawave.query.jexl.lookups;

import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * A cache for actual values retrieved from the index when an unfielded lookup occurs. Keeps track of whether those field / term pairs have previously been
 * expanded.
 */
public class ExpandedFieldCache {
    private Multimap<String,ValueSet> previouslyExpandedFieldCache = HashMultimap.create();

    private boolean containsExpansionsFor(IndexLookupMap fieldstoterms) {
        if (!fieldstoterms.isKeyThresholdExceeded()) {
            for (Map.Entry<String,ValueSet> fieldTermPair : fieldstoterms.entrySet()) {
                if (!fieldTermPair.getValue().isThresholdExceeded()) {
                    if (previouslyExpandedFieldCache.containsEntry(fieldTermPair.getKey(), fieldTermPair.getValue())) {
                        return true;
                    }
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
                if (!fieldTermPair.getValue().isThresholdExceeded()) {
                    previouslyExpandedFieldCache.put(fieldTermPair.getKey(), fieldTermPair.getValue());
                }
            }
        }
    }
}
