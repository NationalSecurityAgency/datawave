package datawave.query.jexl.lookups;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class ExpandedFieldCache {
    private Map<String,UUID> expansionIds = new HashMap<>();
    private Multimap<UUID,String> expandedFields = HashMultimap.create();
    private Map<String,String> normalizedTerms = new HashMap<>();
    private Map<String,List<String>> manyNormalizedTerms = new HashMap<>();

    public boolean containsExpansionsFor(String field, String term, String normalizerType) {
        return expansionIds.containsKey(field + term + normalizerType);
    }

    public Collection<String> getExpansions(String field, String term, String normalizerType) {
        return containsExpansionsFor(field, term, normalizerType) ? expandedFields.get(getExpansionId(field, term, normalizerType)) : Collections.emptySet();
    }

    public String getNormalizedTerm(String field, String term, String normalizerType) {
        return containsExpansionsFor(field, term, normalizerType) ? normalizedTerms.get(field + term + normalizerType) : "";
    }

    // this needs to be looked into regarding how it might be handled if there are more than one normalized terms for a particular field
    // I am thinking a whole other add expansions (multiple) so that it gets stored into another hash map
    public List<String> getNormalizedTerms(String field, String term, String normalizerType) {
        // in progress
        return containsExpansionsFor(field, term, normalizerType) ? manyNormalizedTerms.get(field + term) : null;

    }

    public void addExpansion(String field, String term, String normalizedTerm, String normalizerType) {
        UUID id;
        if (containsExpansionsFor(field, term, normalizerType)) {
            id = getExpansionId(field, term, normalizerType);
        } else {
            id = UUID.randomUUID();
        }
        addExpansion(id, field, term, normalizedTerm, normalizerType);
    }

    public void addExpansions(String field, Collection<String> terms) {
        // in progress
        UUID id;
        if (containsExpansionsFor(field, terms.toString(), null)) {
            id = getExpansionId(field, terms.toString(), null);
        } else {
            id = UUID.randomUUID();
        }
        addExpansions(id, terms);
    }

    private void addExpansion(UUID id, String field, String term, String normalizedTerm, String normalizerType) {
        String ftnString = field + term + normalizerType;
        expansionIds.put(ftnString, id);
        expandedFields.put(id, ftnString);
        normalizedTerms.put(ftnString, normalizedTerm);
    }

    private void addExpansions(UUID id, Collection<String> terms) {
        // in progress
        for (String term : terms) {
            // addExpansion(id, expansion);
            continue;
        }
    }

    private UUID getExpansionId(String field, String term, String normalizerType) {
        return expansionIds.get(field + term + normalizerType);
    }
}
