package datawave.query.jexl.lookups;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class ExpandedFieldCache {
    private Map<String,UUID> expansionIds = new HashMap<>();
    private Multimap<UUID,String> expandedFields = HashMultimap.create();

    public boolean containsExpansionsFor(String field) {
        return expansionIds.containsKey(field);
    }

    public Collection<String> getExpansions(String field) {
        return containsExpansionsFor(field) ? expandedFields.get(getExpansionId(field)) : Collections.emptySet();
    }

    public void addExpansion(String field, String expansion) {
        UUID id;
        if (containsExpansionsFor(field)) {
            id = getExpansionId(field);
        } else {
            id = UUID.randomUUID();
        }
        addExpansion(id, expansion);
    }

    public void addExpansions(String field, Collection<String> expansions) {
        UUID id;
        if (containsExpansionsFor(field)) {
            id = getExpansionId(field);
        } else {
            id = UUID.randomUUID();
        }
        addExpansions(id, expansions);
    }

    private void addExpansion(UUID id, String expansion) {
        expansionIds.put(expansion, id);
        expandedFields.put(id, expansion);
    }

    private void addExpansions(UUID id, Collection<String> expansions) {
        for (String expansion : expansions) {
            addExpansion(id, expansion);
        }
    }

    private UUID getExpansionId(String field) {
        return expansionIds.get(field);
    }
}
