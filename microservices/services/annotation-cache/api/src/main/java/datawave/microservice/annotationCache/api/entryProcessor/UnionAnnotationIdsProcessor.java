package datawave.microservice.annotationCache.api.entryProcessor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.hazelcast.map.EntryProcessor;

/**
 * Add a set of annotationIds to the doc-annotations map
 */
public class UnionAnnotationIdsProcessor implements EntryProcessor<String,Set<String>,Void> {
    private final Set<String> newIds;

    public UnionAnnotationIdsProcessor(Set<String> newIds) {
        this.newIds = newIds;
    }

    @Override
    public Void process(Map.Entry<String,Set<String>> entry) {
        Set<String> ids = entry.getValue();
        if (ids == null) {
            ids = new HashSet<>();
        }

        ids.addAll(newIds);
        entry.setValue(ids);
        return null;
    }
}
