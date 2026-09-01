package datawave.microservice.annotationCache.api.entryProcessor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.hazelcast.map.EntryProcessor;

/**
 * Used to add an id to the doc-annotations map
 */
public class AppendAnnotationIdProcessor implements EntryProcessor<String,Set<String>,Void> {
    private final String annotationId;

    public AppendAnnotationIdProcessor(String annotationId) {
        this.annotationId = annotationId;
    }

    @Override
    public Void process(Map.Entry<String,Set<String>> entry) {
        Set<String> ids = entry.getValue();
        if (ids == null) {
            ids = new HashSet<>();
        }

        ids.add(annotationId);
        entry.setValue(ids);
        return null;
    }
}
