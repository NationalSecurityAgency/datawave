package datawave.microservice.annotationCache.api.entryProcessor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.hazelcast.map.EntryProcessor;

/** Remove an annotation ID from a document index, removing an empty index entry. */
public class RemoveAnnotationIdProcessor implements EntryProcessor<String,Set<String>,Void> {
    private final String annotationId;

    public RemoveAnnotationIdProcessor(String annotationId) {
        this.annotationId = annotationId;
    }

    @Override
    public Void process(Map.Entry<String,Set<String>> entry) {
        Set<String> existingIds = entry.getValue();
        if (existingIds == null || !existingIds.contains(annotationId)) {
            return null;
        }

        Set<String> ids = new HashSet<>(existingIds);
        ids.remove(annotationId);
        entry.setValue(ids.isEmpty() ? null : ids);
        return null;
    }
}
