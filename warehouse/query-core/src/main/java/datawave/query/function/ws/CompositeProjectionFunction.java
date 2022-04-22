package datawave.query.function.ws;

import com.google.common.base.Function;
import com.google.common.collect.Multimap;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.query.attributes.Document;
import datawave.query.function.DocumentProjection;
import org.apache.accumulo.core.data.Key;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Webserver side implementation of {@link datawave.query.iterator.QueryIterator#getCompositeProjection}
 */
public class CompositeProjectionFunction implements Function<Map.Entry<Key,Document>,Map.Entry<Key,Document>> {
    
    private final DocumentProjection projection;
    
    public CompositeProjectionFunction(boolean includeGroupingContext, boolean reducedResponse, Collection<Multimap<String,String>> compositeMaps) {
        
        projection = new DocumentProjection(includeGroupingContext, reducedResponse);
        // build exclude list from composite fields
        Set<String> composites = new HashSet<>();
        for (Multimap<String,String> val : compositeMaps) {
            for (String compositeField : val.keySet()) {
                if (!CompositeIngest.isOverloadedCompositeField(val, compositeField)) {
                    composites.add(compositeField);
                }
            }
        }
        projection.setExcludes(composites);
    }
    
    @Nullable
    @Override
    public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> input) {
        return projection.apply(input);
    }
}
