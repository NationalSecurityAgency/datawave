package datawave.query.function.ws;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;

import com.google.common.base.Function;
import com.google.common.collect.Multimap;

import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.query.attributes.Document;
import datawave.query.function.DocumentProjection;

/**
 * Webserver side implementation of {@link datawave.query.iterator.QueryIterator#getCompositeProjection}
 */
public class CompositeProjectionFunction implements Function<Map.Entry<Key,Document>,Map.Entry<Key,Document>> {

    private final DocumentProjection projection;

    public CompositeProjectionFunction(boolean includeGroupingContext, boolean reducedResponse, Collection<Multimap<String,String>> compositeMaps,
                    Set<String> matchingFieldSets) {

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
        composites.removeAll(matchingFieldSets);
        projection.setExcludes(composites);
    }

    @Nullable
    @Override
    public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> input) {
        return projection.apply(input);
    }
}
