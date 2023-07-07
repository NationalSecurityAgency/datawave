package datawave.query.function.ws;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;

import com.google.common.base.Function;

import datawave.query.attributes.Document;
import datawave.query.function.DocumentProjection;

/**
 * Webserver side implementation of {@link datawave.query.function.DocumentProjection}
 */
public class ProjectionFunction implements Function<Map.Entry<Key,Document>,Map.Entry<Key,Document>> {

    private DocumentProjection projection;

    public ProjectionFunction(boolean includeGroupingContext, boolean reducedResponse, Set<String> includeFields, Set<String> excludeFields) {

        projection = new DocumentProjection(includeGroupingContext, reducedResponse);
        if (!includeFields.isEmpty()) {
            projection.setIncludes(includeFields);
        } else {
            projection.setExcludes(excludeFields);
        }
    }

    @Nullable
    @Override
    public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> input) {
        return projection.apply(input);
    }
}
