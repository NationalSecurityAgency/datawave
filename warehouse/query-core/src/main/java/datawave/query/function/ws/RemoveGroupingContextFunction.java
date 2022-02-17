package datawave.query.function.ws;

import com.google.common.base.Function;
import datawave.query.attributes.Document;
import datawave.query.function.RemoveGroupingContext;
import org.apache.accumulo.core.data.Key;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Webservice side implementation of {@link datawave.query.function.RemoveGroupingContext}
 */
public class RemoveGroupingContextFunction implements Function<Map.Entry<Key,Document>,Map.Entry<Key,Document>> {
    
    private final RemoveGroupingContext function = new RemoveGroupingContext();
    
    @Nullable
    @Override
    public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> input) {
        return function.apply(input);
    }
}
