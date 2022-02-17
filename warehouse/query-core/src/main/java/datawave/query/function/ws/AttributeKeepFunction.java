package datawave.query.function.ws;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import datawave.query.attributes.Document;
import org.apache.accumulo.core.data.Key;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Webserver side implementation of the {@link datawave.query.attributes.AttributeKeepFilter}
 */
public class AttributeKeepFunction implements Function<Map.Entry<Key,Document>,Map.Entry<Key,Document>> {
    
    @Nullable
    @Override
    public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> input) {
        return Maps.immutableEntry(input.getKey(), (Document) (input.getValue().reduceToKeep()));
    }
}
