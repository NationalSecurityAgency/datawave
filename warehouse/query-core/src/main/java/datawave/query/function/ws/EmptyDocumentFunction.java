package datawave.query.function.ws;

import com.google.common.base.Function;
import datawave.query.attributes.Document;
import datawave.query.predicate.EmptyDocumentFilter;
import org.apache.accumulo.core.data.Key;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Webservice side implementation of {@link datawave.query.predicate.EmptyDocumentFilter}
 */
public class EmptyDocumentFunction implements Function<Map.Entry<Key,Document>,Map.Entry<Key,Document>> {
    
    private EmptyDocumentFilter function = new EmptyDocumentFilter();
    
    @Nullable
    @Override
    public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> input) {
        function.apply(input);
        return input;
    }
}
