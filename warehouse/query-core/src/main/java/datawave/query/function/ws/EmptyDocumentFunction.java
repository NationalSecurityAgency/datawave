package datawave.query.function.ws;

import java.util.Map;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;

import com.google.common.base.Function;

import datawave.query.attributes.Document;
import datawave.query.predicate.EmptyDocumentFilter;

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
