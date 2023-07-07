package datawave.query.function.ws;

import java.util.Map;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;

import com.google.common.base.Function;

import datawave.query.attributes.Document;
import datawave.query.function.DocumentMetadata;

/**
 * Webservice side implementation of {@link datawave.query.function.DocumentMetadata}
 */
public class DocumentMetadataFunction implements Function<Map.Entry<Key,Document>,Map.Entry<Key,Document>> {

    private DocumentMetadata function = new DocumentMetadata();

    @Nullable
    @Override
    public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> input) {
        return function.apply(input);
    }
}
