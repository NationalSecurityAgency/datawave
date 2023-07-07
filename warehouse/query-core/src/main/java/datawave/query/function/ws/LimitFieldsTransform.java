package datawave.query.function.ws;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;

import com.google.common.base.Function;

import datawave.query.attributes.Document;
import datawave.query.function.LimitFields;

/**
 * Webservice side implementation of {@link LimitFields}
 */
public class LimitFieldsTransform implements Function<Map.Entry<Key,Document>,Map.Entry<Key,Document>> {

    private final LimitFields limitFields;

    public LimitFieldsTransform(Map<String,Integer> limitFieldsMap, Set<Set<String>> matchingFieldSets) {
        this.limitFields = new LimitFields(limitFieldsMap, matchingFieldSets);
    }

    @Nullable
    @Override
    public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> input) {
        return limitFields.apply(input);
    }
}
