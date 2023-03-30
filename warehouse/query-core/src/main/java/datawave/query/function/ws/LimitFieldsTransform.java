package datawave.query.function.ws;

import com.google.common.base.Function;
import datawave.query.attributes.Document;
import datawave.query.function.LimitFields;
import org.apache.accumulo.core.data.Key;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Webservice side implementation of {@link LimitFields}
 */
public class LimitFieldsTransform implements Function<Map.Entry<Key,Document>,Map.Entry<Key,Document>> {
    
    private final LimitFields limitFields;
    
    public LimitFieldsTransform(Map<String,Integer> limitFieldsMap) {
        this.limitFields = new LimitFields(limitFieldsMap);
    }
    
    @Nullable
    @Override
    public Map.Entry<Key,Document> apply(@Nullable Map.Entry<Key,Document> input) {
        return limitFields.apply(input);
    }
}
