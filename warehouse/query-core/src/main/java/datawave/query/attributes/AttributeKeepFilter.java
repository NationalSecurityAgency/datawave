package datawave.query.attributes;

import java.util.Map.Entry;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

public class AttributeKeepFilter<K> implements Function<Entry<K,Document>,Entry<K,Document>> {

    @Override
    public Entry<K,Document> apply(Entry<K,Document> from) {
        return Maps.immutableEntry(from.getKey(), (Document) (from.getValue().reduceToKeep()));
    }

}
