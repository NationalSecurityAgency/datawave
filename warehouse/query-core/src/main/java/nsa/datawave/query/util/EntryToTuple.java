package nsa.datawave.query.util;

import java.util.Map.Entry;

import com.google.common.base.Function;

public class EntryToTuple<K,V> implements Function<Entry<K,V>,Tuple2<K,V>> {
    @Override
    public Tuple2<K,V> apply(Entry<K,V> from) {
        return Tuples.tuple(from.getKey(), from.getValue());
    }
}
