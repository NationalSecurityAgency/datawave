package datawave.query.util;

import java.util.Map.Entry;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

public class TupleToEntry<K,V> implements Function<Tuple2<K,V>,Entry<K,V>> {

    @Override
    public Entry<K,V> apply(Tuple2<K,V> from) {
        return Maps.immutableEntry(from.first(), from.second());
    }

}
