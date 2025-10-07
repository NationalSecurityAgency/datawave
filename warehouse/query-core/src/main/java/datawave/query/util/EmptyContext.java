package datawave.query.util;

import java.util.Collections;
import java.util.Map;

import com.google.common.base.Function;

public class EmptyContext<K1,V1,K2,V2> implements Function<Tuple2<K1,V1>,Tuple3<K1,V1,Map<K2,V2>>> {
    @Override
    public Tuple3<K1,V1,Map<K2,V2>> apply(Tuple2<K1,V1> from) {
        return Tuples.tuple(from.first(), from.second(), Collections.emptyMap());
    }
}
