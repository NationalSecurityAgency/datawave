package datawave.query.postprocessing.tf;

import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.data.Key;

import com.google.common.base.Function;

import datawave.query.attributes.Document;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuple3;
import datawave.query.util.Tuples;

public class EmptyTermFrequencyFunction implements Function<Tuple2<Key,Document>,Tuple3<Key,Document,Map<String,Object>>> {

    private static final Map<String,Object> emptyContext = Collections.emptyMap();

    @Override
    public Tuple3<Key,Document,Map<String,Object>> apply(Tuple2<Key,Document> from) {
        return Tuples.tuple(from.first(), from.second(), emptyContext);
    }

}
