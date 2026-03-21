package datawave.query.function;

import java.util.Map;

import org.apache.accumulo.core.data.Key;

import com.google.common.base.Function;

import datawave.query.attributes.Document;
import datawave.query.util.Tuple3;

/**
 * No-op document-match context function used when the query does not contain {@code document:match(...)}.
 */
public class EmptyDocumentMatchFunction implements Function<Tuple3<Key,Document,Map<String,Object>>,Tuple3<Key,Document,Map<String,Object>>> {
    @Override
    public Tuple3<Key,Document,Map<String,Object>> apply(Tuple3<Key,Document,Map<String,Object>> from) {
        return from;
    }
}
