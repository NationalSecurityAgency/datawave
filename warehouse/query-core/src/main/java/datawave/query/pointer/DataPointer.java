package datawave.query.pointer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Multimap;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = ViewDataPointer.class, name = "dView")})
public interface DataPointer {
    void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env);

    Multimap<Key,Value> fetch(Key reference) throws IOException;

    List<Key> getTransformKeys(Key reference);
}
