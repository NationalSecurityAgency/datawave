package datawave.query.pointer;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.google.common.collect.Multimap;

import datawave.attribute.pointer.DataPointer;

public interface DataPointerHandler {
    void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env);

    boolean canFetch(DataPointer pointer);

    Multimap<Key,Value> fetch(DataPointer pointer, Key reference) throws IOException;

    boolean isPointer(Key key, Value value);

    DataPointer getPointer(Key key, Value value) throws IOException;
}
