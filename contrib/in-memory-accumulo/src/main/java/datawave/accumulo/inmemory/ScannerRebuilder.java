package datawave.accumulo.inmemory;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.Iterator;
import java.util.Map;

public interface ScannerRebuilder {
    public Iterator<Map.Entry<Key, Value>> rebuild(Key lastKey);
}
