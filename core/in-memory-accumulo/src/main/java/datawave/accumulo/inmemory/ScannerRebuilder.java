package datawave.accumulo.inmemory;

import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public interface ScannerRebuilder {
    /**
     * The rebuild method does the equivalent of a teardown/rebuild of an iterator within a tserver. The lastKey supplied is the last key that was returned by
     * the iterator stack. So this method should create the iterator stack, and subsequently seek the iterators starting with the lastKey, non-inclusive.
     * Providing a null lastKey will result in rebuilding the iterator without updating any of the ranges.
     * 
     * @param lastKey
     * @return The rebuilt iterator stack.
     */
    public Iterator<Map.Entry<Key,Value>> rebuild(Key lastKey);
}
