package datawave.query.iterator;

import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * This interface is for objects that can provide copies of a source. The main purpose for this is to allow the appropriate synchronization for creating source
 * copies when used across threads. QueryIterator is an instance of this which is used in the IteratorBuildingVisitor and the SourcePool used for the ivarators.
 * 
 * @param <K>
 *            type of key
 * @param <V>
 *            type of value
 */
public interface SourceFactory<K extends WritableComparable<?>,V extends Writable> {
    /**
     * Create a deep copy of a source. This is intended to be thread safe.
     *
     * @return the deep copy
     */
    SortedKeyValueIterator<K,V> getSourceDeepCopy();
}
