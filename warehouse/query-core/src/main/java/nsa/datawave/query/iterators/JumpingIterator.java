package nsa.datawave.query.iterators;

import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A Jumping Iterator is a SortedKeyValueIterator with a jump method
 *
 * @param <K>
 * @param <V>
 */
@Deprecated
public interface JumpingIterator<K extends WritableComparable<?>,V extends Writable> extends JumpSeek<K>, SortedKeyValueIterator<K,V> {
    
}
