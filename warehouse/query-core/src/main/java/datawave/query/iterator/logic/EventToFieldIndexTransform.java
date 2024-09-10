package datawave.query.iterator.logic;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

/**
 * Wrap an iterator that returns event keys, and transform the keys into field index keys. KEY NOTE: the resulting key values are NOT guaranteed to be sorted
 */
public class EventToFieldIndexTransform implements SortedKeyValueIterator<Key,Value> {

    private SortedKeyValueIterator<Key,Value> delegate;

    public EventToFieldIndexTransform(EventToFieldIndexTransform other, IteratorEnvironment env) {
        delegate = other.delegate.deepCopy(env);
    }

    public EventToFieldIndexTransform(SortedKeyValueIterator<Key,Value> delegate) {
        this.delegate = delegate;
    }

    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        delegate.init(source, options, env);
    }

    public boolean hasTop() {
        return delegate.hasTop();
    }

    public void next() throws IOException {
        delegate.next();
    }

    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        delegate.seek(range, columnFamilies, inclusive);
    }

    public Key getTopKey() {
        Key k = delegate.getTopKey();
        if (k != null) {
            k = eventKeyToFieldIndexKey(k);
        }
        return k;
    }

    public static Key eventKeyToFieldIndexKey(Key k) {
        // event key is shardId : dataType\0uid : fieldName\0fieldValue
        // field index key is shardId : fi\0fieldName : fieldValue\0datatype\0uid
        String cf = k.getColumnFamily().toString();
        String cq = k.getColumnQualifier().toString();
        int cqNullIndex = cq.indexOf('\0');
        return new Key(new Text(k.getRow()), new Text("fi\0" + cq.substring(0, cqNullIndex)), new Text(cq.substring(cqNullIndex + 1) + '\0' + cf),
                        k.getColumnVisibility(), k.getTimestamp());
    }

    public static Key fieldIndexKeyToEventKey(Key key) {
        // field index key is shardId : fi\0fieldName : fieldValue\0datatype\0uid
        // event key is shardId : dataType\0uid : fieldName\0fieldValue
        String cf = key.getColumnFamily().toString();
        String cq = key.getColumnQualifier().toString();
        int cqNullIndex = cq.indexOf('\0');
        return new Key(key.getRow(), new Text(cq.substring(cqNullIndex + 1)), new Text(cf.substring(3) + '\0' + cq.substring(0, cqNullIndex)),
                        key.getColumnVisibility(), key.getTimestamp());
    }

    public Value getTopValue() {
        return delegate.getTopValue();
    }

    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new EventToFieldIndexTransform(this, env);
    }

}
