package datawave.core.iterators;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * This class can be used to wrap a source pool and tie sources from the pool to a thread. The source will be released back to the pool when this class is
 * closed in that same thread.
 * 
 * @param <K>
 * @param <V>
 */
public class ThreadLocalPooledSource<K extends WritableComparable<?>,V extends Writable> extends ThreadLocal<SortedKeyValueIterator<K,V>> implements
                SortedKeyValueIterator<K,V>, AutoCloseable {
    private SourcePool<K,V> sourcePool;
    
    public ThreadLocalPooledSource(SourcePool<K,V> sourcePool) {
        this.sourcePool = sourcePool;
    }
    
    @Override
    protected SortedKeyValueIterator<K,V> initialValue() {
        return sourcePool.checkOut(-1);
    }
    
    @Override
    public void remove() {
        SortedKeyValueIterator<K,V> source = get();
        sourcePool.checkIn(source);
        super.remove();
    }
    
    @Override
    public void close() {
        remove();
    }
    
    @Override
    public void init(SortedKeyValueIterator<K,V> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        get().init(source, options, env);
    }
    
    @Override
    public boolean hasTop() {
        return get().hasTop();
    }
    
    @Override
    public void next() throws IOException {
        get().next();
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        get().seek(range, columnFamilies, inclusive);
    }
    
    @Override
    public K getTopKey() {
        return get().getTopKey();
    }
    
    @Override
    public V getTopValue() {
        return get().getTopValue();
    }
    
    @Override
    public SortedKeyValueIterator<K,V> deepCopy(IteratorEnvironment env) {
        return new ThreadLocalPooledSource(sourcePool.deepCopy());
    }
}
