package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Not thread safe
 *
 * Limit a source by a given Key without copying the underlying source. Limited sources will be inclusive of the limit. This is to avoid having to re-seek a
 * source which will have arbitrary limits imposed within its original seek range. setLimit() may be called multiple times on the same underlying source
 *
 */
public class LimitedSortedKeyValueIterator implements SortedKeyValueIterator<Key,Value> {
    private SortedKeyValueIterator<Key,Value> delegate;
    private Key limit;
    private IteratorEnvironment environment;

    public LimitedSortedKeyValueIterator(SortedKeyValueIterator<Key,Value> delegate) {
        this.delegate = delegate;
    }

    public LimitedSortedKeyValueIterator(LimitedSortedKeyValueIterator other) {
        this.limit = other.limit;
        this.environment = other.environment;
        this.delegate = other.delegate.deepCopy(environment);
    }

    public void setLimit(Key limit) {
        this.limit = limit;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.delegate = source;
        this.environment = env;
    }

    @Override
    public boolean hasTop() {
        return (delegate.hasTop() && delegate.getTopKey().compareTo(limit) <= 0);
    }

    @Override
    public void next() throws IOException {
        delegate.next();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        delegate.seek(range, columnFamilies, inclusive);
    }

    @Override
    public Key getTopKey() {
        return delegate.getTopKey();
    }

    @Override
    public Value getTopValue() {
        return delegate.getTopValue();
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new LimitedSortedKeyValueIterator(this);
    }
}
