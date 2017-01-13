package nsa.datawave.query.rewrite.iterator;

import nsa.datawave.query.rewrite.attributes.Document;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * 
 */
public class SeekableNestedIterator<T> implements NestedIterator<T>, SeekableIterator {
    private static final Logger log = Logger.getLogger(SeekableNestedIterator.class);
    private NestedIterator<T> source;
    protected Range totalRange = null;
    protected Collection<ByteSequence> columnFamilies = null;
    protected boolean inclusive = false;
    
    public SeekableNestedIterator(NestedIterator<T> source) {
        this.source = source;
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.totalRange = range;
        this.columnFamilies = columnFamilies;
        this.inclusive = inclusive;
        if (source instanceof SeekableIterator) {
            ((SeekableIterator) source).seek(range, columnFamilies, inclusive);
        } else {
            Iterable<? extends NestedIterator<?>> leaves = source.leaves();
            for (NestedIterator<?> leaf : leaves) {
                if (leaf instanceof SeekableIterator) {
                    ((SeekableIterator) leaf).seek(range, columnFamilies, inclusive);
                }
            }
        }
    }
    
    @Override
    public void initialize() {
        source.initialize();
    }
    
    @Override
    public T move(T minimum) {
        return source.move(minimum);
    }
    
    @Override
    public Collection<NestedIterator<T>> leaves() {
        return source.leaves();
    }
    
    @Override
    public Collection<NestedIterator<T>> children() {
        return source.children();
    }
    
    @Override
    public Document document() {
        return source.document();
    }
    
    @Override
    public boolean hasNext() {
        return source.hasNext();
    }
    
    @Override
    public T next() {
        return source.next();
    }
    
    @Override
    public void remove() {
        source.remove();
    }
}
