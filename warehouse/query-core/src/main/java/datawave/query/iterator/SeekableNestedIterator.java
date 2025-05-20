package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.Logger;

import datawave.query.attributes.Document;

/**
 * This class is a delegate/base class now that the SeekableIterator was merged into the NestedIterator interface
 */
public class SeekableNestedIterator<T> implements NestedIterator<T> {
    private static final Logger log = Logger.getLogger(SeekableNestedIterator.class);
    private final NestedIterator<T> source;
    protected Range totalRange = null;
    protected Collection<ByteSequence> columnFamilies = null;
    protected boolean inclusive = false;

    public SeekableNestedIterator(NestedIterator<T> source, IteratorEnvironment env) {
        this.source = source;
        this.source.setEnvironment(env);
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.totalRange = range;
        this.columnFamilies = columnFamilies;
        this.inclusive = inclusive;
        source.seek(range, columnFamilies, inclusive);
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

    @Override
    public boolean isContextRequired() {
        return source.isContextRequired();
    }

    @Override
    public void setContext(T context) {
        source.setContext(context);
    }

    @Override
    public boolean isNonEventField() {
        return source.isNonEventField();
    }
}
