package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import datawave.query.attributes.Document;
import datawave.query.util.TraceIterators;

import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

/**
 * An iterable interface for wrapping a tree of iterators. This class provides a convenience mechanism for holding on to a tree, allowing clients to use Java's
 * enhanced for loop with the tree, as well as tracking Accumulo's initialization sequence by deferring full initialization until seek is called.
 * 
 * @param <T>
 */
public class AccumuloTreeIterable<S,T extends Comparable<T>> implements Iterable<Entry<T,Document>> {
    public NestedIterator<S> tree;
    protected boolean seenSeek;
    protected Function<Entry<S,Document>,Entry<T,Document>> func;
    
    public AccumuloTreeIterable() {
        tree = null;
    }
    
    public AccumuloTreeIterable(NestedIterator<S> tree, Function<Entry<S,Document>,Entry<T,Document>> func) {
        this.tree = tree;
        this.func = func;
    }
    
    @Override
    public Iterator<Entry<T,Document>> iterator() {
        if (seenSeek) {
            Span s = Trace.start("Field Index Initialize Boolean Tree");
            
            try {
                tree.initialize();
            } finally {
                if (null != s) {
                    s.stop();
                }
            }
            
            Iterator<Entry<S,Document>> wrapper = TraceIterators.transform(tree, from -> {
                return Maps.immutableEntry(from, tree.document());
            }, "Field Index");
            
            return Iterators.transform(wrapper, func);
        } else {
            throw new IllegalStateException("You have to seek this tree.");
        }
    }
    
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        Span s = null;
        try {
            s = Trace.start("Field Index Seek Sources");
            Iterable<? extends NestedIterator<?>> leaves = tree.leaves();
            for (NestedIterator<?> leaf : leaves) {
                if (leaf instanceof SeekableIterator) {
                    ((SeekableIterator) leaf).seek(range, columnFamilies, inclusive);
                }
            }
            seenSeek = true;
        } finally {
            if (s != null) {
                s.stop();
            }
        }
    }
}
