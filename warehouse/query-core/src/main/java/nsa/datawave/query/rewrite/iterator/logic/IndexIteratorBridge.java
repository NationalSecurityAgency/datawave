package nsa.datawave.query.rewrite.iterator.logic;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.iterator.DocumentIterator;
import nsa.datawave.query.rewrite.iterator.NestedIterator;
import nsa.datawave.query.rewrite.iterator.SeekableIterator;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.log4j.Logger;

/**
 * Wraps an Accumulo iterator with a NestedIterator interface. This bridges the gap between an IndexIterator and a NestedIterator.
 * 
 * 
 * 
 */
public class IndexIteratorBridge implements NestedIterator<Key>, SeekableIterator {
    private final static Logger log = Logger.getLogger(IndexIteratorBridge.class);
    
    /*
     * The AccumuloIterator this object wraps.
     */
    private DocumentIterator delegate;
    
    /*
     * Pointer to the next Key.
     */
    private Key next;
    private Document prevDocument, nextDocument;
    
    public IndexIteratorBridge(DocumentIterator delegate) {
        this.delegate = delegate;
    }
    
    public Key next() {
        Key k = next;
        prevDocument = nextDocument;
        next = null;
        try {
            
            if (delegate.hasTop()) {
                next = delegate.getTopKey();
                nextDocument = delegate.document();
                delegate.next();
            }
        } catch (IOException e) {
            log.error(e);
            // throw the exception up the stack....
            throw new RuntimeException(e);
        }
        
        return k;
    }
    
    public boolean hasNext() {
        return next != null;
    }
    
    public Key move(Key minimum) {
        /*
         * If we are told to move to the Key that we current have cached, we don't have to do anything
         */
        if (this.hasNext() && this.next.compareTo(minimum) >= 0) {
            return next();
        }
        
        // The IIB is caching the last K/V from the delegate, so we need to check
        // as to avoid the exception thrown by II.move(min)
        //
        // e.g. `next` is 'A', minimum is 'B', but delegate.tk is 'C'
        if (delegate.hasTop() && delegate.getTopKey().compareTo(minimum) >= 0) {
            try {
                next = delegate.getTopKey();
                nextDocument = delegate.document();
                delegate.next();
                return next();
            } catch (IOException e) {
                log.error(e);
                // throw the exception up the stack....
                throw new RuntimeException(e);
            }
        }
        
        try {
            delegate.move(minimum);
            
            if (delegate.hasTop()) {
                next = delegate.getTopKey();
                nextDocument = delegate.document();
                delegate.next();
                return next();
            }
            
        } catch (IOException e) {
            log.error(e);
            // throw the exception up the stack....
            throw new RuntimeException(e);
        }
        
        return null;
    }
    
    /**
     * Calls <code>seek</code> on the wrapped Accumulo iterator. This method is necessary because the tree and source iterators are set in
     * <code>initialize</code> but are not <code>seek</code>'d until called by a higher level iterator.
     * 
     * @param range
     * @param columnFamilies
     * @param includeCFs
     */
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean includeCFs) {
        try {
            delegate.seek(range, columnFamilies, includeCFs);
            if (delegate.hasTop()) {
                next = delegate.getTopKey();
                nextDocument = delegate.document();
                delegate.next();
            } else {
                next = null;
            }
        } catch (IOException e) {
            log.error(e);
            // throw the exception up the stack....
            throw new RuntimeException(e);
        }
    }
    
    public Collection<NestedIterator<Key>> leaves() {
        HashSet<NestedIterator<Key>> s = new HashSet<>(1);
        s.add(this);
        return s;
    }
    
    public Collection<NestedIterator<Key>> children() {
        return Collections.emptyList();
    }
    
    public void remove() {
        throw new UnsupportedOperationException("This iterator does not support remove().");
    }
    
    public void initialize() {}
    
    @Override
    public String toString() {
        return "Bridge: " + delegate.toString();
    }
    
    @Override
    public Document document() {
        // If we can assert that this Document won't be reused, we can use _document()
        return prevDocument;
    }
}
