package datawave.query.iterator.logic;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

import datawave.query.attributes.Document;
import datawave.query.iterator.DocumentIterator;
import datawave.query.iterator.NestedIterator;

/**
 * Wraps an Accumulo iterator with a NestedIterator interface. This bridges the gap between an IndexIterator and a NestedIterator.
 */
public class IndexIteratorBridge implements NestedIterator<Key>, Comparable<IndexIteratorBridge> {
    private static final Logger log = Logger.getLogger(IndexIteratorBridge.class);

    /*
     * The AccumuloIterator this object wraps.
     */
    private DocumentIterator delegate;

    /*
     * Pointer to the next Key.
     */
    private Key next;

    private String field;

    private JexlNode node;
    private boolean nonEventField;

    /**
     * track the last Key returned for move purposes
     */
    private Key prevKey;
    private Document prevDocument;
    private Document nextDocument;

    public IndexIteratorBridge(DocumentIterator delegate, JexlNode node, String field) {
        this.delegate = delegate;
        this.node = node;
        this.field = field;
    }

    public Key next() {
        prevKey = next;
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

        return prevKey;
    }

    public boolean hasNext() {
        return next != null;
    }

    /**
     * Advance to the next Key in the iterator that is greater than or equal to minimum. First check the cached value in next, then check the delegate cached
     * value in getTopValue(), finally advance the delegate
     *
     * @param minimum
     *            the minimum key to advance to
     * @return the first Key greater than or equal to minimum found
     * @throws IllegalStateException
     *             if prevKey is greater than or equal to minimum
     */
    public Key move(Key minimum) {
        if (prevKey != null && prevKey.compareTo(minimum) >= 0) {
            throw new IllegalStateException("Tried to call move when already at or beyond move point: topkey=" + prevKey + ", movekey=" + minimum);
        }

        /*
         * First check if the next Key to be returned meets the criteria and if so simply advance to it
         */
        if (this.hasNext() && this.next.compareTo(minimum) >= 0) {
            // since next already contains the target, just advance to return that
            return next();
        }

        // The IIB is caching the last K/V from the delegate, so we need to check
        // as to avoid the exception thrown by II.move(min)
        //
        // e.g. `next` is 'A', minimum is 'B', but delegate.tk is 'C'
        if (delegate.hasTop() && delegate.getTopKey().compareTo(minimum) < 0) {
            // at this point both layers of caching have been checked and its safe to advance the underlying delegate
            try {
                // advance source and put the first key >= minimum found into getTopKey()/getTopValue()
                delegate.move(minimum);
            } catch (IOException e) {
                log.error(e);
                // throw the exception up the stack....
                throw new RuntimeException(e);
            }
        }

        // either delegate.getTopKey() already contained the target Key, or the move advanced to it
        // advance current delegate.getTopKey() into next, discarding the current next
        next();

        // now return the value which was propagated into next with the previous call
        return next();
    }

    /**
     * Calls <code>seek</code> on the wrapped Accumulo iterator. This method is necessary because the tree and source iterators are set in
     * <code>initialize</code> but are not <code>seek</code>'d until called by a higher level iterator.
     *
     * @param range
     *            a range
     * @param columnFamilies
     *            set of column families
     * @param includeCFs
     *            check to include cfs
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

    @Override
    public void remove() {
        throw new UnsupportedOperationException("This iterator does not support remove().");
    }

    public void initialize() {
        // no-op
    }

    @Override
    public String toString() {
        return "Bridge: " + delegate;
    }

    @Override
    public Document document() {
        // If we can assert that this Document won't be reused, we can use _document()
        return prevDocument;
    }

    public JexlNode getSourceNode() {
        return node;
    }

    public String getField() {
        return field;
    }

    @Override
    public boolean isContextRequired() {
        return false;
    }

    @Override
    public void setContext(Key context) {
        // no-op
    }

    @Override
    public boolean isNonEventField() {
        return nonEventField;
    }

    public void setNonEventField(boolean nonEventField) {
        this.nonEventField = nonEventField;
    }

    @Override
    public int compareTo(IndexIteratorBridge other) {
        //  @formatter:off
        return new CompareToBuilder().append(this.field, other.field)
                        .append(this.node, other.node)
                        .append(this.delegate, other.delegate)
                        .build();
        //  @formatter:on
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof IndexIteratorBridge) {
            return Objects.equals(this, o);
        }
        return false;
    }

    @Override
    public int hashCode() {
        //  @formatter:off
        return new HashCodeBuilder(23,31)
                        .append(field)
                        .append(node)
                        .append(delegate)
                        .toHashCode();
        //  @formatter:on
    }
}
