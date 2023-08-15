package datawave.query.iterator.logic;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

import datawave.query.attributes.Document;
import datawave.query.iterator.DocumentIterator;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.SeekableIterator;

/**
 * Wraps an Accumulo iterator with a NestedIterator interface. This bridges the gap between an IndexIterator and a NestedIterator.
 * <p>
 * This bridge is required due to some idiosyncrasies with the SortedKeyValueIterator and the Iterator interface.
 * <p>
 * There two options for control flow
 * <ol>
 * <li>seek()</li>
 * <li>hasNext()</li>
 * <li>next()</li>
 * <li>document()</li>
 * </ol>
 * Or
 * <ol>
 * <li>seek()</li>
 * <li>move(context)</li>
 * <li>document()</li>
 * </ol>
 */
public class IndexIteratorBridge implements SeekableIterator, NestedIterator<Key>, Comparable<IndexIteratorBridge> {
    private static final Logger log = Logger.getLogger(IndexIteratorBridge.class);

    private final String field;
    private final JexlNode node;
    private boolean nonEventField;

    // cache layer that wraps the delegate iterator
    private Key next;
    private Document document;

    /*
     * The AccumuloIterator this object wraps.
     */
    private final DocumentIterator delegate;

    public IndexIteratorBridge(DocumentIterator delegate, JexlNode node, String field) {
        this.delegate = delegate;
        this.node = node;
        this.field = field;
    }

    /**
     * Populates the next element from the delegate and advances the delegate
     *
     * @return the next element
     */
    public Key next() {
        if (delegate.hasTop()) {
            next = delegate.getTopKey();
            document = delegate.document();
        }

        try {
            delegate.next();
        } catch (IOException e) {
            log.error(e);
            // throw the exception up the stack
            throw new RuntimeException(e);
        }

        return next;
    }

    /**
     * Uses the delegate to determine if a next element exists, otherwise clears the cache layer.
     *
     * @return true if the delegate has a top element
     */
    public boolean hasNext() {
        if (delegate.hasTop()) {
            return true;
        } else {
            next = null;
            document = null;
            return false;
        }
    }

    /**
     * Advance the bridge iterator to the next key that is greater than or equal to the minimum, taking into account the cache layer.
     *
     * @param minimum
     *            the minimum key to advance to
     * @return the first key greater than or equal to minimum found
     */
    public Key move(Key minimum) {

        // check the cache layer first
        if (next != null) {
            int result = minimum.compareTo(next, PartialKey.ROW_COLFAM);
            if (result <= 0) {
                // minimum < next < delegate.topKey or
                // minimum == next < delegate.topKey
                return next;
            }
        }

        // check the delegate top key
        if (hasNext()) {
            int result = minimum.compareTo(delegate.getTopKey(), PartialKey.ROW_COLFAM);
            if (result <= 0) {
                // next < minimum < delegate.topKey or
                // next < minimum == delegate.topKey
                return next();
            }
        }

        // at this point it is safe to move
        try {
            delegate.move(minimum);
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e); // throw it up the stack
        }

        next = null;
        document = null;

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
        return document;
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
    public boolean isNonEventField() {
        return nonEventField;
    }

    public void setNonEventField(boolean nonEventField) {
        this.nonEventField = nonEventField;
    }

    @Override
    public int compareTo(IndexIteratorBridge other) {
        //  @formatter:off
        return new CompareToBuilder()
                        .append(this.field, other.field)
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
            IndexIteratorBridge other = (IndexIteratorBridge) o;
            //  @formatter:off
            return new EqualsBuilder()
                            .append(field, other.field)
                            .append(node, other.node)
                            .append(delegate, other.delegate)
                            .isEquals();
            //  @formatter:on
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
