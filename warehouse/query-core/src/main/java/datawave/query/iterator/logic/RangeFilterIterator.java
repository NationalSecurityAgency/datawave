package datawave.query.iterator.logic;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;

import datawave.query.Constants;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.data.parsers.FieldIndexKey;
import datawave.query.iterator.NestedIterator;
import datawave.query.jexl.DelayedNonEventIndexContext;

/**
 * Allows a range term to be run as a context iterator
 * <p>
 * A {@link NestedIterator} requires outside context to determine its truth state. The prototypical example is a negation. It's very difficult to find all the
 * documents that 'not blue'. However, it's very easy to determine which 'monster trucks' are 'not blue'.
 * <p>
 * The same concept can be applied to range terms. Consider the extreme example of a two-term query A and B when A matches a single document and B matches every
 * document in the shard. We do not want to run the B term as an ivarator, in this case it makes sense to run it as a filter for the A term.
 * <p>
 * Field Index key structure
 * <p>
 * <code>row fi\x00FIELD : value\x00datatype\x00uid</code>
 */
public class RangeFilterIterator implements NestedIterator<Key>, Comparable<IndexIteratorBridge> {

    private static final Logger log = LoggerFactory.getLogger(RangeFilterIterator.class);

    private SortedKeyValueIterator<Key,Value> source;
    private String field;
    private String lowerBound;
    private String upperBound;
    private boolean lowerInclusive;
    private boolean upperInclusive;

    private String row = null;
    private String columnFamily = null;
    private Collection<ByteSequence> seekColumnFamilies = null;
    private Range seekRange = null;

    private Predicate<Key> timeFilter;

    private Document document = new Document();
    private final FieldIndexKey parser = new FieldIndexKey();

    private boolean aggregate = false;
    private AttributeFactory factory;

    private long numMoves = 0L;
    private long numNexts = 0L;
    private long numSeeks = 0L;

    @Override
    public void seek(Range range, Collection<ByteSequence> collection, boolean b) throws IOException {
        source.seek(range, collection, b);
    }

    @Override
    public void initialize() {
        // no-op
    }

    @Override
    public Key move(Key minimum) {
        numMoves++;

        // key is in the form ROW DATATYPE<null>UID
        Text target = minimum.getColumnFamily();

        Range seekRange = buildSeekRange(minimum);
        try {
            source.seek(seekRange, getSeekColumnFamily(), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (aggregate) {
            document = new Document();
        }

        boolean found = false;
        while (!found && source.hasTop()) {

            Key tk = source.getTopKey();
            parser.parse(tk);
            // value always matches given a range and sorted keys

            int cmpr = compareDatatypeUid(parser.getDatatype() + "\0" + parser.getUid(), minimum.getColumnFamily().toString());
            if (cmpr < 0) {
                // advance to target
                log.trace("tk sorted before target, advance to target");
                Text seekCQ = new Text(parser.getValue() + '\u0000' + target.toString());
                Key start = new Key(tk.getRow(), tk.getColumnFamily(), seekCQ);
                seekRange = new Range(start, true, seekRange.getEndKey(), seekRange.isEndKeyInclusive());
                advance(seekRange, getSeekColumnFamily(), true);
                continue;
            } else if (cmpr > 0) {
                // advance to next value
                log.trace("tk sorted after target, advance to next value");
                Key next = new Key(tk.getRow(), tk.getColumnFamily(), new Text(parser.getValue() + '\u0000' + '\uFFFF'));
                seekRange = new Range(next, true, seekRange.getEndKey(), seekRange.isEndKeyInclusive());
                advance(seekRange, getSeekColumnFamily(), true);
                continue;
            }

            if (timeFilter == null || timeFilter.apply(tk)) {
                // value matched, datatype/uid matched, time filter matched
                found = true;
                if (aggregate) {
                    parser.parse(tk);
                    document.put(parser.getField(), factory.create(parser.getField(), parser.getValue(), tk, true));
                }
                continue;
            }

            // key matches everything except the time filter, advance to the next key
            advance();
        }

        // NOTE: This iterator will only return two states
        // the minimum in the case of an exact match
        // the minimum plus a null byte in the case of no match
        if (found) {
            log.trace("target: {} matched: {}", target, document.toString());
            return minimum;
        } else {
            log.trace("target: {} did not match", target);
            String cf = target.toString() + '\u0000';
            return new Key(minimum.getRow().toString(), cf);
        }
    }

    /**
     * Compares the datatype and uid of the current key against the target
     *
     * @param cq
     *            the top key's column qualifier
     * @param target
     *            the context
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object.
     */
    private int compareDatatypeUid(String cq, String target) {
        return cq.compareTo(target);
    }

    private void advance() {
        try {
            numNexts++;
            source.next();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void advance(Range seekRange, Collection<ByteSequence> columnFamilies, boolean inclusive) {
        try {
            numSeeks++;
            source.seek(seekRange, columnFamilies, inclusive);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Range buildSeekRange(Key minimum) {
        if (seekRange == null) {
            Key start = getStartKey(minimum);
            Key stop = getStopKey(minimum);
            seekRange = new Range(start, false, stop, false);
        }
        return seekRange;
    }

    private Key getStartKey(Key minimum) {
        if (lowerInclusive) {
            return new Key(getRow(minimum), getColumnFamily(), lowerBound);
        } else {
            return new Key(getRow(minimum), getColumnFamily(), lowerBound + '\u0001');
        }
    }

    private Key getStopKey(Key minimum) {
        if (upperInclusive) {
            return new Key(getRow(minimum), getColumnFamily(), upperBound + '\u0001');
        } else {
            char c = upperBound.charAt(upperBound.length() - 1);
            c = (char) (c - 1);

            StringBuilder upper = new StringBuilder(upperBound);
            upper.setLength(upper.length() - 1);
            upper.append(c);
            upper.append(Constants.MAX_UNICODE_STRING);
            return new Key(getRow(minimum), getColumnFamily(), upper.toString());
        }
    }

    private String getRow(Key minimum) {
        if (row == null) {
            row = minimum.getRow().toString();
        }
        return row;
    }

    private String getColumnFamily() {
        if (columnFamily == null) {
            columnFamily = "fi\0" + field;
        }
        return columnFamily;
    }

    private Collection<ByteSequence> getSeekColumnFamily() {
        if (seekColumnFamilies == null) {
            seekColumnFamilies = Collections.singleton(new ArrayByteSequence(getColumnFamily()));
        }
        return seekColumnFamilies;
    }

    @Override
    public Collection<NestedIterator<Key>> leaves() {
        return List.of();
    }

    @Override
    public Collection<NestedIterator<Key>> children() {
        return List.of();
    }

    /**
     * This iterator is only concerned with defeating document candidates at the field index. A non-event field will still need to be aggregated via the
     * {@link DelayedNonEventIndexContext}.
     *
     * @return a document
     */
    @Override
    public Document document() {
        return document;
    }

    @Override
    public boolean isContextRequired() {
        // context is always required for this iterator
        return true;
    }

    @Override
    public void setContext(Key context) {
        // no-op. context is always derived from a 'move'
    }

    @Override
    public boolean isNonEventField() {
        // this iterator should never be used for a non-event field
        return false;
    }

    @Override
    public int compareTo(IndexIteratorBridge o) {
        return 0;
    }

    @Override
    public boolean hasNext() {
        // context required iterator technically always has a potential next value
        return true;
    }

    @Override
    public Key next() {
        // this iterator only advances via a call to 'move', it will never advance via next from an external call
        return null;
    }

    // === builder style methods ===

    public RangeFilterIterator withSource(SortedKeyValueIterator<Key,Value> source) {
        this.source = source;
        return this;
    }

    public RangeFilterIterator withField(String field) {
        this.field = field;
        return this;
    }

    public RangeFilterIterator withLowerBound(String lowerBound) {
        this.lowerBound = lowerBound;
        return this;
    }

    public RangeFilterIterator withUpperBound(String upperBound) {
        this.upperBound = upperBound;
        return this;
    }

    public RangeFilterIterator withLowerInclusive(boolean lowerInclusive) {
        this.lowerInclusive = lowerInclusive;
        return this;
    }

    public RangeFilterIterator withUpperInclusive(boolean upperInclusive) {
        this.upperInclusive = upperInclusive;
        return this;
    }

    public RangeFilterIterator withTimeFilter(Predicate<Key> timeFilter) {
        this.timeFilter = timeFilter;
        return this;
    }

    public RangeFilterIterator withAggregation(boolean aggregate) {
        this.aggregate = aggregate;
        return this;
    }

    public RangeFilterIterator withAttributeFactory(AttributeFactory factory) {
        this.factory = factory;
        return this;
    }

    public void logStats() {
        log.trace("move: {} seek: {} next: {}", numMoves, numSeeks, numNexts);
    }
}
