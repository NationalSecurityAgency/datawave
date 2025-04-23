package datawave.query.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;

import datawave.query.attributes.Document;

/**
 * This iterator supports a full table scan over the event column
 */
public class EventDataScanNestedIterator implements NestedIterator<Key> {
    private static final Logger log = Logger.getLogger(EventDataScanNestedIterator.class);
    protected SortedKeyValueIterator<Key,Value> source;
    protected Key topKey = null;
    protected Range totalRange = null;
    protected Collection<ByteSequence> columnFamilies = null;
    protected Predicate<Key> dataTypeFilter;
    protected boolean inclusive = false;

    public EventDataScanNestedIterator(SortedKeyValueIterator<Key,Value> source, Predicate<Key> dataTypeFilter) {
        this.source = source;
        this.dataTypeFilter = dataTypeFilter;
    }

    @Override
    public void initialize() {}

    /**
     * Get the next document start key. TODO: See if we can skip over datatypes as defined by the dataTypeFilter
     *
     * @param key
     *            a key
     * @return the next document key
     */
    protected Key nextStartKey(Key key) {
        return key.followingKey(PartialKey.ROW_COLFAM);
    }

    @Override
    public Key move(Key minimum) {
        if (totalRange != null) {
            Range newRange = totalRange;
            if (totalRange.contains(minimum)) {
                newRange = new Range(minimum, true, totalRange.getEndKey(), totalRange.isEndKeyInclusive());
            } else {
                newRange = new Range(minimum, true, nextStartKey(minimum), false);
            }
            try {
                source.seek(newRange, columnFamilies, inclusive);
            } catch (IOException e) {
                log.error("Failed to move to new key " + minimum, e);
                throw new RuntimeException("Failed to move to new key " + minimum, e);
            }
        } else {
            log.error("Failed to move to new key " + minimum);
            throw new RuntimeException("Failed to move to new key " + minimum);
        }
        findNextDocument();
        return topKey;
    }

    @Override
    public Collection<NestedIterator<Key>> leaves() {
        return Collections.singleton((NestedIterator<Key>) this);
    }

    @Override
    public Collection<NestedIterator<Key>> children() {
        return Collections.singleton((NestedIterator<Key>) this);
    }

    @Override
    public Document document() {
        return new Document();
    }

    @Override
    public boolean hasNext() {
        return source.hasTop();
    }

    @Override
    public Key next() {
        Key rtrn = topKey;
        if (rtrn != null) {
            move(nextStartKey(topKey));
        }
        return rtrn;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("This iterator does not support remove().");
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.totalRange = range;
        this.columnFamilies = columnFamilies;
        this.inclusive = inclusive;

        // determine if we have been torn down and rebuilt
        if (!range.isInfiniteStartKey() && !range.isStartKeyInclusive()) {
            move(nextStartKey(range.getStartKey()));
        } else {
            source.seek(range, columnFamilies, inclusive);
            findNextDocument();
        }
    }

    protected void findNextDocument() {
        topKey = null;

        try {
            Text cf = new Text();

            /*
             * Given that we are already at a document key, this method will continue to advance the underlying source until it is either exhausted (hasTop()
             * returns false), the returned key is not in the totalRange, and the current top key shares the same row and column family as the source's next
             * key.
             */
            while (topKey == null && source.hasTop()) {
                Key k = source.getTopKey();
                if (log.isTraceEnabled())
                    log.trace("Sought to " + k);
                k.getColumnFamily(cf);

                if (!isEventKey(k)) {
                    if (cf.find("fi\0") == 0) {
                        if (log.isDebugEnabled()) {
                            log.debug("Seeking over 'fi')");
                        }
                        // Try to do an optimized jump over the field index
                        cf.set("fi\1");
                        source.seek(new Range(new Key(source.getTopKey().getRow(), cf), false, totalRange.getEndKey(), totalRange.isEndKeyInclusive()),
                                        columnFamilies, inclusive);
                    } else if (cf.getLength() == 1 && cf.charAt(0) == 'd') {
                        if (log.isDebugEnabled()) {
                            log.debug("Seeking over 'd'");
                        }
                        // Try to do an optimized jump over the raw documents
                        cf.set("d\0");
                        source.seek(new Range(new Key(source.getTopKey().getRow(), cf), false, totalRange.getEndKey(), totalRange.isEndKeyInclusive()),
                                        columnFamilies, inclusive);
                    } else if (cf.getLength() == 2 && cf.charAt(0) == 't' && cf.charAt(1) == 'f') {
                        if (log.isDebugEnabled()) {
                            log.debug("Seeking over 'tf'");
                        }
                        // Try to do an optimized jump over the term frequencies
                        cf.set("tf\0");
                        source.seek(new Range(new Key(source.getTopKey().getRow(), cf), false, totalRange.getEndKey(), totalRange.isEndKeyInclusive()),
                                        columnFamilies, inclusive);
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Next()'ing over the current key");
                        }
                        source.next();
                    }
                } else {
                    if (dataTypeFilter.apply(source.getTopKey())) {
                        this.topKey = source.getTopKey();
                    } else {
                        Range nextCF = new Range(nextStartKey(source.getTopKey()), true, totalRange.getEndKey(), totalRange.isEndKeyInclusive());
                        source.seek(nextCF, columnFamilies, inclusive);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not seek in findNextDocument", e);
        }
    }

    boolean isEventKey(Key k) {
        Text cf = k.getColumnFamily();
        return cf.getLength() > 0 && cf.find("\u0000") != -1 && !((cf.charAt(0) == 'f' && cf.charAt(1) == 'i' && cf.charAt(2) == 0)
                        || (cf.getLength() == 1 && cf.charAt(0) == 'd') || (cf.getLength() == 2 && cf.charAt(0) == 't' && cf.charAt(1) == 'f'));
    }

    @Override
    public boolean isContextRequired() {
        return false;
    }

    @Override
    public void setContext(Key context) {
        // no-op
    }

    /**
     * By definition this iterator only scans event keys
     *
     * @return false
     */
    @Override
    public boolean isNonEventField() {
        return false;
    }
}
