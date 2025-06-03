package datawave.next;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.next.stats.DocumentIteratorStats;
import datawave.query.data.parsers.FieldIndexKey;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

/**
 * An abstract document id iterator. Each extending iterator must handle
 * <ul>
 * <li>parsing relevant information from the JexlNode</li>
 * <li>build the correct scan range given context</li>
 * <li>handle a datatype miss</li>
 * </ul>
 */
public abstract class BaseDocIdIterator implements Iterator<Key> {

    private static final Logger log = LoggerFactory.getLogger(BaseDocIdIterator.class);

    protected final JexlNode node;
    protected String nodeString;

    protected final String row;
    protected String prefix;
    protected Collection<ByteSequence> columnFamilies;

    protected TreeSet<String> datatypeFilter;
    protected LongRange timeFilter;
    protected String suffix; // if executing within the context of a document range

    protected String minDatatype;
    protected String maxDatatype;

    protected String minDatatypeUid;
    protected String maxDatatypeUid;

    protected String field;

    protected Key tk;
    protected final SortedKeyValueIterator<Key,Value> source;

    protected boolean seenSeek = false;
    protected final FieldIndexKey parser = new FieldIndexKey();
    protected final DocumentIteratorStats stats = new DocumentIteratorStats();

    /**
     * Minimal constructor requires the source, row, and JexlNode
     *
     * @param source
     *            the source iterator
     * @param row
     *            the row
     * @param node
     *            the JexlNode
     */
    public BaseDocIdIterator(SortedKeyValueIterator<Key,Value> source, String row, JexlNode node) {
        this.source = source;
        this.row = row;
        this.node = node;
        parseNode(node);

        this.prefix = "fi\0" + field;
        this.columnFamilies = Collections.singleton(new ArrayByteSequence(prefix));
    }

    /**
     * Should parse the field from the JexlNode. One or more values may exist depending on the node type.
     *
     * @param node
     *            a JexlNode
     */
    protected abstract void parseNode(JexlNode node);

    /**
     * Sets the datatype filter
     *
     * @param datatypeFilter
     *            the datatypes
     */
    public void withDatatypes(Set<String> datatypeFilter) {
        this.datatypeFilter = new TreeSet<>(datatypeFilter);
    }

    /**
     * Sets the time filter
     *
     * @param timeFilter
     *            the time filter
     */
    public void withTimeFilter(LongRange timeFilter) {
        this.timeFilter = timeFilter;
    }

    /**
     * The suffix is the datatype and uid, only applicable in the context of a document range
     *
     * @param suffix
     *            the datatype and uid
     */
    public void withSuffix(String suffix) {
        this.suffix = suffix;
    }

    /**
     * Allows scan ranges to be bounded by the min and max of a previous scan. This type of scan bounding is more restricted than a datatype filter.
     *
     * @param min
     *            the minimum key
     * @param max
     *            the maximum key
     */
    public void withMinMax(Key min, Key max) {
        this.minDatatypeUid = min.getColumnFamily().toString();
        this.maxDatatypeUid = max.getColumnFamily().toString();
        attemptDatatypeFilterReduction();
    }

    /**
     * Attempt to reduce the datatype filter if one exists.
     * <p>
     * If no datatype filter exists then the min and max datatypes are recorded.
     * <p>
     * If the min or max match the filter is a singleton. Otherwise, the filter becomes a range.
     */
    protected void attemptDatatypeFilterReduction() {
        String minDatatype = this.minDatatypeUid.substring(0, this.minDatatypeUid.indexOf('\u0000'));
        String maxDatatype = this.maxDatatypeUid.substring(0, this.maxDatatypeUid.indexOf('\u0000'));

        if (datatypeFilter == null) {
            // case 0: no filter requested, external context can set a singleton filter
            if (minDatatype.equals(maxDatatype)) {
                log.debug("no datatype filter requested but external context only contained a single datatype: {}", minDatatype);
                datatypeFilter = new TreeSet<>();
                datatypeFilter.add(minDatatype);
            } else {
                // case 1: no filter requested, external context can set a singleton filter
                this.minDatatype = minDatatype;
                this.maxDatatype = maxDatatype;
            }
        }

        // attempt to restrict the datatype filter based on external context
        if (datatypeFilter != null && datatypeFilter.size() != 1) {

            // case 2: multiple datatypes requested but context can restrict to singleton filter
            if (minDatatype.equals(maxDatatype)) {
                if (log.isDebugEnabled()) {
                    log.debug("multiple datatypes requested but external context only contained a single datatype: {}", minDatatype);
                }
                datatypeFilter.clear();
                datatypeFilter.add(minDatatype);
                return;
            }

            // case 3: requested filter and context both have multiple datatypes.
            int prevSize = datatypeFilter.size();

            // unwind the first
            while (datatypeFilter.first().compareTo(minDatatype) < 0) {
                datatypeFilter.pollFirst();
            }

            // unwind the last
            while (datatypeFilter.last().compareTo(maxDatatype) > 0) {
                datatypeFilter.pollLast();
            }

            int nextSize = datatypeFilter.size();
            if (nextSize < prevSize && log.isDebugEnabled()) {
                log.debug("reduced datatype filter from {} to {}", prevSize, nextSize);
            }
        }
    }

    /**
     * Get the top key and set the top key to null
     *
     * @return the top key
     */
    @Override
    public Key next() {
        Key next = tk;
        tk = null;
        return next;
    }

    /**
     * Each implementation will build a slightly different scan range based on context. For example, an EQ iterator with a datatype filter of a single datatype
     * will have a different scan range than an EQ iterator with no datatype filter.
     *
     * @return the scan range
     */
    protected abstract Range createScanRange();

    /**
     * Helper method that calls next on the underlying source while incrementing any backing stats if requested
     */
    protected void safeNext() {
        try {
            source.next();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            stats.incrementNextCount();
        }
    }

    /**
     * Helper method that calls seek on the underlying source while incrementing any backing stats if requested
     *
     * @param range
     *            the seek range
     * @param inclusive
     *            true if the seek is inclusive
     */
    protected void safeSeek(Range range, boolean inclusive) {
        try {
            source.seek(range, columnFamilies, inclusive);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            stats.incrementSeekCount();
        }
    }

    /**
     * Handle a datatype miss. This will look different for each extending iterator.
     */
    protected abstract void handleDatatypeMiss();

    /**
     * Transforms a field index key into an event key
     *
     * @param key
     *            the field index key
     * @return an event key
     */
    protected Key fiToEvent(Key key) {
        Text cf = new Text(parser.getDatatype() + '\u0000' + parser.getUid());
        return new Key(key.getRow(), cf);
    }

    /**
     * Get the string representation of the backing JexlNode
     *
     * @return the node string
     */
    protected String getNode() {
        if (nodeString == null) {
            Preconditions.checkNotNull(node, "Source node was null");
            nodeString = JexlStringBuildingVisitor.buildQuery(node);
        }
        return nodeString;
    }

    /**
     * Stats are always collected
     *
     * @return the iterator stats
     */
    public DocumentIteratorStats getStats() {
        return stats;
    }

}
