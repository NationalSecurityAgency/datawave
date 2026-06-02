package datawave.next;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.JexlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.core.iterators.DatawaveFieldIndexListIteratorJexl;
import datawave.query.Constants;
import datawave.query.jexl.nodes.ExceededOr;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType;

/**
 * An alternate implementation of the {@link DatawaveFieldIndexListIteratorJexl}
 * <p>
 * Sorted order is not guaranteed.
 */
public class ListDocIdIterator extends DocIdIterator {

    private static final Logger log = LoggerFactory.getLogger(ListDocIdIterator.class);

    private List<String> values;
    private List<Range> ranges;
    private Range currentRange = null;

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
    public ListDocIdIterator(SortedKeyValueIterator<Key,Value> source, String row, JexlNode node) {
        super(source, row, node);
    }

    @Override
    protected void parseNode(JexlNode node) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        Preconditions.checkArgument(instance.getType().equals(MarkerType.EXCEEDED_OR));

        ExceededOr exceededOr = new ExceededOr(instance.getSource());
        this.field = exceededOr.getField();
        this.values = new ArrayList<>();
        this.values.addAll(exceededOr.getParams().getValues());
    }

    private void buildRanges() {
        this.ranges = new ArrayList<>();
        Collections.sort(values);
        for (String value : values) {
            if (datatypeFilter != null && !datatypeFilter.isEmpty()) {
                Key start = new Key(this.row, this.prefix, value + "\u0000" + datatypeFilter.first() + "\u0000");
                Key end = new Key(this.row, this.prefix, value + "\u0000" + datatypeFilter.last() + "\u0000" + Constants.MAX_UNICODE_STRING);
                ranges.add(new Range(start, true, end, false));
            } else {
                Key start = new Key(this.row, this.prefix, value + "\u0000");
                Key end = new Key(this.row, this.prefix, value + "\u0000" + Constants.MAX_UNICODE_STRING);
                ranges.add(new Range(start, true, end, false));
            }
        }
        Collections.sort(ranges);
        Preconditions.checkState(!ranges.isEmpty(), "Range list should not be empty");
    }

    @Override
    protected Range createScanRange() {
        throw new IllegalStateException("ListDocIdIterator does not implement createScanRange()");
    }

    @Override
    public boolean hasNext() {
        if (!seenSeek) {
            handleFirstSeek();
        }

        while (tk == null) {

            advanceRangeIfNecessary();
            if (currentRange == null) {
                break;
            }

            Key top = source.getTopKey();
            parser.parse(top);

            // apply time and datatype filter
            if (datatypeFilter != null && !datatypeFilter.isEmpty() && !datatypeFilter.contains(parser.getDatatype())) {
                handleDatatypeMiss();
                continue;
            }

            // ensure key is within the requested date range
            if (timeFilter != null && !timeFilter.contains(top.getTimestamp())) {
                stats.incrementTimeFilterMiss();
                safeNext();
                continue;
            }

            tk = fiToEvent(top);

            safeNext();
        }

        if (tk == null) {
            // iterator exhausted, log stats
            if (log.isDebugEnabled()) {
                log.debug("stats for term: [{}]: {}", getNode(), stats);
            }
        }

        return tk != null;
    }

    @Override
    protected void handleFirstSeek() {
        seenSeek = true;
        buildRanges();
        advanceRangeIfNecessary();
    }

    private void advanceRangeIfNecessary() {

        if (currentRange != null && !source.hasTop()) {
            currentRange = null;
        }

        while (currentRange == null) {
            if (ranges.isEmpty()) {
                break;
            }

            currentRange = ranges.remove(0);
            safeSeek(currentRange, true);

            if (!source.hasTop()) {
                currentRange = null;
            }
        }
    }

    @Override
    protected void handleDatatypeMiss() {
        stats.incrementDatatypeFilterMiss();
        String nextDatatype = datatypeFilter.higher(parser.getDatatype());

        parser.parse(currentRange.getStartKey());

        if (nextDatatype == null) {
            // rollover range?
            Key start = new Key(row, prefix, parser.getValue() + "\0");
            Key stop = new Key(row, prefix, value + '\u0000' + '\uffff' + '\uffff');
            Range range = new Range(start, false, stop, true);
            safeSeek(range, true);
            return; // no further datatypes exist, so we're done
        }

        // otherwise seek to the next datatype and continue iterating
        Key start = new Key(row, prefix, value + '\u0000' + nextDatatype);
        Key stop = new Key(row, prefix, value + '\u0000' + '\uffff');
        Range range = new Range(start, false, stop, true);
        safeSeek(range, true);
    }
}
