package datawave.next;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.JexlNode;

import com.google.common.base.Preconditions;

import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;

/**
 * A document id iterator that supports bounded ranges
 */
public class RangeDocIdIterator extends DocIdIterator {

    private LiteralRange<?> range;
    private String lowerBound;
    private String upperBound;

    /**
     * Constructor that accepts a bounded range ({@link ASTAndNode})
     *
     * @param source
     *            the source
     * @param row
     *            the row
     * @param node
     *            the JexlNode
     */
    public RangeDocIdIterator(SortedKeyValueIterator<Key,Value> source, String row, JexlNode node) {
        super(source, row, node);
    }

    /**
     * Node must be a bounded range
     *
     * @param node
     *            a JexlNode
     */
    protected void parseNode(JexlNode node) {
        LiteralRange<?> range = JexlASTHelper.findRange().getRange(node);
        Preconditions.checkNotNull(range, "Expected to find a range term");

        this.range = range;
        this.field = range.getFieldName();
        this.lowerBound = range.getLower().toString();
        this.upperBound = range.getUpper().toString();
    }

    /**
     * The scan range for a bounded range is build using the upper and lower bounds
     *
     * @return the scan range
     */
    @Override
    protected Range createScanRange() {
        Key start = createLowerBoundKey();
        Key stop = createUpperBoundKey();
        return new Range(start, range.isLowerInclusive(), stop, range.isUpperInclusive());
    }

    private Key createLowerBoundKey() {
        StringBuilder sb = new StringBuilder();
        sb.append(lowerBound);
        if (!range.isLowerInclusive()) {
            sb.append("\u0001");
        }
        sb.append("\u0000");
        return new Key(row, prefix, sb.toString());
    }

    private Key createUpperBoundKey() {
        StringBuilder sb = new StringBuilder();
        if (range.isUpperInclusive()) {
            sb.append(upperBound);
            sb.append("\u0001");
        } else {
            // need to walk it back one byte
            String upper = range.getUpper().toString();
            sb.append(upper.substring(0, upper.length() - 1));
            sb.append((char) (upper.charAt(upper.length() - 1) - 1));
            sb.append(Constants.MAX_UNICODE_STRING);
        }
        return new Key(row, prefix, sb.toString());
    }

    /**
     * Scan range for bounded ranges handle datatype misses a little differently
     */
    protected void handleDatatypeMiss() {
        stats.incrementDatatypeFilterMiss();
        String nextDatatype = datatypeFilter.higher(parser.getDatatype());

        if (nextDatatype == null) {
            // rollover range?
            Key start = new Key(row, prefix, parser.getValue() + '\u0000' + '\uffff');
            Key stop = new Key(row, prefix, upperBound + '\u0000' + '\uffff' + '\uffff');
            Range range = new Range(start, false, stop, true);
            safeSeek(range, false);
            return; // no further datatypes exist, so we're done
        }

        // otherwise seek to the next datatype and continue iterating
        Key start = new Key(row, prefix, parser.getValue() + '\u0000' + nextDatatype);
        Key stop = new Key(row, prefix, upperBound + '\u0000' + '\uffff');
        Range range = new Range(start, false, stop, true);
        safeSeek(range, false);
    }
}
