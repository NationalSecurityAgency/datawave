package datawave.next;

import java.util.Collections;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

/**
 * A document id iterator that supports simple equality terms
 */
public class DocIdIterator extends BaseDocIdIterator {

    private final static Logger log = LoggerFactory.getLogger(DocIdIterator.class);

    protected String value;

    /**
     * Default constructor
     *
     * @param source
     *            the source
     * @param row
     *            the row
     * @param node
     *            an EQ node
     */
    public DocIdIterator(SortedKeyValueIterator<Key,Value> source, String row, JexlNode node) {
        super(source, row, node);
        parseNode(node);
        this.prefix = "fi\0" + field;
        this.columnFamilies = Collections.singleton(new ArrayByteSequence(prefix));
    }

    /**
     * Parses the JexlNode, assumed to be a {@link ASTEQNode}.
     *
     * @param node
     *            a JexlNode
     */
    protected void parseNode(JexlNode node) {
        this.field = JexlASTHelper.getIdentifier(node);
        Preconditions.checkNotNull(field, "Expected field to not be null for node: " + JexlStringBuildingVisitor.buildQuery(node));

        Object literal = JexlASTHelper.getLiteralValue(node);
        Preconditions.checkNotNull(literal, "Expected value to not be null for node: " + JexlStringBuildingVisitor.buildQuery(node));
        this.value = String.valueOf(literal);
    }

    @Override
    public boolean hasNext() {
        if (!seenSeek) {
            handleFirstSeek();
        }

        while (tk == null && source.hasTop()) {
            Key top = source.getTopKey();
            parser.parse(top);

            // apply time and datatype filter
            if (!acceptDatatype(parser.getDatatype())) {
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

    /**
     * Handle the first seek. Need to build the scan range and seek the underlying source, logging all relevant information
     */
    protected void handleFirstSeek() {
        seenSeek = true;
        Range range = createScanRange();

        if (log.isDebugEnabled()) {
            log.debug("first seek for term: {}, suffix: {}, datatypes: {}, timeFilter: {}", getNode(), suffix, datatypeFilter, timeFilter);
            log.debug("seeking to range: {}", range);
        }

        safeSeek(range, true);

        if (!source.hasTop() && log.isDebugEnabled()) {
            log.debug("term: {} did not have a top key", getNode());
        }
    }

    @Override
    protected Range createScanRange() {
        if (minDatatypeUid != null && maxDatatypeUid != null) {
            // executing within the context of another scan
            Key start = new Key(row, prefix, value + '\u0000' + minDatatypeUid);
            Key stop = new Key(row, prefix, value + '\u0000' + maxDatatypeUid + '\u0000');
            return new Range(start, true, stop, true);
        }

        // if this scan is executing in the context of a document range we can build the full key
        if (suffix != null) {
            // need to limit the range based on the document range
            Key start = new Key(row, prefix, value + '\u0000' + suffix);
            Key stop = new Key(row, prefix, value + '\u0000' + suffix + '\uffff');
            return new Range(start, true, stop, false);
        }

        // if there is a datatype filter set we can bound the scan
        if (datatypeFilter != null && !datatypeFilter.isEmpty()) {
            Key start = new Key(row, prefix, value + '\u0000' + datatypeFilter.first());
            Key stop = new Key(row, prefix, value + '\u0000' + datatypeFilter.last() + '\uffff');
            return new Range(start, true, stop, false);
        }

        // otherwise build the full scan range for the field and value
        Key start = new Key(row, prefix, value + '\u0000');
        Key stop = new Key(row, prefix, value + '\u0000' + '\uffff');
        return new Range(start, true, stop, false);
    }

    /**
     * Determine if the datatype is accepted. This method will use the datatype filter, or the min max datatype
     *
     * @param datatype
     *            the datatype
     * @return true if the datatype is accepted
     */
    protected boolean acceptDatatype(String datatype) {
        if (datatypeFilter != null && !datatypeFilter.isEmpty() && !datatypeFilter.contains(datatype)) {
            return false;
        }

        if (minDatatype != null && maxDatatype != null) {
            return datatype.compareTo(minDatatype) >= 0 && datatype.compareTo(maxDatatype) <= 0;
        }

        return true;
    }

    @Override
    protected void handleDatatypeMiss() {
        stats.incrementDatatypeFilterMiss();
        if (datatypeFilter != null) {
            handleDatatypeFilterMiss(parser.getDatatype());
        } else {
            handleMinMaxDatatypeFilterMiss(parser.getDatatype());
        }
    }

    /**
     * Handle a datatype miss using the full set of requested datatypes
     *
     * @param datatype
     *            the current datatype
     */
    protected void handleDatatypeFilterMiss(String datatype) {
        String nextDatatype = datatypeFilter.higher(datatype);
        if (nextDatatype == null) {
            // rollover range?
            Key start = new Key(row, prefix, value + '\u0000' + '\uffff');
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

    /**
     * Handle a datatype miss using just a minimum and maximum datatype
     *
     * @param datatype
     *            the current datatype
     */
    protected void handleMinMaxDatatypeFilterMiss(String datatype) {
        if (datatype.compareTo(minDatatype) < 0) {
            // seek to minimum
            Key start = new Key(row, prefix, value + '\u0000' + minDatatype + '\u0000');
            Key stop = new Key(row, prefix, value + '\u0000' + '\uffff');
            Range range = new Range(start, false, stop, true);
            safeSeek(range, true);
        } else if (datatype.compareTo(maxDatatype) > 0) {
            // rollover seek. For the EQ case generate a rollover range which causes hasNext() to be false
            Key start = new Key(row, prefix, value + '\u0000' + maxDatatype + '\uffff');
            Key stop = new Key(row, prefix, value + '\u0000' + '\uffff' + '\uffff');
            Range range = new Range(start, false, stop, true);
            safeSeek(range, true);
        }
    }
}
