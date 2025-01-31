package datawave.query.iterator.filter;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.codec.binary.Base64;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.core.iterators.ColumnRangeIterator;
import datawave.edge.util.EdgeValue;
import datawave.util.CompositeTimestamp;
import datawave.util.time.DateHelper;

/**
 *
 */
public class LoadDateFilter extends DateTypeFilter {
    protected Range columnRange = null;

    private static Range decodeRange(String e) throws IOException {
        ByteArrayInputStream b = new ByteArrayInputStream(Base64.decodeBase64(e.getBytes()));
        DataInputStream in = new DataInputStream(b);
        Range range = new Range();
        try {
            range.readFields(in);
        } finally {
            in.close();
            b.close();
        }

        return range;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        LoadDateFilter result = (LoadDateFilter) super.deepCopy(env);
        result.columnRange = columnRange;

        return result;
    }

    /**
     * Method to setup the jexl query expression from the iterator options for evaluation.
     *
     * @param options
     *            map of options
     * @throws IOException
     *             for issues with read/write
     */
    private void initOptions(Map<String,String> options) throws IOException {

        String e = options.get(ColumnRangeIterator.RANGE_NAME);
        if (e == null) {
            throw new IllegalArgumentException("LOAD_DATE_RANGE_NAME must be set");
        }
        columnRange = decodeRange(e);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws java.io.IOException {
        super.init(source, options, env);
        initOptions(options);
    }

    /**
     * Determines if the edge key satisfies the conditions expressed in the supplied JEXL query string.
     *
     * @param k
     *            key
     * @param V
     *            value
     * @return boolean - true if it is a match.
     */
    @Override
    public boolean accept(Key k, Value V) {
        String loadDate;

        if (!super.accept(k, V)) {
            return false;
        }

        // now, filter on load date range
        try {
            EdgeValue value = EdgeValue.decode(V);

            if (value.hasLoadDate()) {
                loadDate = value.getLoadDate();
            } else {
                loadDate = extractEventDate(k);
            }
        } catch (InvalidProtocolBufferException e) {
            loadDate = extractEventDate(k);
        }

        Key dateKey = new Key(loadDate);

        if (columnRange.beforeStartKey(dateKey) || columnRange.afterEndKey(dateKey)) {
            return false;
        } else {
            return true;
        }
    }

    // @note old-style edges always used event date in the column qualifier.
    // However, we now have edges that may contain the activity date in the
    // column qualifier. Even though we know that any edge without a load date
    // must be an edge with event date in the column qualifier (since the
    // activity date change came after the load date change) we have the issue
    // of dealing with the InvalidProtocolBufferException case above. That case
    // can't assume an event date edge. Therefore, we now extract the
    // event date from the timestamp value rather than the column qualifier.
    private String extractEventDate(Key k) {

        return DateHelper.format(CompositeTimestamp.getEventDate(k.getTimestamp()));
    }
}
