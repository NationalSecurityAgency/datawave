package datawave.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.SeekingFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

import datawave.query.Constants;
import datawave.query.jexl.LiteralRange;

/**
 * A {@link SeekingFilter} that attempts to expand bounded ranges using the global index
 * <p>
 * The caller is responsible for fetching the appropriate column families. The range is constructed from a {@link LiteralRange}.
 * <p>
 * The only thing this iterator does is advance through datatypes if a filter is supplied, advance to the start date, and advance to the next row within the
 * range.
 */
public class BoundedRangeExpansionIterator extends SeekingFilter implements OptionDescriber {

    private static final Logger log = LoggerFactory.getLogger(BoundedRangeExpansionIterator.class);

    public static final String START_DATE = "start.date";
    public static final String END_DATE = "end.date";
    public static final String DATATYPES_OPT = "dts";

    private TreeSet<String> datatypes;
    private String startDate;
    private String endDate;

    private Text prevRow;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (!validateOptions(options)) {
            throw new IllegalArgumentException("BoundedRangeExpansionIterator not configured with correct options");
        }

        String opt = options.get(DATATYPES_OPT);
        if (StringUtils.isBlank(opt)) {
            datatypes = new TreeSet<>();
        } else {
            datatypes = new TreeSet<>(Splitter.on(',').splitToList(opt));
        }

        startDate = options.get(START_DATE);
        endDate = options.get(END_DATE) + Constants.MAX_UNICODE_STRING;

        super.init(source, options, env);
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions opts = new IteratorOptions(getClass().getName(), "Expands bounded ranges using the global index", null, null);
        opts.addNamedOption(START_DATE, "The start date");
        opts.addNamedOption(END_DATE, "The end date");
        opts.addNamedOption(DATATYPES_OPT, "The set of datatypes used to filter keys (optional)");
        return opts;
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        return options.containsKey(START_DATE) && options.containsKey(END_DATE);
    }

    @Override
    public FilterResult filter(Key k, Value v) {
        log.trace("filter key: {}", k.toStringNoTime());

        // shard + null + datatype
        String cq = k.getColumnQualifier().toString();
        int index = cq.indexOf('\u0000');
        String date = cq.substring(0, index);

        if (date.compareTo(startDate) < 0) {
            log.trace("{} is before the start date {}, advancing to start date", date, startDate);
            return new FilterResult(false, AdvanceResult.USE_HINT);
        }

        if (date.compareTo(endDate) > 0) {
            log.trace("{} is past the end date {}, advancing to next row", date, endDate);
            return new FilterResult(false, AdvanceResult.NEXT_ROW);
        }

        String datatype = cq.substring(index + 1);
        if (!datatypes.isEmpty() && !datatypes.contains(datatype)) {
            log.trace("datatype {} was filtered out, advancing to next key", datatype);
            return new FilterResult(false, AdvanceResult.NEXT);
        }

        if (prevRow != null && prevRow.equals(k.getRow())) {
            // this iterator should only return a single key per unique row, thus the previous row should never match the current row.
            log.warn("should never see a duplicate row -- skip to next row");
            return new FilterResult(false, AdvanceResult.NEXT_ROW);
        }

        prevRow = k.getRow();
        return new FilterResult(true, AdvanceResult.NEXT_ROW);
    }

    /**
     * Hint is only used to seek to the start date
     *
     * @param k
     *            a key
     * @param v
     *            a value
     * @return the key used to seek
     */
    @Override
    public Key getNextKeyHint(Key k, Value v) {
        log.trace("get next key hint: {}", k.toStringNoTime());

        // shard + null + datatype
        String cq = k.getColumnQualifier().toString();
        int index = cq.indexOf('\u0000');
        String date = cq.substring(0, index);

        if (date.compareTo(startDate) < 0) {
            Text columnQualifier;

            if (datatypes.isEmpty()) {
                log.trace("seek to start date");
                columnQualifier = new Text(startDate + '\u0000');
            } else {
                log.trace("seek to start date and datatype");
                columnQualifier = new Text(startDate + '\u0000' + datatypes.first());
            }

            return new Key(k.getRow(), k.getColumnFamily(), columnQualifier);
        }

        log.trace("next hint key was called in a bad state, reverting to no-op");
        return k;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (!range.isStartKeyInclusive()) {
            // need to skip to next row
            Key skip = new Key(range.getStartKey().getRow().toString() + '\u0000');
            if (skip.compareTo(range.getEndKey()) > 0) {
                // handles case of bounded range against single value
                // filter key: +cE1 NUM:20150808_0%00;generic [NA]
                // skip key would be +cE1<null> but then the start key is greater than the end key. so we cheat accumulo.
                Range skipRange = new Range(range.getEndKey(), true, range.getEndKey(), range.isEndKeyInclusive());
                super.seek(skipRange, columnFamilies, inclusive);
            } else {
                Range skipRange = new Range(skip, true, range.getEndKey(), range.isEndKeyInclusive());
                super.seek(skipRange, columnFamilies, inclusive);
            }
        } else {
            super.seek(range, columnFamilies, inclusive);
        }
    }
}
