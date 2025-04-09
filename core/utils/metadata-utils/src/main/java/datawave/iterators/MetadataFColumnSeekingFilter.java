package datawave.iterators;

import java.io.IOException;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
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

import datawave.data.ColumnFamilyConstants;

/**
 * A {@link SeekingFilter} that operates on the metadata table's {@link ColumnFamilyConstants#COLF_F} column.
 * <p>
 * This filter solves the problem of calculating field cardinality for a small date range on a system that contains many days worth of data, i.e., it is not
 * practical to simply filter based on date and/or datatype.
 * <p>
 * Given that the F column is simply the datatype and date concatenated with a null byte, it is easy to calculate a seek range that limits the time spent
 * iterating across useless keys.
 */
public class MetadataFColumnSeekingFilter extends SeekingFilter implements OptionDescriber {
    
    private static final Logger log = LoggerFactory.getLogger(MetadataFColumnSeekingFilter.class);
    
    public static final String DATATYPES_OPT = "datatypes";
    public static final String START_DATE = "start.date";
    public static final String END_DATE = "end.date";
    
    private TreeSet<String> datatypes;
    private String startDate;
    private String endDate;
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (!validateOptions(options)) {
            throw new IllegalArgumentException("Iterator not configured with correct options");
        }
        
        String opt = options.get(DATATYPES_OPT);
        if (StringUtils.isBlank(opt)) {
            datatypes = new TreeSet<>();
        } else {
            datatypes = new TreeSet<>(Splitter.on(',').splitToList(opt));
        }
        
        startDate = options.get(START_DATE);
        endDate = options.get(END_DATE);
        
        super.init(source, options, env);
    }
    
    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions opts = new IteratorOptions(getClass().getName(), "Filter keys by datatype and date range", null, null);
        opts.addNamedOption(DATATYPES_OPT, "The set of datatypes used as a filter");
        opts.addNamedOption(START_DATE, "The start date, used for seeking");
        opts.addNamedOption(END_DATE, "The end date, used for seeking");
        return null;
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        return options.containsKey(DATATYPES_OPT) && options.containsKey(START_DATE) && options.containsKey(END_DATE);
    }
    
    /**
     * A key is filtered if one of the following three conditions is met. Otherwise, the source will call next.
     * <ol>
     * <li>datatype miss</li>
     * <li>key date is before the start date</li>
     * <li>key date is after the end date</li>
     * </ol>
     * 
     * @param k
     *            a key
     * @param v
     *            a value
     * @return a {@link FilterResult}
     */
    @Override
    public FilterResult filter(Key k, Value v) {
        if (log.isTraceEnabled()) {
            log.trace("filter key: {}", k.toStringNoTime());
        }
        String cq = k.getColumnQualifier().toString();
        int index = cq.indexOf('\u0000');
        String datatype = cq.substring(0, index);
        if (!datatypes.isEmpty() && !datatypes.contains(datatype)) {
            return new FilterResult(false, AdvanceResult.USE_HINT);
        }
        
        String date = cq.substring(index + 1);
        if (date.compareTo(startDate) < 0) {
            return new FilterResult(false, AdvanceResult.USE_HINT);
        }
        
        if (date.compareTo(endDate) > 0) {
            return new FilterResult(false, AdvanceResult.USE_HINT);
        }
        
        return new FilterResult(true, AdvanceResult.NEXT);
    }
    
    @Override
    public Key getNextKeyHint(Key k, Value v) {
        if (log.isTraceEnabled()) {
            log.trace("get next hint for key: {}", k.toStringNoTime());
        }
        
        Key hint;
        String cq = k.getColumnQualifier().toString();
        int index = cq.indexOf('\u0000');
        String datatype = cq.substring(0, index);
        
        if (!datatypes.isEmpty() && !datatypes.contains(datatype)) {
            hint = getSeekToNextDatatypeKey(k, datatype);
        } else {
            String date = cq.substring(index + 1);
            if (date.compareTo(startDate) < 0) {
                hint = getSeekToStartDateKey(k, datatype);
            } else if (date.compareTo(endDate) > 0) {
                hint = getDatatypeRolloverKey(k, datatype);
            } else {
                hint = k.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
            }
        }
        
        log.trace("hint: {}", hint);
        return hint;
    }
    
    private Key getSeekToNextDatatypeKey(Key key, String datatype) {
        if (datatypes.isEmpty()) {
            // no datatypes provided, so we must instead produce a 'rollover' start key
            return getDatatypeRolloverKey(key, datatype);
        }
        
        // otherwise datatypes were provided
        String nextDatatype = datatypes.higher(datatype);
        if (nextDatatype != null) {
            log.trace("seek to next datatype");
            Text nextColumnQualifier = new Text(nextDatatype + '\u0000' + startDate);
            return new Key(key.getRow(), key.getColumnFamily(), nextColumnQualifier);
        } else {
            log.trace("seek to next ROW_COLFAM");
            // out of datatypes, we're done. This partial range will trigger a "beyond source" condition
            return key.followingKey(PartialKey.ROW_COLFAM);
        }
    }
    
    private Key getDatatypeRolloverKey(Key key, String datatype) {
        log.trace("seek to rollover datatype");
        Text cq = new Text(datatype + '\u0000' + '\uffff');
        return new Key(key.getRow(), key.getColumnFamily(), cq);
    }
    
    private Key getSeekToStartDateKey(Key k, String datatype) {
        log.trace("seek to start date");
        Text cq = new Text(datatype + '\u0000' + startDate);
        return new Key(k.getRow(), k.getColumnFamily(), cq);
    }
}
