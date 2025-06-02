package datawave.query.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;

import com.google.common.base.Splitter;

import datawave.util.TableName;

/**
 * An iterator optimized for the {@link TableName#DATE_INDEX}
 * <p>
 * Both {@link DateIndexHelper#getTypeDescription(String, Date, Date, Set)} and
 * {@link DateIndexHelper#getShardsAndDaysHint(String, Date, Date, Date, Date, Set)} support a datatype filter.
 * <p>
 * Filtering based on the start/end date is done via the {@link #MINIMUM_DATE} amd {@link #MAXIMUM_DATE} options.
 * <p>
 * Filtering based on the field is done via the {@link #FIELD} option.
 * <p>
 * If {@link #TIME_TRAVEL_ENABLED} is set, then the shards and days hint will include shards prior to the query start date.
 */
public class DateIndexIterator implements SortedKeyValueIterator<Key,Value> {

    public static final String DATATYPE_FILTER = "datatypes";
    public static final String MINIMUM_DATE = "min.date";
    public static final String MAXIMUM_DATE = "max.date";
    public static final String FIELD = "field";
    public static final String TIME_TRAVEL_ENABLED = "time.travel.enabled";

    private static final Splitter splitter = Splitter.on(',');

    private static final String TIME_TRAVEL_FIELD = "ACTIVITY";

    private Set<String> datatypes;
    private String minDate;
    private String maxDate;
    private String field;
    private boolean timeTravelEnabled = false;

    private SortedKeyValueIterator<Key,Value> source;

    private Range range;
    private Collection<ByteSequence> columnFamilies;
    private boolean inclusive;

    private Key tk;
    private Value tv;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;

        if (options.containsKey(DATATYPE_FILTER)) {
            String option = options.get(DATATYPE_FILTER);
            datatypes = new HashSet<>(splitter.splitToList(option));
        }

        if (options.containsKey(MINIMUM_DATE)) {
            minDate = options.get(MINIMUM_DATE);
        }

        if (options.containsKey(MAXIMUM_DATE)) {
            maxDate = options.get(MAXIMUM_DATE);
        }

        if (options.containsKey(FIELD)) {
            field = options.get(FIELD);
        }

        if (options.containsKey(TIME_TRAVEL_ENABLED)) {
            timeTravelEnabled = Boolean.parseBoolean(options.get(TIME_TRAVEL_ENABLED));
        }
    }

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public void next() throws IOException {
        tk = null;
        tv = null;
        while (source.hasTop() && tk == null) {
            tk = source.getTopKey();
            tv = source.getTopValue();

            // CQ is date + NULL + datatype + NULL + field
            String[] parts = StringUtils.split(tk.getColumnQualifier().toString(), '\u0000');
            if (datatypes != null && !datatypes.isEmpty() && !datatypes.contains(parts[1])) {
                tk = null;
                tv = null;
                // probably not worth it to seek, simply advance the iterator
                source.next();
                continue;
            }

            if (minDate != null && parts[0].compareTo(minDate) < 0) {
                seekToMinimumDate(tk, minDate);
                tk = null;
                tv = null;
                continue;
            }

            if (maxDate != null && parts[0].compareTo(maxDate) > 0) {
                seekToNextColumnFamily(tk);
                tk = null;
                tv = null;
                continue;
            }

            if (field != null && !parts[2].equals(field)) {
                tk = null;
                tv = null;
                source.next();
                continue;
            }

            // If the event date is more than one day before the event actually happened,
            // then skip it, unless time-travel has been enabled.
            if (!timeTravelEnabled && tk.getColumnFamily().toString().equals(TIME_TRAVEL_FIELD)) {
                String row = getRowWithoutShard(tk);
                if (parts[0].compareTo(row) < 0) {
                    tk = null;
                    tv = null;
                    source.next();
                    continue;
                }
            }

            source.next();
        }
    }

    /**
     * Get the row, removing the sharded portion if necessary.
     *
     * @param key
     *            the key
     * @return the row
     */
    private String getRowWithoutShard(Key key) {
        String row = key.getRow().toString();
        int index = row.indexOf('_');
        if (index > 0) {
            row = row.substring(0, index);
        }
        return row;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.range = range;
        this.columnFamilies = columnFamilies;
        this.inclusive = inclusive;

        this.source.seek(this.range, this.columnFamilies, this.inclusive);
        next();
    }

    private void seekToMinimumDate(Key key, String date) throws IOException {
        Text cq = new Text(date + '\u0000');
        Key start = new Key(key.getRow(), key.getColumnFamily(), cq);

        Range seekRange = new Range(start, false, range.getEndKey(), range.isEndKeyInclusive());
        source.seek(seekRange, columnFamilies, inclusive);
    }

    private void seekToNextColumnFamily(Key key) throws IOException {
        Key start = key.followingKey(PartialKey.ROW_COLFAM);
        Range seekRange = new Range(start, false, range.getEndKey(), range.isEndKeyInclusive());
        source.seek(seekRange, columnFamilies, inclusive);
    }

    @Override
    public Key getTopKey() {
        return tk;
    }

    @Override
    public Value getTopValue() {
        return tv;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        DateIndexIterator iterator = new DateIndexIterator();
        iterator.source = this.source.deepCopy(env);
        return iterator;
    }
}
