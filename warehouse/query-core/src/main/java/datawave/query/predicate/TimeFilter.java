package datawave.query.predicate;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.math.LongRange;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;

import datawave.util.CompositeTimestamp;

/**
 * Excludes documents which do not fall within the given time range
 */
public class TimeFilter {
    private static final Logger log = Logger.getLogger(TimeFilter.class);

    protected final LongRange acceptedRange;
    protected final KeyTimeFilter keyTimeFilter;
    protected final KeyValueTimeFilter keyValueTimeFilter;

    private static TimeFilter ALWAYS_TRUE = new TimeFilter(Long.MIN_VALUE, Long.MAX_VALUE);

    public static TimeFilter alwaysTrue() {
        return ALWAYS_TRUE;
    }

    public TimeFilter(long start, long end) {
        acceptedRange = new LongRange(start, end);
        keyTimeFilter = new KeyTimeFilter();
        keyValueTimeFilter = new KeyValueTimeFilter();
    }

    private class KeyTimeFilter implements Predicate<Key> {
        @Override
        public boolean apply(Key input) {
            final long timestamp = input.getTimestamp();

            return acceptedRange.containsLong(CompositeTimestamp.getEventDate(timestamp));
        }
    }

    private class KeyValueTimeFilter implements Predicate<Entry<Key,Value>> {
        public boolean apply(Entry<Key,Value> entry) {
            return keyTimeFilter.apply(entry.getKey());
        }
    }

    public boolean apply(Key input) {
        return keyTimeFilter.apply(input);
    }

    public Predicate<Key> getKeyTimeFilter() {
        return keyTimeFilter;
    }

    public boolean apply(Entry<Key,Value> input) {
        return keyValueTimeFilter.apply(input);
    }

    public Predicate<Entry<Key,Value>> getKeyValueTimeFilter() {
        return keyValueTimeFilter;
    }
}
