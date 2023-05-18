package datawave.query.predicate;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.math.LongRange;
import com.google.common.base.Predicate;

/**
 * Excludes documents which do not fall within the given time range
 */
public class TimeFilter {
    
    protected final LongRange acceptedRange;
    protected final KeyTimeFilter keyTimeFilter;
    protected final KeyValueTimeFilter keyValueTimeFilter;
    
    private static final TimeFilter ALWAYS_TRUE = new AlwaysTrueTimeFilter();
    
    public static TimeFilter alwaysTrue() {
        return ALWAYS_TRUE;
    }

    public TimeFilter() {
        acceptedRange = null;
        keyTimeFilter = null;
        keyValueTimeFilter = null;
    }
    
    public TimeFilter(long start, long end) {
        acceptedRange = new LongRange(start, end);
        keyTimeFilter = new KeyTimeFilter();
        keyValueTimeFilter = new KeyValueTimeFilter();
    }

    public boolean apply(Key input) {
        return keyTimeFilter.apply(input);
    }

    public boolean apply(Entry<Key,Value> input) {
        return keyValueTimeFilter.apply(input);
    }

    public Predicate<Key> getKeyTimeFilter() {
        return keyTimeFilter;
    }

    public Predicate<Entry<Key,Value>> getKeyValueTimeFilter() {
        return keyValueTimeFilter;
    }

    private static class AlwaysTrueTimeFilter extends TimeFilter {

        public AlwaysTrueTimeFilter() {
            //  default constructor
        }
        @Override
        public boolean apply(Key k) {
            return true;
        }

        @Override
        public boolean apply(Entry<Key,Value> input) {
            return true;
        }
    }

    private class KeyTimeFilter implements Predicate<Key> {
        @Override
        public boolean apply(Key input) {
            final long timestamp = input.getTimestamp();
            
            return acceptedRange.containsLong(timestamp);
        }
    }
    
    private class KeyValueTimeFilter implements Predicate<Entry<Key,Value>> {
        public boolean apply(Entry<Key,Value> entry) {
            return keyTimeFilter.apply(entry.getKey());
        }
    }

}
