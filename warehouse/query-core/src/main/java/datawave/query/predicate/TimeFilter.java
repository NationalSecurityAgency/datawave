package datawave.query.predicate;

import com.google.common.base.Predicate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.Range;
import org.apache.log4j.Logger;

import java.util.Map.Entry;

/**
 * Excludes documents which do not fall within the given time range
 */
public class TimeFilter {
    private static final Logger log = Logger.getLogger(TimeFilter.class);
    
    protected final Range<Long> acceptedRange;
    protected final KeyTimeFilter keyTimeFilter;
    protected final KeyValueTimeFilter keyValueTimeFilter;
    
    private static TimeFilter ALWAYS_TRUE = new TimeFilter(Long.MIN_VALUE, Long.MAX_VALUE);
    
    public static TimeFilter alwaysTrue() {
        return ALWAYS_TRUE;
    }
    
    public TimeFilter(long start, long end) {
        acceptedRange = Range.between(start, end);
        keyTimeFilter = new KeyTimeFilter();
        keyValueTimeFilter = new KeyValueTimeFilter();
    }
    
    private class KeyTimeFilter implements Predicate<Key> {
        @Override
        public boolean apply(Key input) {
            final long timestamp = input.getTimestamp();
            
            return acceptedRange.contains(timestamp);
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
