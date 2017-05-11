package datawave.query.tables.async;

import java.util.Collection;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

public class RangeDefinition {
    public static boolean isDocSpecific(Range range) {
        Key startKey = range.getStartKey();
        // 1) it's not a shard, meaning that we don't have a shard specific
        // range or doc specific range
        // 2) cf is empty meaning we have a day range or shard specific
        // range
        if (!isShard(startKey.getRow().toString()) || startKey.getColumnFamilyData().length() == 0) {
            return false;
        }
        
        return true;
    }
    
    public static boolean allDocSpecific(Collection<Range> ranges) {
        for (Range range : ranges) {
            if (range.isInfiniteStartKey() || range.isInfiniteStopKey())
                return false;
            Key startKey = range.getStartKey();
            // 1) it's not a shard, meaning that we don't have a shard specific range or doc specific range
            // 2) cf is empty meaning we have a day range or shard specific range
            if (!isShard(startKey.getRow().toString()) || startKey.getColumnFamilyData().length() == 0) {
                return false;
            }
        }
        return true;
    }
    
    public static boolean isDay(String dayOrShard) {
        return (dayOrShard.indexOf('_') < 0);
    }
    
    public static boolean isShard(String dayOrShard) {
        return !isDay(dayOrShard);
    }
}
