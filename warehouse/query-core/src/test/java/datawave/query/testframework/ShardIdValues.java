package datawave.query.testframework;

import org.junit.Assert;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Defines valid dates that are used for shard ids for a specific test. All shard id dates for the test data must be within the range of the earliest and latest
 * dates specified within the shard id date ranges. Only one shard id date is required, however, additional shard id dates will be used to determine the
 * start/end range and a random date range.
 */
public class ShardIdValues {
    // list of shards for testing
    private static final Random rVal = new Random(System.currentTimeMillis());
    
    /**
     * Converts the shard id date into a {@link Date} object.
     * 
     * @param shard
     *            shard date
     * @return Date representation of the shard date
     */
    public static Date convertShardToDate(final String shard) {
        try {
            return AbstractDataTypeConfig.YMD_DateFormat.parse(shard);
        } catch (ParseException pe) {
            throw new AssertionError("invalid date string(" + shard + ")");
        }
    }
    
    private final Map<String,Date> shardIds;
    private final List<Date> sortedDate = new ArrayList<>();
    
    /**
     *
     * @param shardDate
     *            list of valid shard id date entries
     */
    public ShardIdValues(final List<String> shardDate) {
        Assert.assertFalse("there must be at least one shard id value", shardDate.isEmpty());
        this.shardIds = new HashMap<>();
        for (String shard : shardDate) {
            Date date = convertShardToDate(shard);
            this.shardIds.put(shard, date);
        }
        
        this.sortedDate.addAll(this.shardIds.values());
        Collections.sort(sortedDate);
    }
    
    /**
     * Returns a list of valid shard dates that are within the start/end dates inclusive.
     *
     * @param start
     *            start date for inclusion
     * @param end
     *            end date for inclusion
     * @return set of shard date strings
     */
    public Set<String> getShardRange(final Date start, final Date end) {
        final Set<String> shards = new HashSet<>();
        for (final Map.Entry<String,Date> entry : this.shardIds.entrySet()) {
            if (0 >= start.compareTo(entry.getValue()) && 0 <= end.compareTo(entry.getValue())) {
                shards.add(entry.getKey());
            }
        }
        
        return shards;
    }
    
    /**
     * Retrieves the start and end date for the enumerated values.
     *
     * @param random
     *            When true, return a random date range that may start/end within the provided values.
     * @return array containing the start and end date objects
     */
    public Date[] getStartEndDates(final boolean random) {
        Date[] startEndDate = new Date[2];
        if (random) {
            int s = rVal.nextInt(sortedDate.size());
            startEndDate[0] = sortedDate.get(s);
            int remaining = sortedDate.size() - s;
            startEndDate[1] = startEndDate[0];
            if (0 < remaining) {
                int e = rVal.nextInt(sortedDate.size() - s);
                startEndDate[1] = sortedDate.get(s + e);
            }
        } else {
            // retrieve first and last values
            startEndDate[0] = sortedDate.get(0);
            startEndDate[1] = sortedDate.get(sortedDate.size() - 1);
        }
        return startEndDate;
    }
}
