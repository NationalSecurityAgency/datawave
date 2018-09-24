package datawave.query.testframework;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Common list of dates that can be used as a default range for shard ids. The shard dates should provide complete coverage for the test data. At a minimum,only
 * one shard date is required. Adding more dates between the start and end date allows more flexibility for generating random shard date ranges.
 *
 * @see ShardIdValues
 */
public enum BaseShardIdRange {
    
    // list of shards for testing
    DATE_2015_0404("20150404"),
    DATE_2015_0505("20150505"),
    DATE_2015_0606("20150606"),
    DATE_2015_0707("20150707"),
    DATE_2015_0808("20150808"),
    DATE_2015_0909("20150909"),
    DATE_2015_1010("20151010"),
    DATE_2015_1111("20151111");
    
    private final String shardId;
    
    BaseShardIdRange(final String id) {
        this.shardId = id;
    }
    
    private static final List<String> SHARD_IDS = Stream.of(BaseShardIdRange.values()).map(e -> e.shardId).collect(Collectors.toList());
    
    public static List<String> getShardDates() {
        return SHARD_IDS;
    }
    
    /**
     * Returns the shard id date.
     * 
     * @return shard date
     */
    public String getDateStr() {
        return this.shardId;
    }
}
