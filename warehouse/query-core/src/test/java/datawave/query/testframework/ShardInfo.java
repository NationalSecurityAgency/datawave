package datawave.query.testframework;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Handles retrieval of shard ids for a datatype.
 */
class ShardInfo {
    private static final Random rVal = new Random(System.currentTimeMillis());
    
    private List<Date> shards;
    
    ShardInfo(Collection<Date> shardIds) {
        this.shards = new ArrayList<>(shardIds);
        Collections.sort(this.shards);
    }
    
    Date[] getStartEndDates(final boolean random) {
        Date[] startEndDate = new Date[2];
        if (random) {
            int s = rVal.nextInt(this.shards.size());
            startEndDate[0] = this.shards.get(s);
            int remaining = this.shards.size() - s;
            startEndDate[1] = startEndDate[0];
            if (0 < remaining) {
                int e = rVal.nextInt(this.shards.size() - s);
                startEndDate[1] = this.shards.get(s + e);
            }
        } else {
            startEndDate[0] = this.shards.get(0);
            startEndDate[1] = this.shards.get(this.shards.size() - 1);
        }
        return startEndDate;
    }
}
