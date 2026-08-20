package datawave.test.framework.util;

import com.google.common.base.Preconditions;

/**
 * Builds shard row ids in the form {@code YYYYMMDD_SHARD}, where the shard is derived from an event id.
 */
public class ShardKeyUtil {

    public static final String DATE = "20260202";

    private ShardKeyUtil() {
        // enforce static access
    }

    /**
     * Build the shard row id for the given event id.
     *
     * @param eventId
     *            the event id, which must not be negative
     * @param numShards
     *            the number of shards
     * @return the shard row id, e.g. {@code 20260202_3}
     */
    public static String buildRow(int eventId, int numShards) {
        Preconditions.checkArgument(eventId >= 0, "eventId must not be negative");
        Preconditions.checkArgument(numShards > 0, "numShards must be greater than 0");
        int shard = eventId % numShards;
        return DATE + "_" + shard;
    }
}
