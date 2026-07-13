package datawave.test.framework.util;

import com.google.common.base.Preconditions;

import datawave.util.time.DateHelper;

/**
 * Builds shard row ids in the form {@code YYYYMMDD_SHARD}, where the date and shard are both derived from an event id via modulus arithmetic.
 */
public class ShardKeyUtil {

    /**
     * The anchor date that an event's day offset is added to.
     */
    public static final String BASE_DATE = "20260202";

    private static final long MILLIS_PER_DAY = 24L * 60L * 60L * 1000L;

    private ShardKeyUtil() {
        // enforce static access
    }

    /**
     * Build the shard row id for the given event id.
     *
     * @param eventId
     *            the event id
     * @param numShards
     *            the number of shards per day
     * @param numDays
     *            the number of days
     * @return the shard row id, e.g. {@code 20260202_3}
     */
    public static String buildRow(int eventId, int numShards, int numDays) {
        Preconditions.checkArgument(numShards > 0, "numShards must be greater than 0");
        Preconditions.checkArgument(numDays > 0, "numDays must be greater than 0");
        int shard = eventId % numShards;
        return buildDate(eventId, numShards, numDays) + "_" + shard;
    }

    /**
     * Determine the date for the given event id.
     *
     * @param eventId
     *            the event id
     * @param numShards
     *            the number of shards per day
     * @param numDays
     *            the number of days
     * @return the date, in {@code YYYYMMDD} form
     */
    public static String buildDate(int eventId, int numShards, int numDays) {
        Preconditions.checkArgument(numShards > 0, "numShards must be greater than 0");
        Preconditions.checkArgument(numDays > 0, "numDays must be greater than 0");
        int dayOffset = (eventId / numShards) % numDays;
        return DateHelper.format(DateHelper.addDays(DateHelper.parse(BASE_DATE), dayOffset));
    }

    /**
     * Determine the timestamp for the given event id.
     * <p>
     * A shard's position (0..numShards-1) is treated as its position within the day, spreading events evenly across the 24-hour period to simulate continual
     * ingest throughout the day rather than a single ingest at midnight.
     *
     * @param eventId
     *            the event id
     * @param numShards
     *            the number of shards per day
     * @param numDays
     *            the number of days
     * @return the epoch millis for the event's date and time-of-day
     */
    public static long buildTimestamp(int eventId, int numShards, int numDays) {
        Preconditions.checkArgument(numShards > 0, "numShards must be greater than 0");
        Preconditions.checkArgument(numDays > 0, "numDays must be greater than 0");
        long dayStart = DateHelper.parse(buildDate(eventId, numShards, numDays)).getTime();
        int shard = eventId % numShards;
        long timeOfDay = shard * (MILLIS_PER_DAY / numShards);
        return dayStart + timeOfDay;
    }
}
