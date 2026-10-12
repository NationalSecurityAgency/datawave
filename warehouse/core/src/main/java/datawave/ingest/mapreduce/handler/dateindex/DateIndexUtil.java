package datawave.ingest.mapreduce.handler.dateindex;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.util.CompositeTimestamp;
import datawave.util.time.DateHelper;

/**
 * Utility class for handling date operations and shard tracking during MapReduce ingest.
 * <p>
 * This class provides thread-safe static methods to format and parse dates to and from the {@code yyyyMMdd} string format, including resolving dates to the
 * beginning or end of a day. It also includes helper methods for generating and merging {@link BitSet} objects, which are typically used to represent and track
 * individual shards for date-indexed records.
 * <p>
 * Defines standard constants for common date index types (EVENT, LOADED, ACTIVITY).
 */
public class DateIndexUtil {

    public static final String EVENT_DATE_TYPE = "EVENT";
    public static final String LOADED_DATE_TYPE = "LOADED";
    public static final String ACTIVITY_DATE_TYPE = "ACTIVITY";
    private static final long MS_PER_DAY = TimeUnit.DAYS.toMillis(1);
    private static final Logger log = LoggerFactory.getLogger(DateIndexUtil.class);
    public static final DateTimeFormatter formatter = DateTimeFormatter.BASIC_ISO_DATE;
    private static final int MAX_MILLIS = 999_000_000;

    /**
     * Format the date into yyyyMMdd with time zone set to the system default.
     *
     * @param date
     *            then date to be formatted
     * @return the string representation in yyyyMMdd format
     */
    public static String format(Date date) {
        Preconditions.checkNotNull(date, "date cannot be null");
        LocalDate local = date.toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDate();
        return local.format(formatter);
    }

    /**
     * Get the date with time set to 00:00:00.000 and time zone set to the system default.
     *
     * @param dateStr
     *            string representation of the date
     * @return the date
     * @throws ParseException
     *             if there is a problem parsing the date
     */
    public static Date getBeginDate(String dateStr) throws ParseException {
        Preconditions.checkArgument(dateStr != null && !dateStr.isBlank(), "date string cannot be null or blank");
        TemporalAccessor temp = formatter.parse(dateStr);
        LocalDate local = LocalDate.from(temp);
        return Date.from(local.atStartOfDay(ZoneId.systemDefault()).toInstant());
    }

    /**
     * Get the date with time set to 23:59:59.999 and time zone set to the system default.
     *
     * @param dateStr
     *            string representation of the date
     * @return the date
     * @throws ParseException
     *             if there is a problem parsing the date
     */
    public static Date getEndDate(String dateStr) throws ParseException {
        Preconditions.checkArgument(dateStr != null && !dateStr.isBlank(), "date string cannot be null or blank");
        TemporalAccessor temp = formatter.parse(dateStr);
        LocalDate local = LocalDate.from(temp);
        return Date.from(local.atTime(23, 59, 59, MAX_MILLIS).atZone(ZoneId.systemDefault()).toInstant());
    }

    /**
     * Given a specific shard, return the bit array with the shard'th bit set.
     *
     * @param shard
     *            the shard
     * @return a BitSet with the shard'th bit set
     */
    public static BitSet getBits(int shard) {
        BitSet bits = new BitSet(shard);
        bits.set(shard);
        return bits;
    }

    /**
     * Merge to bit sets into one. This will actually modify bits1 or bits2, depending on which one is larger.
     *
     * @param bits1
     *            the first bit set
     * @param bits2
     *            the second bit set
     * @return the larger of bits1 or bits2, with one the shorter set copied into it
     */
    public static BitSet merge(BitSet bits1, BitSet bits2) {
        // ensure bits1 is the larger of the two
        if (bits2.size() > bits1.size()) {
            BitSet tmp = bits1;
            bits1 = bits2;
            bits2 = tmp;
        }
        bits1.or(bits2);
        return bits1;
    }

    /**
     * Constructs a {@link Value} that contains a {@link BitSet}'s backing byte array, with the bit at the day index offset set.
     *
     * @param shardId
     *            the full shard id
     * @return a Value containing a BitSet with the day index offset bit set
     */
    public static Value getValueForDayIndex(String shardId) {
        // get the shard number
        int shardNumber = getOffsetForDayIndex(shardId);
        BitSet bits = new BitSet();
        bits.set(shardNumber);
        return new Value(bits.toByteArray());
    }

    /**
     * Get the shard number used as the offset for the day index
     * <p>
     *
     * @param shard
     *            the shard
     * @return the day index offset
     */
    public static int getOffsetForDayIndex(String shard) {
        int underscoreIndex = shard.indexOf('_');
        Preconditions.checkArgument(underscoreIndex > 0, "shard did not contain an underscore: " + shard);
        String bucket = shard.substring(underscoreIndex + 1);
        return Integer.parseInt(bucket);
    }

    /**
     * Constructs a {@link Value} that contains a {@link BitSet}'s backing byte array, with the bit at the year index offset set.
     *
     * @param shard
     *            the full shard
     * @return a Value containing a BitSet with the year index offset bit set
     */
    public static Value getValueForYearIndex(String shard) {
        // get the shard number
        int shardNumber = getOffsetForYearIndex(shard);
        BitSet bits = new BitSet();
        bits.set(shardNumber);
        return new Value(bits.toByteArray());
    }

    /**
     * Calculate the day of the year used as the offset for the year index
     *
     * @param shard
     *            the shard
     * @return the year index offset
     */
    public static int getOffsetForYearIndex(String shard) {
        int index = shard.indexOf('_');
        Preconditions.checkArgument(index > 0, "shard did not contain an underscore: " + shard);
        String date = shard.substring(0, index);
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        calendar.setTime(DateHelper.parse(date));

        int dayOfYear = calendar.get(Calendar.DAY_OF_YEAR);
        if (log.isTraceEnabled()) {
            log.trace("day of year: " + dayOfYear);
        }
        return dayOfYear;
    }

    /**
     * trim the event date and ageoff portions of the ts to the beginning of the day
     *
     * @param ts
     *            the composite timestamp to trim
     * @return the timestamp to be used for index entries
     */
    public static long getIndexTimestamp(long ts) {
        long tsToDay = trimToBeginningOfDay(CompositeTimestamp.getEventDate(ts));
        long ageOffToDay = trimToBeginningOfDay(CompositeTimestamp.getAgeOffDate(ts));
        return CompositeTimestamp.getCompositeTimeStamp(tsToDay, ageOffToDay);
    }

    /**
     * Trim ms to the beginning of the day
     *
     * @param date
     *            the time in milliseconds since the epoch
     * @return the time at the beginning of the day
     */
    public static long trimToBeginningOfDay(long date) {
        return (date / MS_PER_DAY) * MS_PER_DAY;
    }
}
