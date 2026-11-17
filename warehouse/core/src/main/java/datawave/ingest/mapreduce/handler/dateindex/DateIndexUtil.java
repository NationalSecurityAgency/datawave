package datawave.ingest.mapreduce.handler.dateindex;

import java.text.ParseException;
import java.text.SimpleDateFormat;
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
 *
 */
public class DateIndexUtil {

    public static final String EVENT_DATE_TYPE = "EVENT";
    public static final String LOADED_DATE_TYPE = "LOADED";
    public static final String ACTIVITY_DATE_TYPE = "ACTIVITY";
    public static final ThreadLocal<SimpleDateFormat> format = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd"));
    private static final long MS_PER_DAY = TimeUnit.DAYS.toMillis(1);
    private static final Logger log = LoggerFactory.getLogger(DateIndexUtil.class);

    /**
     * Format the date into yyyyMMdd
     *
     * @param date
     *            then date to be formatted
     * @return the string representation in yyyyMMdd format
     */
    public static String format(Date date) {
        return format.get().format(date);
    }

    /**
     * Get the date with time set to 00:00:00
     *
     * @param dateStr
     *            string representation of the date
     * @return the date
     * @throws ParseException
     *             if there is a problem parsing the date
     */
    public static Date getBeginDate(String dateStr) throws ParseException {
        return format.get().parse(dateStr);
    }

    /**
     * Get the date with time set to 23:59:59
     *
     * @param dateStr
     *            string representation of the date
     * @return the date
     * @throws ParseException
     *             if there is a problem parsing the date
     */
    public static Date getEndDate(String dateStr) throws ParseException {
        Calendar cal = Calendar.getInstance();
        cal.setTime(format.get().parse(dateStr));
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);
        return cal.getTime();
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
     * Merge to bit sets into one. This will actually modify bits1 or bits2, depending which one is larger.
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
