package datawave.ingest.mapreduce.handler.dateindex;

import com.google.common.base.Preconditions;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.BitSet;
import java.util.Date;

/**
 * Utility class for handling date operations and shard tracking during MapReduce ingest.
 * <p>
 * This class provides thread-safe static methods to format and parse dates to and from
 * the {@code yyyyMMdd} string format, including resolving dates to the beginning or
 * end of a day. It also includes helper methods for generating and merging
 * {@link BitSet} objects, which are typically used to represent and track individual
 * shards for date-indexed records.
 * <p>
 * Defines standard constants for common date index types (EVENT, LOADED, ACTIVITY).
 */
public class DateIndexUtil {
    public static final String EVENT_DATE_TYPE = "EVENT";
    public static final String LOADED_DATE_TYPE = "LOADED";
    public static final String ACTIVITY_DATE_TYPE = "ACTIVITY";
    public static final ThreadLocal<DateTimeFormatter> formatter = ThreadLocal.withInitial(() -> DateTimeFormatter.BASIC_ISO_DATE);
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
        return local.format(formatter.get());
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
        Preconditions.checkArgument(dateStr != null && !dateStr.isBlank() , "date string cannot be null or blank");
        TemporalAccessor temp = formatter.get().parse(dateStr);
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
        TemporalAccessor temp = formatter.get().parse(dateStr);
        LocalDate local = LocalDate.from(temp);
        return Date.from(local.atTime(23, 59, 59,MAX_MILLIS).atZone(ZoneId.systemDefault()).toInstant());
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

}
