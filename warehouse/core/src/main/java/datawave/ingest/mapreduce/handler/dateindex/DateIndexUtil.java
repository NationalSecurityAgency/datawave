package datawave.ingest.mapreduce.handler.dateindex;

import java.text.ParseException;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Date;

import java.text.SimpleDateFormat;

/**
 *
 */
public class DateIndexUtil {

    public static final String EVENT_DATE_TYPE = "EVENT";
    public static final String LOADED_DATE_TYPE = "LOADED";
    public static final String ACTIVITY_DATE_TYPE = "ACTIVITY";
    public static final ThreadLocal<SimpleDateFormat> format = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd"));

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

}
