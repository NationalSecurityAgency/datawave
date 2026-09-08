package datawave.util;

import java.util.Calendar;
import java.util.Comparator;
import java.util.TimeZone;

/**
 * The composite timestamp allows us to encode two values in the timestamp to be used in accumulo keys. The event date will take the first (right hand most) 46
 * bits. The last 17 bits (except for the sign bit) will be used to encode how many days after the event date that we should base the ageoff on. If the
 * timestamp is negative, then to calculate the values the complement is taken and then the two values are extracted. The ageoff is encoded as an age off delta
 * which is the number of days after the event date.
 */
public class CompositeTimestamp {

    // The number of bits for the event date
    private static final int allocationForEventDate = 46;
    // A mask for the event date
    private static final long mask = -1L >>> (8 * 8 - allocationForEventDate);
    // The max age off delta in days
    private static final long maxDiff = (-1L << (allocationForEventDate + 1)) >>> (allocationForEventDate + 1);
    // A useful constant
    public static final long MILLIS_PER_DAY = 1000 * 60 * 60 * 24;

    // A constant for an invalid timestamp
    public static final long INVALID_TIMESTAMP = Long.MIN_VALUE;
    // The minimum event date
    public static final long MIN_EVENT_DATE = (0 - mask); // also equivalent to Long.MIN_VALUE + 1
    // the maximum event date
    public static final long MAX_EVENT_DATE = mask;

    /**
     * Determine if the timestamp is composite (i.e. non-zero age off delta)
     *
     * @param ts
     * @return True if composite
     */
    public static boolean isCompositeTimestamp(long ts) {
        validateTimestamp(ts);
        return (Math.abs(ts) >>> allocationForEventDate > 0);
    }

    /**
     * Get the event date portion of the timestamp
     *
     * @param ts
     * @return the event date
     */
    public static long getEventDate(long ts) {
        validateTimestamp(ts);
        long eventTs = (Math.abs(ts) & mask);
        if (ts < 0) {
            eventTs = 0 - eventTs;
        }
        return eventTs;
    }

    /**
     * Determine the age off date portion of the timestamp. This is the event date plus the ageoff delta converted to milliseconds.
     *
     * @param ts
     * @return The age off date
     */
    public static long getAgeOffDate(long ts) {
        validateTimestamp(ts);
        long baseTs = Math.abs(ts);
        long eventTs = (baseTs & mask);
        long ageOffDiff = ((baseTs >>> allocationForEventDate) * MILLIS_PER_DAY);
        if (ts < 0) {
            eventTs = 0 - eventTs;
        }
        return eventTs + ageOffDiff;
    }

    /**
     * Determine the age off delta porton of the timestamp. This is the number of days difference from the event date.
     *
     * @param ts
     * @return the age off delta
     */
    public static int getAgeOffDeltaDays(long ts) {
        validateTimestamp(ts);
        long baseTs = Math.abs(ts);
        long ageOffDiff = (baseTs >>> allocationForEventDate);
        return (int) ageOffDiff;
    }

    /**
     * Calculate an age off delta based on a timezone. This will calculate the begininning of the day in the given timezone for both the event date and the age
     * off date, and then will return that difference in days.
     *
     * @param eventDate
     * @param ageOffDate
     * @param tz
     * @return the age off delta
     */
    public static int computeAgeOffDeltaDays(long eventDate, long ageOffDate, TimeZone tz) {
        validateEventDate(eventDate);
        Calendar c = Calendar.getInstance(tz);
        c.setTimeInMillis(eventDate);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        long eventDateStart = c.getTimeInMillis();

        c.setTimeInMillis(ageOffDate);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        long ageOffDateStart = c.getTimeInMillis();

        long delta = (ageOffDateStart - eventDateStart) / MILLIS_PER_DAY;
        validateAgeOffDelta(delta);
        return (int) delta;
    }

    /**
     * Get the composite timestamp using the supplied event date and age off delta in days.
     *
     * @param eventDate
     * @param ageOffDeltaDays
     * @return The composite timestamp
     */
    public static long getCompositeDeltaTimeStamp(long eventDate, int ageOffDeltaDays) {
        validateEventDate(eventDate);
        validateAgeOffDelta(ageOffDeltaDays);
        long eventBase = Math.abs(eventDate);
        long compositeTS = ((long) ageOffDeltaDays << allocationForEventDate) | eventBase;
        if (eventDate < 0) {
            compositeTS = 0 - compositeTS;
        }
        return compositeTS;
    }

    /**
     * Get the composite timestamp using the supplied eventDate and an age off delta in days based on the supplied age off date and time zone.
     *
     * @param eventDate
     * @param ageOffDate
     * @param tz
     * @return The composite timestamp
     */
    public static long getCompositeTimeStamp(long eventDate, long ageOffDate, TimeZone tz) {
        return getCompositeDeltaTimeStamp(eventDate, computeAgeOffDeltaDays(eventDate, ageOffDate, tz));
    }

    /**
     * Get the composite timestamp using the supplied eventDate and an age off delta in days based on the supplied age off date using the GMT timezone
     *
     * @param eventDate
     * @param ageOffDate
     * @return The composite timestamp
     */
    public static long getCompositeTimeStamp(long eventDate, long ageOffDate) {
        return getCompositeTimeStamp(eventDate, ageOffDate, TimeZone.getTimeZone("GMT"));
    }

    /**
     * Return a comparator for composite timestamps. Orders firstly on the event date, and if equal then on the ageoff date. Note for values with the same event
     * date, the timestamps will order naturally for positive event dates, but need to be reversed for negative event dates. This should only be an issue for
     * the global index and hence this comparator can be used in that case.
     *
     * @return a comparison of the timestamps
     */
    public static Comparator<Long> comparator() {
        return (o1, o2) -> {
            long eventDate1 = CompositeTimestamp.getEventDate(o1);
            long eventDate2 = CompositeTimestamp.getEventDate(o2);
            if (eventDate1 < eventDate2) {
                return -1;
            } else if (eventDate1 == eventDate2) {
                long ageOffDate1 = CompositeTimestamp.getAgeOffDate(o1);
                long ageOffDate2 = CompositeTimestamp.getAgeOffDate(o2);
                if (ageOffDate1 < ageOffDate2) {
                    return -1;
                } else if (ageOffDate1 == ageOffDate2) {
                    return 0;
                } else {
                    return 1;
                }
            } else {
                return 1;
            }
        };
    }

    private static void validateTimestamp(long ts) {
        if (ts == INVALID_TIMESTAMP) {
            throw new IllegalArgumentException("Invalid timestamp");
        }
    }

    private static void validateEventDate(long eventDate) {
        if (eventDate < MIN_EVENT_DATE) {
            throw new IllegalArgumentException("Event date cannot be less than " + MIN_EVENT_DATE);
        }
        if (eventDate > MAX_EVENT_DATE) {
            throw new IllegalArgumentException("Event Date cannot be greater than " + MAX_EVENT_DATE);
        }
    }

    private static void validateAgeOffDelta(long ageOffDeltaDays) {
        if (ageOffDeltaDays < 0) {
            throw new IllegalArgumentException("Age off date must be greater to or equal to the event date");
        }
        if (ageOffDeltaDays > maxDiff) {
            throw new IllegalArgumentException("Difference between event date and age off date cannot be more than " + maxDiff + " days");
        }
    }

}
