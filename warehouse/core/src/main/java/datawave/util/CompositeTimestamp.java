package datawave.util;

/**
 * The composite timestamp allows us to encode two values in the timestamp to be used in accumulo keys. The event date will take the first (right hand most) 46
 * bits. The last 17 bits (except for the sign bit) will be used to encode how many days after the event date that we should base the ageoff on. If the
 * timestamp is negative, then to calculate the values the complement is taken and then the two values are extracted. The ageoff is encoded as the number of
 * days after the event date.
 */
public class CompositeTimestamp {

    private static final int allocationForEventDate = 46;
    private static final long mask = -1L >>> (8 * 8 - allocationForEventDate);
    private static final long maxDiff = (-1L << (allocationForEventDate + 1)) >>> (allocationForEventDate + 1);
    public static final long MILLIS_PER_DAY = 1000 * 60 * 60 * 24;

    public static boolean isCompositeTimestamp(long ts) {
        return (Math.abs(ts) >>> allocationForEventDate > 0);
    }

    public static long getEventDate(long ts) {
        long eventTs = (Math.abs(ts) & mask);
        if (ts < 0) {
            eventTs = 0 - eventTs;
        }
        return eventTs;
    }

    public static long getAgeOffDate(long ts) {
        long baseTs = Math.abs(ts);
        long eventTs = (baseTs & mask);
        long ageOffDiff = ((baseTs >>> allocationForEventDate) * MILLIS_PER_DAY);
        if (ts < 0) {
            eventTs = 0 - eventTs;
        }
        return eventTs + ageOffDiff;
    }

    public static long getCompositeTimeStamp(long eventDate, long ageOffDate) {
        if (ageOffDate < eventDate) {
            throw new IllegalArgumentException("age off date must be greater than or equal to the event date");
        }
        long eventBase = Math.abs(eventDate);
        if (eventBase > mask) {
            throw new IllegalArgumentException("|event date| cannot be greater than " + mask);
        }
        long diffInDays = (ageOffDate - eventDate + MILLIS_PER_DAY - 1) / MILLIS_PER_DAY;
        if (diffInDays > maxDiff) {
            throw new IllegalArgumentException("Difference between event date and age off date cannot be more than " + maxDiff + " days");
        }
        long compositeTS = (diffInDays << allocationForEventDate) | eventBase;
        if (eventDate < 0) {
            compositeTS = 0 - compositeTS;
        }
        return compositeTS;

    }
}
