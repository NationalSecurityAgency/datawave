package datawave.util;

/**
 * The composite timestamp allows us to encode two values in the timestamp to be used in accumulo keys. The event date will take the first (right hand most) 47
 * bits if the value. The last 17 bits will be used to encode how many days after the event date that we should base the ageoff on.
 */
public class CompositeTimestamp {

    private static final int allocationForEventDate = 47;
    private static final long mask = -1L >>> (8 * 8 - allocationForEventDate);
    // if we want to avoid negative timestamps, then change the maxDiff to
    // private static final long maxDiff = (-1L << (allocationForEventDate + 1)) >>> (allocationForEventDate + 1);
    private static final long maxDiff = (-1L << allocationForEventDate) >>> allocationForEventDate;
    public static final long MILLIS_PER_DAY = 1000 * 60 * 60 * 24;

    public static boolean isCompositeTimestamp(long ts) {
        if (ts >>> allocationForEventDate > 0) {
            return true;
        } else {
            return false;
        }
    }

    public static long getEventDate(long ts) {
        if (isCompositeTimestamp(ts)) {
            return (ts & mask);
        } else {
            return ts;
        }
    }

    public static long getAgeOffDate(long ts) {
        if (isCompositeTimestamp(ts)) {
            return ((ts >>> allocationForEventDate) * MILLIS_PER_DAY) + (ts & mask);
        } else {
            return ts;
        }
    }

    public static long getCompositeTimeStamp(long eventDate, long ageOffDate) {
        if (ageOffDate < eventDate) {
            throw new IllegalArgumentException("age off date must be greater than or equal to the event date");
        }
        if (eventDate > mask || eventDate < 0) {
            throw new IllegalArgumentException("event date cannot be negative or greater than " + mask);
        }
        long diffInDays = (ageOffDate - eventDate + MILLIS_PER_DAY - 1) / MILLIS_PER_DAY;
        if (diffInDays > maxDiff) {
            throw new IllegalArgumentException("Difference between event date and age off date cannot be more than " + maxDiff + " days");
        }
        long compositeTS = (diffInDays << allocationForEventDate) | (eventDate & mask);
        return compositeTS;

    }
}
