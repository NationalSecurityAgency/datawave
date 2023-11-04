package datawave.util;

public class CompositeTimestamp {

    private static final int allocationForEventDate = 47;
    private static final long mask = 0x7fffffffffffL;
    public static final long MILLIS_PER_DAY = 1000 * 60 * 60 * 24;

    public static boolean isCompositeTimestamp(long ts) {
        if (ts >> allocationForEventDate > 0) {
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
            return ((ts >> allocationForEventDate) * MILLIS_PER_DAY) + (ts & mask);
        } else {
            return ts;
        }
    }

    public static long getCompositeTimeStamp(long eventDate, long ageOffDate) {
        if (ageOffDate < eventDate) {
            throw new IllegalArgumentException("age off date must be greater than the event date");
        }
        long diffInDays = (ageOffDate - eventDate) / MILLIS_PER_DAY;
        long compositeTS = (diffInDays << allocationForEventDate) | (eventDate & mask);
        return compositeTS;

    }
}
