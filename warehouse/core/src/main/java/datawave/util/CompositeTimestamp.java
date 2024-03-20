package datawave.util;

import java.util.Comparator;

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

    public static final long INVALID_TIMESTAMP = Long.MIN_VALUE;
    public static final long MIN_EVENT_DATE = (0 - mask); // also equivalent to Long.MIN_VALUE + 1
    public static final long MAX_EVENT_DATE = mask;

    public static boolean isCompositeTimestamp(long ts) {
        if (ts == INVALID_TIMESTAMP) {
            throw new IllegalArgumentException("Invalid timestamp");
        }
        return (Math.abs(ts) >>> allocationForEventDate > 0);
    }

    public static long getEventDate(long ts) {
        if (ts == INVALID_TIMESTAMP) {
            throw new IllegalArgumentException("Invalid timestamp");
        }
        long eventTs = (Math.abs(ts) & mask);
        if (ts < 0) {
            eventTs = 0 - eventTs;
        }
        return eventTs;
    }

    public static long getAgeOffDate(long ts) {
        if (ts == INVALID_TIMESTAMP) {
            throw new IllegalArgumentException("Invalid timestamp");
        }
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
}
