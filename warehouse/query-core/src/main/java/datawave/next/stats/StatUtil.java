package datawave.next.stats;

import java.util.concurrent.TimeUnit;

/**
 * Utility class that helps pretty print stats
 */
public class StatUtil {

    /**
     * Given an elapsed time in nanoseconds, print the time in hours minutes seconds and milliseconds
     *
     * @param ns
     *            the elapsed time in nanoseconds
     * @return The given nanoseconds formatted into 0h 0m 0s 0ms [0000ms]. Time units whose value is 0 will not be returned in the formatted time. The original
     *         elapsed time is also provided in milliseconds within brackets for future conversion purposes. For example, if given 14413346000000L, 4h 13s 346ms
     *         [14413346ms] would be returned.
     */
    public static String formatNanos(long ns) {
        String totalMilliseconds = format(TimeUnit.MILLISECONDS, ns, 0);
        // @formatter:off
        StringBuilder formattedTime = new StringBuilder()
                .append(format(TimeUnit.HOURS, ns, 0))
                .append(format(TimeUnit.MINUTES, ns, 60))
                .append(format(TimeUnit.SECONDS, ns, 60))
                .append(format(TimeUnit.MILLISECONDS, ns, 1000));
        // @formatter:on
        if (formattedTime.length() == 0) {
            formattedTime.append("0ns");
        }
        return formattedTime.append(" [").append(totalMilliseconds).append("]").toString();
    }

    private static String format(TimeUnit target, long duration, int modulus) {
        long time = target.convert(duration, TimeUnit.NANOSECONDS);
        if (modulus != 0) {
            time = time % modulus;
        }
        if (target != TimeUnit.MILLISECONDS && time != 0) {
            return time + getAbbreviation(target) + " ";
        } else if (target == TimeUnit.MILLISECONDS && time != 0) {
            return time + getAbbreviation(target);
        } else {
            return "";
        }
    }

    private static String getAbbreviation(TimeUnit unit) {
        switch (unit) {
            case MILLISECONDS:
                return "ms";
            case SECONDS:
                return "s";
            case MINUTES:
                return "m";
            case HOURS:
                return "h";
            default:
                return "unknown";
        }
    }

    private StatUtil() {
        throw new UnsupportedOperationException();
    }
}
