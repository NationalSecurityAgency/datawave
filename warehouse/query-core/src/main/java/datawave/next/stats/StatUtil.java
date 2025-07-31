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
     * @return the formatted time
     */
    public static String formatNanos(long ns) {
        StringBuilder sb = new StringBuilder();
        return sb.append(format(TimeUnit.NANOSECONDS, TimeUnit.HOURS, ns, 0)).append(format(TimeUnit.NANOSECONDS, TimeUnit.MINUTES, ns, 60))
                        .append(format(TimeUnit.NANOSECONDS, TimeUnit.SECONDS, ns, 60)).append(format(TimeUnit.NANOSECONDS, TimeUnit.MILLISECONDS, ns, 1000))
                        .toString();
    }

    public static String format(TimeUnit source, TimeUnit target, long duration, int modulus) {
        long time = target.convert(duration, source);
        if (modulus != 0) {
            time = time % modulus;
        }
        return time == 0 ? "" : time + getAbbreviation(target);
    }

    public static String getAbbreviation(TimeUnit unit) {
        switch (unit) {
            case NANOSECONDS:
                return "ns";
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
}
