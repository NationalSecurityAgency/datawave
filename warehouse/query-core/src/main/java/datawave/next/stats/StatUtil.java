package datawave.next.stats;

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
        long millis = ns / 1_000_000;
        if (millis > 0) {

        }
        return "formatted string";
    }
}
