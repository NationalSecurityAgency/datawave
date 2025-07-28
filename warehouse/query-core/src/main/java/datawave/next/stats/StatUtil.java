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
        long hours = (millis / 1000) / 60 / 60;
        long minutes = (millis / 1000) / 60 % 60;
        long seconds = (millis / 1000) % 60;
        long remainingMillis = millis % 1000;

        StringBuilder sb = new StringBuilder();
        sb.append(hours > 0 ? hours + "h " : "").append(minutes > 0 || (hours > 0 && seconds > 0) ? minutes + "m " : "")
                        .append(seconds > 0 ? seconds + "s " : "").append(remainingMillis > 0 && sb.length() > 0 ? remainingMillis + "ms" : "")
                        .append(sb.length() == 0 ? millis + "ms" : "");
        return sb.toString();
    }
}
