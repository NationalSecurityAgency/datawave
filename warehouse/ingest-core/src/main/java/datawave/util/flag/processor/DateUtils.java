package datawave.util.flag.processor;

import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class DateUtils {

    static final HashSet<String> GROUPS = new HashSet<>(Arrays.asList("none", "year", "month", "day"));
    // Pattern: /YYYY/MM/DD followed by a / or the end of the string, (?: ) is a non-capturing group....
    public static final Pattern pattern = Pattern.compile("/([0-9]{4})((/[0-9]{2}){2})(?:/|$)");
    public static final long A_MINUTE = 1000L * 60L;
    public static final long ADAY = A_MINUTE * 60L * 24L;
    static final TimeZone GMT = TimeZone.getTimeZone("GMT");

    /**
     * Factory method to generate new Bucket objects based on group and searching the path. It is assumed that the path ends with a year, month, and date in the
     * format:<br>
     * YYYY/MM/DD<br>
     * The month and date fields are optional
     *
     * @param group
     *            the group
     * @param path
     *            the file path
     * @return the time in milleseconds
     * @throws UnusableFileException
     *             if the file is unusable
     */
    public static long getBucket(String group, String path) throws UnusableFileException {
        if (group == null || "none".equals(group)) {
            return 0L;
        }

        Matcher m = pattern.matcher(path);
        if (m.find()) {
            Calendar c = Calendar.getInstance(GMT);
            // set it to midnight
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);

            int year = Integer.parseInt(m.group(1));
            c.set(year, Calendar.JANUARY, 1);
            if ("year".equals(group))
                return c.getTimeInMillis();
            String mthDay = m.group(2);
            if (mthDay.length() > 2) {
                c.set(Calendar.MONTH, (Integer.parseInt(mthDay.substring(1, 3)) - 1));
            }
            if ("month".equals(group))
                return c.getTimeInMillis();
            if (mthDay.length() > 4) {
                c.set(Calendar.DAY_OF_MONTH, Integer.parseInt(mthDay.substring(4, mthDay.length())));
            }
            // default to day
            return c.getTimeInMillis();
        } else {
            throw new UnusableFileException("Could not pull yyyy/mm/dd from " + path);
        }
    }

    /**
     * Factory method to determine the timestamp for a file from the folder that contains it. This will be the yyyy/mm/dd pulled from the folder.
     *
     * @param path
     *            file path
     * @return the folder timestamp
     * @throws UnusableFileException
     *             if the file is unusable
     */
    public static long getFolderTimestamp(String path) throws UnusableFileException {
        return getBucket("day", path);
    }
}
