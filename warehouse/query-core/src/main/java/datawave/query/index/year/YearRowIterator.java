package datawave.query.index.year;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;

/**
 * Given a start and end date, iterate through all years
 */
public class YearRowIterator implements Iterator<String> {

    private final Calendar start;
    private final Calendar end;

    // top key
    private String year;

    private static final String YEAR_FORMAT = "yyyy";
    private static final DateTimeFormatter DTF_year = DateTimeFormatter.ofPattern(YEAR_FORMAT).withZone(ZoneOffset.UTC);

    public YearRowIterator(Date startDate, Date stopDate) {
        this.start = getCalendarDay(startDate);
        this.end = getCalendarDay(stopDate);
    }

    @Override
    public boolean hasNext() {
        if (year == null) {
            if (start.compareTo(end) <= 0) {
                year = DTF_year.format(start.toInstant());
                start.add(Calendar.YEAR, 1);
            } else {
                // shouldn't need this, but just to be safe
                year = null;
            }
        }

        return year != null;
    }

    @Override
    public String next() {
        String next = year;
        year = null;
        return next;
    }

    /**
     * Get the {@link Calendar} representation of a {@link Date}.
     *
     * @param date
     *            the Date
     * @return the Calendar equivalent
     */
    private Calendar getCalendarDay(Date date) {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        calendar.setTime(date);
        // increment month and day to avoid GMT conversion that rolls back to previous year
        calendar.set(Calendar.MONTH, 1);
        calendar.set(Calendar.DAY_OF_YEAR, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar;
    }

    public static DateTimeFormatter getFormatter() {
        return DTF_year;
    }
}
