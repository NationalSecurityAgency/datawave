package datawave.query.index.day;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;

import datawave.util.time.DateHelper;

/**
 * Given a start and end date, iterate through all days
 */
public class DayRowIterator implements Iterator<String> {

    // date bound
    private final Calendar start;
    private final Calendar end;

    // top key
    private String day;

    public DayRowIterator(Date startDate, Date stopDate) {
        this.start = getCalendarDay(startDate);
        this.end = getCalendarDay(stopDate);
    }

    @Override
    public boolean hasNext() {
        if (day == null) {
            if (start.compareTo(end) <= 0) {
                day = DateHelper.format(start.getTime());
                start.add(Calendar.DAY_OF_YEAR, 1);
            } else {
                // shouldn't need this, but just to be safe
                day = null;
            }
        }

        return day != null;
    }

    @Override
    public String next() {
        String next = day;
        day = null;
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
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar;
    }
}
