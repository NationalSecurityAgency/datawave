package datawave.query.tables;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import datawave.util.time.DateHelper;

/**
 * An iterator that can present each day given a start and end date in yyyyMMdd format
 */
public class DateIterator implements Iterator<String> {

    private final Calendar start;
    private final Calendar end;

    private String top;

    public DateIterator(String startDate, String endDate) {
        start = getCalendarStartOfDay(DateHelper.parse(startDate));
        end = getCalendarStartOfDay(DateHelper.parse(endDate));
        top = DateHelper.format(start.getTime());
    }

    @Override
    public boolean hasNext() {
        return top != null;
    }

    @Override
    public String next() {
        String next = top;
        top = null;

        start.add(Calendar.DAY_OF_YEAR, 1);
        if (start.compareTo(end) <= 0) {
            top = DateHelper.format(start.getTime());
        }

        return next;
    }

    private Calendar getCalendarStartOfDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar;
    }
}
