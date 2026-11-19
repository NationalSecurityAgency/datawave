package datawave.query.index.day;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Iterator;

import datawave.util.time.DateHelper;

/**
 * Given a start and end date, iterate through all days
 */
public class DayRowIterator implements Iterator<String> {

    private static final DateTimeFormatter DAY_FORMAT = DateTimeFormatter.ofPattern(DateHelper.DATE_FORMAT_STRING_TO_DAY);

    // date bound
    private LocalDate start;
    private final LocalDate end;

    // top key
    private String day;

    public DayRowIterator(Date startDate, Date stopDate) {
        this.start = getDay(startDate);
        this.end = getDay(stopDate);
    }

    @Override
    public boolean hasNext() {
        if (day == null) {
            if (start.compareTo(end) <= 0) {
                day = DAY_FORMAT.format(start);
                start = start.plusDays(1);
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
     * Get the GMT day of a {@link Date}.
     *
     * @param date
     *            the Date
     * @return the GMT day
     */
    private LocalDate getDay(Date date) {
        return date.toInstant().atZone(ZoneOffset.UTC).toLocalDate();
    }
}
