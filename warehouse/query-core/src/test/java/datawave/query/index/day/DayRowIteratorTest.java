package datawave.query.index.day;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import datawave.util.time.DateHelper;

public class DayRowIteratorTest {

    String start;
    String stop;
    Set<String> expected = new HashSet<>();

    @BeforeEach
    public void before() {
        start = null;
        stop = null;
        expected.clear();
    }

    @Test
    public void testSmallScan() {
        setStart("20240404");
        setStop("20240406");
        setExpected(Set.of("20240404", "20240405", "20240406"));
        test();
    }

    @Test
    public void testSingleDayScan() {
        setStart("20240407");
        setStop("20240407");
        setExpected(Set.of("20240407"));
        test();
    }

    @Test
    public void testWeekScan() {
        setStart("20240407");
        setStop("20240414");
        setExpected(Set.of("20240407", "20240408", "20240409", "20240410", "20240411", "20240412", "20240413", "20240414"));
        test();
    }

    @Test
    public void scanCrossesMonthBoundary() {
        setStart("20240430");
        setStop("20240501");
        setExpected(Set.of("20240430", "20240501"));
        test();
    }

    @Test
    public void testScanCrossesYearBoundary() {
        setStart("20221231");
        setStop("20230101");
        setExpected(Set.of("20221231", "20230101"));
        test();
    }

    @Test
    public void testMultiYearScan() {
        // all of 2021 plus a day on each end
        setStart("20201231");
        setStop("20220101");

        // build expected
        Calendar begin = getCalendarDay(timeToDate("20201231"));
        Calendar end = getCalendarDay(timeToDate("20220101"));
        while (begin.compareTo(end) <= 0) {
            expected.add(DateHelper.format(begin.getTime()));
            begin.add(Calendar.DAY_OF_YEAR, 1);
        }

        // should be 365 + 2
        assertEquals(367, expected.size());

        test();
    }

    @ParameterizedTest
    @CsvSource({"20240228, 20240301, '20240228,20240229,20240301'", "20000228, 20000301, '20000228,20000229,20000301'",
            "21000228, 21000301, '21000228,21000301'", "20241231, 20250101, '20241231,20250101'", "20240430, 20240501, '20240430,20240501'"})
    public void testOrderedInclusiveRange(String first, String last, String days) {
        assertDays(timeToDate(first), timeToDate(last), List.of(days.split(",")));
    }

    @Test
    public void testReversedRange() {
        assertDays(timeToDate("20240408"), timeToDate("20240407"), List.of());
    }

    @Test
    public void testTimesAreTruncatedToGmtDays() {
        assertDays(Date.from(Instant.parse("2024-03-09T23:59:59.999Z")), Date.from(Instant.parse("2024-03-11T00:00:00Z")),
                        List.of("20240309", "20240310", "20240311"));
    }

    @Test
    public void testReversedTimesWithinSameDay() {
        assertDays(Date.from(Instant.parse("2024-11-03T23:59:59.999Z")), Date.from(Instant.parse("2024-11-03T00:00:00Z")), List.of("20241103"));
    }

    @Test
    public void testSupportedDateLimits() {
        assertDays(DateHelper.MIN_SUPPORTED_DATE, DateHelper.MIN_SUPPORTED_DATE, List.of("00010101"));
        assertDays(DateHelper.MAX_SUPPORTED_DATE, DateHelper.MAX_SUPPORTED_DATE, List.of("99991231"));
    }

    private void assertDays(Date first, Date last, List<String> expectedDays) {
        DayRowIterator iter = new DayRowIterator(first, last);
        List<String> actual = new ArrayList<>();
        while (iter.hasNext()) {
            assertTrue(iter.hasNext());
            actual.add(iter.next());
        }
        assertFalse(iter.hasNext());
        assertEquals(expectedDays, actual);
    }

    private void test() {
        assertNotNull(start);
        assertNotNull(stop);
        assertNotNull(expected);

        Date startDate = timeToDate(start);
        Date endDate = timeToDate(stop);

        Set<String> days = new HashSet<>();
        DayRowIterator iter = new DayRowIterator(startDate, endDate);
        while (iter.hasNext()) {
            String day = iter.next();
            assertNotNull(day);
            days.add(day);
        }

        assertEquals(expected, days);
    }

    private void setStart(String start) {
        this.start = start;
    }

    private void setStop(String stop) {
        this.stop = stop;
    }

    private void setExpected(Set<String> expected) {
        this.expected = expected;
    }

    private Date timeToDate(String time) {
        return DateHelper.parse(time);
    }

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
