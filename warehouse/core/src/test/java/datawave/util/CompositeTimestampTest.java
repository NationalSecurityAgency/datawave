package datawave.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.zone.ZoneRulesException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class CompositeTimestampTest {

    @Test
    public void testConversion() {
        long eventDate = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2022-10-26T01:00:00Z")).toEpochMilli();
        long ageOff = eventDate + CompositeTimestamp.MILLIS_PER_DAY;
        long expectedTS = 72035490177664L;

        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);

        assertEquals(expectedTS, compositeTS);
        assertTrue(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        assertFalse(CompositeTimestamp.isCompositeTimestamp(eventDate));
        assertFalse(CompositeTimestamp.isCompositeTimestamp(ageOff));
        assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), ageOff);
        assertEquals(CompositeTimestamp.getAgeOffDeltaDays(compositeTS), (ageOff - eventDate) / CompositeTimestamp.MILLIS_PER_DAY);
    }

    @Test
    public void testOldDateWithOldAgeoff() {
        long eventDate = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("1900-01-01T00:00:00Z")).toEpochMilli();
        long ageOff = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("1950-01-01T00:00:00Z")).toEpochMilli();
        long expectedTS = -1285076215161299968L;

        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);

        assertEquals(expectedTS, compositeTS);
        assertTrue(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        assertFalse(CompositeTimestamp.isCompositeTimestamp(eventDate));
        assertFalse(CompositeTimestamp.isCompositeTimestamp(ageOff));
        assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), ageOff);
        assertEquals(CompositeTimestamp.getAgeOffDeltaDays(compositeTS), (ageOff - eventDate) / CompositeTimestamp.MILLIS_PER_DAY);
    }

    @Test
    public void testOldDateWithModernAgeoff() {
        long eventDate = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("1960-01-01T00:00:00Z")).toEpochMilli();
        long ageOff = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2025-01-01T00:00:00Z")).toEpochMilli();
        long expectedTS = -1670695039885298688L;

        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);

        assertEquals(expectedTS, compositeTS);
        assertTrue(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        assertFalse(CompositeTimestamp.isCompositeTimestamp(eventDate));
        assertFalse(CompositeTimestamp.isCompositeTimestamp(ageOff));
        assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), ageOff);
        assertEquals(CompositeTimestamp.getAgeOffDeltaDays(compositeTS), (ageOff - eventDate) / CompositeTimestamp.MILLIS_PER_DAY);
    }

    @Test
    public void testOldDateWithMaxAgeoff() {
        long eventDate = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("1960-01-01T00:00:00Z")).toEpochMilli();
        long ageOff = eventDate + (131071L * CompositeTimestamp.MILLIS_PER_DAY);
        long expectedTS = -9223301983729798144L;

        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);

        assertEquals(expectedTS, compositeTS);
        assertTrue(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        assertFalse(CompositeTimestamp.isCompositeTimestamp(eventDate));
        assertFalse(CompositeTimestamp.isCompositeTimestamp(ageOff));
        assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), ageOff);
        assertEquals(CompositeTimestamp.getAgeOffDeltaDays(compositeTS), (ageOff - eventDate) / CompositeTimestamp.MILLIS_PER_DAY);
    }

    @Test
    public void testVeryOldDateWithMaxAgeoff() {
        long eventDate = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("1654-01-01T00:00:00Z")).toEpochMilli();
        long ageOff = eventDate + (131071L * CompositeTimestamp.MILLIS_PER_DAY);
        long expectedTS = -9223311640052998144L;

        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);

        assertEquals(expectedTS, compositeTS);
        assertTrue(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        assertFalse(CompositeTimestamp.isCompositeTimestamp(eventDate));
        assertFalse(CompositeTimestamp.isCompositeTimestamp(ageOff));
        assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), ageOff);
        assertEquals(CompositeTimestamp.getAgeOffDeltaDays(compositeTS), (ageOff - eventDate) / CompositeTimestamp.MILLIS_PER_DAY);
    }

    @Test
    public void testEventUpperDateBound() {
        long eventDate = (-1L >>> 18);
        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, eventDate);
        assertEquals(eventDate, compositeTS);
        assertFalse(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        assertEquals(eventDate, CompositeTimestamp.getEventDate(compositeTS));
        assertEquals(eventDate, CompositeTimestamp.getAgeOffDate(compositeTS));
        assertEquals((eventDate - eventDate) / CompositeTimestamp.MILLIS_PER_DAY, CompositeTimestamp.getAgeOffDeltaDays(compositeTS));

        try {
            CompositeTimestamp.getCompositeTimeStamp(eventDate + 1, 0);
            fail("Expected event date greater than 17 bits to fail");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testEventLowerDateBound() {
        long eventDate = (0 - (-1L >>> 18));
        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, eventDate);
        assertEquals(eventDate, compositeTS);
        assertFalse(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        assertEquals(eventDate, CompositeTimestamp.getEventDate(compositeTS));
        assertEquals(eventDate, CompositeTimestamp.getAgeOffDate(compositeTS));
        assertEquals((eventDate - eventDate) / CompositeTimestamp.MILLIS_PER_DAY, CompositeTimestamp.getAgeOffDeltaDays(compositeTS));

        try {
            CompositeTimestamp.getCompositeTimeStamp(eventDate - 1, 0);
            fail("Expected event date greater than 17 bits to fail");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testAgeOffDateBounds() {
        long eventDate = (0 - (-1L >>> 18));
        long ageOffEventDays = (-1L >>> 47);
        long ageOffEventDelta = ageOffEventDays * 1000 * 60 * 60 * 24;
        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, eventDate + ageOffEventDelta);
        assertTrue(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        assertEquals(eventDate, CompositeTimestamp.getEventDate(compositeTS));
        assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), eventDate + ageOffEventDelta);
        assertEquals(CompositeTimestamp.getAgeOffDeltaDays(compositeTS), (ageOffEventDelta) / CompositeTimestamp.MILLIS_PER_DAY);

        try {
            CompositeTimestamp.getCompositeTimeStamp(eventDate, eventDate - CompositeTimestamp.MILLIS_PER_DAY);
            fail("Expected ageoff date less than event date to fail");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            CompositeTimestamp.getCompositeTimeStamp(CompositeTimestamp.MILLIS_PER_DAY, 0);
            fail("Expected ageoff date less than event date to fail");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            CompositeTimestamp.getCompositeTimeStamp(eventDate, eventDate + ageOffEventDelta + CompositeTimestamp.MILLIS_PER_DAY);
            fail("Expected age off date greater than " + ageOffEventDays + " days from event date to fail");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testMaxEntropy() {
        long eventDate = -1L;
        long ageOff = -1L;

        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);

        assertEquals(-1L, compositeTS);
        // since the ageoff is equal to the event date, this is not considered a composite timestamp
        assertFalse(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        assertEquals(eventDate, CompositeTimestamp.getEventDate(compositeTS));
        assertEquals(ageOff, CompositeTimestamp.getAgeOffDate(compositeTS));
        assertEquals((eventDate - ageOff) / CompositeTimestamp.MILLIS_PER_DAY, CompositeTimestamp.getAgeOffDeltaDays(compositeTS));
    }

    @Test
    public void testDoomsday() {
        long compositeTs = Long.MAX_VALUE - 5L;

        long eventDate = CompositeTimestamp.getEventDate(compositeTs); // 4199-11-24
        long now = System.currentTimeMillis();

        Date endGame = new Date(TimeUnit.MILLISECONDS.toMillis(eventDate));
        assertTrue((now + (365L * CompositeTimestamp.MILLIS_PER_DAY)) < eventDate,
                        "Doomsday is " + endGame + ".  You have less than one year before timestamps roll over.  Get cracking.");

    }

    @Test
    public void testPostDoomsday() {
        long eventDate = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("4199-11-25T00:00:00Z")).toEpochMilli();
        long ageOff = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("4299-06-01T00:00:00Z")).toEpochMilli();

        try {
            CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);
            fail("Expected event date to fail");
        } catch (IllegalArgumentException e) {
            // expected
        }

    }

    @Test
    public void testMin() {
        long ts = CompositeTimestamp.getCompositeTimeStamp(CompositeTimestamp.MIN_EVENT_DATE, CompositeTimestamp.MIN_EVENT_DATE);
        long event = CompositeTimestamp.getEventDate(ts);
        long age = CompositeTimestamp.getEventDate(ts);
        assertEquals(event, age);
    }

    @Test
    public void testInvalid() {
        try {
            CompositeTimestamp.getEventDate(CompositeTimestamp.INVALID_TIMESTAMP);
            fail("Invalid timestamp not detected");
        } catch (IllegalArgumentException e) {

        }
        try {
            CompositeTimestamp.getAgeOffDate(CompositeTimestamp.INVALID_TIMESTAMP);
            fail("Invalid timestamp not detected");
        } catch (IllegalArgumentException e) {

        }
        try {
            CompositeTimestamp.isCompositeTimestamp(CompositeTimestamp.INVALID_TIMESTAMP);
            fail("Invalid timestamp not detected");
        } catch (IllegalArgumentException e) {

        }
    }

    protected boolean isOrdered(Long... times) {
        List<Long> list1 = new ArrayList<>(Arrays.asList(times));
        List<Long> list2 = new ArrayList<>(Arrays.asList(times));
        Collections.sort(list2);
        return list1.equals(list2);
    }

    protected boolean isOrdered(Comparator<Long> comparator, Long... times) {
        List<Long> list1 = new ArrayList<>(Arrays.asList(times));
        List<Long> list2 = new ArrayList<>(Arrays.asList(times));
        Collections.sort(list2, comparator);
        return list1.equals(list2);
    }

    @Test
    public void testOrderingAgeOffDates() {
        Calendar cal = Calendar.getInstance();
        long twoMonthsLater = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, -1);
        long aMonthLater = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, -1);
        long aDate = cal.getTimeInMillis();

        // test the same positive eventdate, but different ageoff dates
        long t1 = CompositeTimestamp.getCompositeTimeStamp(aDate, aDate);
        long t2 = CompositeTimestamp.getCompositeTimeStamp(aDate, aMonthLater);
        long t3 = CompositeTimestamp.getCompositeTimeStamp(aDate, twoMonthsLater);

        // in this case the natural ordering will be correct
        assertTrue(isOrdered(t1, t2, t3));
        // and the comparator will maintain that ordering
        assertTrue(isOrdered(CompositeTimestamp.comparator(), t1, t2, t3));

        cal = Calendar.getInstance();
        cal.setTimeInMillis(0);
        cal.add(Calendar.MONTH, -1);
        twoMonthsLater = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, -1);
        aMonthLater = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, -1);
        aDate = cal.getTimeInMillis();

        // test the same negative eventdate, but different ageoff dates
        t1 = CompositeTimestamp.getCompositeTimeStamp(aDate, aDate);
        t2 = CompositeTimestamp.getCompositeTimeStamp(aDate, aMonthLater);
        t3 = CompositeTimestamp.getCompositeTimeStamp(aDate, twoMonthsLater);

        // in this case the natural ordering will be incorrect ( and in fact exactly opposite )
        assertFalse(isOrdered(t1, t2, t3));
        assertTrue(isOrdered(t2, t2, t1));
        // but the comparator will maintain the correct ordering
        assertTrue(isOrdered(CompositeTimestamp.comparator(), t1, t2, t3));
    }

    @Test
    public void testOrderingEventDates() {
        Calendar cal = Calendar.getInstance();
        long twoMonthsLater = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, -1);
        long aMonthLater = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, -1);
        long aDate = cal.getTimeInMillis();

        // test different event dates with the equivalent ageoff dates
        long t1 = CompositeTimestamp.getCompositeTimeStamp(aDate, aDate);
        long t2 = CompositeTimestamp.getCompositeTimeStamp(aMonthLater, aMonthLater);
        long t3 = CompositeTimestamp.getCompositeTimeStamp(twoMonthsLater, twoMonthsLater);

        // in this case the natural ordering will be correct
        assertTrue(isOrdered(t1, t2, t3));
        // and the comparator will maintain that ordering
        assertTrue(isOrdered(CompositeTimestamp.comparator(), t1, t2, t3));

        cal = Calendar.getInstance();
        cal.setTimeInMillis(0);
        cal.add(Calendar.MONTH, -1);
        twoMonthsLater = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, -1);
        aMonthLater = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, -1);
        aDate = cal.getTimeInMillis();

        // test different negative event dates with the equivalent ageoff dates
        t1 = CompositeTimestamp.getCompositeTimeStamp(aDate, aDate);
        t2 = CompositeTimestamp.getCompositeTimeStamp(aMonthLater, aMonthLater);
        t3 = CompositeTimestamp.getCompositeTimeStamp(twoMonthsLater, twoMonthsLater);

        // in this case the natural ordering will be correct
        assertTrue(isOrdered(t1, t2, t3));
        // and the comparator will maintain that ordering
        assertTrue(isOrdered(CompositeTimestamp.comparator(), t1, t2, t3));
    }

    @Test
    public void testOrderingEventAndAgeoffDates() {
        Calendar cal = Calendar.getInstance();
        long twoMonthsLater = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, -1);
        long aMonthLater = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, -1);
        long aDate = cal.getTimeInMillis();

        // a mix of ageoff dates and event dates
        long t1 = CompositeTimestamp.getCompositeTimeStamp(aDate, twoMonthsLater);
        long t2 = CompositeTimestamp.getCompositeTimeStamp(aMonthLater, aMonthLater);
        long t3 = CompositeTimestamp.getCompositeTimeStamp(twoMonthsLater, twoMonthsLater);

        // in this case the natural ordering will be incorrect
        assertFalse(isOrdered(t1, t2, t3));
        // but the comparator will maintain the correct ordering
        assertTrue(isOrdered(CompositeTimestamp.comparator(), t1, t2, t3));

        cal = Calendar.getInstance();
        cal.setTimeInMillis(0);
        cal.add(Calendar.MONTH, -1);
        twoMonthsLater = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, -1);
        aMonthLater = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, -1);
        aDate = cal.getTimeInMillis();

        // a mix of negative ageoff dates and negative event dates
        t1 = CompositeTimestamp.getCompositeTimeStamp(aDate, twoMonthsLater);
        t2 = CompositeTimestamp.getCompositeTimeStamp(aMonthLater, aMonthLater);
        t3 = CompositeTimestamp.getCompositeTimeStamp(twoMonthsLater, twoMonthsLater);

        // in this case the natural ordering will be correct (surprisingly)
        assertTrue(isOrdered(t1, t2, t3));
        // and the comparator will maintain that ordering
        assertTrue(isOrdered(CompositeTimestamp.comparator(), t1, t2, t3));
    }

    /**
     * Verify that {@link CompositeTimestamp#computeAgeOffDeltaDays} accurately calculates the difference in days between an event date and an age off date for
     * a given time zone.
     */
    @Test
    public void testComputeAgeOffDeltaDays() {
        ZoneId zone = ZoneId.of("GMT");

        // Event Date: August 1, 2026 at 10:00 AM
        ZonedDateTime eventTime = ZonedDateTime.of(2026, 8, 1, 10, 0, 0, 0, zone);
        long eventDateMillis = eventTime.toInstant().toEpochMilli();

        // Event Date: August 6, 2026 at 3:00 PM
        ZonedDateTime ageOffTime = ZonedDateTime.of(2026, 8, 6, 15, 0, 0, 0, zone);
        long ageOffDateMillis = ageOffTime.toInstant().toEpochMilli();

        int deltaDays = CompositeTimestamp.computeAgeOffDeltaDays(eventDateMillis, ageOffDateMillis, zone);

        assertEquals(5, deltaDays);
    }

    /**
     * * Verify that {@link CompositeTimestamp#computeAgeOffDeltaDays} throws a ZoneRulesException when an invalid time zone ID is used.
     */
    @Test
    public void testComputeAgeOffDeltaDaysInvalidTimeZone() {
        // Event Date: August 1, 2026 at 10:00 AM
        long eventDateMillis = ZonedDateTime.of(LocalDateTime.of(2026, 8, 1, 10, 0, 0), ZoneId.of("GMT")).toInstant().toEpochMilli();

        // Event Date: August 6, 2026 at 3:00 PM
        long ageOffDateMillis = ZonedDateTime.of(LocalDateTime.of(2026, 8, 1, 15, 0, 0), ZoneId.of("GMT")).toInstant().toEpochMilli();

        assertThrows(ZoneRulesException.class, () -> CompositeTimestamp.computeAgeOffDeltaDays(eventDateMillis, ageOffDateMillis, ZoneId.of("GT3RS")));
    }

    /**
     * Verify that {@link CompositeTimestamp#getCompositeTimeStamp} throws a ZoneRulesException when an invalid time zone ID is used.
     */
    @Test
    public void testGetCompositeTimeStampInvalidTimeZone() {
        // Event Date: August 1, 2026 at 10:00 AM
        long eventDateMillis = ZonedDateTime.of(LocalDateTime.of(2026, 8, 1, 10, 0, 0), ZoneId.of("GMT")).toInstant().toEpochMilli();

        // Event Date: August 6, 2026 at 3:00 PM
        long ageOffDateMillis = ZonedDateTime.of(LocalDateTime.of(2026, 8, 1, 15, 0, 0), ZoneId.of("GMT")).toInstant().toEpochMilli();

        assertThrows(ZoneRulesException.class, () -> CompositeTimestamp.getCompositeTimeStamp(eventDateMillis, ageOffDateMillis, ZoneId.of("GTR")));

    }

    /**
     * Verify that {@link CompositeTimestamp#getCompositeTimeStamp} correctly handles and flips the sign for a negative event date (pre-1970).
     */
    @Test
    public void testGetCompositeTimeStampNegativeEventDate() {
        ZoneId zone = ZoneId.of("GMT");

        // A date before Jan 1 1970 will be a negative long
        long eventDateMillis = ZonedDateTime.of(1969, 8, 1, 10, 0, 0, 0, zone).toInstant().toEpochMilli();
        long ageOffDateMillis = ZonedDateTime.of(1969, 8, 6, 15, 0, 0, 0, zone).toInstant().toEpochMilli();

        long compositeTs = CompositeTimestamp.getCompositeTimeStamp(eventDateMillis, ageOffDateMillis, zone);

        assertTrue(compositeTs < 0, "Composite timestamp should be negative when event date is negative");
    }

    /**
     * Verify that {@link CompositeTimestamp#computeAgeOffDeltaDays} throws an exception when the calculated delta is invalid (e.g., negative delta or exceeding
     * bit allocation).
     */
    @Test
    public void testComputeAgeOffDeltaDaysInvalidDelta() {
        ZoneId zone = ZoneId.of("GMT");
        long eventDateMillis = ZonedDateTime.of(2026, 8, 10, 10, 0, 0, 0, zone).toInstant().toEpochMilli();

        // Age off date is before the event date, resulting in a negative delta
        long ageOffDateMillis = ZonedDateTime.of(2026, 8, 1, 15, 0, 0, 0, zone).toInstant().toEpochMilli();

        assertThrows(IllegalArgumentException.class, () -> CompositeTimestamp.computeAgeOffDeltaDays(eventDateMillis, ageOffDateMillis, zone));

    }

    /**
     * Verify that {@link CompositeTimestamp#computeAgeOffDeltaDays} returns a delta of 0 when both dates fall on the same day in the given time zone.
     */
    @Test
    public void testComputeAgeOffDeltaDaysSameDay() {
        ZoneId zone = ZoneId.of("GMT");

        // Both are on August 1, 2026
        long eventDateMillis = ZonedDateTime.of(2026, 8, 1, 1, 0, 0, 0, zone).toInstant().toEpochMilli();
        long ageOffDateMillis = ZonedDateTime.of(2026, 8, 1, 23, 0, 0, 0, zone).toInstant().toEpochMilli();

        int deltaDays = CompositeTimestamp.computeAgeOffDeltaDays(eventDateMillis, ageOffDateMillis, zone);

        assertEquals(0, deltaDays);
    }
}
