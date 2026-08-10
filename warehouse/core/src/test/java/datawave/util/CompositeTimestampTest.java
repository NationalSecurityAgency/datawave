package datawave.util;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class CompositeTimestampTest {

    @Test
    public void testConversion() {
        long eventDate = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2022-10-26T01:00:00Z")).toEpochMilli();
        long ageOff = eventDate + CompositeTimestamp.MILLIS_PER_DAY;
        long expectedTS = 72035490177664L;

        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);

        Assert.assertEquals(expectedTS, compositeTS);
        Assert.assertTrue(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(eventDate));
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(ageOff));
        Assert.assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), ageOff);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDeltaDays(compositeTS), (ageOff - eventDate) / CompositeTimestamp.MILLIS_PER_DAY);
    }

    @Test
    public void testOldDateWithOldAgeoff() {
        long eventDate = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("1900-01-01T00:00:00Z")).toEpochMilli();
        long ageOff = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("1950-01-01T00:00:00Z")).toEpochMilli();
        long expectedTS = -1285076215161299968L;

        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);

        Assert.assertEquals(expectedTS, compositeTS);
        Assert.assertTrue(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(eventDate));
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(ageOff));
        Assert.assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), ageOff);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDeltaDays(compositeTS), (ageOff - eventDate) / CompositeTimestamp.MILLIS_PER_DAY);
    }

    @Test
    public void testOldDateWithModernAgeoff() {
        long eventDate = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("1960-01-01T00:00:00Z")).toEpochMilli();
        long ageOff = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2025-01-01T00:00:00Z")).toEpochMilli();
        long expectedTS = -1670695039885298688L;

        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);

        Assert.assertEquals(expectedTS, compositeTS);
        Assert.assertTrue(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(eventDate));
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(ageOff));
        Assert.assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), ageOff);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDeltaDays(compositeTS), (ageOff - eventDate) / CompositeTimestamp.MILLIS_PER_DAY);
    }

    @Test
    public void testOldDateWithMaxAgeoff() {
        long eventDate = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("1960-01-01T00:00:00Z")).toEpochMilli();
        long ageOff = eventDate + (131071L * CompositeTimestamp.MILLIS_PER_DAY);
        long expectedTS = -9223301983729798144L;

        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);

        Assert.assertEquals(expectedTS, compositeTS);
        Assert.assertTrue(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(eventDate));
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(ageOff));
        Assert.assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), ageOff);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDeltaDays(compositeTS), (ageOff - eventDate) / CompositeTimestamp.MILLIS_PER_DAY);
    }

    @Test
    public void testVeryOldDateWithMaxAgeoff() {
        long eventDate = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("1654-01-01T00:00:00Z")).toEpochMilli();
        long ageOff = eventDate + (131071L * CompositeTimestamp.MILLIS_PER_DAY);
        long expectedTS = -9223311640052998144L;

        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);

        Assert.assertEquals(expectedTS, compositeTS);
        Assert.assertTrue(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(eventDate));
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(ageOff));
        Assert.assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), ageOff);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDeltaDays(compositeTS), (ageOff - eventDate) / CompositeTimestamp.MILLIS_PER_DAY);
    }

    @Test
    public void testEventUpperDateBound() {
        long eventDate = (-1L >>> 18);
        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, eventDate);
        Assert.assertEquals(eventDate, compositeTS);
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        Assert.assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), eventDate);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDeltaDays(compositeTS), (eventDate - eventDate) / CompositeTimestamp.MILLIS_PER_DAY);

        try {
            CompositeTimestamp.getCompositeTimeStamp(eventDate + 1, 0);
            Assert.fail("Expected event date greater than 17 bits to fail");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testEventLowerDateBound() {
        long eventDate = (0 - (-1L >>> 18));
        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, eventDate);
        Assert.assertEquals(eventDate, compositeTS);
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        Assert.assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), eventDate);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDeltaDays(compositeTS), (eventDate - eventDate) / CompositeTimestamp.MILLIS_PER_DAY);

        try {
            CompositeTimestamp.getCompositeTimeStamp(eventDate - 1, 0);
            Assert.fail("Expected event date greater than 17 bits to fail");
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
        Assert.assertTrue(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        Assert.assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), eventDate + ageOffEventDelta);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDeltaDays(compositeTS), (ageOffEventDelta) / CompositeTimestamp.MILLIS_PER_DAY);

        try {
            CompositeTimestamp.getCompositeTimeStamp(eventDate, eventDate - CompositeTimestamp.MILLIS_PER_DAY);
            Assert.fail("Expected ageoff date less than event date to fail");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            CompositeTimestamp.getCompositeTimeStamp(CompositeTimestamp.MILLIS_PER_DAY, 0);
            Assert.fail("Expected ageoff date less than event date to fail");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            CompositeTimestamp.getCompositeTimeStamp(eventDate, eventDate + ageOffEventDelta + CompositeTimestamp.MILLIS_PER_DAY);
            Assert.fail("Expected age off date greater than " + ageOffEventDays + " days from event date to fail");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testMaxEntropy() {
        long eventDate = -1L;
        long ageOff = -1L;

        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);

        Assert.assertEquals(-1L, compositeTS);
        // since the ageoff is equal to the event date, this is not considered a composite timestamp
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        Assert.assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), ageOff);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDeltaDays(compositeTS), (eventDate - ageOff) / CompositeTimestamp.MILLIS_PER_DAY);
    }

    @Test
    public void testDoomsday() {
        long compositeTs = Long.MAX_VALUE - 5L;

        long eventDate = CompositeTimestamp.getEventDate(compositeTs); // 4199-11-24
        long now = System.currentTimeMillis();

        Date endGame = new Date(TimeUnit.MILLISECONDS.toMillis(eventDate));
        Assert.assertTrue("Doomsday is " + endGame + ".  You have less than one year before timestamps roll over.  Get cracking.",
                        (now + (365L * CompositeTimestamp.MILLIS_PER_DAY)) < eventDate);

    }

    @Test
    public void testPostDoomsday() {
        long eventDate = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("4199-11-25T00:00:00Z")).toEpochMilli();
        long ageOff = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("4299-06-01T00:00:00Z")).toEpochMilli();

        try {
            CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);
            Assert.fail("Expected event date to fail");
        } catch (IllegalArgumentException e) {
            // expected
        }

    }

    @Test
    public void testMin() {
        long ts = CompositeTimestamp.getCompositeTimeStamp(CompositeTimestamp.MIN_EVENT_DATE, CompositeTimestamp.MIN_EVENT_DATE);
        long event = CompositeTimestamp.getEventDate(ts);
        long age = CompositeTimestamp.getEventDate(ts);
        Assert.assertEquals(event, age);
    }

    @Test
    public void testInvalid() {
        try {
            CompositeTimestamp.getEventDate(CompositeTimestamp.INVALID_TIMESTAMP);
            Assert.fail("Invalid timestamp not detected");
        } catch (IllegalArgumentException e) {

        }
        try {
            CompositeTimestamp.getAgeOffDate(CompositeTimestamp.INVALID_TIMESTAMP);
            Assert.fail("Invalid timestamp not detected");
        } catch (IllegalArgumentException e) {

        }
        try {
            CompositeTimestamp.isCompositeTimestamp(CompositeTimestamp.INVALID_TIMESTAMP);
            Assert.fail("Invalid timestamp not detected");
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
        Assert.assertTrue(isOrdered(t1, t2, t3));
        // and the comparator will maintain that ordering
        Assert.assertTrue(isOrdered(CompositeTimestamp.comparator(), t1, t2, t3));

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
        Assert.assertFalse(isOrdered(t1, t2, t3));
        Assert.assertTrue(isOrdered(t2, t2, t1));
        // but the comparator will maintain the correct ordering
        Assert.assertTrue(isOrdered(CompositeTimestamp.comparator(), t1, t2, t3));
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
        Assert.assertTrue(isOrdered(t1, t2, t3));
        // and the comparator will maintain that ordering
        Assert.assertTrue(isOrdered(CompositeTimestamp.comparator(), t1, t2, t3));

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
        Assert.assertTrue(isOrdered(t1, t2, t3));
        // and the comparator will maintain that ordering
        Assert.assertTrue(isOrdered(CompositeTimestamp.comparator(), t1, t2, t3));
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
        Assert.assertFalse(isOrdered(t1, t2, t3));
        // but the comparator will maintain the correct ordering
        Assert.assertTrue(isOrdered(CompositeTimestamp.comparator(), t1, t2, t3));

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
        Assert.assertTrue(isOrdered(t1, t2, t3));
        // and the comparator will maintain that ordering
        Assert.assertTrue(isOrdered(CompositeTimestamp.comparator(), t1, t2, t3));
    }

}
