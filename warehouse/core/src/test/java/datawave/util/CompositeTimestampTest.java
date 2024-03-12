package datawave.util;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Date;
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
    }

    @Test
    public void testVeryOldDateWithMaxAgeoff() {
        long eventDate = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("1654-01-01T00:00:00Z")).toEpochMilli();
        long ageOff = eventDate + (131071L * CompositeTimestamp.MILLIS_PER_DAY);
        long expectedTS = -9223311640052998144L;

        long compositeTs1 = Long.MIN_VALUE + 1;
        long eventDate1 = CompositeTimestamp.getEventDate(compositeTs1);

        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);

        Assert.assertEquals(expectedTS, compositeTS);
        Assert.assertTrue(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(eventDate));
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(ageOff));
        Assert.assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), ageOff);
    }

    @Test
    public void testEventUpperDateBound() {
        long eventDate = (-1L >>> 18);
        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, eventDate);
        Assert.assertEquals(eventDate, compositeTS);
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        Assert.assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), eventDate);

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

        try {
            CompositeTimestamp.getCompositeTimeStamp(eventDate, eventDate - 1);
            Assert.fail("Expected ageoff date less than event date to fail");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            CompositeTimestamp.getCompositeTimeStamp(1, 0);
            Assert.fail("Expected ageoff date less than event date to fail");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            CompositeTimestamp.getCompositeTimeStamp(eventDate, eventDate + ageOffEventDelta + 1);
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

}
