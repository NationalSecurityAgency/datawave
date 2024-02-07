package datawave.util;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

import org.junit.Assert;
import org.junit.Test;

public class CompositeTimestampTest {

    @Test
    public void testConversion() {
        long eventDate = Instant.from(DateTimeFormatter.ISO_INSTANT.parse("2022-10-26T01:00:00Z")).toEpochMilli();
        long ageOff = eventDate + CompositeTimestamp.MILLIS_PER_DAY;
        long expectedTS = 142404234355328L;

        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, ageOff);

        Assert.assertEquals(expectedTS, compositeTS);
        Assert.assertTrue(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(eventDate));
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(ageOff));
        Assert.assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), eventDate + CompositeTimestamp.MILLIS_PER_DAY);
    }

    @Test
    public void testEventDateBounds() {
        long eventDate = (-1L >>> 17);
        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, eventDate);
        Assert.assertEquals(eventDate, compositeTS);
        Assert.assertFalse(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        Assert.assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), eventDate);

        try {
            CompositeTimestamp.getCompositeTimeStamp(-1, 0);
            Assert.fail("Expected negative event date to fail");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            CompositeTimestamp.getCompositeTimeStamp(eventDate + 1, 0);
            Assert.fail("Expected event date greater than 17 bits to fail");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testAgeOffDateBounds() {
        long eventDate = (-1L >>> 17);
        long ageOffEventDays = (-1L >>> 47);
        long ageOffEventDate = ageOffEventDays * 1000 * 60 * 60 * 24;
        long compositeTS = CompositeTimestamp.getCompositeTimeStamp(eventDate, eventDate + ageOffEventDate);
        Assert.assertEquals(-1L, compositeTS);
        Assert.assertTrue(CompositeTimestamp.isCompositeTimestamp(compositeTS));
        Assert.assertEquals(CompositeTimestamp.getEventDate(compositeTS), eventDate);
        Assert.assertEquals(CompositeTimestamp.getAgeOffDate(compositeTS), eventDate + ageOffEventDate);

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
            CompositeTimestamp.getCompositeTimeStamp(eventDate, eventDate + ageOffEventDate + 1);
            Assert.fail("Expected age off date greater than " + ageOffEventDays + " days from event date to fail");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

}
