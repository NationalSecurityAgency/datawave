package datawave.test.framework.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import datawave.util.time.DateHelper;

public class ShardKeyUtilTest {

    @Test
    public void testSingleDayMatchesBaseDate() {
        assertEquals(ShardKeyUtil.BASE_DATE, ShardKeyUtil.buildDate(0, 10, 1));
        assertEquals(ShardKeyUtil.BASE_DATE, ShardKeyUtil.buildDate(37, 10, 1));
        assertEquals(ShardKeyUtil.BASE_DATE + "_7", ShardKeyUtil.buildRow(37, 10, 1));
    }

    @Test
    public void testMultiDayRollover() {
        int numShards = 10;
        int numDays = 3;

        // eventId 0-9 -> day 0, eventId 10-19 -> day 1, eventId 20-29 -> day 2, eventId 30-39 -> day 0 again
        assertEquals("20260202", ShardKeyUtil.buildDate(0, numShards, numDays));
        assertEquals("20260202", ShardKeyUtil.buildDate(9, numShards, numDays));
        assertEquals("20260203", ShardKeyUtil.buildDate(10, numShards, numDays));
        assertEquals("20260203", ShardKeyUtil.buildDate(19, numShards, numDays));
        assertEquals("20260204", ShardKeyUtil.buildDate(20, numShards, numDays));
        assertEquals("20260204", ShardKeyUtil.buildDate(29, numShards, numDays));
        assertEquals("20260202", ShardKeyUtil.buildDate(30, numShards, numDays));

        // shard rolls over every numShards events, independent of the day
        assertEquals("20260202_0", ShardKeyUtil.buildRow(0, numShards, numDays));
        assertEquals("20260202_9", ShardKeyUtil.buildRow(9, numShards, numDays));
        assertEquals("20260203_0", ShardKeyUtil.buildRow(10, numShards, numDays));
        assertEquals("20260204_5", ShardKeyUtil.buildRow(25, numShards, numDays));
    }

    @Test
    public void testInvalidNumShards() {
        Exception e1 = assertThrows(IllegalArgumentException.class, () -> ShardKeyUtil.buildRow(0, 0, 1));
        assertEquals("numShards must be greater than 0", e1.getMessage());

        Exception e2 = assertThrows(IllegalArgumentException.class, () -> ShardKeyUtil.buildRow(0, -1, 1));
        assertEquals("numShards must be greater than 0", e2.getMessage());

        Exception e3 = assertThrows(IllegalArgumentException.class, () -> ShardKeyUtil.buildTimestamp(0, 0, 1));
        assertEquals("numShards must be greater than 0", e3.getMessage());

        Exception e4 = assertThrows(IllegalArgumentException.class, () -> ShardKeyUtil.buildIndexTimestamp(0, 0, 1));
        assertEquals("numShards must be greater than 0", e4.getMessage());
    }

    @Test
    public void testInvalidNumDays() {
        Exception e1 = assertThrows(IllegalArgumentException.class, () -> ShardKeyUtil.buildRow(0, 10, 0));
        assertEquals("numDays must be greater than 0", e1.getMessage());

        Exception e2 = assertThrows(IllegalArgumentException.class, () -> ShardKeyUtil.buildRow(0, 10, -1));
        assertEquals("numDays must be greater than 0", e2.getMessage());

        Exception e3 = assertThrows(IllegalArgumentException.class, () -> ShardKeyUtil.buildTimestamp(0, 10, 0));
        assertEquals("numDays must be greater than 0", e3.getMessage());

        Exception e4 = assertThrows(IllegalArgumentException.class, () -> ShardKeyUtil.buildIndexTimestamp(0, 10, 0));
        assertEquals("numDays must be greater than 0", e4.getMessage());
    }

    @Test
    public void testTimestampSpreadAcrossDay() {
        int numShards = 10;
        int numDays = 1;
        long dayStart = DateHelper.parse(ShardKeyUtil.BASE_DATE).getTime();
        long millisPerShard = (24L * 60L * 60L * 1000L) / numShards;

        // shard 0 lands exactly at the start of the day
        assertEquals(dayStart, ShardKeyUtil.buildTimestamp(0, numShards, numDays));

        // each subsequent shard is later in the day, spaced evenly
        for (int eventId = 1; eventId < numShards; eventId++) {
            long expected = dayStart + eventId * millisPerShard;
            assertEquals(expected, ShardKeyUtil.buildTimestamp(eventId, numShards, numDays));
        }

        // every timestamp for the day falls within [dayStart, dayStart + 24h)
        for (int eventId = 0; eventId < numShards; eventId++) {
            long timestamp = ShardKeyUtil.buildTimestamp(eventId, numShards, numDays);
            assertTrue(timestamp >= dayStart && timestamp < dayStart + (24L * 60L * 60L * 1000L));
        }
    }

    @Test
    public void testTimestampAdvancesWithDay() {
        int numShards = 10;
        int numDays = 2;

        // eventId 5 (day 0, shard 5) should be earlier than eventId 15 (day 1, shard 5)
        long day0Timestamp = ShardKeyUtil.buildTimestamp(5, numShards, numDays);
        long day1Timestamp = ShardKeyUtil.buildTimestamp(15, numShards, numDays);
        assertEquals(24L * 60L * 60L * 1000L, day1Timestamp - day0Timestamp);
    }

    @Test
    public void testIndexTimestampIsTruncatedToMidnight() {
        int numShards = 10;
        int numDays = 2;
        long day0Start = DateHelper.parse(ShardKeyUtil.BASE_DATE).getTime();

        // unlike buildTimestamp, buildIndexTimestamp is midnight for every shard on the same day
        for (int eventId = 0; eventId < numShards; eventId++) {
            assertEquals(day0Start, ShardKeyUtil.buildIndexTimestamp(eventId, numShards, numDays));
        }

        // it still advances by a full day once the day offset rolls over
        long day1Start = ShardKeyUtil.buildIndexTimestamp(numShards, numShards, numDays);
        assertEquals(day0Start + 24L * 60L * 60L * 1000L, day1Start);
    }
}
