package datawave.test.framework.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

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
    }

    @Test
    public void testInvalidNumDays() {
        Exception e1 = assertThrows(IllegalArgumentException.class, () -> ShardKeyUtil.buildRow(0, 10, 0));
        assertEquals("numDays must be greater than 0", e1.getMessage());

        Exception e2 = assertThrows(IllegalArgumentException.class, () -> ShardKeyUtil.buildRow(0, 10, -1));
        assertEquals("numDays must be greater than 0", e2.getMessage());
    }
}
