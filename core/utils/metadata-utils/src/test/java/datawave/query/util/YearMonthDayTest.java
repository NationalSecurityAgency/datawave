package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class YearMonthDayTest {
    
    @Test
    public void testGetYear() {
        YearMonthDay ymd = new YearMonthDay("20210101");
        assertEquals(2021, ymd.getYear());
    }
    
    @Test
    public void testGetJulian() {
        YearMonthDay ymd = new YearMonthDay("202101001");
        assertEquals(1, ymd.getJulian());
    }
    
    @Test
    public void testGetYyyymmdd() {
        YearMonthDay ymd = new YearMonthDay("20210101");
        assertEquals("20210101", ymd.getYyyymmdd());
    }
    
    @Test
    public void testCompareTo() {
        YearMonthDay ymd1 = new YearMonthDay("20210101");
        YearMonthDay ymd2 = new YearMonthDay("20210102");
        assertEquals(1, ymd2.compareTo(ymd1));
        assertEquals(-1, ymd1.compareTo(ymd2));
        assertEquals(0, ymd1.compareTo(ymd1));
    }
    
    @Test
    public void testTestEquals() {
        YearMonthDay ymd1 = new YearMonthDay("20210101");
        YearMonthDay ymd2 = new YearMonthDay("20210101");
        YearMonthDay ymd3 = new YearMonthDay("20210102");
        assertEquals(ymd1, ymd1);
        assertEquals(ymd1, ymd2);
        assertEquals(ymd1.hashCode(), ymd2.hashCode());
        assertNotEquals(ymd1, ymd3);
    }
    
    @Test
    public void testNextDay() {
        YearMonthDay ymd1 = new YearMonthDay("20210101");
        YearMonthDay ymd2 = new YearMonthDay("20210102");
        assertEquals(ymd2, YearMonthDay.nextDay(ymd1.getYyyymmdd()));
    }
    
    @Test
    public void testPreviousDay() {
        YearMonthDay ymd1 = new YearMonthDay("20210101");
        YearMonthDay ymd2 = new YearMonthDay("20210102");
        assertEquals(ymd1, YearMonthDay.previousDay(ymd2.getYyyymmdd()));
    }
    
    @Test
    public void testCalculateMMDD() {
        assertEquals("20210101", YearMonthDay.calculateMMDD(1, 2021));
        assertEquals("20211231", YearMonthDay.calculateMMDD(365, 2021));
    }
    
    @Test
    public void testCalculateJulian() {
        assertEquals(1, YearMonthDay.calculateJulian("20210101"));
        assertEquals(365, YearMonthDay.calculateJulian("20211231"));
    }
    
    @Test
    public void testWithingBounds() {
        YearMonthDay.Bounds bounds = new YearMonthDay.Bounds("20210101", true, "20210102", true);
        assertTrue(bounds.withinBounds("20210101"));
        assertTrue(bounds.withinBounds("20210102"));
        assertFalse(bounds.withinBounds("20210103"));
        bounds = new YearMonthDay.Bounds("20210101", false, "20210103", false);
        assertFalse(bounds.withinBounds("20210101"));
        assertTrue(bounds.withinBounds("20210102"));
        assertFalse(bounds.withinBounds("20210103"));
    }
    
    @Test
    public void testIntersectsYear() {
        YearMonthDay.Bounds bounds = new YearMonthDay.Bounds("20201231", false, "202101231", true);
        assertFalse(bounds.intersectsYear("2020"));
        assertTrue(bounds.intersectsYear("2021"));
        assertFalse(bounds.intersectsYear("2022"));
        bounds = new YearMonthDay.Bounds("20201231", true, "20220101", false);
        assertTrue(bounds.intersectsYear("2020"));
        assertTrue(bounds.intersectsYear("2021"));
        assertFalse(bounds.intersectsYear("2022"));
    }
    
    @Test
    public void testStartOrdinal() {
        YearMonthDay.Bounds bounds = new YearMonthDay.Bounds("20201231", false, "20211231", true);
        assertEquals(1, bounds.getStartOrdinal("2020"));
        assertEquals(1, bounds.getStartOrdinal("2021"));
        assertEquals(1, bounds.getStartOrdinal("2022"));
        bounds = new YearMonthDay.Bounds("20201231", true, "20220101", false);
        assertEquals(366, bounds.getStartOrdinal("2020"));
        assertEquals(1, bounds.getStartOrdinal("2021"));
        assertEquals(1, bounds.getStartOrdinal("2022"));
    }
    
    @Test
    public void testEndOrdinal() {
        YearMonthDay.Bounds bounds = new YearMonthDay.Bounds("20201231", false, "20211231", true);
        assertEquals(366, bounds.getEndOrdinal("2020"));
        assertEquals(365, bounds.getEndOrdinal("2021"));
        assertEquals(366, bounds.getEndOrdinal("2022"));
        bounds = new YearMonthDay.Bounds("20201231", true, "20220101", false);
        assertEquals(366, bounds.getEndOrdinal("2020"));
        assertEquals(365, bounds.getEndOrdinal("2021"));
        assertEquals(366, bounds.getEndOrdinal("2022"));
    }
}
