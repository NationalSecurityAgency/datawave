package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Calendar;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import datawave.util.time.DateHelper;

@ResourceLock(Resources.TIME_ZONE)
public class YearMonthDayTest {

    @ParameterizedTest
    @CsvSource({"20240229, 2024, 60", "20000229, 2000, 60", "21000301, 2100, 60", "20241231, 2024, 366", "20230229, 2023, 60", "20240001, 2023, 335",
            "20240100, 2023, 365", "202101001, 2021, 1"})
    public void testDateFieldsAndLenientInput(String value, int year, int ordinal) {
        YearMonthDay day = new YearMonthDay(value);
        assertEquals(value, day.getYyyymmdd());
        assertEquals(year, day.getYear());
        assertEquals(ordinal, day.getJulian());
    }

    @ParameterizedTest
    @CsvSource({"2024, 60, 20240229", "2000, 60, 20000229", "2100, 60, 21000301", "2023, 366, 20240101", "2024, 367, 20250101", "2024, 0, 20231231",
            "2024, -1, 20231230"})
    public void testOrdinalRollover(int year, int ordinal, String expected) {
        TimeZone previous = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            YearMonthDay day = new YearMonthDay(year, ordinal);
            assertEquals(expected, day.getYyyymmdd());
            assertEquals(new YearMonthDay(expected).getYear(), day.getYear());
            assertEquals(new YearMonthDay(expected).getJulian(), day.getJulian());
            assertEquals(expected, YearMonthDay.calculateMMDD(ordinal, year));
        } finally {
            TimeZone.setDefault(previous);
        }
    }

    @ParameterizedTest
    @CsvSource({"20240228, 20240229", "20240229, 20240301", "20231231, 20240101", "21000228, 21000301"})
    public void testDayArithmetic(String previousDay, String nextDay) {
        TimeZone previous = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            YearMonthDay next = YearMonthDay.nextDay(previousDay);
            YearMonthDay before = YearMonthDay.previousDay(nextDay);
            assertEquals(new YearMonthDay(nextDay), next);
            assertEquals(new YearMonthDay(nextDay).getJulian(), next.getJulian());
            assertEquals(new YearMonthDay(previousDay), before);
            assertEquals(new YearMonthDay(previousDay).getYear(), before.getYear());
        } finally {
            TimeZone.setDefault(previous);
        }
    }

    @ParameterizedTest
    @CsvSource({"UTC", "America/New_York", "Pacific/Kiritimati", "Asia/Kathmandu", "America/Sao_Paulo"})
    public void testCalendarCompatibilityInDefaultTimeZone(String zone) {
        TimeZone previous = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zone));
            Calendar calendar = Calendar.getInstance();
            calendar.clear();
            calendar.set(2024, Calendar.FEBRUARY, 29);
            YearMonthDay fromOrdinal = new YearMonthDay(2024, 60);
            assertEquals(calendar.get(Calendar.YEAR), fromOrdinal.getYear());
            assertEquals(calendar.get(Calendar.DAY_OF_YEAR), fromOrdinal.getJulian());
            assertEquals(DateHelper.format(calendar.getTime()), fromOrdinal.getYyyymmdd());
            calendar.add(Calendar.DATE, 1);
            assertEquals(DateHelper.format(calendar.getTime()), YearMonthDay.nextDay("20240229").getYyyymmdd());
            calendar.add(Calendar.DATE, -2);
            assertEquals(DateHelper.format(calendar.getTime()), YearMonthDay.previousDay("20240229").getYyyymmdd());
            assertEquals(60, YearMonthDay.calculateJulian("20240229"));
            Calendar current = Calendar.getInstance();
            current.set(Calendar.YEAR, 2024);
            current.set(Calendar.DAY_OF_YEAR, 60);
            assertEquals(DateHelper.format(current.getTime()), YearMonthDay.calculateMMDD(60, 2024));
        } finally {
            TimeZone.setDefault(previous);
        }
    }

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
