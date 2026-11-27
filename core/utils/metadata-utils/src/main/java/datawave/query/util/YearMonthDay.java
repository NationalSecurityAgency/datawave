package datawave.query.util;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

import datawave.util.time.DateHelper;

public class YearMonthDay implements Comparable<YearMonthDay> {
    public static final int DAYS_IN_LEAP_YEAR = 366;
    private final String yyyymmdd;
    private final int year;
    private final int julian;

    public YearMonthDay(String value) {
        LocalDate date = parse(value);
        yyyymmdd = value;
        year = date.getYear();
        julian = date.getDayOfYear();
    }

    public YearMonthDay(int year, int julian) {
        this(LocalDate.of(year, 1, 1).plusDays((long) julian - 1));
    }

    private YearMonthDay(LocalDate date) {
        year = date.getYear();
        julian = date.getDayOfYear();
        yyyymmdd = DateHelper.format(Date.from(date.atStartOfDay(ZoneId.systemDefault()).toInstant()));
    }

    private static LocalDate parse(String value) {
        int year = Integer.parseInt(value.substring(0, 4));
        int month = Integer.parseInt(value.substring(4, 6));
        int day = Integer.parseInt(value.substring(6));
        return LocalDate.of(year, 1, 1).plusMonths((long) month - 1).plusDays((long) day - 1);
    }

    public int getYear() {
        return year;
    }

    public int getJulian() {
        return julian;
    }

    public String getYyyymmdd() {
        return yyyymmdd;
    }

    @Override
    public String toString() {
        return "String date: " + yyyymmdd + " ordinal day: " + getJulian();
    }

    @Override
    public int compareTo(YearMonthDay yearMonthDay) {
        return yyyymmdd.compareTo(yearMonthDay.yyyymmdd);
    }

    @Override
    public int hashCode() {
        return yyyymmdd.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof YearMonthDay) {
            YearMonthDay yearMonthDay = (YearMonthDay) o;
            return yyyymmdd.equals(yearMonthDay.yyyymmdd);
        }
        return false;
    }

    public static class Bounds {
        String start;
        String end;

        public Bounds(String start, boolean startInclusive, String end, boolean endInclusive) {
            this.start = (startInclusive ? start : (start == null ? null : nextDay(start).getYyyymmdd()));
            this.end = (endInclusive ? end : (end == null ? end : previousDay(end).getYyyymmdd()));
        }

        public boolean withinBounds(String yyyymmdd) {
            return ((start == null || yyyymmdd.compareTo(start) >= 0) && (end == null || yyyymmdd.compareTo(end) <= 0));
        }

        public boolean withinBounds(YearMonthDay ymd) {
            return withinBounds(ymd.getYyyymmdd());
        }

        public boolean intersectsYear(String year) {
            return ((start == null || (year.compareTo(start.substring(0, 4)) >= 0)) && (end == null || (year.compareTo(end.substring(0, 4)) <= 0)));
        }

        public int getStartOrdinal(String year) {
            int julian = 1;
            if (start != null && year.equals(start.substring(0, 4))) {
                julian = calculateJulian(start);
            }
            return julian;
        }

        public int getEndOrdinal(String year) {
            int julian = DAYS_IN_LEAP_YEAR;
            if (end != null && year.equals(end.substring(0, 4))) {
                julian = calculateJulian(end);
            }
            return julian;
        }
    }

    public static YearMonthDay nextDay(String value) {
        return new YearMonthDay(parse(value).plusDays(1));
    }

    public static YearMonthDay previousDay(String value) {
        return new YearMonthDay(parse(value).minusDays(1));
    }

    public static String calculateMMDD(int ordinal, int year) {
        ZonedDateTime date = ZonedDateTime.now().withDayOfYear(1).withYear(year).plusDays((long) ordinal - 1);
        return DateHelper.format(Date.from(date.toInstant()));
    }

    public static int calculateJulian(String value) {
        return parse(value.substring(0, 8)).getDayOfYear();
    }

}
