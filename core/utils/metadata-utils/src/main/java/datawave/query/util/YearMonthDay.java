package datawave.query.util;

import java.util.Calendar;

import datawave.util.time.DateHelper;

public class YearMonthDay implements Comparable<YearMonthDay> {
    public static final int DAYS_IN_LEAP_YEAR = 366;
    private String yyyymmdd;
    private Calendar cal = Calendar.getInstance();
    
    public YearMonthDay(String value) {
        yyyymmdd = value;
        cal.clear();
        cal.set(Integer.parseInt(value.substring(0, 4)), Integer.parseInt(value.substring(4, 6)) - 1, Integer.parseInt(value.substring(6)));
    }
    
    public YearMonthDay(int year, int julian) {
        cal.clear();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.DAY_OF_YEAR, julian);
        yyyymmdd = DateHelper.format(cal.getTime());
    }
    
    public int getYear() {
        return cal.get(Calendar.YEAR);
    }
    
    public int getJulian() {
        return cal.get(Calendar.DAY_OF_YEAR);
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
        YearMonthDay ymd = new YearMonthDay(value);
        ymd.cal.add(Calendar.DATE, 1);
        ymd.yyyymmdd = DateHelper.format(ymd.cal.getTime());
        return ymd;
    }
    
    public static YearMonthDay previousDay(String value) {
        YearMonthDay ymd = new YearMonthDay(value);
        ymd.cal.add(Calendar.DATE, -1);
        ymd.yyyymmdd = DateHelper.format(ymd.cal.getTime());
        return ymd;
    }
    
    public static String calculateMMDD(int ordinal, int year) {
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, year);
        c.set(Calendar.DAY_OF_YEAR, ordinal);
        return DateHelper.format(c.getTime());
    }
    
    public static int calculateJulian(String value) {
        int year = Integer.parseInt(value.substring(0, 4));
        String month = value.substring(4, 6);
        String day = value.substring(6, 8);
        int nmonth = month.charAt(0) == '0' ? Integer.parseInt(month.substring(1, 2)) : Integer.parseInt(month);
        int nday = day.charAt(0) == '0' ? Integer.parseInt(day.substring(1, 2)) : Integer.parseInt(day);
        Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.set(year, nmonth - 1, nday);
        return cal.get(Calendar.DAY_OF_YEAR);
    }
    
}
