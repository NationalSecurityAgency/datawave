package datawave.util.time;

import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.TimeZone;

public class DateHelperTest {
    
    public static final String DATE_HELPER_SHOULD_IGNORE_TRAILING_CHARS = "SimpleDateFormat is lenient with extra characters and we rely upon that, so DateHelper needs to.";
    public static final String TO_SECOND = "yyyyMMddHHmmss";
    public static final String TO_DAY = "yyyyMMdd";
    
    @Test
    public void parseIgnoresTrailingCharacters() throws Exception {
        String dateStrWithTrailingCharacters = "20130201_1";
        
        SimpleDateFormat sdf = new SimpleDateFormat(TO_DAY);
        sdf.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
        long expected = sdf.parse(dateStrWithTrailingCharacters).getTime();
        long actual = DateHelper.parse(dateStrWithTrailingCharacters).getTime();
        Assert.assertEquals(DATE_HELPER_SHOULD_IGNORE_TRAILING_CHARS, expected, actual);
    }
    
    @Test
    public void testFormatOfParseIsIdentityFunction() throws Exception {
        String secondsInput = "20150102030459";
        Assert.assertEquals(secondsInput, DateHelper.formatToTimeExactToSeconds(DateHelper.parseTimeExactToSeconds(secondsInput)));
        
        String dayInput = "20150127";
        Assert.assertEquals(dayInput, DateHelper.format(DateHelper.parse(dayInput)));
    }
    
    @Test
    public void testFormatToTimeExactToSeconds() {
        long millisInput = 1436463044319L;
        Assert.assertEquals("20150709173044", DateHelper.formatToTimeExactToSeconds(millisInput));
    }
    
    @Test
    public void testFormatHourAndParseHourResultInIdentityFunction() throws Exception {
        String hourInput = "2015012703";
        Assert.assertEquals(hourInput, DateHelper.formatToHour(DateHelper.parseHour(hourInput)));
        
        String lateHour = "2015013123";
        Assert.assertEquals(lateHour, DateHelper.formatToHour(DateHelper.parseHour(lateHour)));
        
        String earlyHour = "2015010100";
        Assert.assertEquals(earlyHour, DateHelper.formatToHour(DateHelper.parseHour(earlyHour)));
    }
    
    @Test
    public void parseToSecondsIgnoresTrailingCharacters() throws Exception {
        String dateStrToSecondsWithTrailingCharacters = "20130201010101_1";
        
        SimpleDateFormat sdf = new SimpleDateFormat(TO_SECOND);
        sdf.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
        long expected = sdf.parse(dateStrToSecondsWithTrailingCharacters).getTime();
        long actual = DateHelper.parseTimeExactToSeconds(dateStrToSecondsWithTrailingCharacters).getTime();
        Assert.assertEquals(DATE_HELPER_SHOULD_IGNORE_TRAILING_CHARS, expected, actual);
    }
    
    @Test
    public void parseToSecondsWithGmtIgnoresTrailingCharacters() throws Exception {
        String dateStrWithTrailingCharacters = "20130201_1";
        
        SimpleDateFormat gmtFormat = new SimpleDateFormat(TO_DAY);
        gmtFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        long expected = gmtFormat.parse(dateStrWithTrailingCharacters).getTime();
        @SuppressWarnings("deprecation")
        // testing a deprecated method
        long actual = DateHelper.parseWithGMT(dateStrWithTrailingCharacters).getTime();
        Assert.assertEquals(DATE_HELPER_SHOULD_IGNORE_TRAILING_CHARS, expected, actual);
    }
    
    @Test
    public void testAddDays() {
        String actual = DateHelper.format(DateHelper.addDays(DateHelper.parse("20180101"), 3));
        Assert.assertEquals("20180104", actual);
    }
    
    @Test
    public void testAddHours() {
        String actual = DateHelper.formatToHour(DateHelper.addHours(DateHelper.parseHour("2018010114"), 3));
        Assert.assertEquals("2018010117", actual);
    }
    
    @Test
    public void testDateAtHour() {
        Date date = DateHelper.parseTimeExactToSeconds("20180101204242");
        
        Assert.assertTrue(DateHelper.dateAtHour(date, 20));
        Assert.assertFalse(DateHelper.dateAtHour(date, 8));
    }
    
    @Test(expected = DateTimeParseException.class)
    public void testFailsIfMissingDigits() throws Exception {
        String dateStrMissingADayDigit = "2013020_1";
        DateHelper.parse(dateStrMissingADayDigit).getTime();
    }
    
    @Test(expected = DateTimeParseException.class)
    public void testFailsIfNotISOCompliant() throws Exception {
        DateHelper.parse8601("2014-01-07'T'12:01:01'Z'").getTime();
    }
    
    @Test(expected = NullPointerException.class)
    public void testDateHelperParseHandlesNullTheSameWay() throws Exception {
        DateHelper.parse(null).getTime();
    }
    
    @Test(expected = NullPointerException.class)
    public void testDateHelperFormat() throws Exception {
        DateHelper.format(null);
    }
}
