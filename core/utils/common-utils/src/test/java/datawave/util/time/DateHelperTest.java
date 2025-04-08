package datawave.util.time;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

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
        assertEquals(expected, actual, DATE_HELPER_SHOULD_IGNORE_TRAILING_CHARS);
    }
    
    @Test
    public void testFormatOfParseIsIdentityFunction() {
        String secondsInput = "20150102030459";
        assertEquals(secondsInput, DateHelper.formatToTimeExactToSeconds(DateHelper.parseTimeExactToSeconds(secondsInput)));
        
        String dayInput = "20150127";
        assertEquals(dayInput, DateHelper.format(DateHelper.parse(dayInput)));
    }
    
    @Test
    public void testFormatToTimeExactToSeconds() {
        long millisInput = 1436463044319L;
        assertEquals("20150709173044", DateHelper.formatToTimeExactToSeconds(millisInput));
    }
    
    @Test
    public void testFormatHourAndParseHourResultInIdentityFunction() {
        String hourInput = "2015012703";
        assertEquals(hourInput, DateHelper.formatToHour(DateHelper.parseHour(hourInput)));
        
        String lateHour = "2015013123";
        assertEquals(lateHour, DateHelper.formatToHour(DateHelper.parseHour(lateHour)));
        
        String earlyHour = "2015010100";
        assertEquals(earlyHour, DateHelper.formatToHour(DateHelper.parseHour(earlyHour)));
    }
    
    @Test
    public void parseToSecondsIgnoresTrailingCharacters() throws Exception {
        String dateStrToSecondsWithTrailingCharacters = "20130201010101_1";
        
        SimpleDateFormat sdf = new SimpleDateFormat(TO_SECOND);
        sdf.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
        long expected = sdf.parse(dateStrToSecondsWithTrailingCharacters).getTime();
        long actual = DateHelper.parseTimeExactToSeconds(dateStrToSecondsWithTrailingCharacters).getTime();
        assertEquals(expected, actual, DATE_HELPER_SHOULD_IGNORE_TRAILING_CHARS);
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
        assertEquals(expected, actual, DATE_HELPER_SHOULD_IGNORE_TRAILING_CHARS);
    }
    
    @Test
    public void testAddDays() {
        String actual = DateHelper.format(DateHelper.addDays(DateHelper.parse("20180101"), 3));
        assertEquals("20180104", actual);
    }
    
    @Test
    public void testAddHours() {
        String actual = DateHelper.formatToHour(DateHelper.addHours(DateHelper.parseHour("2018010114"), 3));
        assertEquals("2018010117", actual);
    }
    
    @Test
    public void testDateAtHour() {
        Date date = DateHelper.parseTimeExactToSeconds("20180101204242");
        
        assertTrue(DateHelper.dateAtHour(date, 20));
        assertFalse(DateHelper.dateAtHour(date, 8));
    }
    
    @Test
    public void testFailsIfMissingDigits() {
        String dateStrMissingADayDigit = "2013020_1";
        assertThrows(DateTimeParseException.class, () -> DateHelper.parse(dateStrMissingADayDigit));
    }
    
    @Test
    public void testFailsIfNotISOCompliant() {
        assertThrows(DateTimeParseException.class, () -> DateHelper.parse8601("2014-01-07'T'12:01:01'Z'"));
    }
    
    @Test
    public void testDateHelperParseHandlesNullTheSameWay() {
        assertThrows(NullPointerException.class, () -> DateHelper.parse(null));
    }
    
    @Test
    public void testDateHelperFormat() {
        // noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> DateHelper.format(null));
    }
    
    @Test
    public void testCustomParse() throws ParseException {
        String date = "20091231 000001_11";
        String pattern = "yyyyMMdd HHmmss";
        
        testCustomParse(date, pattern);
    }
    
    @Test
    public void testCustomParseYearAndDayOfYear() throws ParseException {
        String date = "2023001";
        String pattern = "yyyyDDD";
        
        testCustomParse(date, pattern);
    }
    
    @Test
    public void testCustomParseYearMonthDate() throws ParseException {
        String date = "2023-01-01";
        String pattern = "yyyy-MM-dd";
        
        testCustomParse(date, pattern);
    }
    
    @Test
    public void testCustomParseYearAndDayOfYearNoPad() throws ParseException {
        String date = "202311";
        String pattern = "yyyyDDD";
        
        testCustomParse(date, pattern);
    }
    
    private void testCustomParse(String date, String pattern) throws ParseException {
        AtomicLong actual = new AtomicLong(Long.MIN_VALUE);
        assertDoesNotThrow((() -> actual.set(DateHelper.parseCustom(date, pattern).getTime())));
        
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        sdf.setTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC));
        long expected = sdf.parse(date).getTime();
        
        assertEquals(expected, actual.get());
    }
}
