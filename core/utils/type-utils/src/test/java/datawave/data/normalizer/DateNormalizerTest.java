package datawave.data.normalizer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

/**
 *
 * 
 */
public class DateNormalizerTest {
    
    private static final Logger log = Logger.getLogger(DateNormalizerTest.class);
    DateNormalizer normalizer = new DateNormalizer();
    
    String[] inputDateStrings = {"2014-10-20T00:00:00.0000000", "2014-10-20T00:00:00.000Z", "20141020000000", "2014-10-20 00:00:00GMT", "2014-10-20 00:00:00Z",
            "2014-10-20 00:00:00", "2014-10-20", "2014-10-20T00|00", "Mon Oct 20 00:00:00 GMT 2014", "2014-10-20T00:00:00Z", "2014-10-20t00:00:00z",
            "2014-10-20T00:00:00+00:00", "Mon Oct 20 00:00:00 +00:00 2014"};
    
    @BeforeAll
    public static void setupClass() {
        System.setProperty("user.timezone", "GMT");
    }
    
    @Test
    public void testAllFormats() {
        assertEquals(inputDateStrings.length, DateNormalizer.FORMAT_STRINGS.length, "The DateNormalizer may have an new untested format");
        Set<Date> dateSet = Sets.newLinkedHashSet();
        Set<String> normalizedDates = Sets.newLinkedHashSet();
        Set<Long> dateTimes = Sets.newLinkedHashSet();
        for (String inputDateString : inputDateStrings) {
            Date date = normalizer.denormalize(inputDateString);
            dateSet.add(date);
            String normalized = normalizer.normalizeDelegateType(date);
            normalizedDates.add(normalized);
            dateTimes.add(date.getTime());
        }
        assertEquals(1, dateSet.size(), "There can be only one dateSet utilized for this unit test.");
        assertEquals(1, normalizedDates.size(), "There should be only one normalizedDate produced from this test.");
        assertEquals(1, dateTimes.size(), "There should be only one dateTimes produced from this test.");
    }
    
    @Test
    public void testExpectedResults() {
        String input = "2014-10-20T17:20:20.001Z";
        String normalized = normalizer.normalize(input);
        assertEquals("2014-10-20T17:20:20.001Z", normalized);
        
        input = "20141020172020";
        normalized = normalizer.normalize(input);
        assertEquals("2014-10-20T17:20:20.000Z", normalized);
        
        input = "2014-10-20 17:20:20GMT";
        normalized = normalizer.normalize(input);
        assertEquals("2014-10-20T17:20:20.000Z", normalized);
        
        input = "2014-10-20 17:20:20Z";
        normalized = normalizer.normalize(input);
        assertEquals("2014-10-20T17:20:20.000Z", normalized);
        
        input = "2014-10-20";
        normalized = normalizer.normalize(input);
        assertEquals("2014-10-20T00:00:00.000Z", normalized);
        
        input = "2014-10-20 17:20:20";
        normalized = normalizer.normalize(input);
        assertEquals("2014-10-20T17:20:20.000Z", normalized);
        
        input = "2014-10-20T17|20";
        normalized = normalizer.normalize(input);
        assertEquals("2014-10-20T17:20:00.000Z", normalized);
        
        input = "Mon Oct 20 17:20:20 GMT 2014";
        normalized = normalizer.normalize(input);
        assertEquals("2014-10-20T17:20:20.000Z", normalized);
        
        input = "2014-10-20T17:20:20Z";
        normalized = normalizer.normalize(input);
        assertEquals("2014-10-20T17:20:20.000Z", normalized);
        
        input = "2014-10-20t17:20:20z";
        normalized = normalizer.normalize(input);
        assertEquals("2014-10-20T17:20:20.000Z", normalized);
        
        input = "Thu Jan 1 00:00:00 GMT 1970";
        normalized = normalizer.normalize(input);
        assertEquals("1970-01-01T00:00:00.000Z", normalized);
        
        input = "2014-10-20T17:20:20.345007Z";
        normalized = normalizer.normalize(input);
        assertEquals("2014-10-20T17:20:20.345Z", normalized);
        
        input = "2014-10-20T00:00:00.0000000";
        normalized = normalizer.normalize(input);
        assertEquals("2014-10-20T00:00:00.000Z", normalized);
        
        input = "2014-10-20T00:00:00.1111";
        normalized = normalizer.normalize(input);
        assertEquals("2014-10-20T00:00:00.111Z", normalized);
        
        input = "2014-10-20T00:00:00.222";
        normalized = normalizer.normalize(input);
        assertEquals("2014-10-20T00:00:00.222Z", normalized);
    }
    
    @Test
    public void testFromLong() {
        Date now = new Date();
        long rightNow = now.getTime();
        String normalizedFromLong = normalizer.normalize("" + rightNow);
        String normalizedFromDate = normalizer.normalizeDelegateType(now);
        assertEquals(normalizedFromLong, normalizedFromDate);
    }
    
    /**
     * Show that an un-protected SimpleDateFormat will cause this test to have more than 4 Dates, or cause it to throw an Exception:
     */
    @Test
    public void showThreadUnsafeDateFormat() {
        
        try {
            DateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            final Date[] thedates = new Date[] {sdf.parse("20170101"), sdf.parse("20170201"), sdf.parse("20170102"), sdf.parse("20160101"),
            
            };
            final DateFormat unsafeDateFormat = new SimpleDateFormat("yyyyMMdd");
            Callable<String> task = () -> unsafeDateFormat.format(thedates[(int) (Math.random() * 4)]);
            
            ExecutorService exec = Executors.newFixedThreadPool(2);
            List<Future<String>> results = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                results.add(exec.submit(task));
            }
            exec.shutdown();
            Set<String> dates = Sets.newHashSet();
            for (Future<String> result : results) {
                dates.add(result.get());
            }
            log.info("unsafe threading on DateFormat got back this many dates instead of 4:" + dates.size());
        } catch (Exception ex) {
            log.info("sometimes, the DateFormat will throw an exception when used in multiple threads:" + ex);
        }
    }
    
    /**
     * this test uses the ThreadLocal in DateNormalizer to give correct results with multi-threading
     */
    @Test
    public void testThreadSafeConversions() throws Exception {
        DateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        final Date[] thedates = new Date[] {sdf.parse("20170101"), sdf.parse("20170201"), sdf.parse("20170102"), sdf.parse("20160101"),
        
        };
        Callable<String> task = () -> normalizer.parseToString(thedates[(int) (Math.random() * 4)]);
        
        ExecutorService exec = Executors.newFixedThreadPool(2);
        List<Future<String>> results = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            results.add(exec.submit(task));
        }
        exec.shutdown();
        Set<String> dates = Sets.newHashSet();
        for (Future<String> result : results) {
            dates.add(result.get());
        }
        assertEquals(4, dates.size());
    }
}
