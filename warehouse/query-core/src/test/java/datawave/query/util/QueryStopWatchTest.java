package datawave.query.util;

import com.google.common.collect.ImmutableList;
import datawave.util.time.TraceStopwatch;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class QueryStopWatchTest {
    
    Pattern totalElapsedPattern = Pattern.compile("Total elapsed: ([0-9]+\\.[0-9]+) ms");
    
    @Test
    public void testNone() {
        QueryStopwatch stopwatch = new QueryStopwatch();
        
        Assert.assertEquals(0, stopwatch.summarizeAsList().size());
    }
    
    boolean linesStartWith(List<String> expectedStrings, List<String> lines) {
        if (expectedStrings.size() > lines.size()) {
            return false;
        }
        for (int i = 0; i < expectedStrings.size(); i++) {
            if (!lines.get(i).trim().startsWith(expectedStrings.get(i))) {
                return false;
            }
        }
        return true;
    }
    
    float extractTotalElapsed(String totalElapsedTime) {
        java.util.regex.Matcher m = totalElapsedPattern.matcher(totalElapsedTime.trim());
        if (m.matches()) {
            return Float.valueOf(m.group(1));
        }
        return -1F;
    }
    
    @Test
    public void testSingleCounted() {
        QueryStopwatch stopwatch = new QueryStopwatch();
        stopwatch.newStartedStopwatch("Testtimer").stop();
        
        List<String> expected = ImmutableList.of("1) Testtimer", "Total elapsed:");
        Assert.assertEquals(2, stopwatch.summarizeAsList().size());
        Assert.assertTrue(linesStartWith(expected, stopwatch.summarizeAsList()));
    }
    
    // unit tests with timing analyses are not always a good idea but this test exists in conjunction
    // with testSingleNonSummarized to show that the non summarized TraceStopWatch will not impact
    // the total elapsed time.
    
    @Test
    public void testSingleLongTime() throws InterruptedException {
        QueryStopwatch stopwatch = new QueryStopwatch();
        TraceStopwatch timer = stopwatch.newStartedStopwatch("Testtimer");
        Thread.sleep(20); // 20 ms;
        timer.stop();
        List<String> expected = ImmutableList.of("1) Testtimer", "Total elapsed:");
        Assert.assertEquals(2, stopwatch.summarizeAsList().size());
        Assert.assertTrue(linesStartWith(expected, stopwatch.summarizeAsList()));
        Assert.assertTrue(extractTotalElapsed(stopwatch.summarizeAsList().get(1)) > 10);
    }
    
    @Test
    public void testSingleNonSummarized() throws InterruptedException {
        QueryStopwatch stopwatch = new QueryStopwatch();
        TraceStopwatch timer = stopwatch.newStartedStopwatch("Testtimer", true);
        Thread.sleep(20); // 20 ms;
        timer.stop();
        List<String> expected = ImmutableList.of("1) Testtimer", "Total elapsed:");
        Assert.assertEquals(2, stopwatch.summarizeAsList().size());
        Assert.assertTrue(linesStartWith(expected, stopwatch.summarizeAsList()));
        Assert.assertTrue(extractTotalElapsed(stopwatch.summarizeAsList().get(1)) < 5);
    }
}
