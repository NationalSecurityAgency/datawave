package datawave.query.util;

import datawave.query.util.QueryStopwatch.NonSummarizedStopWatch;
import datawave.util.time.TraceStopwatch;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyString;

@RunWith(PowerMockRunner.class)
public class QueryStopwatchTest  {

    @Test
    public void testNoTimer() throws Exception {
        final QueryStopwatch sw = new QueryStopwatch();
        QueryStopwatch stopwatch = PowerMockito.spy(  sw);

        List<String> summaries = new ArrayList<>();

        Assert.assertEquals(summaries,stopwatch.summarizeAsList());
    }

    @Test
    public void testSingletimer() throws Exception {
        final QueryStopwatch sw = new QueryStopwatch();
        QueryStopwatch stopwatch = PowerMockito.spy(  sw);
        //EasyMock.expect()

        List<String> summaries = new ArrayList<>();
        summaries.add("    1) test: 1.000 s");
        summaries.add("    Total elapsed: 1.000 s");
        NoTimeTraceStopWatch traceStopWatch = new NoTimeTraceStopWatch("test","1.000 s",1000);
        PowerMockito.doReturn(traceStopWatch).when(stopwatch).newStopwatch(anyString());
        stopwatch.newStartedStopwatch("test");

        Assert.assertEquals(summaries,stopwatch.summarizeAsList());
    }

    @Test
    public void testSummarization() throws Exception {
        final QueryStopwatch sw = new QueryStopwatch();
        QueryStopwatch stopwatch = PowerMockito.spy(  sw);
        //EasyMock.expect()

        List<String> summaries = new ArrayList<>();
        summaries.add("    1) test (total): 2.000 s");
        summaries.add("    2) test part: 1.000 s");
        summaries.add("    Total elapsed: 2.000 s");
        NoTimeTraceStopWatch traceStopWatch = new NoTimeTraceStopWatch("test (total)","2.000 s",2000);
        NoTimeTraceStopWatchNonSummarized nonSummarizedStopWatch = new NoTimeTraceStopWatchNonSummarized("test part","1.000 s",1000);
        PowerMockito.doReturn(traceStopWatch).when(stopwatch).newStopwatch("test (total)");
        PowerMockito.doReturn(nonSummarizedStopWatch).when(stopwatch).newStopwatch("test part");
        /***
         * When we have summarized times
         */
        stopwatch.newStartedStopwatch("test (total)");
        stopwatch.newStartedStopwatch("test part");

        Assert.assertEquals(summaries,stopwatch.summarizeAsList());
    }

    @Test(expected =  IllegalStateException.class)
    public void testAllSummarization() throws Exception {
        final QueryStopwatch sw = new QueryStopwatch();
        QueryStopwatch stopwatch = PowerMockito.spy(  sw);

        List<String> summaries = new ArrayList<>();
        summaries.add("    1) test (total): 2.000 s");
        summaries.add("    2) test part: 1.000 s");
        summaries.add("    Total elapsed: 2.000 s");
        NoTimeTraceStopWatchNonSummarized traceStopWatch = new NoTimeTraceStopWatchNonSummarized("test (total)","2.000 s",2000);
        NoTimeTraceStopWatchNonSummarized nonSummarizedStopWatch = new NoTimeTraceStopWatchNonSummarized("test part","1.000 s",1000);
        PowerMockito.doReturn(traceStopWatch).when(stopwatch).newStopwatch("test (total)");
        PowerMockito.doReturn(nonSummarizedStopWatch).when(stopwatch).newStopwatch("test part");
        /***
         * When we have summarized times
         */
        stopwatch.newStartedStopwatch("test (total)");
        stopwatch.newStartedStopwatch("test part");

        Assert.assertEquals(summaries,stopwatch.summarizeAsList());
    }


    private class NoTimeTraceStopWatch extends TraceStopwatch{

        private final long time;
        private final String verificationString;

        public NoTimeTraceStopWatch(String description, String verificationString, long time) {
            super(description);
            this.time=time;
            this.verificationString=verificationString;
        }

        @Override
        public void start() {
            // do nothing.
        }
        @Override
        public void stop() {
            // do nothing.
        }

        @Override
        public long elapsed(TimeUnit desiredUnit) {
            switch (desiredUnit){
                case SECONDS:
                    return time / 1000;
                case MILLISECONDS:
                    return time;
                case MINUTES:
                    return time / 60000;
                case NANOSECONDS:
                    return time * 1000000;
            }
            return 1000;
        }

        @Override
        public String toString() {
            return verificationString;
        }
    }

    private class NoTimeTraceStopWatchNonSummarized extends NonSummarizedStopWatch {

        private final long time;
        private final String verificationString;

        public NoTimeTraceStopWatchNonSummarized(String description, String verificationString, long time) {
            super(description);
            this.time=time;
            this.verificationString=verificationString;
        }

        @Override
        public void start() {
            // do nothing.
        }
        @Override
        public void stop() {
            // do nothing.
        }

        @Override
        public long elapsed(TimeUnit desiredUnit) {
            switch (desiredUnit){
                case SECONDS:
                    return time / 1000;
                case MILLISECONDS:
                    return time;
                case MINUTES:
                    return time / 60000;
                case NANOSECONDS:
                    return time * 1000000;
            }
            return 1000;
        }

        @Override
        public String toString() {
            return verificationString;
        }
    }

}
