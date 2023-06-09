package datawave.query.tables.stats;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import datawave.query.util.QueryStopwatch;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang.time.StopWatch;
import org.apache.log4j.Logger;

/**
 *
 */
public class ScanSessionStats {
    public enum TIMERS {
        HASNEXT, SCANNER_ITERATE, SCANNER_START, RUNTIME
    }

    private static final Logger log = Logger.getLogger(ScanSessionStats.class);

    protected Map<TIMERS,StopWatch> timers;

    protected Map<TIMERS,MutableLong> mergedTimers;

    protected MutableLong keysSeen;

    public ScanSessionStats() {
        timers = new EnumMap<>(TIMERS.class);

        mergedTimers = new EnumMap<>(TIMERS.class);

        for (TIMERS timer : TIMERS.values()) {
            timers.put(timer, new StopWatch());
            mergedTimers.put(timer, new MutableLong());
        }

        keysSeen = new MutableLong();
    }

    public ScanSessionStats merge(ScanSessionStats other) {
        for (TIMERS timer : mergedTimers.keySet()) {
            MutableLong timerValue = mergedTimers.get(timer);
            if (null == timerValue) {
                timerValue = new MutableLong(0);
            }
            long otherTimer = other.getValue(timer);
            timerValue.add(otherTimer);
        }
        return this;
    }

    public long getValue(TIMERS timer) {
        return (timers.get(timer).getTime() + mergedTimers.get(timer).longValue());
    }

    public StopWatch getTimer(TIMERS timer) {
        return timers.get(timer);
    }

    public long getKeysSeen() {
        return keysSeen.longValue();
    }

    public void incrementKeysSeen(long keys) {
        keysSeen.add(keys);
    }

    public void initializeTimers() {
        timers.get(TIMERS.HASNEXT).start();
        timers.get(TIMERS.HASNEXT).suspend();

        timers.get(TIMERS.SCANNER_ITERATE).start();
        timers.get(TIMERS.SCANNER_ITERATE).suspend();

        timers.get(TIMERS.SCANNER_START).start();
        timers.get(TIMERS.SCANNER_START).suspend();
    }

    public void stopOnFailure() {
        for (TIMERS timer : mergedTimers.keySet()) {
            try {
                timers.get(timer).stop();
            } catch (Exception e) {

            }
        }
    }

    public void logSummary(final Logger log) {

        Logger logToUse = log;
        if (null != log)
            logToUse = this.log;
        final StringBuilder sb = new StringBuilder(256);

        int count = 1;
        long totalDurationMillis = 0l;

        logToUse.debug("Elapsed time running query");

        final int length = Integer.toString(TIMERS.values().length).length();
        for (TIMERS timer : TIMERS.values()) {

            final String countStr = Integer.toString(count);

            final String paddedCount = new StringBuilder(QueryStopwatch.INDENT).append(StringUtils.leftPad(countStr, length, "0")).append(") ").toString();

            long myValue = getValue(timer);
            // Stopwatch.toString() will give us appropriate units for the timing
            sb.append(paddedCount).append(timer.name()).append(": ").append(formatMillis(myValue));

            totalDurationMillis += myValue;
            count++;
            logToUse.debug(sb.toString());
            sb.setLength(0);
        }

        sb.append(QueryStopwatch.INDENT).append("Total elapsed: ").append(formatMillis(totalDurationMillis));
        logToUse.debug(sb.toString());

    }

    protected String formatMillis(long elapsedMillis) {
        TimeUnit unit = chooseUnit(elapsedMillis);
        double value = (double) elapsedMillis / MILLISECONDS.convert(1, unit);

        // Too bad this functionality is not exposed as a regular method call
        return String.format("%.4g %s", value, abbreviate(unit));
    }

    protected TimeUnit chooseUnit(long millis) {
        if (SECONDS.convert(millis, MILLISECONDS) > 0) {
            return SECONDS;
        }
        return MILLISECONDS;
    }

    protected String abbreviate(TimeUnit unit) {
        switch (unit) {
            case MILLISECONDS:
                return "ms";
            case SECONDS:
                return "s";
            default:
                throw new AssertionError();
        }
    }

}
