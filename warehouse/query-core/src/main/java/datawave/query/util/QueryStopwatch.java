package datawave.query.util;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import datawave.util.time.TraceStopwatch;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;

/**
 *
 */
public class QueryStopwatch {
    public static final String NEWLINE = "\n";
    public static final String INDENT = "    ";
    protected final ArrayDeque<Entry<String,TraceStopwatch>> watches = Queues.newArrayDeque();

    /**
     * Creates a new Stopwatch for use but does not start it
     *
     * @param header
     *            the string header
     * @return new Stopwatch
     */
    private TraceStopwatch newStopwatch(String header) {
        checkNotNull(header);

        TraceStopwatch sw = new TraceStopwatch(header);

        synchronized (watches) {
            watches.add(Maps.immutableEntry(header, sw));
        }

        return sw;
    }

    public TraceStopwatch newStartedStopwatch(String header) {
        TraceStopwatch sw = newStopwatch(header);
        sw.start();

        return sw;
    }

    public synchronized TraceStopwatch peek() {
        Entry<String,TraceStopwatch> entry = watches.peekLast();
        if (null == entry) {
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.STOPWATCH_MISSING);
            throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
        }

        return entry.getValue();
    }

    /**
     * Get the stopwatch associated with the stage name, or null if no such stopwatch exists
     *
     * @param stageName
     *            the stage name
     * @return the stopwatch, or null if no such stopwatch exists
     */
    public TraceStopwatch get(String stageName) {
        synchronized (watches) {
            for (Map.Entry<String,TraceStopwatch> entry : watches) {
                if (entry.getKey().equals(stageName)) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    public String summarize() {
        List<String> logLines = summarizeAsList();

        return Joiner.on('\n').join(logLines);
    }

    public synchronized List<String> summarizeAsList() {
        if (this.watches.isEmpty()) {
            return Collections.emptyList();
        }

        final List<String> lines = Lists.newArrayListWithCapacity(10);
        final StringBuilder sb = new StringBuilder(256);

        int count = 1;
        long totalDurationMillis = 0l;
        for (Entry<String,TraceStopwatch> entry : watches) {
            String description = entry.getKey();
            TraceStopwatch sw = entry.getValue();

            Preconditions.checkArgument(!sw.isRunning(), "Encountered a non-stopped stopwatch with description " + description);

            final String countStr = Integer.toString(count);

            final int length = Integer.toString(watches.size()).length();

            final String paddedCount = new StringBuilder(INDENT).append(StringUtils.leftPad(countStr, length, "0")).append(") ").toString();

            // Stopwatch.toString() will give us appropriate units for the timing
            sb.append(paddedCount).append(description).append(": ").append(sw);
            lines.add(sb.toString());

            totalDurationMillis += sw.elapsed(TimeUnit.MILLISECONDS);
            count++;
            sb.setLength(0);
        }

        sb.append(INDENT).append("Total elapsed: ").append(formatMillis(totalDurationMillis));
        lines.add(sb.toString());

        return lines;
    }

    public void appendTimers(QueryStopwatch queryStopwatch) {
        synchronized (watches) {
            this.watches.addAll(queryStopwatch.watches);
        }
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
