package datawave.query.statsd;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.timgroup.statsd.ConvenienceMethodProvidingStatsDClient;
import com.timgroup.statsd.NonBlockingUdpSender;
import com.timgroup.statsd.StatsDClientErrorHandler;
import com.timgroup.statsd.StatsDClientException;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.Locale;

import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A client that can be used to record live query metrics. This client will cache results and will periodically send them with a specified max cache size.
 */
public class QueryStatsDClient extends ConvenienceMethodProvidingStatsDClient {
    private static final Logger log = Logger.getLogger(QueryStatsDClient.class);
    
    // the statsd configuration
    private final String queryId;
    private final String host;
    private final int port;
    private final int maxCacheSize;
    
    // thread safe caches
    private final AtomicInteger nextCalls = new AtomicInteger(0);
    private final AtomicInteger seekCalls = new AtomicInteger(0);
    private final AtomicInteger yieldCalls = new AtomicInteger(0);
    private final AtomicInteger sources = new AtomicInteger(0);
    private final Multimap<String,Long> timings;
    private final String prefix;
    
    private static final Charset STATS_D_ENCODING = Charset.forName("UTF-8");
    
    private static final StatsDClientErrorHandler NO_OP_HANDLER = e -> { /* No-op */};
    
    // the client
    private static NonBlockingUdpSender client = null;
    // this monitor controls when the client is being used
    private static final Object clientMonitor = new Object();
    
    public QueryStatsDClient(String queryId, String host, int port, int maxCacheSize) {
        this.queryId = queryId;
        this.host = host;
        this.port = port;
        this.maxCacheSize = maxCacheSize;
        Multimap<String,Long> temp = HashMultimap.create();
        this.timings = Multimaps.synchronizedMultimap(temp);
        this.prefix = queryId + ".dwquery.";
    }
    
    /**
     * Get the client, creating it if needed. This should only be called when the clientMonitor has been acquired.
     * 
     * @return the client
     */
    private NonBlockingUdpSender client() {
        if (this.client == null) {
            synchronized (clientMonitor) {
                if (this.client == null) {
                    try {
                        this.client = new NonBlockingUdpSender(host, port, STATS_D_ENCODING, NO_OP_HANDLER);
                    } catch (Exception e) {
                        throw new StatsDClientException("Failed to start StatsD client", e);
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("Created STATSD client with host = " + host + "; port = " + port + "; prefix = " + queryId + ".dwquery");
                    }
                }
            }
        }
        return this.client;
    }
    
    /**
     * Flush the stats to the stats d client.
     * 
     * @return true if there were any stats to flush.
     */
    private boolean flushStats() {
        if (log.isDebugEnabled()) {
            log.debug("Flushing STATSD stats for " + queryId);
        }
        boolean flushed = false;
        synchronized (clientMonitor) {
            long value = nextCalls.getAndSet(0);
            if (value > 0) {
                count("next_calls", value);
                flushed = true;
            }
            value = seekCalls.getAndSet(0);
            if (value > 0) {
                count("seek_calls", value);
                flushed = true;
            }
            value = yieldCalls.getAndSet(0);
            if (value > 0) {
                count("yield_calls", value);
                flushed = true;
            }
            value = sources.getAndSet(0);
            if (value > 0) {
                count("sources", value);
                flushed = true;
            }
            if (!timings.isEmpty()) {
                synchronized (timings) {
                    if (!timings.isEmpty()) {
                        for (Map.Entry<String,Long> timing : timings.entries()) {
                            count(timing.getKey(), timing.getValue());
                        }
                        flushed = true;
                        timings.clear();
                    }
                }
            }
            if (flushed && log.isTraceEnabled()) {
                log.trace("Flushed some STATSD stats for " + queryId);
            }
        }
        return flushed;
    }
    
    private void flushAsNeeded() {
        if (getSize() > maxCacheSize) {
            flushStats();
        }
    }
    
    public void flush() {
        flushStats();
    }
    
    public void next() {
        nextCalls.incrementAndGet();
        flushAsNeeded();
    }
    
    public void seek() {
        seekCalls.incrementAndGet();
        flushAsNeeded();
    }
    
    public void yield() {
        yieldCalls.incrementAndGet();
        flushAsNeeded();
    }
    
    public void addSource() {
        sources.incrementAndGet();
        flushAsNeeded();
    }
    
    public void timing(String call, long time) {
        timings.put(call, time);
        flushAsNeeded();
    }
    
    public int getSize() {
        return nextCalls.get() + seekCalls.get() + yieldCalls.get() + sources.get() + timings.size();
    }
    
    /**
     * Cleanly shut down this StatsD client. This method may throw an exception if the socket cannot be closed.
     */
    @Override
    public void stop() {
        if (this.client != null) {
            synchronized (this.clientMonitor) {
                if (this.client != null) {
                    this.client.stop();
                    this.client = null;
                }
            }
        }
    }
    
    /**
     * Adjusts the specified counter by a given delta.
     *
     * <p>
     * This method is non-blocking and is guaranteed not to throw an exception.
     * </p>
     *
     * @param aspect
     *            the name of the counter to adjust
     * @param delta
     *            the amount to adjust the counter by
     * @param sampleRate
     *            the sampling rate being employed. For example, a rate of 0.1 would tell StatsD that this counter is being sent sampled every 1/10th of the
     *            time.
     */
    @Override
    public void count(String aspect, long delta, double sampleRate) {
        send(messageFor(aspect, Long.toString(delta), "c", sampleRate));
    }
    
    /**
     * Records the latest fixed value for the specified named gauge.
     *
     * <p>
     * This method is non-blocking and is guaranteed not to throw an exception.
     * </p>
     *
     * @param aspect
     *            the name of the gauge
     * @param value
     *            the new reading of the gauge
     */
    @Override
    public void recordGaugeValue(String aspect, long value) {
        recordGaugeCommon(aspect, Long.toString(value), value < 0, false);
    }
    
    @Override
    public void recordGaugeValue(String aspect, double value) {
        recordGaugeCommon(aspect, stringValueOf(value), value < 0, false);
    }
    
    @Override
    public void recordGaugeDelta(String aspect, long value) {
        recordGaugeCommon(aspect, Long.toString(value), value < 0, true);
    }
    
    @Override
    public void recordGaugeDelta(String aspect, double value) {
        recordGaugeCommon(aspect, stringValueOf(value), value < 0, true);
    }
    
    private void recordGaugeCommon(String aspect, String value, boolean negative, boolean delta) {
        final StringBuilder message = new StringBuilder();
        if (!delta && negative) {
            message.append(messageFor(aspect, "0", "g")).append('\n');
        }
        message.append(messageFor(aspect, (delta && !negative) ? ("+" + value) : value, "g"));
        send(message.toString());
    }
    
    /**
     * StatsD supports counting unique occurrences of events between flushes, Call this method to records an occurrence of the specified named event.
     *
     * <p>
     * This method is non-blocking and is guaranteed not to throw an exception.
     * </p>
     *
     * @param aspect
     *            the name of the set
     * @param eventName
     *            the value to be added to the set
     */
    @Override
    public void recordSetEvent(String aspect, String eventName) {
        send(messageFor(aspect, eventName, "s"));
    }
    
    /**
     * Records an execution time in milliseconds for the specified named operation.
     *
     * <p>
     * This method is non-blocking and is guaranteed not to throw an exception.
     * </p>
     *
     * @param aspect
     *            the name of the timed operation
     * @param timeInMs
     *            the time in milliseconds
     */
    @Override
    public void recordExecutionTime(String aspect, long timeInMs, double sampleRate) {
        send(messageFor(aspect, Long.toString(timeInMs), "ms", sampleRate));
    }
    
    private String messageFor(String aspect, String value, String type) {
        return messageFor(aspect, value, type, 1.0);
    }
    
    private String messageFor(String aspect, String value, String type, double sampleRate) {
        final String message = prefix + aspect + ':' + value + '|' + type;
        return (sampleRate == 1.0) ? message : (message + "|@" + stringValueOf(sampleRate));
    }
    
    private void send(final String message) {
        synchronized (this.clientMonitor) {
            client().send(message);
        }
    }
    
    private String stringValueOf(double value) {
        NumberFormat formatter = NumberFormat.getInstance(Locale.US);
        formatter.setGroupingUsed(false);
        formatter.setMaximumFractionDigits(19);
        return formatter.format(value);
    }
    
}
