package datawave.query.rewrite.statsd;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import datawave.query.rewrite.iterator.profile.QuerySpanCollector;
import org.apache.log4j.Logger;

/**
 * A client that can be used to record live query metrics. This client will cache results and will periodically send them with a specified max latency and a
 * specified max cache size. A keepAlive value is also specified which will dictate how long the client and/or timer threads are kept alive without any
 * activity.
 */
public class QueryStatsDClient {
    private static final Logger log = Logger.getLogger(QueryStatsDClient.class);
    
    // the statsd configuration
    private final String queryId;
    private final String host;
    private final int port;
    private final long latencyMs;
    private final int maxCacheSize;
    private final long keepAliveMs;
    
    // thread safe caches
    private final AtomicInteger nextCalls = new AtomicInteger(0);
    private final AtomicInteger seekCalls = new AtomicInteger(0);
    private final AtomicInteger sources = new AtomicInteger(0);
    private final Multimap<String,Long> timings;
    
    // the client
    private StatsDClient client = null;
    // this monitor controls when the client is being used
    private final Object clientMonitor = new Object();
    
    // the timer
    private Timer timer = null;
    // this boolean controls when a timer task has been submitted and the timer can be used
    private final AtomicBoolean taskRunning = new AtomicBoolean(false);
    private volatile long lastSend = System.currentTimeMillis();
    
    public QueryStatsDClient(String queryId, String host, int port, long latencyMs, int maxCacheSize, long keepAliveMs) {
        this.queryId = queryId;
        this.host = host;
        this.port = port;
        this.latencyMs = latencyMs;
        this.maxCacheSize = maxCacheSize;
        this.keepAliveMs = keepAliveMs;
        Multimap<String,Long> temp = HashMultimap.create();
        this.timings = Multimaps.synchronizedMultimap(temp);
    }
    
    /**
     * Get the client, creating it if needed. This should only be called when the clientMonitor has been acquired.
     */
    private StatsDClient client() {
        if (this.client == null) {
            this.client = new NonBlockingStatsDClient(queryId + ".dwquery", host, port);
            if (log.isDebugEnabled()) {
                log.debug("Created STATSD client with host = " + host + "; port = " + port + "; prefix = " + queryId + ".dwquery");
            }
        }
        return this.client;
    }
    
    /**
     * Close the client. This should only be called when the clientMonitor has been acquired.
     */
    private void closeClient() {
        if (this.client != null) {
            this.client.stop();
            this.client = null;
            if (log.isDebugEnabled()) {
                log.debug("Stopped STATSD client with host = " + host + "; port = " + port + "; prefix = " + queryId + ".dwquery");
            }
        }
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
                client().count("next_calls", value);
                flushed = true;
            }
            value = seekCalls.getAndSet(0);
            if (value > 0) {
                client().count("seek_calls", value);
                flushed = true;
            }
            value = sources.getAndSet(0);
            if (value > 0) {
                client().count("sources", value);
                flushed = true;
            }
            if (!timings.isEmpty()) {
                synchronized (timings) {
                    if (!timings.isEmpty()) {
                        for (Map.Entry<String,Long> timing : timings.entries()) {
                            client().count(timing.getKey(), timing.getValue());
                        }
                        flushed = true;
                        timings.clear();
                    }
                }
            }
            if (flushed) {
                if (log.isDebugEnabled()) {
                    log.debug("Flushed some STATSD stats for " + queryId);
                }
                lastSend = System.currentTimeMillis();
            } else if (System.currentTimeMillis() - lastSend >= keepAliveMs) {
                closeClient();
            }
        }
        return flushed;
    }
    
    private void startTimer() {
        if (taskRunning.compareAndSet(false, true)) {
            if (timer == null) {
                timer = new Timer("StatsD Timer tThread for " + queryId, true);
            }
            long delta = Math.min(System.currentTimeMillis() - lastSend, latencyMs);
            timer.schedule(new QueryStatsDTimer(), latencyMs - delta);
            if (log.isDebugEnabled()) {
                log.debug("Created STATSD timer for " + (latencyMs - delta) + "ms from now for " + queryId);
            }
        }
    }
    
    private void flushAsNeeded() {
        if (getSize() > maxCacheSize) {
            flushStats();
        }
        startTimer();
    }
    
    public void next() {
        nextCalls.incrementAndGet();
        flushAsNeeded();
    }
    
    public void seek() {
        seekCalls.incrementAndGet();
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
        return nextCalls.get() + seekCalls.get() + sources.get() + timings.size();
    }
    
    private class QueryStatsDTimer extends TimerTask {
        @Override
        public void run() {
            if (log.isDebugEnabled()) {
                log.debug("STATSD Timer fired for " + queryId);
            }
            long delta = System.currentTimeMillis() - lastSend;
            
            // if at our latency threshold, then flush any existing stats
            if (delta >= latencyMs) {
                if (flushStats()) {
                    delta = System.currentTimeMillis() - lastSend;
                }
            }
            
            // if we are over the keepalive limit, then terminate our threads (client and timer)
            if (delta >= keepAliveMs) {
                if (log.isDebugEnabled()) {
                    log.debug("Closing STATSD timer and client for " + queryId);
                }
                synchronized (clientMonitor) {
                    closeClient();
                }
                timer.cancel();
                timer = null;
                if (log.isDebugEnabled()) {
                    log.debug("Stopped STATSD timer for " + queryId);
                }
                taskRunning.getAndSet(false);
            }
            // otherwise lets restart the timer
            else {
                timer.schedule(new QueryStatsDTimer(), latencyMs - delta);
                if (log.isDebugEnabled()) {
                    log.debug("Created STATSD timer for " + (latencyMs - delta) + "ms from now for " + queryId);
                }
            }
        }
    }
}
