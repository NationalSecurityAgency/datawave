package datawave.query.rewrite.statsd;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A client that can be used to record live query metrics. This client will cache results and will periodically send them with a specified max cache size.
 */
public class QueryStatsDClient {
    private static final Logger log = Logger.getLogger(QueryStatsDClient.class);
    
    // the statsd configuration
    private final String queryId;
    private final String host;
    private final int port;
    private final int maxCacheSize;
    
    // thread safe caches
    private final AtomicInteger nextCalls = new AtomicInteger(0);
    private final AtomicInteger seekCalls = new AtomicInteger(0);
    private final AtomicInteger sources = new AtomicInteger(0);
    private final Multimap<String,Long> timings;
    
    // the client
    private static StatsDClient client = null;
    // this monitor controls when the client is being used
    private static final Object clientMonitor = new Object();
    
    public QueryStatsDClient(String queryId, String host, int port, int maxCacheSize) {
        this.queryId = queryId;
        this.host = host;
        this.port = port;
        this.maxCacheSize = maxCacheSize;
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
}
