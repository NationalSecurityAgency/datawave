package nsa.datawave.query.rewrite.statsd;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import java.util.Map;
import nsa.datawave.query.rewrite.iterator.profile.QuerySpanCollector;
import org.apache.log4j.Logger;

/**
 * A client that can be used to record live query metrics
 */
public class QueryStatsDClient {
    private static Logger log = Logger.getLogger(QueryStatsDClient.class);
    
    protected StatsDClient client = null;
    
    public QueryStatsDClient(String queryId, String host, int port) {
        this.client = createClient(queryId + ".dwquery", host, port);
    }
    
    protected StatsDClient createClient(String prefix, String host, int port) {
        StatsDClient client = new NonBlockingStatsDClient(prefix, host, port);
        log.info("Created STATSD client with host = " + host + "; port = " + port + "; prefix = " + prefix);
        return client;
    }
    
    public void sendFinalStats(QuerySpanCollector stats) {
        if (client != null) {
            client.gauge("next_call_total", stats.getNextCount());
            client.gauge("seek_call_total", stats.getSeekCount());
            client.gauge("source_total", stats.getSourceCount());
            for (Map.Entry<String,Long> timing : stats.getStageTimers().entrySet()) {
                client.recordExecutionTime(timing.getKey() + "_total", timing.getValue());
            }
        }
    }
    
    public void next() {
        if (client != null) {
            client.increment("next_calls");
        }
    }
    
    public void seek() {
        if (client != null) {
            client.increment("seek_calls");
        }
    }
    
    public void addSource() {
        if (client != null) {
            client.increment("sources");
        }
    }
    
    public void timing(String call, long time) {
        if (client != null) {
            client.count(call, time);
        }
    }
    
    public void close() {
        if (client != null) {
            synchronized (this) {
                if (client != null) {
                    log.info("Closing the STATSD client");
                    client.stop();
                    client = null;
                }
            }
        }
    }
}
