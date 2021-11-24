package datawave.microservice.query.messaging.hazelcast;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.collection.IQueue;
import datawave.microservice.query.messaging.QueryResultsListener;
import datawave.microservice.query.messaging.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class HazelcastQueryResultsListener implements QueryResultsListener {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final IQueue<String> queue;
    private final String listenerId;
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    private boolean stopped = false;
    
    public HazelcastQueryResultsListener(IQueue<String> queue, String listenerId) {
        this.queue = queue;
        this.listenerId = listenerId;
    }
    
    @Override
    public String getListenerId() {
        return listenerId;
    }
    
    @Override
    public Result receive(long interval, TimeUnit timeUnit) {
        Result result = null;
        if (!stopped) {
            try {
                String data = queue.poll(interval, timeUnit);
                if (data != null) {
                    result = objectMapper.readerFor(Result.class).readValue(data);
                }
            } catch (InterruptedException e) {
                if (log.isTraceEnabled()) {
                    log.trace("Interrupted while waiting for query results");
                }
            } catch (JsonProcessingException e) {
                if (log.isTraceEnabled()) {
                    log.trace("Unable to deserialize result");
                }
            }
        }
        return result;
    }
    
    @Override
    public boolean hasResults() {
        return !queue.isEmpty();
    }
    
    @Override
    public void close() throws IOException {
        stopped = true;
    }
}
