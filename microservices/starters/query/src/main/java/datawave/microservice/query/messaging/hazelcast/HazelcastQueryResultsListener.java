package datawave.microservice.query.messaging.hazelcast;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.collection.IQueue;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;

import datawave.microservice.query.messaging.QueryResultsListener;
import datawave.microservice.query.messaging.Result;

public class HazelcastQueryResultsListener implements QueryResultsListener {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final IQueue<String> queue;
    private final ObjectMapper objectMapper;
    private final String listenerId;
    
    private boolean stopped = false;
    
    public HazelcastQueryResultsListener(IQueue<String> queue, ObjectMapper objectMapper, String listenerId) {
        this.queue = queue;
        this.objectMapper = objectMapper;
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
                log.debug("Interrupted while waiting for query results");
            } catch (JsonProcessingException e) {
                log.debug("Unable to deserialize result");
            } catch (DistributedObjectDestroyedException e) {
                log.debug("Unable to poll results from destroyed queue");
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
