package datawave.microservice.query.messaging.hazelcast;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.collection.IQueue;

import datawave.microservice.query.messaging.QueryResultsPublisher;
import datawave.microservice.query.messaging.Result;

public class HazelcastQueryResultsPublisher implements QueryResultsPublisher {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final IQueue<String> queue;
    private final ObjectMapper objectMapper;
    
    public HazelcastQueryResultsPublisher(IQueue<String> queue, ObjectMapper objectMapper) {
        this.queue = queue;
        this.objectMapper = objectMapper;
    }
    
    @Override
    public boolean publish(Result result, long interval, TimeUnit timeUnit) {
        if (log.isDebugEnabled()) {
            log.debug("Publishing message to " + queue.getName());
        }
        
        boolean success = false;
        try {
            success = queue.offer(objectMapper.writeValueAsString(result), interval, timeUnit);
        } catch (JsonProcessingException e) {
            log.error("Unable to serialize result", e);
        } catch (InterruptedException e) {
            log.error("Interrupted waiting for hazelcast offer", e);
        }
        return success;
    }
    
    @Override
    public void close() throws IOException {
        // do nothing
    }
}
