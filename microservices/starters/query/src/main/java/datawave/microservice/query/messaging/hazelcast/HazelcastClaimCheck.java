package datawave.microservice.query.messaging.hazelcast;

import static datawave.microservice.query.messaging.hazelcast.HazelcastQueryResultsManager.HAZELCAST;
import static datawave.microservice.query.messaging.hazelcast.HazelcastQueryResultsManager.QUEUE_PREFIX;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;

import datawave.microservice.query.messaging.ClaimCheck;
import datawave.microservice.query.messaging.config.MessagingProperties;

@Component
@ConditionalOnExpression("${datawave.query.messaging.claimCheck.enabled:true} and ${datawave.query.messaging.claimCheck.backend:'" + HAZELCAST + "'} == '"
                + HAZELCAST + "'")
public class HazelcastClaimCheck implements ClaimCheck {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final MessagingProperties messagingProperties;
    private final HazelcastInstance hazelcastInstance;
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String,IQueue<String>> claimCheckMap = new ConcurrentHashMap<>();
    
    public HazelcastClaimCheck(MessagingProperties messagingProperties, HazelcastInstance hazelcastInstance) {
        this.messagingProperties = messagingProperties;
        this.hazelcastInstance = hazelcastInstance;
    }
    
    @Override
    public <T> void check(String queryId, T data) throws InterruptedException, JsonProcessingException {
        if (log.isTraceEnabled()) {
            log.trace("Checking large payload for query {}", queryId);
        }
        
        String stringData = objectMapper.writeValueAsString(new DataWrapper<>(data));
        getQueueForQueryId(QUEUE_PREFIX + queryId).put(stringData);
    }
    
    @Override
    public <T> T claim(String queryId) throws InterruptedException, JsonProcessingException {
        if (log.isTraceEnabled()) {
            log.trace("Claiming large payload for query {}", queryId);
        }
        
        String stringData = getQueueForQueryId(QUEUE_PREFIX + queryId).take();
        DataWrapper<T> wrapper = objectMapper.readerFor(DataWrapper.class).readValue(stringData);
        return wrapper.data;
    }
    
    private IQueue<String> getQueueForQueryId(String queryId) {
        // @formatter:off
        return claimCheckMap.computeIfAbsent(
                QUEUE_PREFIX + queryId,
                key -> HazelcastMessagingUtils.getOrCreateQueue(
                        hazelcastInstance,
                        messagingProperties.getHazelcast().getBackupCount(),
                        key));
        // @formatter:on
    }
    
    public void empty(String queryId) {
        if (log.isTraceEnabled()) {
            log.trace("Emptying claim check queue for query {}", queryId);
        }
        
        IQueue<?> queue = claimCheckMap.remove(QUEUE_PREFIX + queryId);
        if (queue != null) {
            queue.clear();
        }
    }
    
    public void delete(String queryId) {
        if (log.isTraceEnabled()) {
            log.trace("Deleting claim check queue for query {}", queryId);
        }
        
        IQueue<?> queue = claimCheckMap.remove(QUEUE_PREFIX + queryId);
        if (queue != null) {
            queue.destroy();
        }
    }
    
    static class DataWrapper<T> {
        @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
        public T data;
        
        public DataWrapper(@JsonProperty("data") T data) {
            this.data = data;
        }
    }
}
