package datawave.microservice.query.messaging.hazelcast;

import static datawave.microservice.query.messaging.hazelcast.HazelcastQueryResultsManager.HAZELCAST;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;

import datawave.microservice.query.messaging.QueryResultsListener;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.messaging.QueryResultsPublisher;
import datawave.microservice.query.messaging.config.MessagingProperties;

@Component
@ConditionalOnProperty(name = "datawave.query.messaging.backend", havingValue = HAZELCAST)
public class HazelcastQueryResultsManager implements QueryResultsManager {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String HAZELCAST = "hazelcast";
    
    static final String QUEUE_PREFIX = "queryResults.";
    
    static final String SPLIT_BRAIN_PROTECTION_NAME = "splitBrainDefault";
    
    private final MessagingProperties messagingProperties;
    private final HazelcastInstance hazelcastInstance;
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    public HazelcastQueryResultsManager(MessagingProperties messagingProperties, HazelcastInstance hazelcastInstance) {
        this.messagingProperties = messagingProperties;
        this.hazelcastInstance = hazelcastInstance;
    }
    
    @Override
    public QueryResultsListener createListener(String listenerId, String queryId) {
        return new HazelcastQueryResultsListener(HazelcastMessagingUtils.getOrCreateQueue(hazelcastInstance,
                        messagingProperties.getHazelcast().getBackupCount(), QUEUE_PREFIX + queryId), objectMapper, listenerId);
    }
    
    @Override
    public QueryResultsPublisher createPublisher(String queryId) {
        return new HazelcastQueryResultsPublisher(HazelcastMessagingUtils.getOrCreateQueue(hazelcastInstance,
                        messagingProperties.getHazelcast().getBackupCount(), QUEUE_PREFIX + queryId), objectMapper);
    }
    
    @Override
    public void deleteQuery(String queryId) {
        try {
            hazelcastInstance.getQueue(QUEUE_PREFIX + queryId).destroy();
        } catch (Exception e) {
            log.error("Failed to delete queue {}", queryId, e);
        }
    }
    
    @Override
    public void emptyQuery(String queryId) {
        try {
            hazelcastInstance.getQueue(QUEUE_PREFIX + queryId).clear();
        } catch (Exception e) {
            log.error("Unable to empty queue {}", queryId, e);
        }
    }
    
    @Override
    public int getNumResultsRemaining(String queryId) {
        return hazelcastInstance.getQueue(QUEUE_PREFIX + queryId).size();
    }
}
