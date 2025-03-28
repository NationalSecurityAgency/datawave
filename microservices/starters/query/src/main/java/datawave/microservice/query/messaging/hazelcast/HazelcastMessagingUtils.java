package datawave.microservice.query.messaging.hazelcast;

import static datawave.microservice.query.messaging.hazelcast.HazelcastQueryResultsManager.SPLIT_BRAIN_PROTECTION_NAME;

import com.hazelcast.collection.IQueue;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastMessagingUtils {
    
    static <T> IQueue<T> getOrCreateQueue(HazelcastInstance hazelcastInstance, int backupCount, String queryId) {
        QueueConfig queueConfig = new QueueConfig(queryId);
        queueConfig.setSplitBrainProtectionName(SPLIT_BRAIN_PROTECTION_NAME);
        
        if (backupCount >= 0) {
            queueConfig.setBackupCount(backupCount);
        }
        
        hazelcastInstance.getConfig().addQueueConfig(queueConfig);
        
        return hazelcastInstance.getQueue(queryId);
    }
}
