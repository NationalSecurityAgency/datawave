package datawave.microservice.query.messaging.kafka;

import datawave.microservice.query.messaging.QueryResultsListener;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.messaging.QueryResultsPublisher;
import datawave.microservice.query.messaging.config.MessagingProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static datawave.microservice.query.messaging.kafka.KafkaQueryResultsManager.KAFKA;

@Component
@ConditionalOnProperty(name = "query.messaging.backend", havingValue = KAFKA)
public class KafkaQueryResultsManager implements QueryResultsManager {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String KAFKA = "kafka";
    
    private final MessagingProperties messagingProperties;
    private final AdminClient adminClient;
    private final ProducerFactory<String,String> kafkaProducerFactory;
    private final ConsumerFactory<String,String> kafkaConsumerFactory;
    
    public KafkaQueryResultsManager(MessagingProperties messagingProperties, KafkaAdmin adminClient, ProducerFactory<String,String> kafkaProducerFactory,
                    ConsumerFactory<String,String> kafkaConsumerFactory) {
        this.messagingProperties = messagingProperties;
        this.adminClient = AdminClient.create(adminClient.getConfigurationProperties());
        this.kafkaProducerFactory = kafkaProducerFactory;
        this.kafkaConsumerFactory = kafkaConsumerFactory;
    }
    
    /**
     * Create a listener for a specified listener id
     *
     * @param listenerId
     *            The listener id
     * @return a query queue listener
     */
    @Override
    public QueryResultsListener createListener(String listenerId, String queryId) {
        return new KafkaQueryResultsListener(messagingProperties, kafkaConsumerFactory, listenerId, queryId);
    }
    
    /**
     * Create a publisher for a specific query id.
     * 
     * @param queryId
     *            The query ID to publish to
     * @return a query result publisher
     */
    @Override
    public QueryResultsPublisher createPublisher(String queryId) {
        KafkaTemplate<String,String> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
        kafkaTemplate.setDefaultTopic(queryId);
        return new KafkaQueryResultsPublisher(kafkaTemplate);
    }
    
    /**
     * Delete a queue for a query
     *
     * @param queryId
     *            the query ID
     */
    @Override
    public void deleteQuery(String queryId) {
        deleteTopic(queryId);
    }
    
    private void deleteTopic(String topic) {
        try {
            if (adminClient.listTopics().names().get().contains("topic")) {
                // @formatter:off
                adminClient
                        .deleteTopics(Collections.singleton(topic))
                        .values()
                        .get(topic)
                        .get();
                // @formatter:on
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to delete topic " + topic, e);
        }
    }
    
    @Override
    public void emptyQuery(String name) {
        TopicDescription topic = describeTopic(name);
        if (topic != null) {
            Map<TopicPartition,RecordsToDelete> partitions = new HashMap<>();
            RecordsToDelete records = RecordsToDelete.beforeOffset(Long.MAX_VALUE);
            for (TopicPartitionInfo info : topic.partitions()) {
                TopicPartition partition = new TopicPartition(name, info.partition());
                partitions.put(partition, records);
            }
            DeleteRecordsResult result2 = adminClient.deleteRecords(partitions);
            try {
                result2.all();
            } catch (Exception e) {
                log.debug("Unable to empty queue " + name, e);
            }
        }
    }
    
    @Override
    public int getNumResultsRemaining(final String topic) {
        Map<TopicPartition,Long> consumerOffsetMap = new HashMap<>();
        try {
            // @formatter:off
            adminClient
                    .listConsumerGroupOffsets(topic)
                    .partitionsToOffsetAndMetadata()
                    .get()
                    .forEach((key, value) -> consumerOffsetMap.putIfAbsent(new TopicPartition(key.topic(), key.partition()), value.offset()));
            // @formatter:on
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Unable to list consumer group offsets " + topic, e);
        }
        
        long combinedLag = 0L;
        if (!consumerOffsetMap.isEmpty()) {
            Map<TopicPartition,Long> endOffsetMap = new HashMap<>();
            
            try (Consumer<String,String> consumer = kafkaConsumerFactory.createConsumer()) {
                // @formatter:off
                consumer.endOffsets(consumerOffsetMap.keySet())
                        .forEach((key, value) -> endOffsetMap.putIfAbsent(new TopicPartition(key.topic(), key.partition()), value));
                // @formatter:on
            }
            
            if (!endOffsetMap.isEmpty()) {
                long totalLag = 0L;
                for (Map.Entry<TopicPartition,Long> entry : consumerOffsetMap.entrySet()) {
                    totalLag += (endOffsetMap.get(entry.getKey()) - consumerOffsetMap.get(entry.getKey()));
                }
                
                combinedLag = totalLag;
            }
        }
        
        return (int) combinedLag;
    }
    
    private TopicDescription describeTopic(String topic) {
        TopicDescription topicDesc = null;
        try {
            // @formatter:off
            topicDesc = adminClient
                    .describeTopics(Collections.singleton(topic))
                    .values()
                    .get(topic)
                    .get();
            // @formatter:on
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Unable to describe topic " + topic, e);
        }
        return topicDesc;
    }
}
