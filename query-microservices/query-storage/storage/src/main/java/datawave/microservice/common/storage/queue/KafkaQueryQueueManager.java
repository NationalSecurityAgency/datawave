package datawave.microservice.common.storage.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import datawave.microservice.common.storage.QueryPool;
import datawave.microservice.common.storage.QueryQueueListener;
import datawave.microservice.common.storage.QueryQueueManager;
import datawave.microservice.common.storage.QueryTask;
import datawave.microservice.common.storage.QueryTaskNotification;
import datawave.microservice.common.storage.Result;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.common.storage.config.QueryStorageProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

import static datawave.microservice.common.storage.queue.KafkaQueryQueueManager.KAFKA;

@Component
@ConditionalOnProperty(name = "query.storage.backend", havingValue = KAFKA)
@ConditionalOnMissingBean(type = "QueryQueueManager")
public class KafkaQueryQueueManager implements QueryQueueManager {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String KAFKA = "kafka";
    
    private final QueryStorageProperties properties;
    private final AdminClient kafkaAdmin;
    private final KafkaTemplate kafkaTemplate;
    private final ConsumerFactory kafkaConsumerFactory;
    
    // A mapping of queue names to routing keys
    private Map<String,String> queues = new HashMap<>();
    
    public KafkaQueryQueueManager(QueryStorageProperties properties, AdminClient kafkaAdmin, ProducerFactory kafkaProducerFactory,
                    ConsumerFactory kafkaConsumerFactory) {
        this.properties = properties;
        this.kafkaAdmin = kafkaAdmin;
        this.kafkaTemplate = new KafkaTemplate(kafkaProducerFactory);
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
    public QueryQueueListener createListener(String listenerId, String topicId) {
        QueryQueueListener listener = new KafkaQueryQueueManager.KafkaQueueListener(listenerId, topicId);
        return listener;
    }
    
    /**
     * Ensure a queue is created for a pool. This will create an exchange, a queue, and a binding between them for the query pool.
     *
     * @param queryPool
     *            the query poll
     */
    @Override
    public void ensureQueueCreated(QueryPool queryPool) {
        QueryTaskNotification testMessage = new QueryTaskNotification(new TaskKey(UUID.randomUUID(), queryPool, UUID.randomUUID(), "NA"),
                        QueryTask.QUERY_ACTION.TEST);
        ensureQueueCreated(queryPool.getName(), testMessage.getTaskKey().toRoutingKey(), testMessage.getTaskKey().toKey());
    }
    
    /**
     * A mechanism to delete a queue for a pool.
     *
     * @param queryPool
     *            the query pool
     */
    @Override
    public void deleteQueue(QueryPool queryPool) {
        deleteQueue(queryPool.getName());
    }
    
    /**
     * A mechanism to empty a queues messages for a pool
     *
     * @param queryPool
     *            the query pool
     */
    @Override
    public void emptyQueue(QueryPool queryPool) {
        emptyQueue(queryPool.getName());
    }
    
    /**
     * Ensure a queue is created for a query results queue. This will create an exchange, a queue, and a binding between them for the results queue.
     *
     * @param queryId
     *            the query ID
     */
    @Override
    public void ensureQueueCreated(UUID queryId) {
        ensureQueueCreated(queryId.toString(), queryId.toString(), queryId.toString());
    }
    
    /**
     * Delete a queue for a query
     *
     * @param queryId
     *            the query ID
     */
    @Override
    public void deleteQueue(UUID queryId) {
        deleteQueue(queryId.toString());
    }
    
    /**
     * Empty a queue for a query
     *
     * @param queryId
     *            the query ID
     */
    @Override
    public void emptyQueue(UUID queryId) {
        emptyQueue(queryId.toString());
    }
    
    /**
     * This will send a result message. This will call ensureQueueCreated before sending the message.
     * <p>
     * TODO Should the result be more strongly typed?
     *
     * @param queryId
     *            the query ID
     * @param result
     *            the result
     */
    @Override
    public void sendMessage(UUID queryId, Result result) {
        ensureQueueCreated(queryId);
        
        String exchangeName = queryId.toString();
        if (log.isDebugEnabled()) {
            log.debug("Publishing message to " + exchangeName);
        }
        try {
            kafkaTemplate.send(exchangeName, new ObjectMapper().writeValueAsString(result));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to serialize result", e);
        }
    }
    
    /**
     * Ensure a queue is created for a given pool
     *
     * @param queueName
     *            The name of the queue
     * @param routingPattern
     *            The routing pattern used to bind the exchange to the queue
     * @param routingKey
     *            A routing key to use for a test message
     */
    private void ensureQueueCreated(String queueName, String routingPattern, String routingKey) {
        if (queues.get(queueName) == null) {
            synchronized (queues) {
                if (queues.get(queueName) == null) {
                    if (log.isInfoEnabled()) {
                        log.debug("Creating topic " + queueName + " with routing pattern " + routingPattern);
                    }
                    
                    CreateTopicsResult createFuture = kafkaAdmin.createTopics(Collections.singleton(new NewTopic(queueName, 10, (short) 1)));
                    try {
                        createFuture.all().get();
                    } catch (Exception e) {
                        Throwables.propagate(e);
                    }
                    
                    queues.put(queueName, routingPattern);
                }
            }
        }
    }
    
    private void deleteQueue(String name) {
        try {
            DeleteTopicsResult result = kafkaAdmin.deleteTopics(Collections.singleton(name));
            try {
                result.all().get();
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        } catch (Exception e) {
            log.error("Failed to delete queue " + name, e);
        } finally {
            queues.remove(name);
        }
    }
    
    private void emptyQueue(String name) {
        DescribeTopicsResult result = kafkaAdmin.describeTopics(Collections.singleton(name));
        TopicDescription topic = null;
        try {
            topic = result.values().get(name).get();
        } catch (Exception e) {
            log.error("Unable to describe topic " + name, e);
        }
        if (topic != null) {
            Map<TopicPartition,RecordsToDelete> partitions = new HashMap<>();
            RecordsToDelete records = RecordsToDelete.beforeOffset(Long.MAX_VALUE);
            for (TopicPartitionInfo info : topic.partitions()) {
                TopicPartition partition = new TopicPartition(name, info.partition());
                partitions.put(partition, records);
            }
            DeleteRecordsResult result2 = kafkaAdmin.deleteRecords(partitions);
            try {
                result2.all();
            } catch (Exception e) {
                log.error("Unable to empty queue " + name, e);
            }
        }
    }
    
    /**
     * A listener for local queues
     */
    public class KafkaQueueListener implements QueryQueueListener, MessageListener<String,String> {
        private java.util.Queue<Message<Result>> messageQueue = new ArrayBlockingQueue<>(100);
        private final String listenerId;
        private final String topicId;
        private ConcurrentMessageListenerContainer container;
        
        public KafkaQueueListener(String listenerId, String topicId) {
            this.listenerId = listenerId;
            this.topicId = topicId;
            
            ContainerProperties props = new ContainerProperties(topicId);
            props.setMessageListener(this);
            props.setGroupId(topicId);
            
            container = new ConcurrentMessageListenerContainer<>(kafkaConsumerFactory, props);
            
            container.start();
            
            while (!container.isRunning()) {
                try {
                    Thread.sleep(1L);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
        
        @Override
        public String getListenerId() {
            return listenerId;
        }
        
        public String getTopicId() {
            return topicId;
        }
        
        @Override
        public void stop() {
            container.stop();
        }
        
        @Override
        public Message<Result> receive(long waitMs) {
            long start = System.currentTimeMillis();
            int count = 0;
            while (messageQueue.isEmpty() && ((System.currentTimeMillis() - start) < waitMs)) {
                count++;
                try {
                    Thread.sleep(1L);
                } catch (InterruptedException e) {
                    break;
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("Cycled " + count + " rounds looking for message");
            }
            if (messageQueue.isEmpty()) {
                return null;
            } else {
                return messageQueue.remove();
            }
        }
        
        /**
         * Invoked with data from kafka.
         *
         * @param data
         *            the data to be processed.
         */
        @Override
        public void onMessage(ConsumerRecord<String,String> data) {
            if (log.isTraceEnabled()) {
                log.trace("Listener " + getListenerId() + " got message " + data.key());
            }
            MessagingMessageConverter converter = new MessagingMessageConverter();
            Message<String> message = (Message<String>) converter.toMessage(data, null, null, String.class);
            Message<Result> resultMessage = null;
            try {
                resultMessage = new GenericMessage<>(new ObjectMapper().readerFor(Result.class).readValue(message.getPayload()), message.getHeaders());
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Unable to deserialize results", e);
            }
            messageQueue.add(resultMessage);
        }
        
        /**
         * Invoked with data from kafka. The default implementation throws {@link UnsupportedOperationException}.
         *
         * @param data
         *            the data to be processed.
         * @param acknowledgment
         *            the acknowledgment.
         */
        @Override
        public void onMessage(ConsumerRecord<String,String> data, Acknowledgment acknowledgment) {
            onMessage(data);
            acknowledgment.acknowledge();
        }
        
        /**
         * Invoked with data from kafka and provides access to the {@link Consumer}. The default implementation throws {@link UnsupportedOperationException}.
         *
         * @param data
         *            the data to be processed.
         * @param consumer
         *            the consumer.
         * @since 2.0
         */
        @Override
        public void onMessage(ConsumerRecord<String,String> data, Consumer<?,?> consumer) {
            onMessage(data);
        }
        
        /**
         * Invoked with data from kafka and provides access to the {@link Consumer}. The default implementation throws {@link UnsupportedOperationException}.
         *
         * @param data
         *            the data to be processed.
         * @param acknowledgment
         *            the acknowledgment.
         * @param consumer
         *            the consumer.
         * @since 2.0
         */
        @Override
        public void onMessage(ConsumerRecord<String,String> data, Acknowledgment acknowledgment, Consumer<?,?> consumer) {
            onMessage(data, acknowledgment);
        }
    }
    
}
