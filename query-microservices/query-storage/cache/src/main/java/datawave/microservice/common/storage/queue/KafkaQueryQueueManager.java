package datawave.microservice.common.storage.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.common.storage.QueryPool;
import datawave.microservice.common.storage.QueryQueueListener;
import datawave.microservice.common.storage.QueryQueueManager;
import datawave.microservice.common.storage.QueryTask;
import datawave.microservice.common.storage.QueryTaskNotification;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.common.storage.config.QueryStorageProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.messaging.Message;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static datawave.microservice.common.storage.queue.KafkaQueryQueueManager.TestMessageConsumer.TEST_MESSAGE;

public class KafkaQueryQueueManager implements QueryQueueManager {
    private static final Logger log = Logger.getLogger(QueryQueueManager.class);
    
    @Autowired
    QueryStorageProperties properties;
    
    @Autowired
    private AdminClient kafkaAdmin;
    
    @Autowired
    private KafkaTemplate kafkaTemplate;
    
    @Autowired
    private DefaultKafkaConsumerFactory kafkaConsumerFactory;
    
    // A mapping of queue names to routing keys
    private Map<String,String> queues = new HashMap<>();
    
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
     * Passes task notifications to the messaging infrastructure.
     *
     * @param taskNotification
     *            The task notification to be sent
     */
    @Override
    public void sendMessage(QueryTaskNotification taskNotification) {
        ensureQueueCreated(taskNotification.getTaskKey().getQueryPool());
        
        String exchangeName = taskNotification.getTaskKey().getQueryPool().getName();
        if (log.isDebugEnabled()) {
            log.debug("Publishing message to " + exchangeName + " for " + taskNotification.getTaskKey().toKey());
        }
        try {
            kafkaTemplate.send(exchangeName, taskNotification.getTaskKey().toKey(), new ObjectMapper().writeValueAsBytes(taskNotification));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize QueryTaskNotification as json", e);
        }
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
     * @param resultId
     *            a unique id for the result
     * @param result
     *            the result
     */
    @Override
    public void sendMessage(UUID queryId, String resultId, Object result) {
        ensureQueueCreated(queryId);
        
        String exchangeName = queryId.toString();
        if (log.isDebugEnabled()) {
            log.debug("Publishing message to " + exchangeName);
        }
        kafkaTemplate.send(exchangeName, resultId, result);
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
                    
                    // should be created automatically when sending the message
                    
                    if (log.isInfoEnabled()) {
                        log.debug("Sending test message to verify topic " + queueName);
                    }
                    // add our test listener to the queue and wait for a test message
                    boolean received = false;
                    TestMessageConsumer testMessageConsumer = null;
                    try {
                        testMessageConsumer = new TestMessageConsumer(queueName);
                        kafkaTemplate.send(queueName, routingKey, new ObjectMapper().writeValueAsBytes(TEST_MESSAGE));
                        received = testMessageConsumer.receiveTest();
                    } catch (JsonProcessingException e) {
                        throw new IllegalStateException("Unable to serialize a string as json?", e);
                    } finally {
                        if (testMessageConsumer != null) {
                            testMessageConsumer.stop();
                        }
                    }
                    if (!received) {
                        throw new RuntimeException("Unable to verify that queue and exchange were created for " + queueName);
                    }
                    
                    queues.put(queueName, routingPattern);
                }
            }
        }
    }
    
    private void deleteQueue(String name) {
        DeleteTopicsResult result = kafkaAdmin.deleteTopics(Collections.singleton(name));
        try {
            result.all();
        } catch (Exception e) {
            log.error("Failed to delete queue " + name, e);
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
    
    public class TestMessageConsumer extends KafkaQueueListener {
        private static final String LISTENER_ID = "KafkaQueryQueueManagerTestListener";
        public static final String TEST_MESSAGE = "TEST_MESSAGE";
        
        private ConcurrentMessageListenerContainer container;
        private AtomicInteger semaphore = new AtomicInteger(0);
        
        // default wait for 1 minute
        private static final long WAIT_MS_DEFAULT = 60L * 1000L;
        
        public TestMessageConsumer(String topicId) {
            super(LISTENER_ID, topicId);
        }
        
        @Override
        public void onMessage(ConsumerRecord<String,byte[]> data) {
            if (log.isTraceEnabled()) {
                log.trace("Test Listener " + getListenerId() + " got message " + data.key());
            }
            String body = null;
            try {
                body = new ObjectMapper().readerFor(String.class).readValue(data.value());
            } catch (Exception e) {
                // body is not a json string
            }
            // determine if this is a test message
            if (TEST_MESSAGE.equals(body)) {
                if (log.isTraceEnabled()) {
                    log.trace("Test Listener " + getListenerId() + " got a test message");
                }
                semaphore.incrementAndGet();
            }
        }
        
        public boolean receiveTest() {
            return receiveTest(WAIT_MS_DEFAULT);
        }
        
        public boolean receiveTest(long waitMs) {
            long start = System.currentTimeMillis();
            while ((semaphore.get() == 0) && ((System.currentTimeMillis() - start) < waitMs)) {
                try {
                    Thread.sleep(1L);
                } catch (InterruptedException e) {
                    break;
                }
            }
            if (semaphore.get() > 0) {
                semaphore.decrementAndGet();
                return true;
            } else {
                return false;
            }
        }
    }
    
    /**
     * A listener for local queues
     */
    public class KafkaQueueListener implements QueryQueueListener, MessageListener<String,byte[]> {
        private java.util.Queue<Message> messageQueue = new ArrayBlockingQueue<>(100);
        private final String listenerId;
        private final String topicId;
        private ConcurrentMessageListenerContainer container;
        
        public KafkaQueueListener(String listenerId, String topicId) {
            this.listenerId = listenerId;
            this.topicId = topicId;
            
            ContainerProperties props = new ContainerProperties(topicId);
            props.setMessageListener(this);
            
            container = new ConcurrentMessageListenerContainer<>(kafkaConsumerFactory, props);
            
            container.start();
            
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
        public Message<byte[]> receive(long waitMs) {
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
        public void onMessage(ConsumerRecord<String,byte[]> data) {
            if (log.isTraceEnabled()) {
                log.trace("Listener " + getListenerId() + " got message " + data.key());
            }
            MessagingMessageConverter converter = new MessagingMessageConverter();
            Message message = converter.toMessage(data, null, null, byte[].class);
            messageQueue.add(message);
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
        public void onMessage(ConsumerRecord<String,byte[]> data, Acknowledgment acknowledgment) {
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
        public void onMessage(ConsumerRecord<String,byte[]> data, Consumer<?,?> consumer) {
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
        public void onMessage(ConsumerRecord<String,byte[]> data, Acknowledgment acknowledgment, Consumer<?,?> consumer) {
            onMessage(data, acknowledgment);
        }
    }
    
}
