package datawave.microservice.query.storage.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import datawave.microservice.query.storage.QueryQueueListener;
import datawave.microservice.query.storage.QueryQueueManager;
import datawave.microservice.query.storage.Result;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
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
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static datawave.microservice.query.storage.queue.KafkaQueryQueueManager.KAFKA;
import static org.apache.kafka.common.ConsumerGroupState.STABLE;

@Component
@ConditionalOnProperty(name = "query.storage.backend", havingValue = KAFKA)
@ConditionalOnMissingBean(type = "QueryQueueManager")
public class KafkaQueryQueueManager implements QueryQueueManager {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String KAFKA = "kafka";
    
    private final AdminClient adminClient;
    private final KafkaTemplate kafkaTemplate;
    private final ConsumerFactory kafkaConsumerFactory;
    
    // A mapping of queue names to routing keys
    private Map<String,String> topics = new HashMap<>();
    
    public KafkaQueryQueueManager(KafkaAdmin adminClient, KafkaTemplate kafkaTemplate, ConsumerFactory kafkaConsumerFactory) {
        this.adminClient = AdminClient.create(adminClient.getConfigurationProperties());
        this.kafkaTemplate = kafkaTemplate;
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
     * Ensure a queue is created for a query results queue. This will create an exchange, a queue, and a binding between them for the results queue.
     *
     * @param queryId
     *            the query ID
     */
    @Override
    public void ensureQueueCreated(String queryId) {
        ensureTopicCreated(queryId, queryId, queryId);
    }
    
    /**
     * Delete a queue for a query
     *
     * @param queryId
     *            the query ID
     */
    @Override
    public void deleteQueue(String queryId) {
        deleteTopic(queryId);
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
    public void sendMessage(String queryId, Result result) {
        ensureQueueCreated(queryId);
        
        String exchangeName = queryId;
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
     * @param topic
     *            The name of the queue
     * @param routingPattern
     *            The routing pattern used to bind the exchange to the queue
     * @param routingKey
     *            A routing key to use for a test message
     */
    private void ensureTopicCreated(String topic, String routingPattern, String routingKey) {
        if (topics.get(topic) == null) {
            synchronized (topics) {
                if (topics.get(topic) == null) {
                    if (log.isInfoEnabled()) {
                        log.debug("Creating topic " + topic + " with routing pattern " + routingPattern);
                    }
                    
                    try {
                        createTopic(topic, 10, (short) 1);
                        topicReadyWait(topic, 10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        Throwables.propagate(e);
                    }
                    
                    topics.put(topic, routingPattern);
                }
            }
        }
    }
    
    private void groupReadyWait(String group, long duration, TimeUnit unit) throws TimeoutException {
        long nanoStopTime = System.nanoTime() + unit.toNanos(duration);
        while (!isGroupReady(group)) {
            if (System.nanoTime() >= nanoStopTime) {
                throw new TimeoutException("Timed out waiting for Kafka group to be ready");
            }
            log.debug("Kafka Group [" + group + "] is not ready.");
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                log.debug("Timed out waiting to call group descriptions");
            }
        }
    }
    
    private boolean isGroupReady(String group) {
        return isGroupReady(group, -1L, null);
    }
    
    private boolean isGroupReady(String group, long interval, TimeUnit unit) {
        ConsumerGroupDescription groupDesc = describeGroup(group, interval, unit);
        boolean groupReady = groupDesc != null && groupDesc.state().equals(STABLE);
        if (!groupReady) {
            log.debug("Kafka Group [" + group + "] is not ready.");
        }
        return groupReady;
    }
    
    private void topicReadyWait(String topic, long duration, TimeUnit unit) throws TimeoutException {
        long nanoStopTime = System.nanoTime() + unit.toNanos(duration);
        while (!isTopicReady(topic)) {
            if (System.nanoTime() >= nanoStopTime) {
                throw new TimeoutException("Timed out waiting for Kafka topic to be ready");
            }
            log.warn("Kafka Topic [" + topic + "] is not ready.");
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                log.debug("Timed out waiting to call topic descriptions");
            }
        }
    }
    
    private boolean isTopicReady(String topic) {
        return isTopicReady(topic, -1L, null);
    }
    
    private boolean isTopicReady(String topic, long interval, TimeUnit unit) {
        TopicDescription topicDesc = describeTopic(topic);
        boolean topicReady = topicDesc != null;
        if (!topicReady) {
            log.debug("Kafka Topic [" + topic + "] is not ready.");
        }
        return topicReady;
    }
    
    private void createTopic(String topic, int partitions, short replicas) throws InterruptedException, ExecutionException, TimeoutException {
        createTopic(topic, partitions, replicas, -1L, null);
    }
    
    private void createTopic(String topic, int partitions, short replicas, long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
        // @formatter:off
        KafkaFuture<Void> voidFuture = adminClient
                .createTopics(Collections.singleton(new NewTopic(topic, partitions, replicas)))
                .values()
                .get(topic);
        // @formatter:on
        
        if (timeout >= 0 && unit != null) {
            voidFuture.get(timeout, unit);
        } else {
            voidFuture.get();
        }
    }
    
    private void deleteTopic(String topic) {
        deleteTopic(topic, -1L, null);
    }
    
    private void deleteTopic(String topic, long timeout, TimeUnit unit) {
        try {
            // @formatter:off
            KafkaFuture<Void> voidFuture = adminClient
                    .deleteTopics(Collections.singleton(topic))
                    .values()
                    .get(topic);
            // @formatter:on
            
            if (timeout >= 0 && unit != null) {
                voidFuture.get(timeout, unit);
            } else {
                voidFuture.get();
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to delete topic " + topic, e);
        } finally {
            topics.remove(topic);
        }
    }
    
    private void deleteGroup(String group) {
        deleteGroup(group, -1L, null);
    }
    
    private void deleteGroup(String group, long timeout, TimeUnit unit) {
        try {
            // @formatter:off
            KafkaFuture<Void> voidFuture = adminClient
                    .deleteConsumerGroups(Collections.singleton(group))
                    .deletedGroups()
                    .get(group);
            // @formatter:on
            
            if (timeout >= 0 && unit != null) {
                voidFuture.get(timeout, unit);
            } else {
                voidFuture.get();
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Failed to delete group " + group, e);
        }
    }
    
    @Override
    public void emptyQueue(String name) {
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
    public int getQueueSize(final String topic) {
        TopicDescription topicDescription = describeTopic(topic);
        Set<TopicPartition> partitions = topicDescription.partitions().stream().map(d -> new TopicPartition(topic, d.partition())).collect(Collectors.toSet());
        Consumer consumer = kafkaConsumerFactory.createConsumer();
        Map<TopicPartition,Long> offsets = consumer.endOffsets(partitions);
        Long total = offsets.entrySet().stream().mapToLong(e -> e.getValue() - consumer.position(e.getKey())).sum();
        if (total > Integer.MAX_VALUE) {
            throw new ArithmeticException("Queue size is over max int: total");
        }
        return total.intValue();
    }
    
    private ConsumerGroupDescription describeGroup(String group) {
        return describeGroup(group, -1L, null);
    }
    
    private ConsumerGroupDescription describeGroup(String group, long timeout, TimeUnit unit) {
        ConsumerGroupDescription groupDesc = null;
        try {
            // @formatter:off
            KafkaFuture<ConsumerGroupDescription> groupDescFuture = adminClient
                    .describeConsumerGroups(Collections.singletonList(group))
                    .describedGroups()
                    .get(group);
            // @formatter:on
            
            if (timeout >= 0 && unit != null) {
                groupDesc = groupDescFuture.get(timeout, unit);
            } else {
                groupDesc = groupDescFuture.get();
            }
            
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.warn("Unable to describe group " + group, e);
        }
        return groupDesc;
    }
    
    private TopicDescription describeTopic(String topic) {
        return describeTopic(topic, -1L, null);
    }
    
    private TopicDescription describeTopic(String topic, long timeout, TimeUnit unit) {
        TopicDescription topicDesc = null;
        try {
            // @formatter:off
            KafkaFuture<TopicDescription> topicDescFuture = adminClient
                    .describeTopics(Collections.singleton(topic))
                    .values()
                    .get(topic);
            // @formatter:on
            
            if (timeout >= 0 && unit != null) {
                topicDesc = topicDescFuture.get(timeout, unit);
            } else {
                topicDesc = topicDescFuture.get();
            }
            
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.warn("Unable to describe topic " + topic, e);
        }
        return topicDesc;
    }
    
    /**
     * A listener for local queues
     */
    public class KafkaQueueListener implements QueryQueueListener, MessageListener<String,String> {
        private ArrayBlockingQueue<Message<Result>> messageQueue = new ArrayBlockingQueue<>(100);
        private final String listenerId;
        private final String topicId;
        private final String groupId;
        private ConcurrentMessageListenerContainer container;
        
        public KafkaQueueListener(String listenerId, String topicId) {
            this(listenerId, topicId, null);
        }
        
        public KafkaQueueListener(String listenerId, String topicId, String groupId) {
            this.listenerId = listenerId;
            this.topicId = topicId;
            
            ContainerProperties props = new ContainerProperties(topicId);
            props.setMessageListener(this);
            props.setGroupId(groupId);
            
            container = new ConcurrentMessageListenerContainer<>(kafkaConsumerFactory, props);
            
            this.groupId = container.getGroupId();
            
            container.start();
            
            while (!container.isRunning()) {
                try {
                    Thread.sleep(1L);
                } catch (InterruptedException e) {
                    break;
                }
            }
            
            try {
                topicReadyWait(this.topicId, 10, TimeUnit.SECONDS);
                groupReadyWait(this.groupId, 10, TimeUnit.SECONDS);
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        }
        
        @Override
        public String getListenerId() {
            return listenerId;
        }
        
        public String getTopicId() {
            return topicId;
        }
        
        public String getGroupId() {
            return groupId;
        }
        
        @Override
        public void stop() {
            container.stop();
        }
        
        @Override
        public Message<Result> receive(long waitMs) {
            Message<Result> result = null;
            try {
                result = messageQueue.poll(waitMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                if (log.isTraceEnabled()) {
                    log.trace("Interrupted while waiting for query results");
                }
            }
            return result;
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
