package datawave.microservice.query.messaging.kafka;

import static datawave.microservice.query.messaging.AcknowledgementCallback.Status.ACK;
import static datawave.microservice.query.messaging.AcknowledgementCallback.Status.NACK;
import static datawave.microservice.query.messaging.kafka.KafkaQueryResultsManager.TOPIC_PREFIX;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import datawave.microservice.query.messaging.AcknowledgementCallback;
import datawave.microservice.query.messaging.QueryResultsListener;
import datawave.microservice.query.messaging.Result;
import datawave.microservice.query.messaging.config.MessagingProperties;

/**
 * A listener for Kafka Query Results
 */
class KafkaQueryResultsListener implements QueryResultsListener, AcknowledgingMessageListener<String,String> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final LinkedBlockingQueue<Result> resultQueue = new LinkedBlockingQueue<>();
    private final AbstractMessageListenerContainer<String,String> container;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String queryId;
    private boolean stopped = false;
    
    public KafkaQueryResultsListener(MessagingProperties messagingProperties, ConsumerFactory<String,String> kafkaConsumerFactory, String listenerId,
                    String queryId) {
        if (log.isTraceEnabled()) {
            log.trace("Creating kafka query results listener for " + queryId + " with listenerId " + listenerId);
        }
        this.queryId = queryId;
        ContainerProperties containerProps = new ContainerProperties(TOPIC_PREFIX + queryId);
        containerProps.setClientId(listenerId);
        
        // use the topicId (i.e. queryId) as the groupId. this makes it possible
        // to get the size of the queue later on using just the query id
        containerProps.setGroupId(TOPIC_PREFIX + queryId);
        
        containerProps.setMessageListener(this);
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        containerProps.setPollTimeout(messagingProperties.getKafka().getPollTimeoutMillis());
        containerProps.setIdleBetweenPolls(messagingProperties.getKafka().getIdleBetweenPollsMillis());
        
        if (messagingProperties.getConcurrency() > 1) {
            ConcurrentMessageListenerContainer<String,String> concurrentContainer = new ConcurrentMessageListenerContainer<>(kafkaConsumerFactory,
                            containerProps);
            concurrentContainer.setConcurrency(messagingProperties.getConcurrency());
            container = concurrentContainer;
        } else {
            container = new KafkaMessageListenerContainer<>(kafkaConsumerFactory, containerProps);
        }
        
        container.setBeanName(listenerId + "-" + queryId);
        container.start();
        if (log.isTraceEnabled()) {
            if (container.isRunning()) {
                log.trace("Container started for " + listenerId);
            } else {
                log.trace("Container not yet started for " + listenerId);
            }
        }
    }
    
    @Override
    public String getListenerId() {
        return container.getListenerId();
    }
    
    public String getQueryId() {
        return queryId;
    }
    
    @Override
    public void close() {
        stopped = true;
        
        // synchronizing on the resultQueue to ensure onMessage() does not add anymore results
        synchronized (resultQueue) {
            // nack all of the extra messages we have received
            for (Result result : resultQueue) {
                result.acknowledge(NACK);
            }
        }
        
        container.stop(true);
        container.destroy();
    }
    
    @Override
    public boolean hasResults() {
        return !resultQueue.isEmpty();
    }
    
    @Override
    public Result receive(long interval, TimeUnit timeUnit) {
        Result result = null;
        try {
            result = resultQueue.poll(interval, timeUnit);
        } catch (InterruptedException e) {
            if (log.isTraceEnabled()) {
                log.trace("Interrupted while waiting for query results");
            }
        }
        return result;
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
    public void onMessage(ConsumerRecord<String,String> data, final Acknowledgment acknowledgment) {
        if (!stopped) {
            if (log.isTraceEnabled()) {
                log.trace("Query " + queryId + " Listener " + getListenerId() + " got message " + data.key());
            }
            
            Result result;
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<AcknowledgementCallback.Status> ackStatus = new AtomicReference<>();
            String resultId = null;
            try {
                result = objectMapper.readerFor(Result.class).readValue(data.value());
                resultId = result.getId();
                
                if (log.isTraceEnabled()) {
                    log.trace("Query {} Received record {} from topic {} and partition {} at offset {}", queryId, resultId, data.topic(), data.partition(),
                                    data.offset());
                }
                
                result.setAcknowledgementCallback(status -> {
                    ackStatus.set(status);
                    latch.countDown();
                });
            } catch (JsonProcessingException e) {
                acknowledgment.nack(0);
                if (log.isTraceEnabled()) {
                    log.trace("Query {} Nacking record from topic {} and partition {} at offset {} because the result could not be deserialized", queryId,
                                    data.topic(), data.partition(), data.offset());
                }
                throw new RuntimeException("Unable to deserialize results for " + queryId, e);
            }
            
            // add the result if we're still running, otherwise nack it right away
            synchronized (resultQueue) {
                if (!stopped) {
                    // synchronize on resultQueue to ensure we don't add any results if in the close call.
                    resultQueue.add(result);
                } else {
                    result.acknowledge(NACK);
                }
            }
            
            try {
                latch.await();
                if (ackStatus.get() == ACK) {
                    acknowledgment.acknowledge();
                    if (log.isTraceEnabled()) {
                        log.trace("Query {} Acking record {} from topic {} and partition {} at offset {}", queryId, resultId, data.topic(), data.partition(),
                                        data.offset());
                    }
                } else if (ackStatus.get() == NACK) {
                    acknowledgment.nack(0);
                    if (log.isTraceEnabled()) {
                        log.trace("Query {} Nacking record {} from topic {} and partition {} at offset {} because the record was rejected", queryId, resultId,
                                        data.topic(), data.partition(), data.offset());
                    }
                }
            } catch (InterruptedException ie) {
                acknowledgment.nack(0);
                if (log.isTraceEnabled()) {
                    log.trace("Query {} Nacking record {} from topic {} and partition {} at offset {} because the latch was interrupted", queryId, resultId,
                                    data.topic(), data.partition(), data.offset());
                }
            }
        } else {
            acknowledgment.nack(0);
            if (log.isTraceEnabled()) {
                log.trace("Query {} Nacking record from topic {} and partition {} at offset {} because the container was stopped", queryId, data.topic(),
                                data.partition(), data.offset());
            }
        }
    }
}
