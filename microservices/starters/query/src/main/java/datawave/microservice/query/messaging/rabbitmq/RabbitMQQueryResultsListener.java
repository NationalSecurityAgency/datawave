package datawave.microservice.query.messaging.rabbitmq;

import static datawave.microservice.query.messaging.AcknowledgementCallback.Status.ACK;
import static datawave.microservice.query.messaging.AcknowledgementCallback.Status.NACK;
import static datawave.microservice.query.messaging.rabbitmq.RabbitMQQueryResultsManager.QUERY_QUEUE_PREFIX;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.config.DirectRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;

import datawave.microservice.query.messaging.AcknowledgementCallback;
import datawave.microservice.query.messaging.ClaimCheck;
import datawave.microservice.query.messaging.QueryResultsListener;
import datawave.microservice.query.messaging.Result;

/**
 * A listener for RabbitMQ Query Results
 */
class RabbitMQQueryResultsListener extends MessageListenerAdapter implements QueryResultsListener {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final RabbitListenerEndpointRegistry endpointRegistry;
    private final ClaimCheck claimCheck;
    private final String listenerId;
    private final String queryId;
    
    private final LinkedBlockingQueue<Result> resultQueue = new LinkedBlockingQueue<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private boolean stopped = false;
    
    public RabbitMQQueryResultsListener(DirectRabbitListenerContainerFactory listenerContainerFactory, RabbitListenerEndpointRegistry endpointRegistry,
                    ClaimCheck claimCheck, String listenerId, String queryId) {
        this.endpointRegistry = endpointRegistry;
        this.claimCheck = claimCheck;
        this.listenerId = listenerId;
        this.queryId = queryId;
        
        SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
        endpoint.setMessageListener(this);
        endpoint.setId(listenerId);
        endpoint.setQueueNames(QUERY_QUEUE_PREFIX + queryId);
        endpoint.setAckMode(AcknowledgeMode.MANUAL);
        
        this.endpointRegistry.registerListenerContainer(endpoint, listenerContainerFactory, true);
    }
    
    @Override
    public String getListenerId() {
        return listenerId;
    }
    
    public String getQueryId() {
        return queryId;
    }
    
    @Override
    public void close() {
        stopped = true;
        
        // nack all of the extra messages we have received
        for (Result result : resultQueue) {
            result.acknowledge(NACK);
        }
        
        MessageListenerContainer container = endpointRegistry.unregisterListenerContainer(listenerId);
        if (container != null) {
            container.stop();
        } else {
            log.error("Could not find listener container to stop");
        }
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
    
    @Override
    public void onMessage(Message message, final Channel channel) throws Exception {
        if (!stopped) {
            if (log.isTraceEnabled()) {
                log.trace("Query " + queryId + " Listener " + getListenerId() + " got a message");
            }
            
            Result result;
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<AcknowledgementCallback.Status> ackStatus = new AtomicReference<>();
            String resultId = null;
            try {
                result = objectMapper.readerFor(Result.class).readValue(message.getBody());
                resultId = result.getId();
                
                // if the payload is null, setup a claim check
                if (result.getPayload() == null && claimCheck != null) {
                    result.setClaimCheckCallback(() -> claimCheck.claim(queryId));
                }
                
                if (log.isTraceEnabled()) {
                    log.trace("Query {} Received record {} from queue {}", queryId, resultId, queryId);
                }
                
                result.setAcknowledgementCallback(status -> {
                    ackStatus.set(status);
                    latch.countDown();
                });
            } catch (JsonProcessingException e) {
                channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
                if (log.isTraceEnabled()) {
                    log.trace("Query {} Nacking record {} from queue {} because the latch was interrupted", queryId, resultId, queryId);
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
                    channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
                    if (log.isTraceEnabled()) {
                        log.trace("Query {} Acking record {} from queue {}", queryId, result.getId(), queryId);
                    }
                } else if (ackStatus.get() == NACK) {
                    channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
                    if (log.isTraceEnabled()) {
                        log.trace("Query {} Nacking record {} from queue {} because the record was rejected", queryId, result.getId(), queryId);
                    }
                }
            } catch (InterruptedException e) {
                channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
                if (log.isTraceEnabled()) {
                    log.trace("Query {} Nacking record {} from queue {} because the latch was interrupted", queryId, result.getId(), queryId);
                }
            }
        } else {
            channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
            if (log.isTraceEnabled()) {
                log.trace("Query {} Nacking record from queue {} because the container was stopped", queryId, queryId);
            }
        }
    }
}
