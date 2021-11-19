package datawave.microservice.query.messaging.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import datawave.microservice.query.messaging.AcknowledgementCallback;
import datawave.microservice.query.messaging.QueryResultsListener;
import datawave.microservice.query.messaging.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.config.DirectRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static datawave.microservice.query.messaging.AcknowledgementCallback.Status.ACK;
import static datawave.microservice.query.messaging.AcknowledgementCallback.Status.NACK;
import static datawave.microservice.query.messaging.rabbitmq.RabbitMQQueryResultsManager.QUERY_QUEUE_PREFIX;

/**
 * A listener for RabbitMQ Query Results
 */
class RabbitMQQueryResultsListener extends MessageListenerAdapter implements QueryResultsListener {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final RabbitListenerEndpointRegistry endpointRegistry;
    private final String listenerId;
    private final String queryId;
    
    private final LinkedBlockingQueue<Result> resultQueue = new LinkedBlockingQueue<>();
    private boolean stopped = false;
    
    public RabbitMQQueryResultsListener(DirectRabbitListenerContainerFactory listenerContainerFactory, RabbitListenerEndpointRegistry endpointRegistry,
                    String listenerId, String queryId) {
        this.endpointRegistry = endpointRegistry;
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
    public void onMessage(Message message, final Channel channel) throws Exception {
        if (!stopped) {
            Result result = new ObjectMapper().readerFor(Result.class).readValue(message.getBody());
            
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<AcknowledgementCallback.Status> ackStatus = new AtomicReference<>();
            result.setAcknowledgementCallback(status -> {
                ackStatus.set(status);
                latch.countDown();
            });
            
            resultQueue.add(result);
            
            try {
                latch.await();
                if (ackStatus.get() == ACK) {
                    channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
                    if (log.isTraceEnabled()) {
                        log.trace("Acking record {} from queue {}", result.getResultId(), queryId);
                    }
                } else if (ackStatus.get() == NACK) {
                    channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
                    if (log.isTraceEnabled()) {
                        log.trace("Nacking record {} from queue {} because the record was rejected", result.getResultId(), queryId);
                    }
                }
            } catch (InterruptedException e) {
                channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
                if (log.isTraceEnabled()) {
                    log.trace("Nacking record {} from queue {} because the latch was interrupted", result.getResultId(), queryId);
                }
            }
        } else {
            channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
            if (log.isTraceEnabled()) {
                log.trace("Nacking record from queue {} because the container was stopped", queryId);
            }
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
}
