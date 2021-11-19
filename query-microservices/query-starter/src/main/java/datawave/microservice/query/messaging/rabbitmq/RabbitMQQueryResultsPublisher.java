package datawave.microservice.query.messaging.rabbitmq;

import datawave.microservice.query.messaging.QueryResultsPublisher;
import datawave.microservice.query.messaging.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static datawave.microservice.query.messaging.rabbitmq.RabbitMQQueryResultsManager.QUERY_RESULTS_EXCHANGE;

class RabbitMQQueryResultsPublisher implements QueryResultsPublisher {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final RabbitTemplate rabbitTemplate;
    
    private final Map<String,CountDownLatch> latchMap = new HashMap<>();
    private final Map<String,Boolean> ackMap = new HashMap<>();
    
    public RabbitMQQueryResultsPublisher(RabbitTemplate rabbitTemplate, String queryId) {
        this.rabbitTemplate = rabbitTemplate;
        
        this.rabbitTemplate.setExchange(QUERY_RESULTS_EXCHANGE);
        this.rabbitTemplate.setRoutingKey(queryId);
        this.rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            if (correlationData != null) {
                ackMap.put(correlationData.getId(), ack);
                latchMap.get(correlationData.getId()).countDown();
            }
        });
        this.rabbitTemplate.setMessageConverter(new Jackson2JsonMessageConverter());
    }
    
    @Override
    public boolean publish(Result result, long interval, TimeUnit timeUnit) {
        if (log.isDebugEnabled()) {
            log.debug("Publishing message to " + rabbitTemplate.getExchange());
        }
        
        final CountDownLatch latch = new CountDownLatch(1);
        latchMap.put(result.getResultId(), latch);
        
        try {
            rabbitTemplate.correlationConvertAndSend(result, new CorrelationData(result.getResultId()));
        } catch (Exception e) {
            throw new RuntimeException("Unable to serialize results", e);
        }
        
        boolean success = false;
        try {
            if (latch.await(interval, timeUnit) && ackMap.get(result.getResultId())) {
                if (log.isTraceEnabled()) {
                    log.trace("Received RabbitMQ producer confirm ack for {}", result.getResultId());
                    success = true;
                }
            } else {
                log.error("Timed out while waiting for RabbitMQ producer confirm");
            }
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for RabbitMQ producer confirm", e);
        } finally {
            latchMap.remove(result.getResultId());
            ackMap.remove(result.getResultId());
        }
        
        return success;
    }
}
