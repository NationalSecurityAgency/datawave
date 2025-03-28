package datawave.microservice.query.messaging.kafka;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import datawave.microservice.query.messaging.QueryResultsPublisher;
import datawave.microservice.query.messaging.Result;

class KafkaQueryResultsPublisher implements QueryResultsPublisher {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final KafkaTemplate<String,String> kafkaTemplate;
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    public KafkaQueryResultsPublisher(KafkaTemplate<String,String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    @Override
    public boolean publish(Result result, long interval, TimeUnit timeUnit) {
        if (log.isDebugEnabled()) {
            log.debug("Publishing message to " + kafkaTemplate.getDefaultTopic());
        }
        
        boolean success = false;
        try {
            // @formatter:off
            SendResult<String,String> sendResult = kafkaTemplate
                    .send(MessageBuilder.withPayload(objectMapper.writeValueAsString(result)).build())
                    .get(interval, timeUnit);
            if (log.isTraceEnabled()) {
                log.trace("Send result: " + sendResult);
            }
            // @formatter:on
            success = true;
        } catch (JsonProcessingException e) {
            log.error("Unable to serialize result", e);
        } catch (TimeoutException e) {
            log.error("Timed out waiting for kafka send result", e);
        } catch (InterruptedException e) {
            log.error("Interrupted waiting for kafka send result", e);
        } catch (ExecutionException e) {
            log.error("Execution exception waiting for kafka send result", e);
        }
        return success;
    }
    
    @Override
    public void close() throws IOException {
        kafkaTemplate.flush();
        kafkaTemplate.destroy();
    }
}
