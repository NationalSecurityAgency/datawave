package datawave.microservice.annotationCache;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;

import datawave.annotation.protobuf.v1.Annotation;

@Component
public class AnnotationMapStore implements MapStore<String,Object>, HazelcastInstanceAware, MapLoaderLifecycleSupport {
    private static Logger log = LoggerFactory.getLogger(AnnotationMapStore.class);

    private HazelcastInstance hazelcastInstance;

    private final StreamBridge streamBridge;

    public AnnotationMapStore(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
        log.info("injected bridge: " + streamBridge);
    }

    // this is done by hazelcast to inject the instance
    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        this.hazelcastInstance = hazelcastInstance;
        log.info("initialized map: " + mapName);
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        log.info("set instance: " + hazelcastInstance);
    }

    @Override
    public void store(String s, Object o) {
        if (!(o instanceof Annotation)) {
            // not storing an annotation, bypass anything that might be on the queue
            log.trace("ignoring non-annotation object of type: " + o.getClass() + " value: " + o);
            return;
        }
        String correlationId = UUID.randomUUID().toString();
        CorrelationData correlationData = new CorrelationData(correlationId);

        Message<Annotation> message = MessageBuilder.withPayload((Annotation) o).setHeader("amqp_correlationData", correlationData)
                        .setHeader("amqp_publishConfirmCorrelation", correlationData).build();

        log.info("Sending message synchronously, ID: {}", correlationId);

        try {
            log.info("sending with streamBridge");
            boolean sent = streamBridge.send("persisted-out-0", message);

            if (!sent) {
                throw new RuntimeException("StreamBridge failed to hand off the message to the internal channel.");
            }

            // 2. BLOCK the current thread until RabbitMQ responds with an ACK/NACK (or times out)
            // Adjust timeout (e.g., 5 seconds) to match your SLA requirements
            CorrelationData.Confirm confirm = correlationData.getFuture().get(5, TimeUnit.SECONDS);

            if (correlationData.getReturned() != null) {
                String replyText = correlationData.getReturned().getReplyText();
                throw new RuntimeException("Message was RETURNED by broker (No queue bound to exchange!). Reason: " + replyText);
            }

            if (confirm.isAck()) {
                log.info("Successfully delivered and ACKed by broker for ID: {}", correlationId);
            } else {
                // This covers Nack scenarios (e.g., broker disk full, internal rabbit errors)
                throw new RuntimeException("Broker rejected message (NACK). Reason: " + confirm.getReason());
            }
        } catch (MessagingException | AmqpException e) {
            log.info("caught messaging exception", e);
            throw new RuntimeException(e);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void storeAll(Map<String,Object> map) {

    }

    @Override
    public void delete(String s) {

    }

    @Override
    public void deleteAll(Collection<String> collection) {

    }

    @Override
    public Object load(String s) {
        return null;
    }

    @Override
    public Map<String,Object> loadAll(Collection<String> collection) {
        return Map.of();
    }

    @Override
    public Iterable<String> loadAllKeys() {
        return null;
    }

    @Override
    public void destroy() {

    }
}
