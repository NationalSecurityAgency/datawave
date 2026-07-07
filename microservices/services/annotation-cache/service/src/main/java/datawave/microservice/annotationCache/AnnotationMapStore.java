package datawave.microservice.annotationCache;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.cloud.stream.function.StreamBridge;
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
    // not actively used, trying to use streamBridge instead
    private final RabbitTemplate rabbitTemplate;

    public AnnotationMapStore(StreamBridge streamBridge, RabbitTemplate rabbitTemplate) {
        this.streamBridge = streamBridge;
        this.rabbitTemplate = rabbitTemplate;
        log.info("injected bridge: " + streamBridge);
        log.info("injected template: " + rabbitTemplate);
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
        if (streamBridge.send("persisted-out-0", o)) {
            log.info("Stored " + s + " with value " + o);
        } else {
            log.warn("failed to persist");
        }
        // Haven't gotten this mechanism to work yet, but I think it's the preferred way
        //
        // CorrelationData correlationData = new CorrelationData();
        // rabbitTemplate.convertAndSend("annotation", "", o, correlationData);
        //
        // try {
        // CorrelationData.Confirm confirm = correlationData.getFuture().get(5, TimeUnit.SECONDS);
        // if (!confirm.isAck()) {
        // log.info("failed!");
        // } else {
        // log.info("stored!");
        // }
        // } catch (InterruptedException e) {
        // throw new RuntimeException(e);
        // } catch (ExecutionException e) {
        // throw new RuntimeException(e);
        // } catch (TimeoutException e) {
        // throw new RuntimeException(e);
        // }
    }

    // no loner needed with producer.sync=true streamBridge.send() will block until confirmed
    //
    // @ServiceActivator(inputChannel = "producerAckChannel")
    // public MessageHandler ackHandler() {
    // return message -> {
    // Boolean isAck = message.getHeaders().get(AmqpHeaders.PUBLISH_CONFIRM, Boolean.class);
    // if (Boolean.TRUE.equals(isAck)) {
    // System.out.println("Broker Confirmed delivery.");
    // } else {
    // String reason = message.getHeaders().get(AmqpHeaders.PUBLISH_CONFIRM_NACK_CAUSE, String.class);
    // System.err.println("Broker NACKed message. Reason: " + reason);
    // }
    // };
    // }

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
