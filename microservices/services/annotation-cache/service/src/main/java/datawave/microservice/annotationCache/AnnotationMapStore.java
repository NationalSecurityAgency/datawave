package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.ID_TYPE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.PERSISTENCE_MODE_PARAMETER;
import static datawave.microservice.annotationCache.api.Constants.REGION_ID_PARAMETER;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import com.hazelcast.map.MapStore;

import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotationCache.api.AnnotationKey;
import datawave.microservice.annotationCache.api.AnnotationStorageException;
import datawave.microservice.annotationCache.api.PersistenceMode;
import datawave.microservice.annotationCache.api.RegionConfiguration;

/**
 * Publishes local write-through annotations to RabbitMQ before their map write is accepted for downstream services. A broker ACK is the acceptance boundary;
 * surviving a broker restart also requires durable RabbitMQ exchange/queue topology and persistent message delivery. Cache-only, non-annotation, and
 * remote-origin entries are not published. Invalid write-through metadata or a failed, returned, rejected, or unconfirmed publication causes the write to fail
 * with {@link AnnotationStorageException}.
 */
@Component
public class AnnotationMapStore implements MapStore<AnnotationKey,Object> {
    public static final String AMQP_CORRELATION_DATA_HEADER = "amqp_correlationData";
    public static final String AMQP_PUBLISH_CONFIRM_CORRELATION_HEADER = "amqp_publishConfirmCorrelation";
    private static Logger log = LoggerFactory.getLogger(AnnotationMapStore.class);

    private final AnnotationMessagePublisher publisher;
    private final String localRegion;

    public AnnotationMapStore(AnnotationMessagePublisher publisher, RegionConfiguration regionConfiguration) {
        this.publisher = publisher;
        if (regionConfiguration == null || regionConfiguration.getName() == null || regionConfiguration.getName().isBlank()) {
            throw new IllegalStateException("region.name must be configured for the annotation map store");
        }
        this.localRegion = regionConfiguration.getName();
        log.info("Initialized annotation MapStore for region {}", localRegion);
    }

    /**
     *
     * @param key
     *            key of the entry to store
     * @param o
     *            value of the entry to store
     * @throws AnnotationStorageException
     *             if a problem is encountered
     */
    @Override
    public void store(AnnotationKey key, Object o) {
        if (!(o instanceof AnnotationMessage)) {
            // not storing an annotation, bypass anything that might be on the queue
            log.trace("Ignoring non-annotation value for key {}: {}", key, o == null ? "null" : o.getClass().getName());
            return;
        }

        AnnotationMessage annotationMessage = (AnnotationMessage) o;
        PersistenceMode persistenceMode;
        String configuredMode = annotationMessage.getParametersOrDefault(PERSISTENCE_MODE_PARAMETER, PersistenceMode.WRITE_THROUGH.value());
        try {
            persistenceMode = PersistenceMode.fromValue(configuredMode);
        } catch (IllegalArgumentException e) {
            throw new AnnotationStorageException("Invalid persistence mode for annotation " + key + ": " + configuredMode, e);
        }

        if (persistenceMode == PersistenceMode.CACHE_ONLY) {
            log.debug("Skipping RabbitMQ publication of cache-only annotation {}", key);
            return;
        }

        String sourceRegion = annotationMessage.getParametersOrDefault(REGION_ID_PARAMETER, "");
        if (sourceRegion.isBlank()) {
            throw new AnnotationStorageException("Write-through annotation " + key + " is missing source region parameter " + REGION_ID_PARAMETER);
        }
        if (!localRegion.equals(sourceRegion)) {
            log.debug("Skipping RabbitMQ publication of annotation {} originating in region {}", key, sourceRegion);
            return;
        }

        String idType = annotationMessage.getParametersOrDefault(ID_TYPE_PARAMETER, "");
        if (idType.isBlank()) {
            throw new AnnotationStorageException("Write-through annotation " + key + " is missing parameter " + ID_TYPE_PARAMETER);
        }

        String correlationId = UUID.randomUUID().toString();
        CorrelationData correlationData = new CorrelationData(correlationId);

        Message<AnnotationMessage> message = MessageBuilder.withPayload(annotationMessage).setHeader(AMQP_CORRELATION_DATA_HEADER, correlationData)
                        .setHeader(AMQP_PUBLISH_CONFIRM_CORRELATION_HEADER, correlationData).build();

        log.info("Sending message synchronously, ID: {}", correlationId);

        try {
            boolean sent = publisher.send(message);

            if (!sent) {
                throw new AnnotationStorageException("StreamBridge failed to hand off the message to the internal channel.");
            }

            // BLOCK the current thread until RabbitMQ responds with an ACK/NACK (or times out)
            CorrelationData.Confirm confirm = correlationData.getFuture().get(5, TimeUnit.SECONDS);

            if (correlationData.getReturned() != null) {
                String replyText = correlationData.getReturned().getReplyText();
                throw new AnnotationStorageException("Message was RETURNED by broker (No queue bound to exchange!). Reason: " + replyText);
            }

            if (confirm.isAck()) {
                log.info("Successfully delivered and ACKed by broker for ID: {}", correlationId);
            } else {
                // This covers Nack scenarios (e.g., broker disk full, internal rabbit errors)
                throw new AnnotationStorageException("Broker rejected message (NACK). Reason: " + confirm.getReason());
            }
        } catch (MessagingException | AmqpException e) {
            log.info("caught messaging exception", e);
            throw new AnnotationStorageException("Problem sending message", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new AnnotationStorageException("Failed to send message", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AnnotationStorageException("Interrupted while waiting for message confirmation", e);
        }
    }

    /**
     * Hazelcast may call either this or the single entry method depending on locality and threading
     *
     * @see #store(AnnotationKey, Object)
     * @param map
     *            map of entries to store
     */
    @Override
    public void storeAll(Map<AnnotationKey,Object> map) {
        for (Entry<AnnotationKey,Object> entry : map.entrySet()) {
            store(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void delete(AnnotationKey key) {

    }

    @Override
    public void deleteAll(Collection<AnnotationKey> collection) {

    }

    @Override
    public Object load(AnnotationKey key) {
        return null;
    }

    @Override
    public Map<AnnotationKey,Object> loadAll(Collection<AnnotationKey> collection) {
        return Map.of();
    }

    @Override
    public Iterable<AnnotationKey> loadAllKeys() {
        return null;
    }
}
