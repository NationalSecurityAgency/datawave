package datawave.microservice.annotation.service;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.integration.support.MessageBuilder;

import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.microservice.annotation.common.AnnotationSupplier;
import datawave.microservice.annotation.health.HealthChecker;
import datawave.microservice.annotation.service.config.AnnotationProperties;
import lombok.extern.slf4j.Slf4j;

/** Publishes annotations and waits for broker acceptance; persistence is performed separately by queue consumers. */
@Slf4j
public class AnnotationPublisher {
    private final AnnotationSupplier annotationSource;
    private final AnnotationProperties properties;
    private final HealthChecker healthChecker;
    private final ConcurrentMap<String,CorrelationData> inFlight = new ConcurrentHashMap<>();

    public AnnotationPublisher(AnnotationSupplier annotationSource, AnnotationProperties properties, HealthChecker healthChecker) {
        this.annotationSource = annotationSource;
        this.properties = properties;
        this.healthChecker = healthChecker;
    }

    public Optional<AnnotationMessage> publishAndConfirm(AnnotationMessage message) {
        if (healthChecker != null && !healthChecker.isHealthy()) {
            log.debug("Skipping annotation publication because the messaging health check is unhealthy");
            return Optional.empty();
        }
        AnnotationMessage identifiedMessage = message.getAnnotationMessageId().isBlank() ? AnnotationUtils.injectAnnotationMessageHash(message) : message;
        String messageId = identifiedMessage.getAnnotationMessageId();
        // Each physical publication needs a unique correlation id, including retries of the same logical annotation.
        // A late confirmation for a timed-out attempt must never confirm a later attempt.
        CorrelationData newCorrelation = new CorrelationData(UUID.randomUUID().toString());
        boolean awaitConfirm = properties.isAnnotationAckEnabled();
        CorrelationData existing = awaitConfirm ? inFlight.putIfAbsent(messageId, newCorrelation) : null;
        boolean owner = existing == null;
        CorrelationData correlation = owner ? newCorrelation : existing;
        try {
            // @formatter:off
            if (owner && !annotationSource.send(
                    MessageBuilder.withPayload(identifiedMessage)
                            .setCorrelationId(messageId)
                            .setHeader(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION, correlation)
                            .build())) {
                // Release concurrent duplicate callers as well as the original sender.
                correlation.getFuture().set(new CorrelationData.Confirm(false, "Local publication rejected"));
                return Optional.empty();
            }
            // @formatter:on
            if (!awaitConfirm) {
                return Optional.of(identifiedMessage);
            }
            CorrelationData.Confirm confirm = correlation.getFuture().get(properties.getAnnotationAckTimeoutMillis(), TimeUnit.MILLISECONDS);
            // Rabbit can return an unroutable message and still acknowledge its publication to the exchange.
            // Spring records the return before completing this future.
            if (confirm.isAck() && correlation.getReturned() == null) {
                return Optional.of(identifiedMessage);
            }
            log.warn("Annotation publication {} was rejected or returned by the broker", messageId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (TimeoutException | ExecutionException e) {
            log.warn("Annotation publication {} was not confirmed: {}", messageId, e.toString());
        } catch (RuntimeException e) {
            if (owner) {
                correlation.getFuture().setException(e);
            }
            log.warn("Annotation publication {} failed: {}", messageId, e.toString());
        } finally {
            if (awaitConfirm && owner) {
                inFlight.remove(messageId, correlation);
            }
        }
        return Optional.empty();
    }

    /* package-private for delivery tests */
    boolean isEmpty() {
        return inFlight.isEmpty();
    }
}
