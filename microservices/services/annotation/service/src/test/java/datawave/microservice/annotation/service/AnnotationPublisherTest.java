package datawave.microservice.annotation.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.Message;

import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotation.common.AnnotationSupplier;
import datawave.microservice.annotation.service.config.AnnotationProperties;

class AnnotationPublisherTest {
    private final AnnotationSupplier source = mock(AnnotationSupplier.class);
    private final AnnotationProperties properties = new AnnotationProperties();
    private final AnnotationPublisher publisher = new AnnotationPublisher(source, properties, null);
    private final AnnotationMessage message = AnnotationMessage.newBuilder().setAnnotationMessageId("logical-message").build();

    @BeforeEach
    void configureTimeout() {
        properties.setAnnotationAckTimeoutMillis(100);
    }

    @Test
    void positiveConfirmSucceeds() {
        when(source.send(any())).thenAnswer(invocation -> {
            correlation(invocation.getArgument(0)).getFuture().set(new CorrelationData.Confirm(true, null));
            return true;
        });
        assertThat(publisher.publishAndConfirm(message)).contains(message);
        assertThat(publisher.isEmpty()).isTrue();
    }

    @Test
    void nackFails() {
        when(source.send(any())).thenAnswer(invocation -> {
            correlation(invocation.getArgument(0)).getFuture().set(new CorrelationData.Confirm(false, "broker rejection"));
            return true;
        });
        assertThat(publisher.publishAndConfirm(message)).isEmpty();
        assertThat(publisher.isEmpty()).isTrue();
    }

    @Test
    void unroutableReturnFailsEvenWithPositiveConfirm() {
        when(source.send(any())).thenAnswer(invocation -> {
            CorrelationData correlation = correlation(invocation.getArgument(0));
            correlation.setReturned(new ReturnedMessage(new org.springframework.amqp.core.Message(new byte[0], new MessageProperties()), 312, "NO_ROUTE",
                            "annotation", "annotation"));
            correlation.getFuture().set(new CorrelationData.Confirm(true, null));
            return true;
        });
        assertThat(publisher.publishAndConfirm(message)).isEmpty();
        assertThat(publisher.isEmpty()).isTrue();
    }

    @Test
    void failedConfirmationFutureFails() {
        when(source.send(any())).thenAnswer(invocation -> {
            correlation(invocation.getArgument(0)).getFuture().setException(new IllegalStateException("connection closed"));
            return true;
        });
        assertThat(publisher.publishAndConfirm(message)).isEmpty();
        assertThat(publisher.isEmpty()).isTrue();
    }

    @Test
    void localSendExceptionFailsAndCleansUp() {
        when(source.send(any())).thenThrow(new IllegalStateException("sink closed"));
        assertThat(publisher.publishAndConfirm(message)).isEmpty();
        assertThat(publisher.isEmpty()).isTrue();
    }

    @Test
    void retryUsesIndependentConfirmationAndIgnoresLateAck() throws Exception {
        AtomicReference<CorrelationData> previous = new AtomicReference<>();
        when(source.send(any())).thenAnswer(invocation -> {
            previous.set(correlation(invocation.getArgument(0)));
            return true;
        });
        assertThat(publisher.publishAndConfirm(message)).isEmpty();
        CorrelationData firstAttempt = previous.get();

        properties.setAnnotationAckTimeoutMillis(5000);
        CountDownLatch retryPublished = new CountDownLatch(1);
        AtomicReference<CorrelationData> retryCorrelation = new AtomicReference<>();
        doAnswer(invocation -> {
            retryCorrelation.set(correlation(invocation.getArgument(0)));
            retryPublished.countDown();
            return true;
        }).when(source).send(any());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Optional<AnnotationMessage>> retry = executor.submit(() -> publisher.publishAndConfirm(message));
            assertThat(retryPublished.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(retryCorrelation.get().getId()).isNotEqualTo(firstAttempt.getId());
            firstAttempt.getFuture().set(new CorrelationData.Confirm(true, null));
            assertThat(retryCorrelation.get().getFuture().isDone()).isFalse();
            assertThat(retry.isDone()).isFalse();
            retryCorrelation.get().getFuture().set(new CorrelationData.Confirm(false, "retry rejected"));
            assertThat(retry.get(5, TimeUnit.SECONDS)).isEmpty();
            assertThat(publisher.isEmpty()).isTrue();
        } finally {
            executor.shutdownNow();
        }
    }

    private static CorrelationData correlation(Message<?> message) {
        CorrelationData correlation = message.getHeaders().get(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION, CorrelationData.class);
        assertThat(correlation).isNotNull();
        return correlation;
    }
}
