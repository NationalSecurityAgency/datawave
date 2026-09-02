package datawave.microservice.annotation.service;

import static datawave.annotation.test.v1.AnnotationTestDataUtil.generateTestAnnotation;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

import datawave.annotation.data.transform.DefaultTimestampTransformer;
import datawave.annotation.data.transform.DefaultVisibilityTransformer;
import datawave.annotation.data.transform.TimestampTransformer;
import datawave.annotation.data.transform.VisibilityTransformer;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.microservice.annotation.common.AnnotationSupplier;
import datawave.microservice.annotation.service.config.AnnotationProperties;
import datawave.microservice.annotation.util.lookup.service.LookupService;
import datawave.microservice.annotation.writers.AnnotationWriter;
import lombok.extern.slf4j.Slf4j;

/**
 * Focused unit tests for the message delivery path in {@link AnnotationControllerV1}: {@code writeAnnotation}, {@code sendAnnotation}, and
 * {@code sendAnnotationMessage}. These methods are package-private (visible for testing), so they are called directly here. The durable-write acknowledgement
 * protocol is simulated by invoking {@link AnnotationControllerV1#processConfirmAck(Message)} directly rather than standing up a real message broker.
 */
@SuppressWarnings("SpellCheckingInspection")
@ExtendWith(MockitoExtension.class)
@Slf4j
public class TestAnnotationControllerV1Delivery {

    private static final TimestampTransformer timestampTransformer = new DefaultTimestampTransformer();
    private static final VisibilityTransformer visibilityTransformer = new DefaultVisibilityTransformer();

    @Mock
    private AccumuloConnectionFactory connectionFactory;

    @Mock
    private LookupService lookupService;

    @Mock
    private AnnotationSupplier annotationSink;

    private AnnotationProperties annotationProperties;

    private AnnotationControllerV1 annotationController;

    @BeforeEach
    void setUp() {
        annotationProperties = new AnnotationProperties();
        annotationProperties.setSystemFrom("annotation");
        annotationProperties.setAnnotationAckEnabled(true);
        // keep timeouts/backoffs short so failure/retry paths run quickly in tests
        annotationProperties.setAnnotationAckTimeoutMillis(150L);
        annotationProperties.getRetry().setMaxAttempts(3);
        annotationProperties.getRetry().setBackoffIntervalMillis(5L);
        annotationProperties.getRetry().setFailTimeoutMillis(TimeUnit.SECONDS.toMillis(10));

        annotationController = new AnnotationControllerV1(connectionFactory, lookupService, annotationProperties, timestampTransformer, visibilityTransformer,
                        annotationSink, Executors.newCachedThreadPool());

        // ensure no latches leak between tests, since correlationLatchMap is static
        getCorrelationLatchMap().clear();
    }

    /**
     * Simulates a message broker that immediately (synchronously) acknowledges every message sent through {@code annotationSink}, mimicking a healthy
     * downstream in the producer-confirms protocol. Only the successful-send case is simulated here; failed sends are covered directly by tests that stub
     * {@code annotationSink.send(...)} to return {@code false} (see e.g. {@code testSendAnnotationMessage_SendFailureReturnsEmptyWithoutWaitingForAck}),
     * since a failed send never waits on an acknowledgement.
     */
    private void configureImmediateAck() {
        when(annotationSink.send(any())).thenAnswer(invocation -> {
            Message<AnnotationMessage> sent = invocation.getArgument(0);
            Object correlationId = sent.getHeaders().get(IntegrationMessageHeaderAccessor.CORRELATION_ID);
            assertNotNull(correlationId);
            Message<String> ack = MessageBuilder.withPayload("ack").setCorrelationId(correlationId).build();
            annotationController.processConfirmAck(ack);
            return true;
        });
    }

    // Use reflection to assert that no latches leak between test invocations.
    @SuppressWarnings("unchecked")
    private Map<String,CountDownLatch> getCorrelationLatchMap() {
        try {
            Field field = AnnotationControllerV1.class.getDeclaredField("correlationLatchMap");
            field.setAccessible(true);
            return (Map<String,CountDownLatch>) field.get(null);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    // fileAnnotationWriter is normally populated via @Autowired(required = false); inject a mock directly with reflection
    private void setFileAnnotationWriter(AnnotationWriter writer) {
        try {
            Field field = AnnotationControllerV1.class.getDeclaredField("fileAnnotationWriter");
            field.setAccessible(true);
            field.set(annotationController, writer);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static AnnotationMessage buildMessage(Annotation annotation) {
        //@formatter:off
        return AnnotationMessage.newBuilder()
                .addAnnotations(annotation)
                .setSource("annotation")
                .build();
        //@formatter:on
    }

    // ----------------------------------------------------------------------------------------------------------------
    // sendAnnotationMessage(...) tests
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testSendAnnotationMessage_AssignsMessageIdWhenBlank() {
        configureImmediateAck();
        Annotation annotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());
        AnnotationMessage annotationMessage = buildMessage(annotation);
        assertTrue(annotationMessage.getAnnotationMessageId().isBlank(), "test precondition: message id should start blank");

        Optional<AnnotationMessage> result = annotationController.sendAnnotationMessage(annotationMessage);

        assertTrue(result.isPresent());
        assertFalse(result.get().getAnnotationMessageId().isBlank(), "annotationMessageId should have been assigned before sending");
        assertEquals(AnnotationUtils.calculateAnnotationMessageHash(annotationMessage), result.get().getAnnotationMessageId());
    }

    @Test
    public void testSendAnnotationMessage_PreservesExistingMessageId() {
        configureImmediateAck();
        Annotation annotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());
        AnnotationMessage annotationMessage = buildMessage(annotation).toBuilder().setAnnotationMessageId("PRE-ASSIGNED-ID").build();

        Optional<AnnotationMessage> result = annotationController.sendAnnotationMessage(annotationMessage);

        assertTrue(result.isPresent());
        assertEquals("PRE-ASSIGNED-ID", result.get().getAnnotationMessageId());
    }

    @Test
    public void testSendAnnotationMessage_UsesDistinctCorrelationIdsForDistinctMessages() {
        when(annotationSink.send(any())).thenAnswer(invocation -> {
            Message<AnnotationMessage> sent = invocation.getArgument(0);
            Object correlationId = sent.getHeaders().get(IntegrationMessageHeaderAccessor.CORRELATION_ID);
            assertNotNull(correlationId);
            annotationController.processConfirmAck(MessageBuilder.withPayload("ack").setCorrelationId(correlationId).build());
            return true;
        });

        Annotation annotationOne = AnnotationUtils.injectAllHashes(generateTestAnnotation());
        Annotation annotationTwo = AnnotationUtils.injectAllHashes(
                        generateTestAnnotation().toBuilder().setAnnotationType("aCompletelyDifferentAnnotationType").build());

        Optional<AnnotationMessage> resultOne = annotationController.sendAnnotationMessage(buildMessage(annotationOne));
        Optional<AnnotationMessage> resultTwo = annotationController.sendAnnotationMessage(buildMessage(annotationTwo));

        assertTrue(resultOne.isPresent());
        assertTrue(resultTwo.isPresent());

        // Validate that two distinct annotations produce two distinct, non-blank correlation ids.
        String idOne = resultOne.get().getAnnotationMessageId();
        String idTwo = resultTwo.get().getAnnotationMessageId();
        assertFalse(idOne.isBlank());
        assertFalse(idTwo.isBlank());
        assertNotEquals(idOne, idTwo, "distinct annotation messages must not share a correlation id");

        // and the map should be clean afterward -- no leaked latches
        assertTrue(getCorrelationLatchMap().isEmpty());
    }

    @Test
    public void testSendAnnotationMessage_DuplicateConcurrentSendsForSameMessageIdShareLatchAndBothSucceed() throws Exception {
        // Two callers attempt to send the exact same (pre-assigned) annotationMessageId concurrently, simulating a
        // client retry racing with the original in-flight send. Only one ack ever arrives for that correlation id.
        // Both callers must be satisfied by that single ack -- the second call must not silently overwrite/orphan
        // the first caller's latch, which would otherwise leave the first caller waiting out the full ack timeout
        // and incorrectly reporting failure.
        AnnotationMessage annotationMessage = buildMessage(AnnotationUtils.injectAllHashes(generateTestAnnotation())).toBuilder()
                        .setAnnotationMessageId("DUPLICATE-ID").build();

        AtomicInteger sendCount = new AtomicInteger(0);
        when(annotationSink.send(any())).thenAnswer(invocation -> {
            // only the original sender should ever reach annotationSink.send(...); a duplicate should observe the
            // existing latch and skip sending entirely.
            sendCount.incrementAndGet();
            Message<AnnotationMessage> sent = invocation.getArgument(0);
            Object correlationId = sent.getHeaders().get(IntegrationMessageHeaderAccessor.CORRELATION_ID);
            assertNotNull(correlationId);
            annotationController.processConfirmAck(MessageBuilder.withPayload("ack").setCorrelationId(correlationId).build());
            return true;
        });

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Optional<AnnotationMessage>> futureOne = executor.submit(() -> annotationController.sendAnnotationMessage(annotationMessage));
            Future<Optional<AnnotationMessage>> futureTwo = executor.submit(() -> annotationController.sendAnnotationMessage(annotationMessage));

            Optional<AnnotationMessage> resultOne = futureOne.get(10, TimeUnit.SECONDS);
            Optional<AnnotationMessage> resultTwo = futureTwo.get(10, TimeUnit.SECONDS);

            assertTrue(resultOne.isPresent(), "the original sender must be satisfied by the single ack");
            assertTrue(resultTwo.isPresent(), "the duplicate sender must also be satisfied by the same ack rather than timing out");
            assertEquals(1, sendCount.get(), "only the original sender should have actually sent a message; the duplicate should reuse its latch");
            assertTrue(getCorrelationLatchMap().isEmpty(), "no latch should remain once both duplicate callers complete");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSendAnnotationMessage_SendFailureReturnsEmptyWithoutWaitingForAck() {
        when(annotationSink.send(any())).thenReturn(false);
        Annotation annotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());

        long start = System.currentTimeMillis();
        Optional<AnnotationMessage> result = annotationController.sendAnnotationMessage(buildMessage(annotation));
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(result.isEmpty());
        // failed sends should short-circuit rather than waiting out the ack timeout
        assertTrue(elapsed < annotationProperties.getAnnotationAckTimeoutMillis(), "send failure should not wait for the ack timeout");
        assertTrue(getCorrelationLatchMap().isEmpty(), "latch should be cleaned up even when send fails");
    }

    @Test
    public void testSendAnnotationMessage_AckTimeoutReturnsEmptyAndCleansUpLatch() {
        // send succeeds, but no acknowledgement ever arrives
        when(annotationSink.send(any())).thenReturn(true);
        Annotation annotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());

        long start = System.currentTimeMillis();
        Optional<AnnotationMessage> result = annotationController.sendAnnotationMessage(buildMessage(annotation));
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(result.isEmpty());
        assertTrue(elapsed >= annotationProperties.getAnnotationAckTimeoutMillis(), "should have waited for the full ack timeout");
        assertTrue(getCorrelationLatchMap().isEmpty(), "latch should be removed after timeout");
    }

    @Test
    public void testSendAnnotationMessage_AckDisabledSucceedsWithoutWaitingForAck() {
        annotationProperties.setAnnotationAckEnabled(false);
        when(annotationSink.send(any())).thenReturn(true);
        Annotation annotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());

        Optional<AnnotationMessage> result = annotationController.sendAnnotationMessage(buildMessage(annotation));

        assertTrue(result.isPresent());
        assertTrue(getCorrelationLatchMap().isEmpty(), "no latch should be registered when acks are disabled");
    }

    @Test
    public void testSendAnnotationMessage_AckDisabledSendFailureReturnsEmpty() {
        annotationProperties.setAnnotationAckEnabled(false);
        when(annotationSink.send(any())).thenReturn(false);
        Annotation annotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());

        Optional<AnnotationMessage> result = annotationController.sendAnnotationMessage(buildMessage(annotation));

        assertTrue(result.isEmpty());
    }

    // ----------------------------------------------------------------------------------------------------------------
    // sendAnnotation(...) wrapper tests
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testSendAnnotation_SuccessReturnsTheAnnotationThatWasSent() {
        configureImmediateAck();
        Annotation annotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());

        Optional<Annotation> result = annotationController.sendAnnotation(annotation);

        assertTrue(result.isPresent());
        assertEquals(annotation.getAnnotationId(), result.get().getAnnotationId());
        assertEquals(annotation, result.get());
    }

    @Test
    public void testSendAnnotation_FailurePropagatesEmpty() {
        when(annotationSink.send(any())).thenReturn(false);
        Annotation annotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());

        Optional<Annotation> result = annotationController.sendAnnotation(annotation);

        assertTrue(result.isEmpty());
    }

    // ----------------------------------------------------------------------------------------------------------------
    // writeAnnotation(...) tests
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testWriteAnnotation_SucceedsOnFirstAttempt() {
        configureImmediateAck();
        Annotation annotation = generateTestAnnotation();

        Optional<Annotation> result = annotationController.writeAnnotation(annotation);

        assertTrue(result.isPresent());
        assertEquals(AnnotationUtils.calculateAnnotationHash(annotation), result.get().getAnnotationId());
        verify(annotationSink, times(1)).send(any());
    }

    @Test
    public void testWriteAnnotation_RetriesUntilSendSucceeds() {
        // fail on the first two attempts, then succeed (with acknowledgement) on the third
        Annotation annotation = generateTestAnnotation();

        AtomicInteger callCount = new AtomicInteger(0);
        when(annotationSink.send(any())).thenAnswer(invocation -> {
            boolean succeeds = callCount.incrementAndGet() >= 3;
            if (succeeds) {
                Message<AnnotationMessage> sent = invocation.getArgument(0);
                Object correlationId = sent.getHeaders().get(IntegrationMessageHeaderAccessor.CORRELATION_ID);
                assertNotNull(correlationId);
                annotationController.processConfirmAck(MessageBuilder.withPayload("ack").setCorrelationId(correlationId).build());
            }
            return succeeds;
        });

        Optional<Annotation> result = annotationController.writeAnnotation(annotation);

        assertTrue(result.isPresent());
        assertEquals(AnnotationUtils.calculateAnnotationHash(annotation), result.get().getAnnotationId());
        verify(annotationSink, times(3)).send(any());
    }

    @Test
    public void testWriteAnnotation_FallsBackToFileWriterWhenAllAttemptsFail() throws Exception {
        when(annotationSink.send(any())).thenReturn(false);
        AnnotationWriter fileWriter = mock(AnnotationWriter.class);
        setFileAnnotationWriter(fileWriter);

        Annotation annotation = generateTestAnnotation();
        Optional<Annotation> result = annotationController.writeAnnotation(annotation);

        assertTrue(result.isPresent(), "should fall back to the file writer when all send attempts fail");
        assertEquals(AnnotationUtils.calculateAnnotationHash(annotation), result.get().getAnnotationId());
        verify(fileWriter, times(1)).write(any());
        verify(annotationSink, times(annotationProperties.getRetry().getMaxAttempts())).send(any());
    }

    @Test
    public void testWriteAnnotation_FailsWhenAllAttemptsFailAndNoFileWriterConfigured() {
        when(annotationSink.send(any())).thenReturn(false);
        setFileAnnotationWriter(null);

        Annotation annotation = generateTestAnnotation();
        Optional<Annotation> result = annotationController.writeAnnotation(annotation);

        assertTrue(result.isEmpty());
        assertTrue(getCorrelationLatchMap().isEmpty(), "no latches should remain after exhausting retries");
    }

    @Test
    public void testWriteAnnotation_FileWriterFailureAlsoResultsInEmpty() throws Exception {
        when(annotationSink.send(any())).thenReturn(false);
        AnnotationWriter fileWriter = mock(AnnotationWriter.class);
        when(fileWriter.write(any())).thenThrow(new RuntimeException("simulating a disk full condition"));
        setFileAnnotationWriter(fileWriter);

        Annotation annotation = generateTestAnnotation();
        Optional<Annotation> result = annotationController.writeAnnotation(annotation);

        assertTrue(result.isEmpty());
    }

    // ----------------------------------------------------------------------------------------------------------------
    // concurrency: two independent writes must not cross-satisfy each other's latches
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testConcurrentWriteAnnotationCallsDoNotCrossSatisfyLatches() throws Exception {
        // each send only acknowledges its own correlation id, and does so after a small delay to encourage interleaving
        Map<String,Integer> sendCounts = new ConcurrentHashMap<>();
        when(annotationSink.send(any())).thenAnswer(invocation -> {
            Message<AnnotationMessage> sent = invocation.getArgument(0);
            Object correlationId = sent.getHeaders().get(IntegrationMessageHeaderAccessor.CORRELATION_ID);
            sendCounts.merge(String.valueOf(correlationId), 1, Integer::sum);
            Thread.sleep(10);
            assertNotNull(correlationId);
            annotationController.processConfirmAck(MessageBuilder.withPayload("ack").setCorrelationId(correlationId).build());
            return true;
        });

        Annotation annotationOne = generateTestAnnotation();
        Annotation annotationTwo = generateTestAnnotation().toBuilder().setAnnotationType("someOtherAnnotationType").build();

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Optional<Annotation>> futureOne = executor.submit(() -> annotationController.writeAnnotation(annotationOne));
            Future<Optional<Annotation>> futureTwo = executor.submit(() -> annotationController.writeAnnotation(annotationTwo));

            Optional<Annotation> resultOne = futureOne.get(10, TimeUnit.SECONDS);
            Optional<Annotation> resultTwo = futureTwo.get(10, TimeUnit.SECONDS);

            assertTrue(resultOne.isPresent());
            assertTrue(resultTwo.isPresent());
            assertNotEquals(resultOne.get().getAnnotationId(), resultTwo.get().getAnnotationId());
            assertTrue(getCorrelationLatchMap().isEmpty(), "no latches should remain once both concurrent writes complete");

            // each write should have used its own distinct correlation id, and been sent exactly once (no unnecessary retries,
            // and no cross-talk where one write's ack satisfies the other's latch)
            assertEquals(2, sendCounts.size(), "expected two distinct correlation ids to have been used: " + sendCounts.keySet());
            sendCounts.values().forEach(count -> assertEquals(1, count, "expected each correlation id to have been sent exactly once"));
        } finally {
            executor.shutdownNow();
        }
    }
}
