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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
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
import datawave.microservice.annotation.health.HealthChecker;
import datawave.microservice.annotation.service.config.AnnotationProperties;
import datawave.microservice.annotation.util.lookup.service.LookupService;
import datawave.microservice.annotation.writers.AnnotationWriter;
import lombok.extern.slf4j.Slf4j;

/**
 * Focused unit tests for the message delivery path in {@link AnnotationControllerV1}: {@code writeAnnotation}, {@code sendAnnotation}, and
 * {@code sendAnnotationMessage}. These methods are package-private (visible for testing), so they are called directly here. The broker-confirmation protocol is
 * simulated by completing the broker confirmation future rather than standing up a real message broker.
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
                        new AnnotationPublisher(annotationSink, annotationProperties, null), Executors.newCachedThreadPool());

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
            confirm(sent);
            return true;
        });
    }

    // Inspect the publisher to verify completed publications do not retain in-flight entries.
    private AnnotationPublisher getAnnotationPublisher() {
        try {
            Field field = AnnotationControllerV1.class.getDeclaredField("annotationPublisher");
            field.setAccessible(true);
            return (AnnotationPublisher) field.get(annotationController);
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

    private void setHealthChecker(HealthChecker checker) {
        annotationController = new AnnotationControllerV1(connectionFactory, lookupService, annotationProperties, timestampTransformer, visibilityTransformer,
                        new AnnotationPublisher(annotationSink, annotationProperties, checker), mock(ExecutorService.class));
    }

    private static void confirm(Message<?> sent) {
        CorrelationData correlation = sent.getHeaders().get(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION, CorrelationData.class);
        assertNotNull(correlation);
        correlation.getFuture().set(new CorrelationData.Confirm(true, null));
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
            confirm(sent);
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

        // and the map should be clean afterward -- no leaked in-flight entries
        assertTrue(getAnnotationPublisher().isEmpty());
    }

    @Test
    public void testSendAnnotationMessage_DuplicateConcurrentSendsForSameMessageIdShareLatchAndBothSucceed() throws Exception {
        AnnotationMessage message = buildMessage(AnnotationUtils.injectAllHashes(generateTestAnnotation())).toBuilder().setAnnotationMessageId("DUPLICATE-ID")
                        .build();
        annotationProperties.setAnnotationAckTimeoutMillis(10000);
        CountDownLatch firstSend = new CountDownLatch(1);
        CountDownLatch releaseConfirm = new CountDownLatch(1);
        AtomicReference<Thread> duplicateThread = new AtomicReference<>();
        when(annotationSink.send(any())).thenAnswer(invocation -> {
            firstSend.countDown();
            assertTrue(releaseConfirm.await(10, TimeUnit.SECONDS));
            confirm(invocation.getArgument(0));
            return true;
        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Optional<AnnotationMessage>> first = executor.submit(() -> annotationController.sendAnnotationMessage(message));
            assertTrue(firstSend.await(5, TimeUnit.SECONDS));
            Future<Optional<AnnotationMessage>> second = executor.submit(() -> {
                duplicateThread.set(Thread.currentThread());
                return annotationController.sendAnnotationMessage(message);
            });
            // The only timed wait in the duplicate task is the shared confirmation future.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while ((duplicateThread.get() == null || duplicateThread.get().getState() != Thread.State.TIMED_WAITING) && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            assertNotNull(duplicateThread.get());
            assertEquals(Thread.State.TIMED_WAITING, duplicateThread.get().getState());
            releaseConfirm.countDown();
            assertTrue(first.get(5, TimeUnit.SECONDS).isPresent());
            assertTrue(second.get(5, TimeUnit.SECONDS).isPresent());
            verify(annotationSink, times(1)).send(any());
            assertTrue(getAnnotationPublisher().isEmpty());
        } finally {
            releaseConfirm.countDown();
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
        assertTrue(getAnnotationPublisher().isEmpty(), "in-flight entry should be cleaned up even when send fails");
    }

    @Test
    public void testSendAnnotationMessage_AckTimeoutReturnsEmptyAndCleansUpPublication() {
        // send succeeds, but no acknowledgement ever arrives
        when(annotationSink.send(any())).thenReturn(true);
        Annotation annotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());

        long start = System.currentTimeMillis();
        Optional<AnnotationMessage> result = annotationController.sendAnnotationMessage(buildMessage(annotation));
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(result.isEmpty());
        assertTrue(elapsed >= annotationProperties.getAnnotationAckTimeoutMillis(), "should have waited for the full ack timeout");
        assertTrue(getAnnotationPublisher().isEmpty(), "in-flight entry should be removed after timeout");
    }

    @Test
    public void testSendAnnotationMessage_InterruptedWhileAwaitingAckReturnsEmptyRestoresInterruptStatusAndCleansUpPublication() throws Exception {
        // send succeeds, but no ack ever arrives -- the calling thread is interrupted while awaiting the confirmation future instead.
        annotationProperties.setAnnotationAckTimeoutMillis(TimeUnit.SECONDS.toMillis(30));
        when(annotationSink.send(any())).thenReturn(true);
        Annotation annotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            AtomicReference<Optional<AnnotationMessage>> resultRef = new AtomicReference<>();
            AtomicBoolean interruptedStatusObservedAfterReturn = new AtomicBoolean(false);
            CountDownLatch threadStarted = new CountDownLatch(1);

            Future<?> future = executor.submit(() -> {
                threadStarted.countDown();
                resultRef.set(annotationController.sendAnnotationMessage(buildMessage(annotation)));
                // the interrupt status must be restored by the time sendAnnotationMessage returns, so that the executing
                // thread/pool can still observe that this task was asked to stop.
                interruptedStatusObservedAfterReturn.set(Thread.currentThread().isInterrupted());
            });

            assertTrue(threadStarted.await(5, TimeUnit.SECONDS));
            // give the worker thread a moment to begin waiting on the confirmation future before interrupting it
            Thread.sleep(50);
            executor.shutdownNow();

            future.get(5, TimeUnit.SECONDS);

            assertNotNull(resultRef.get());
            assertTrue(resultRef.get().isEmpty(), "an interrupted wait for the ack must be treated as a failed send");
            assertTrue(interruptedStatusObservedAfterReturn.get(), "the thread's interrupt status must be restored, not swallowed, by sendAnnotationMessage");
            assertTrue(getAnnotationPublisher().isEmpty(), "in-flight entry should still be cleaned up when the wait is interrupted");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSendAnnotationMessage_LateAckAfterTimeoutIsNoop() {
        // send succeeds, no ack arrives for the duration of the timeout window,
        // then a late ack arrives. Completing the old future must be safe
        // without recreating the completed publication or changing the previous result.
        when(annotationSink.send(any())).thenReturn(true);
        Annotation annotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());

        // Send the message — this will register an in-flight entry, wait for the ack timeout,
        // receive no ack, and remove the in-flight entry in the finally block
        long start = System.currentTimeMillis();
        Optional<AnnotationMessage> result = annotationController.sendAnnotationMessage(buildMessage(annotation));
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(result.isEmpty());
        assertTrue(elapsed >= annotationProperties.getAnnotationAckTimeoutMillis(), "should have waited for the full ack timeout");

        // Capture the correlation ID from what was sent
        ArgumentCaptor<Message<AnnotationMessage>> captor = ArgumentCaptor.forClass(Message.class);
        verify(annotationSink, times(1)).send(captor.capture());
        Object correlationId = captor.getValue().getHeaders().get(IntegrationMessageHeaderAccessor.CORRELATION_ID);
        assertNotNull(correlationId);

        // Now simulate the late ack arriving after the in-flight entry was already removed
        confirm(captor.getValue());

        // Verify no exceptions were thrown and no publication remains in flight
        assertTrue(getAnnotationPublisher().isEmpty(), "no in-flight entry should remain after late ack");
    }

    @Test
    public void testSendAnnotationMessage_AckDisabledSucceedsWithoutWaitingForAck() {
        annotationProperties.setAnnotationAckEnabled(false);
        when(annotationSink.send(any())).thenReturn(true);
        Annotation annotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());

        Optional<AnnotationMessage> result = annotationController.sendAnnotationMessage(buildMessage(annotation));

        assertTrue(result.isPresent());
        assertTrue(getAnnotationPublisher().isEmpty(), "no in-flight entry should be registered when acks are disabled");
    }

    @Test
    public void testSendAnnotationMessage_AckDisabledSendFailureReturnsEmpty() {
        annotationProperties.setAnnotationAckEnabled(false);
        when(annotationSink.send(any())).thenReturn(false);
        Annotation annotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());

        Optional<AnnotationMessage> result = annotationController.sendAnnotationMessage(buildMessage(annotation));

        assertTrue(result.isEmpty());
    }

    @Test
    public void testSendAnnotationMessage_UnhealthyMessagingInfrastructureSkipsSendEntirely() {
        HealthChecker healthChecker = mock(HealthChecker.class);
        when(healthChecker.isHealthy()).thenReturn(false);
        setHealthChecker(healthChecker);

        Annotation annotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());

        Optional<AnnotationMessage> result = annotationController.sendAnnotationMessage(buildMessage(annotation));

        assertTrue(result.isEmpty(), "an unhealthy messaging infrastructure should short-circuit to failure");
        verify(annotationSink, times(0)).send(any());
        assertTrue(getAnnotationPublisher().isEmpty(), "no in-flight entry should be registered when the send is skipped due to health");
    }

    @Test
    public void testSendAnnotationMessage_HealthyMessagingInfrastructureSendsNormally() {
        HealthChecker healthChecker = mock(HealthChecker.class);
        when(healthChecker.isHealthy()).thenReturn(true);
        setHealthChecker(healthChecker);
        configureImmediateAck();

        Annotation annotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());

        Optional<AnnotationMessage> result = annotationController.sendAnnotationMessage(buildMessage(annotation));

        assertTrue(result.isPresent());
        verify(annotationSink, times(1)).send(any());
    }

    @Test
    public void testSendAnnotationMessage_NoHealthCheckerConfiguredSendsNormally() {
        // healthChecker is left unset (null), mirroring production when 'annotation.health.rabbit.enabled' is false
        configureImmediateAck();
        Annotation annotation = AnnotationUtils.injectAllHashes(generateTestAnnotation());

        Optional<AnnotationMessage> result = annotationController.sendAnnotationMessage(buildMessage(annotation));

        assertTrue(result.isPresent());
        verify(annotationSink, times(1)).send(any());
    }

    @Test
    public void testWriteAnnotation_UnhealthyMessagingInfrastructureRetriesThenFallsBackToFileWriter() throws Exception {
        HealthChecker healthChecker = mock(HealthChecker.class);
        when(healthChecker.isHealthy()).thenReturn(false);
        setHealthChecker(healthChecker);

        AnnotationWriter fileWriter = mock(AnnotationWriter.class);
        setFileAnnotationWriter(fileWriter);

        Annotation annotation = generateTestAnnotation();

        Optional<Annotation> result = annotationController.writeAnnotation(annotation);

        assertTrue(result.isPresent(), "the file-writer fallback should still report success");
        verify(annotationSink, times(0)).send(any());
        verify(fileWriter, times(1)).write(any());
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
    public void testSendAnnotation_UsesDefaultSystemFrom() {
        AnnotationProperties defaultProperties = new AnnotationProperties();
        defaultProperties.setAnnotationAckEnabled(false);
        AnnotationControllerV1 defaultController = new AnnotationControllerV1(connectionFactory, lookupService, defaultProperties, timestampTransformer,
                        visibilityTransformer, new AnnotationPublisher(annotationSink, defaultProperties, null), mock(ExecutorService.class));
        when(annotationSink.send(any())).thenReturn(true);

        Optional<Annotation> result = defaultController.sendAnnotation(AnnotationUtils.injectAllHashes(generateTestAnnotation()));

        assertTrue(result.isPresent());
        ArgumentCaptor<Message<AnnotationMessage>> captor = ArgumentCaptor.forClass(Message.class);
        verify(annotationSink).send(captor.capture());
        assertEquals("datawave-annotation-service", captor.getValue().getPayload().getSource());
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
        assertEquals(AnnotationUtils.injectAllHashes(annotation).getAnnotationId(), result.get().getAnnotationId());
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
                confirm(sent);
            }
            return succeeds;
        });

        Optional<Annotation> result = annotationController.writeAnnotation(annotation);

        assertTrue(result.isPresent());
        assertEquals(AnnotationUtils.injectAllHashes(annotation).getAnnotationId(), result.get().getAnnotationId());
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
        assertEquals(AnnotationUtils.injectAllHashes(annotation).getAnnotationId(), result.get().getAnnotationId());
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
        assertTrue(getAnnotationPublisher().isEmpty(), "no in-flight entries should remain after exhausting retries");
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

    @Test
    public void testWriteAnnotation_InterruptedDuringRetryBackoffStopsRetryingAndRestoresInterruptStatus() throws Exception {
        // Use a long backoff interval so there's a comfortable window to interrupt the thread while it's sleeping between
        // retry attempts, and a send that always fails so the loop actually reaches that backoff sleep.
        AnnotationProperties longBackoffProperties = new AnnotationProperties();
        longBackoffProperties.setSystemFrom("annotation");
        longBackoffProperties.setAnnotationAckEnabled(true);
        longBackoffProperties.setAnnotationAckTimeoutMillis(150L);
        longBackoffProperties.getRetry().setMaxAttempts(5);
        longBackoffProperties.getRetry().setBackoffIntervalMillis(TimeUnit.SECONDS.toMillis(30));
        longBackoffProperties.getRetry().setFailTimeoutMillis(TimeUnit.SECONDS.toMillis(60));

        AnnotationControllerV1 controller = new AnnotationControllerV1(connectionFactory, lookupService, longBackoffProperties, timestampTransformer,
                        visibilityTransformer, new AnnotationPublisher(annotationSink, longBackoffProperties, null), Executors.newCachedThreadPool());

        CountDownLatch firstSendAttempted = new CountDownLatch(1);
        when(annotationSink.send(any())).thenAnswer(invocation -> {
            firstSendAttempted.countDown();
            return false;
        });

        Annotation annotation = generateTestAnnotation();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            AtomicReference<Optional<Annotation>> resultRef = new AtomicReference<>();
            AtomicBoolean interruptedStatusObservedAfterReturn = new AtomicBoolean(false);

            long start = System.currentTimeMillis();
            Future<?> future = executor.submit(() -> {
                resultRef.set(controller.writeAnnotation(annotation));
                // the interrupt status must be restored by the time writeAnnotation returns.
                interruptedStatusObservedAfterReturn.set(Thread.currentThread().isInterrupted());
            });

            assertTrue(firstSendAttempted.await(5, TimeUnit.SECONDS), "the first send attempt should occur promptly");
            // give the worker thread a moment to enter the backoff Thread.sleep(...) after its first failed attempt
            Thread.sleep(50);
            executor.shutdownNow();

            future.get(5, TimeUnit.SECONDS);
            long elapsed = System.currentTimeMillis() - start;

            assertNotNull(resultRef.get());
            assertTrue(resultRef.get().isEmpty(), "an interrupted retry backoff must be treated as a failed write (no file writer configured here)");
            assertTrue(interruptedStatusObservedAfterReturn.get(), "the thread's interrupt status must be restored, not swallowed, by writeAnnotation");
            assertTrue(elapsed < longBackoffProperties.getRetry().getBackoffIntervalMillis(),
                            "interrupting the backoff sleep should abandon retrying promptly rather than waiting out the full backoff interval");
            verify(annotationSink, times(1)).send(any());
        } finally {
            executor.shutdownNow();
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // concurrency: two independent writes must not cross-satisfy each other's in-flight entries
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
            confirm(sent);
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
            assertTrue(getAnnotationPublisher().isEmpty(), "no in-flight entries should remain once both concurrent writes complete");

            // each write should have used its own distinct correlation id, and been sent exactly once (no unnecessary retries,
            // and no cross-talk where one write's ack satisfies the other's confirmation future)
            assertEquals(2, sendCounts.size(), "expected two distinct correlation ids to have been used: " + sendCounts.keySet());
            sendCounts.values().forEach(count -> assertEquals(1, count, "expected each correlation id to have been sent exactly once"));
        } finally {
            executor.shutdownNow();
        }
    }
}
