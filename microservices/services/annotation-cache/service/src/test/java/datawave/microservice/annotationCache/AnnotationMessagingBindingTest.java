package datawave.microservice.annotationCache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeTypeUtils;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotation.messaging.AnnotationMessagingConfiguration;

/**
 * Verifies that Spring Cloud Stream uses the annotation protobuf converter on both binding directions. The outbound test checks that
 * {@link AnnotationMessagePublisher} sends protobuf bytes with the protobuf content type; the inbound test sends bytes to the application-side
 * {@code receive-in-0} channel and checks that the function receives an {@link AnnotationMessage}. The configured {@code annotation-cache-in} destination is
 * the broker-side address; this in-memory test targets the binding channel directly and does not test broker routing. Consumer cache behavior is covered by
 * separate tests.
 */
@SpringBootTest(classes = AnnotationMessagingBindingTest.StreamTestApplication.class,
                properties = {"spring.cloud.config.enabled=false", "spring.cloud.function.definition=receive", "spring.cloud.stream.defaultBinder=integration",
                        "spring.cloud.stream.bindings.receive-in-0.destination=annotation-cache-in",
                        "spring.cloud.stream.bindings.receive-in-0.contentType=application/x-protobuf",
                        "spring.cloud.stream.bindings.persisted-out-0.destination=annotation-cache-out",
                        "spring.cloud.stream.bindings.persisted-out-0.contentType=application/x-protobuf"})
class AnnotationMessagingBindingTest {
    private static final String PROTOBUF_CONTENT_TYPE = "application/x-protobuf";

    @Autowired
    private AnnotationMessagePublisher publisher;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private OutputDestination outputDestination;

    @Autowired
    private BlockingQueue<AnnotationMessage> receivedMessages;

    @Test
    void streamBridgePublishesProtobufPayloadOnConfiguredBinding() {
        AnnotationMessage original = message();
        Message<AnnotationMessage> message = MessageBuilder.withPayload(original)
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.parseMimeType(PROTOBUF_CONTENT_TYPE)).build();

        assertTrue(publisher.send(message));

        Message<byte[]> published = outputDestination.receive(TimeUnit.SECONDS.toMillis(5), "annotation-cache-out");
        assertNotNull(published);
        assertArrayEquals(original.toByteArray(), published.getPayload());
        assertEquals(PROTOBUF_CONTENT_TYPE, published.getHeaders().get(MessageHeaders.CONTENT_TYPE).toString());
    }

    @Test
    void inputBindingDeserializesProtobufPayloadBeforeInvokingFunction() throws InterruptedException {
        AnnotationMessage original = message();
        Message<byte[]> encoded = MessageBuilder.withPayload(original.toByteArray())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.parseMimeType(PROTOBUF_CONTENT_TYPE)).build();

        MessageChannel inputChannel = applicationContext.getBean("receive-in-0", MessageChannel.class);
        inputChannel.send(encoded);

        assertEquals(original, receivedMessages.poll(5, TimeUnit.SECONDS));
    }

    private AnnotationMessage message() {
        return AnnotationMessage.newBuilder().setAnnotationMessageId("message-id")
                        .addAnnotations(Annotation.newBuilder().setAnnotationId("annotation-id").setDocumentId("document-id").build()).build();
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({TestChannelBinderConfiguration.class, AnnotationMessagingConfiguration.class, AnnotationMessagePublisher.class})
    static class StreamTestApplication {
        @Bean
        BlockingQueue<AnnotationMessage> receivedMessages() {
            return new LinkedBlockingQueue<>();
        }

        @Bean
        Consumer<AnnotationMessage> receive(BlockingQueue<AnnotationMessage> receivedMessages) {
            return receivedMessages::add;
        }
    }
}
