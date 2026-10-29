package datawave.microservice.annotationCache.config;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotation.messaging.AnnotationProtobufMessageConverter;

class AnnotationMessagingConfigurationTest {
    @Test
    void componentScannedConverterSerializesAndDeserializesAnnotationMessages() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            // AnnotationCacheService scans datawave.microservice, which includes this shared configuration package.
            context.scan("datawave.microservice.annotation.messaging");
            context.refresh();

            MessageConverter converter = context.getBean("annotationProtobufMessageConverter", MessageConverter.class);
            assertInstanceOf(AnnotationProtobufMessageConverter.class, converter);

            AnnotationMessage original = AnnotationMessage.newBuilder().setAnnotationMessageId("message-id")
                            .addAnnotations(Annotation.newBuilder().setAnnotationId("annotation-id").build()).build();
            Message<?> encoded = converter.toMessage(original, new MessageHeaders(Map.of(MessageHeaders.CONTENT_TYPE, "application/x-protobuf")));

            assertNotNull(encoded);
            byte[] payload = assertInstanceOf(byte[].class, encoded.getPayload());
            assertArrayEquals(original.toByteArray(), payload);

            Message<byte[]> wireMessage = MessageBuilder.withPayload(payload).setHeader(MessageHeaders.CONTENT_TYPE, "application/x-protobuf").build();
            AnnotationMessage decoded = (AnnotationMessage) converter.fromMessage(wireMessage, AnnotationMessage.class);
            assertEquals(original, decoded);
        }
    }
}
