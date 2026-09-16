package datawave.microservice.annotation.messaging;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationMessage;

class AnnotationProtobufMessageConverterTest {
    private final MessageConverter converter = new AnnotationProtobufMessageConverter();

    @Test
    void convertsAnnotationMessageToAndFromProtobufBytes() {
        AnnotationMessage annotationMessage = AnnotationMessage.newBuilder().setAnnotationMessageId("message-id")
                        .addAnnotations(Annotation.newBuilder().setAnnotationId("annotation-id").build()).build();
        Message<?> encoded = converter.toMessage(annotationMessage,
                        new MessageHeaders(Map.of(MessageHeaders.CONTENT_TYPE, "application/x-protobuf")));

        assertNotNull(encoded);
        byte[] payload = assertInstanceOf(byte[].class, encoded.getPayload());
        assertArrayEquals(annotationMessage.toByteArray(), payload);

        Message<byte[]> wireMessage = MessageBuilder.withPayload(payload).setHeader(MessageHeaders.CONTENT_TYPE, "application/x-protobuf").build();
        AnnotationMessage decoded = (AnnotationMessage) converter.fromMessage(wireMessage, AnnotationMessage.class);
        assertEquals(annotationMessage, decoded);
    }

    @Test
    void configurationExposesDatawavePackagedConverter() {
        MessageConverter configuredConverter = new AnnotationMessagingConfiguration().annotationProtobufMessageConverter();
        assertInstanceOf(AnnotationProtobufMessageConverter.class, configuredConverter);
    }
}
