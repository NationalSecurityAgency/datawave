package datawave.microservice.annotationCache;

import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import datawave.annotation.protobuf.v1.AnnotationMessage;

/** Publishes annotation messages through the configured Spring Cloud Stream binding. */
@Component
public class AnnotationMessagePublisher {
    private final StreamBridge streamBridge;

    public AnnotationMessagePublisher(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }

    public boolean send(Message<AnnotationMessage> message) {
        return streamBridge.send("persisted-out-0", message);
    }
}
