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

    /**
     * Sends the message on the {@code persisted-out-0} binding. The result indicates channel handoff, not broker confirmation; {@link AnnotationMapStore} waits
     * for the broker confirm.
     *
     * @param message
     *            annotation message to publish
     * @return {@code true} if the message was accepted by the binding channel
     */
    public boolean send(Message<AnnotationMessage> message) {
        return streamBridge.send("persisted-out-0", message);
    }
}
