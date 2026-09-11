package datawave.microservice.annotation.common;

import java.util.function.Consumer;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotation.writers.AnnotationWriter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AnnotationConsumer implements Consumer<AnnotationMessage> {

    private final AnnotationWriter annotationWriter;

    public AnnotationConsumer(AnnotationWriter annotationWriter) {
        this.annotationWriter = annotationWriter;
    }

    @Override
    public void accept(AnnotationMessage annotationMessage) {
        try {
            for (Annotation a : annotationMessage.getAnnotationsList()) {
                // consider whether we need an all-or-nothing operation here, possibly not
                // because annotation writes should be idempotent.
                annotationWriter.write(a);
            }
        } catch (Exception e) {
            log.error("Error processing annotation message: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
