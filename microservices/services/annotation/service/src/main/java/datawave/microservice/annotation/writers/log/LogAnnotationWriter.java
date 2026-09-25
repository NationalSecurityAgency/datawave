package datawave.microservice.annotation.writers.log;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.util.v1.AnnotationJsonUtils;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.microservice.annotation.writers.AnnotationWriter;

public class LogAnnotationWriter implements AnnotationWriter {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public Optional<Annotation> write(Annotation annotation) throws Exception {

        // ensure that identifiers are assigned to the annotation, annotationSource and segments if they don't exist.
        Annotation identifiedAnnotation = AnnotationUtils.injectAllHashes(annotation);

        // convert the messages to JSON
        final String annotationJson = AnnotationJsonUtils.annotationToJsonWithIds(identifiedAnnotation) + "\n";
        log.info(annotationJson);

        return Optional.of(identifiedAnnotation);
    }
}
