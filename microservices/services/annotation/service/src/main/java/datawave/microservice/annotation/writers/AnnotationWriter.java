package datawave.microservice.annotation.writers;

import java.util.Optional;

import datawave.annotation.protobuf.v1.Annotation;

/** An object that writes annotations to a destination */
public interface AnnotationWriter {

    String ISO_8601_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    /**
     *
     * @param annotation
     *            the annotation to write
     * @return the written annotation when one is produced by the implementation, otherwise an empty optional
     * @throws Exception
     *             if there is a problem writing the annotation
     */
    Optional<Annotation> write(Annotation annotation) throws Exception;
}
