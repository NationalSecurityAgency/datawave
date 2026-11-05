package datawave.microservice.annotationCache.api;

/** Signals that an annotation cache storage or federation operation could not be completed. */
public class AnnotationStorageException extends RuntimeException {
    public AnnotationStorageException(String message) {
        super(message);
    }

    public AnnotationStorageException(String message, Exception e) {
        super(message, e);
    }
}
