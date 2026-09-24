package datawave.microservice.annotationCache.api;

public class AnnotationStorageException extends RuntimeException {
    public AnnotationStorageException(String message) {
        super(message);
    }

    public AnnotationStorageException(String message, Exception e) {
        super(message, e);
    }
}
