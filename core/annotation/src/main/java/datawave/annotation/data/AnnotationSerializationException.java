package datawave.annotation.data;

public class AnnotationSerializationException extends Exception {
    private static final long serialVersionUID = 2368127012321663252L;

    public AnnotationSerializationException(String message) {
        super(message);
    }

    public AnnotationSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
