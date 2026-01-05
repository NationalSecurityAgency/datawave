package datawave.annotation;

public class AnnotationRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 4072754162574238510L;

    public AnnotationRuntimeException() {
        super();
    }

    public AnnotationRuntimeException(Throwable cause) {
        super(cause);
    }

    public AnnotationRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public AnnotationRuntimeException(String message) {
        super(message);
    }
}
