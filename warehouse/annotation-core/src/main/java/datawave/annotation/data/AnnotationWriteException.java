package datawave.annotation.data;

public class AnnotationWriteException extends RuntimeException {

    private static final long serialVersionUID = -4456557208304173953L;

    public AnnotationWriteException(String message) {
        super(message);
    }

    public AnnotationWriteException(String message, Throwable cause) {
        super(message, cause);
    }

}
