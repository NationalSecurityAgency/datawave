package datawave.annotation.data;

import datawave.annotation.AnnotationRuntimeException;

public class AnnotationWriteException extends AnnotationRuntimeException {

    private static final long serialVersionUID = -4456557208304173953L;

    public AnnotationWriteException(String message) {
        super(message);
    }

    public AnnotationWriteException(String message, Throwable cause) {
        super(message, cause);
    }

}
