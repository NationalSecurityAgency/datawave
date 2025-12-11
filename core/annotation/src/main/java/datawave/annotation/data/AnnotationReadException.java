package datawave.annotation.data;

import datawave.annotation.AnnotationRuntimeException;

public class AnnotationReadException extends AnnotationRuntimeException {

    private static final long serialVersionUID = 4091754162164238510L;

    public AnnotationReadException(String message) {
        super(message);
    }

    public AnnotationReadException(String message, Throwable cause) {
        super(message, cause);
    }

}
