package datawave.annotation.data;

import datawave.annotation.AnnotationRuntimeException;

public class AnnotationUpdateException extends AnnotationRuntimeException {

    private static final long serialVersionUID = 4091754162164238510L;

    public AnnotationUpdateException(String message) {
        super(message);
    }

    public AnnotationUpdateException(String message, Throwable cause) {
        super(message, cause);
    }

}
