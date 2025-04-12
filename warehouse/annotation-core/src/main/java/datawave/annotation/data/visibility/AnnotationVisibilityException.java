package datawave.annotation.data.visibility;

import datawave.annotation.data.AnnotationSerializationException;

public class AnnotationVisibilityException extends AnnotationSerializationException {

    private static final long serialVersionUID = 302720118279328441L;

    public AnnotationVisibilityException(String message) {
        super(message);
    }

    public AnnotationVisibilityException(String message, Throwable cause) {
        super(message, cause);
    }
}
