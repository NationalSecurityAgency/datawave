package datawave.annotation.data.transform;

import datawave.annotation.data.AnnotationSerializationException;

public class AnnotationTransformException extends AnnotationSerializationException {

    private static final long serialVersionUID = 302720118279328441L;

    public AnnotationTransformException(String message) {
        super(message);
    }

    public AnnotationTransformException(String message, Throwable cause) {
        super(message, cause);
    }
}
