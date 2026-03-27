package datawave.annotation.data;

public interface AnnotationSerializer<U,V> {
    U serialize(V annotation) throws AnnotationSerializationException;

    V deserialize(U input) throws AnnotationSerializationException;
}
