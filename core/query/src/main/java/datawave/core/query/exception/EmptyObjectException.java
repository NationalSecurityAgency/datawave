package datawave.core.query.exception;

// used when a transformer gets a non-null empty object
// and the TransformIterator should call next instead of returning null
public class EmptyObjectException extends RuntimeException {
    private static final long serialVersionUID = 1545558691183965869L;
}
