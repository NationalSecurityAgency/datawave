package datawave.webservice.query.exception;

// used when a transformer gets a non-null empty object
// and the TransformIterator should call next instead of returning null
public class EmptyObjectException extends RuntimeException {

}
