package nsa.datawave.webservice.query.exception;

// used when a transformer gets a non-null an empty object
// and the TransformIterator should call next instead of returning null
public class EmptyObjectException extends RuntimeException {
    
}
