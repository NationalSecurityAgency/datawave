package datawave.query.transformer.annotation;

/**
 * Checked Exception to manage errors in building AllHits
 */
public class AllHitsException extends Exception {
    public AllHitsException(String message) {
        super(message);
    }
}
