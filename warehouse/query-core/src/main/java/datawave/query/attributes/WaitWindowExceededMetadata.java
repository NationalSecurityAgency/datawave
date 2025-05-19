package datawave.query.attributes;

/**
 * This marker Indicates that the current scan session was ended after exceeding the wait window. This empty Metadata is added with the WAIT_WINDOW_OVERRUN key
 * and travels with a Document so that the transformer in the client can ignore this document and iterate to the next result from the tablet server.
 */
public class WaitWindowExceededMetadata extends Metadata {

    /**
     * Return size = 1 so that Document doesn't just ignore (remove) this Attribute
     */
    @Override
    public int size() {
        return 1;
    }
}
