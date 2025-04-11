package datawave.query.attributes;

/**
 * Indicates that the scan session was ended after exceeding the wait window
 */
public class WaitWindowExceededMetadata extends Metadata {

    @Override
    public int size() {
        return 1;
    }
}
