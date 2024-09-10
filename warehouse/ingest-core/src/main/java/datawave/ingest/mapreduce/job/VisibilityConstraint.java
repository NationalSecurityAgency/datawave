package datawave.ingest.mapreduce.job;

/**
 * A constraint on an Accumulo column visibility.
 */
public interface VisibilityConstraint {

    /**
     * Checks if a ColumnVisibility is valid.
     *
     * @param visibility
     *            the visibility array
     * @return True if valid, false, otherwise
     */
    boolean isValid(byte[] visibility);
}
