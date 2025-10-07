package datawave.core.query.configuration;

public interface CheckpointableQueryConfiguration {

    /**
     * Create an instance of this configuration suitable for a checkpoint. Basically ensure that everything is copied that is required to continue execution of
     * the query post create.
     *
     * @return The configuration
     */
    GenericQueryConfiguration checkpoint();
}
