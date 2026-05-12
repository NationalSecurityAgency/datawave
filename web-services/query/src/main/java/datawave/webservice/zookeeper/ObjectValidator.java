package datawave.webservice.zookeeper;

/**
 * An interface for defining a validator that can be provided to a {@link ZkObjectPublisher} for pre-validating any updated objects before publishing them to
 * subscribers.
 */
public interface ObjectValidator {

    /**
     * Validate the given object. Any implementations should throw an exception if the provided object is not considered valid.
     *
     * @param object
     *            the object to validate
     */
    void validate(Object object) throws Exception;
}
