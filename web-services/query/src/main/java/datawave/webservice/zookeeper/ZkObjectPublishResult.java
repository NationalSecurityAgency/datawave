package datawave.webservice.zookeeper;

import java.time.Instant;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * Represents a result from {@link ZkObjectPublisher#getObjectFromZk()}.
 */
public class ZkObjectPublishResult {

    /**
     * The updated object. This will be null if no object update could be successfully loaded.
     */
    private final Object updatedObject;

    /**
     * The status of loading the object.
     */
    private final ZkObjectPublishStatus status;

    /**
     * A list of any errors that occurred while trying to load the results.
     */
    private final List<ZkObjectPublishError> errors;

    /**
     * The time that loading the object was attempted.
     */
    private final Instant time;

    public static ZkObjectPublishResult success(Instant time, Object pojo) {
        return new ZkObjectPublishResult(pojo, ZkObjectPublishStatus.SUCCESS, null, time);
    }

    public static ZkObjectPublishResult error(Instant time, String message) {
        return new ZkObjectPublishResult(null, ZkObjectPublishStatus.RELOAD_ERROR, List.of(new ZkObjectPublishError(message, null)), time);
    }

    public static ZkObjectPublishResult error(Instant time, String message, Exception exception) {
        return new ZkObjectPublishResult(null, ZkObjectPublishStatus.RELOAD_ERROR, List.of(new ZkObjectPublishError(message, exception)), time);
    }

    public static ZkObjectPublishResult subscriberErrors(Instant time, Object pojo, List<Exception> exceptions) {
        List<ZkObjectPublishError> errors = exceptions.stream().map((e) -> new ZkObjectPublishError("Exception thrown by listener: " + e.getMessage(), e))
                        .collect(Collectors.toList());
        return new ZkObjectPublishResult(pojo, ZkObjectPublishStatus.SUBSCRIBER_ERROR, errors, time);
    }

    public ZkObjectPublishResult(Object updatedObject, ZkObjectPublishStatus status, List<ZkObjectPublishError> errors, Instant time) {
        this.updatedObject = updatedObject;
        this.status = status;
        this.errors = errors != null ? List.copyOf(errors) : List.of();
        this.time = time;
    }

    public Object getUpdatedObject() {
        return updatedObject;
    }

    public ZkObjectPublishStatus getStatus() {
        return status;
    }

    public List<ZkObjectPublishError> getErrors() {
        return errors;
    }

    public Instant getTime() {
        return time;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ZkObjectPublishResult.class.getSimpleName() + "[", "]").add("updatedObject=" + updatedObject).add("status=" + status)
                        .add("errors=" + errors).add("time=" + time).toString();
    }
}
