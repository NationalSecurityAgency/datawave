package datawave.zookeeper;

public enum ZkObjectPublishStatus {
    /**
     * Indicates an object update was successfully loaded from Zookeeper and, if triggered by a trigger event, successfully published to all subscribers.
     */
    SUCCESS,

    /**
     * Indicates an error occurred when trying to load an object update from Zookeeper.
     */
    RELOAD_ERROR,

    /**
     * Indicates an object update was successfully loaded from Zookeeper, but one or more subscribers threw an error when provided the updated object.
     */
    SUBSCRIBER_ERROR
}
