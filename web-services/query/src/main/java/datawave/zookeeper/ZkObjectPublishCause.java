package datawave.zookeeper;

public enum ZkObjectPublishCause {
    /**
     * Indicates the triggering event was the creation of the node {@value ZkObjectPublisher#NODE_PATH} with non-empty data.
     */
    PATH_NODE_CREATED,
    /**
     * Indicates the triggering event was the modification of the node {@value ZkObjectPublisher#NODE_PATH} with non-empty data.
     */
    PATH_NODE_MODIFIED,
    /**
     * Indicates the triggering event was the creation of the node {@value ZkObjectPublisher#NODE_TRIGGER}.
     */
    TRIGGER_NODE_CREATED,
    /**
     * Indicates the triggering event was the modification of the node {@value ZkObjectPublisher#NODE_TRIGGER}.
     */
    TRIGGER_NODE_MODIFIED,
    /**
     * Indicates the triggering event was the deletion of the node {@value ZkObjectPublisher#NODE_TRIGGER}.
     */
    TRIGGER_NODE_DELETED
}
