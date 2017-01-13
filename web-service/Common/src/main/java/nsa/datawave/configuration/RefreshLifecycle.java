package nsa.datawave.configuration;

/**
 * This enum is used for CDI events regarding the lifecycle of a configuration refresh. An event is fired with each value of the enum at the appropriate point
 * during the configuration refresh operation.
 *
 * @see ConfigurationBean
 * @see RefreshableScope
 */
public enum RefreshLifecycle {
    INITIATED, COMPLETE
}
