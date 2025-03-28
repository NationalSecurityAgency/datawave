package datawave.query.config;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

import datawave.core.query.configuration.CheckpointableQueryConfiguration;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.configuration.QueryData;
import datawave.query.tables.RemoteEventQueryLogic;

/**
 * <p>
 * A GenericQueryConfiguration implementation that provides the additional logic on top of the traditional query that is needed to run a remote query logic
 *
 */
public class RemoteQueryConfiguration extends GenericQueryConfiguration implements Serializable, CheckpointableQueryConfiguration {

    private static final long serialVersionUID = -4354990715046146110L;

    // the id of the remote query
    private String remoteId;

    private String remoteQueryLogic;

    /**
     * Default constructor
     */
    public RemoteQueryConfiguration() {
        super();
    }

    /**
     * Performs a deep copy of the provided RemoteQueryConfiguration into a new instance
     *
     * @param other
     *            - another RemoteQueryConfiguration instance
     */
    public RemoteQueryConfiguration(RemoteQueryConfiguration other) {

        // GenericQueryConfiguration copy first
        super(other);

        // RemoteQueryConfiguration copy
        this.remoteId = other.getRemoteId();
        this.remoteQueryLogic = other.getRemoteQueryLogic();
    }

    /**
     * This constructor is used when we are creating a checkpoint for a set of ranges (i.e. QueryData objects). All configuration required for post planning
     * needs to be copied over here.
     *
     * @param other
     * @param queries
     */
    public RemoteQueryConfiguration(RemoteQueryConfiguration other, Collection<QueryData> queries) {
        this(other);
    }

    @Override
    public RemoteQueryConfiguration checkpoint() {
        return new RemoteQueryConfiguration(this, Collections.EMPTY_LIST);
    }

    /**
     * Delegates deep copy work to appropriate constructor, sets additional values specific to the provided RemoteRemoteQueryLogic
     *
     * @param logic
     *            - a RemoteQueryLogic instance or subclass
     */
    public RemoteQueryConfiguration(RemoteEventQueryLogic logic) {
        this(logic.getConfig());
    }

    /**
     * Factory method that instantiates an fresh RemoteQueryConfiguration
     *
     * @return - a clean RemoteQueryConfiguration
     */
    public static RemoteQueryConfiguration create() {
        return new RemoteQueryConfiguration();
    }

    /**
     * Factory method that returns a deep copy of the provided RemoteQueryConfiguration
     *
     * @param other
     *            - another instance of a RemoteQueryConfiguration
     * @return - copy of provided RemoteQueryConfiguration
     */
    public static RemoteQueryConfiguration create(RemoteQueryConfiguration other) {
        return new RemoteQueryConfiguration(other);
    }

    public String getRemoteId() {
        return remoteId;
    }

    public void setRemoteId(String remoteId) {
        this.remoteId = remoteId;
    }

    public String getRemoteQueryLogic() {
        return remoteQueryLogic;
    }

    public void setRemoteQueryLogic(String remoteQueryLogic) {
        this.remoteQueryLogic = remoteQueryLogic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        RemoteQueryConfiguration that = (RemoteQueryConfiguration) o;
        return Objects.equals(getRemoteId(), that.getRemoteId()) && Objects.equals(getRemoteQueryLogic(), that.getRemoteQueryLogic())
                        && Objects.equals(getQuery(), that.getQuery());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getRemoteId(), getRemoteQueryLogic(), getQuery());
    }

    // Part of the Serializable interface used to initialize any transient members during deserialization
    protected Object readResolve() throws ObjectStreamException {
        return this;
    }

}
