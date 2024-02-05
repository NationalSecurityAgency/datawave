package datawave.query.config;

import java.io.Serializable;
import java.util.Objects;

import datawave.query.tables.RemoteEventQueryLogic;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;

/**
 * <p>
 * A GenericQueryConfiguration implementation that provides the additional logic on top of the traditional query that is needed to run a remote query logic
 *
 */
public class RemoteQueryConfiguration extends GenericQueryConfiguration implements Serializable {

    private static final long serialVersionUID = -4354990715046146110L;

    // the id of the remote query
    private String remoteId;

    private String remoteQueryLogic;

    private Query query;

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
        this.query = other.getQuery();
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

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
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

}
