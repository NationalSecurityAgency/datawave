package datawave.core.query.logic;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryKey implements Serializable {
    private static final long serialVersionUID = -2589618312956104322L;

    public static final String QUERY_ID_PREFIX = "Q-";
    public static final String POOL_PREFIX = "P-";
    public static final String LOGIC_PREFIX = "L-";

    @JsonProperty
    private String queryPool;
    @JsonProperty
    private String queryId;
    @JsonProperty
    private String queryLogic;

    /**
     * Default constructor for deserialization
     */
    public QueryKey() {}

    /**
     * This method id to allow deserialization of the toKey() or toString() value used when this is in a map
     *
     * @param value
     *            The toString() from a task key
     */
    public QueryKey(String value) {
        String[] parts = StringUtils.split(value, '.');
        for (String part : parts) {
            setPart(part);
        }
    }

    protected void setPart(String part) {
        if (part.startsWith(QUERY_ID_PREFIX)) {
            queryId = part.substring(QUERY_ID_PREFIX.length());
        } else if (part.startsWith(POOL_PREFIX)) {
            queryPool = part.substring(POOL_PREFIX.length());
        } else if (part.startsWith(LOGIC_PREFIX)) {
            queryLogic = part.substring(LOGIC_PREFIX.length());
        }
    }

    @JsonCreator
    public QueryKey(@JsonProperty("queryPool") String queryPool, @JsonProperty("queryId") String queryId, @JsonProperty("queryLogic") String queryLogic) {
        this.queryPool = queryPool;
        this.queryId = queryId;
        this.queryLogic = queryLogic;
    }

    public String getQueryPool() {
        return queryPool;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getQueryLogic() {
        return queryLogic;
    }

    public static String toUUIDKey(String queryId) {
        return QUERY_ID_PREFIX + queryId;
    }

    public String toUUIDKey() {
        return toUUIDKey(queryId);
    }

    public String toKey() {
        return toUUIDKey() + '.' + POOL_PREFIX + queryPool + '.' + LOGIC_PREFIX + queryLogic;
    }

    @Override
    public String toString() {
        return toKey();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryKey) {
            QueryKey other = (QueryKey) o;
            return new EqualsBuilder().append(getQueryPool(), other.getQueryPool()).append(getQueryId(), other.getQueryId())
                            .append(getQueryLogic(), other.getQueryLogic()).isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getQueryPool()).append(getQueryId()).append(getQueryLogic()).toHashCode();
    }
}
