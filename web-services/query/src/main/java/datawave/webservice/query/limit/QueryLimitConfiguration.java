package datawave.webservice.query.limit;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for query limits.
 */
public class QueryLimitConfiguration {

    /**
     * The default maximum number of active concurrent queries a user may have across all systems.
     */
    @JsonProperty
    private int defaultUserQueryLimit;

    /**
     * The default maximum number of active concurrent queries that may be running on a system.
     */
    @JsonProperty
    private int defaultSystemQueryLimit;

    /**
     * The maximum size to use for internal caches in {@link GroupLimitCache} and {@link PatternMatcher}. This value should be large enough to hold the number
     * of distinct query logics.
     */
    @JsonProperty
    private long internalCacheMaxSize = 200;

    /**
     * The custom user limit configurations.
     */
    @JsonProperty
    private List<UserLimitConfiguration> userConfigs;

    /**
     * The custom system limit configurations.
     */
    @JsonProperty
    private List<SystemLimitConfiguration> systemConfigs;

    /**
     * The custom query logic group configurations.
     */
    @JsonProperty
    private List<QueryLogicGroupLimitConfiguration> queryLogicGroupConfigs;

    public int getDefaultUserQueryLimit() {
        return defaultUserQueryLimit;
    }

    public void setDefaultUserQueryLimit(int defaultUserQueryLimit) {
        this.defaultUserQueryLimit = defaultUserQueryLimit;
    }

    public int getDefaultSystemQueryLimit() {
        return defaultSystemQueryLimit;
    }

    public void setDefaultSystemQueryLimit(int defaultSystemQueryLimit) {
        this.defaultSystemQueryLimit = defaultSystemQueryLimit;
    }

    public long getInternalCacheMaxSize() {
        return internalCacheMaxSize;
    }

    public void setInternalCacheMaxSize(long internalCacheMaxSize) {
        this.internalCacheMaxSize = internalCacheMaxSize;
    }

    public List<UserLimitConfiguration> getUserConfigs() {
        return userConfigs;
    }

    public void setUserConfigs(List<UserLimitConfiguration> userConfigs) {
        this.userConfigs = userConfigs;
    }

    public List<SystemLimitConfiguration> getSystemConfigs() {
        return systemConfigs;
    }

    public void setSystemConfigs(List<SystemLimitConfiguration> systemConfigs) {
        this.systemConfigs = systemConfigs;
    }

    public List<QueryLogicGroupLimitConfiguration> getQueryLogicGroupConfigs() {
        return queryLogicGroupConfigs;
    }

    public void setQueryLogicGroupConfigs(List<QueryLogicGroupLimitConfiguration> queryLogicGroupConfigs) {
        this.queryLogicGroupConfigs = queryLogicGroupConfigs;
    }

    /**
     * Return whether this {@link QueryLimitConfiguration} is considered equal to the given object. This {@code equals(Object)} implementation allows this
     * instance to be equal to an object that is a subclass of {@link QueryLimitConfiguration}, such as {@link ImmutableQueryLimitConfiguration}.
     *
     * @param o
     *            the object to compare
     * @return true if the object is equal to this {@link QueryLimitConfiguration}, or false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        // Allow this instance to be considered equal to subclasses.
        if (!(o instanceof QueryLimitConfiguration)) {
            return false;
        }

        QueryLimitConfiguration that = (QueryLimitConfiguration) o;
        return defaultUserQueryLimit == that.defaultUserQueryLimit && defaultSystemQueryLimit == that.defaultSystemQueryLimit
                        && internalCacheMaxSize == that.internalCacheMaxSize && Objects.equals(userConfigs, that.userConfigs)
                        && Objects.equals(systemConfigs, that.systemConfigs) && Objects.equals(queryLogicGroupConfigs, that.queryLogicGroupConfigs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(defaultUserQueryLimit, defaultSystemQueryLimit, internalCacheMaxSize, userConfigs, systemConfigs, queryLogicGroupConfigs);
    }

    @Override
    public String toString() {
        return toString(QueryLimitConfiguration.class);
    }

    /**
     * Return a String representation of this {@link QueryLimitConfiguration} referencing the given class as the instance of this
     * {@link QueryLimitConfiguration}.
     *
     * @param clazz
     *            the class
     * @return the string representation
     */
    protected String toString(Class<? extends QueryLimitConfiguration> clazz) {
        return new StringJoiner(", ", clazz.getSimpleName() + "[", "]").add("defaultUserQueryLimit=" + defaultUserQueryLimit)
                        .add("defaultSystemQueryLimit=" + defaultSystemQueryLimit).add("internalCacheMaxSize=" + internalCacheMaxSize)
                        .add("userConfigs=" + userConfigs).add("systemConfigs=" + systemConfigs).add("queryLogicGroupConfigs=" + queryLogicGroupConfigs)
                        .toString();
    }
}
