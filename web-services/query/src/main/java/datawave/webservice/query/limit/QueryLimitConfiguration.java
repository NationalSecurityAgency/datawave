package datawave.webservice.query.limit;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Configuration for query limits.
 */
public class QueryLimitConfiguration {

    /**
     * The default maximum number of active concurrent queries a user may have across all systems.
     */
    private int defaultUserQueryLimit;

    /**
     * The default maximum number of active concurrent queries that may be running on a system.
     */
    private int defaultSystemQueryLimit;

    /**
     * The maximum size to use for internal caches in {@link GroupLimitCache} and {@link PatternMatcher}. This value should be large enough to hold the number
     * of distinct query logics.
     */
    private long internalCacheMaxSize = 200;

    /**
     * The custom user limit configurations.
     */
    private List<UserLimitConfiguration> userConfigs;

    /**
     * The custom system limit configurations.
     */
    private List<SystemLimitConfiguration> systemConfigs;

    /**
     * The custom query logic group configurations.
     */
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

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
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
        return new StringJoiner(", ", QueryLimitConfiguration.class.getSimpleName() + "[", "]").add("defaultUserQueryLimit=" + defaultUserQueryLimit)
                        .add("defaultSystemQueryLimit=" + defaultSystemQueryLimit).add("internalCacheMaxSize=" + internalCacheMaxSize)
                        .add("userConfigs=" + userConfigs).add("systemConfigs=" + systemConfigs).add("queryLogicGroupConfigs=" + queryLogicGroupConfigs)
                        .toString();
    }
}
