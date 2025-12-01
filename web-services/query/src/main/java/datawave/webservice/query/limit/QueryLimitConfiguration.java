package datawave.webservice.query.limit;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Configuration for query limits.
 */
public class QueryLimitConfiguration {

    private int defaultUserQueryLimit;
    private int defaultSystemQueryLimit;

    private List<UserLimitConfiguration> userConfigs;
    private List<SystemLimitConfiguration> systemConfigs;
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
                        && Objects.equals(userConfigs, that.userConfigs) && Objects.equals(systemConfigs, that.systemConfigs)
                        && Objects.equals(queryLogicGroupConfigs, that.queryLogicGroupConfigs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(defaultUserQueryLimit, defaultSystemQueryLimit, userConfigs, systemConfigs, queryLogicGroupConfigs);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", QueryLimitConfiguration.class.getSimpleName() + "[", "]").add("defaultUserQueryLimit=" + defaultUserQueryLimit)
                        .add("defaultSystemQueryLimit=" + defaultSystemQueryLimit).add("userConfigs=" + userConfigs).add("systemConfigs=" + systemConfigs)
                        .add("queryLogicGroupConfigs=" + queryLogicGroupConfigs).toString();
    }
}
