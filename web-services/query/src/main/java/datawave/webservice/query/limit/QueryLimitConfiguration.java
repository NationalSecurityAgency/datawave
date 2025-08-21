package datawave.webservice.query.limit;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import org.springframework.stereotype.Component;

/**
 * Configuration bean for {@link QueryLimitConfiguration}. In practice, this should be a singleton.
 */
@Component("queryLimitProviderConfig")
public class QueryLimitConfiguration {

    private int defaultUserQueryLimit;
    private int defaultSystemQueryLimit;

    private List<UserLimitConfiguration> userConfigs;
    private List<SystemLimitConfiguration> systemConfigs;
    private List<QueryLogicGroupLimitConfiguration> queryLogicGroupConfigs;

    public QueryLimitConfiguration() {
        this(-1, -1, null, null, null);
    }

    public QueryLimitConfiguration(QueryLimitConfiguration config) {
        this(config.getDefaultUserQueryLimit(), config.getDefaultSystemQueryLimit(), config.getUserConfigs(), config.getSystemConfigs(),
                        config.getQueryLogicGroupConfigs());
    }

    public QueryLimitConfiguration(int defaultUserQueryLimit, int defaultSystemQueryLimit, List<UserLimitConfiguration> userConfigs,
                    List<SystemLimitConfiguration> systemConfigs, List<QueryLogicGroupLimitConfiguration> queryLogicGroupConfigs) {
        this.defaultUserQueryLimit = defaultUserQueryLimit;
        this.defaultSystemQueryLimit = defaultSystemQueryLimit;
        this.userConfigs = userConfigs == null ? List.of() : List.copyOf(userConfigs);
        this.systemConfigs = systemConfigs == null ? List.of() : List.copyOf(systemConfigs);
        this.queryLogicGroupConfigs = queryLogicGroupConfigs == null ? List.of() : List.copyOf(queryLogicGroupConfigs);
    }

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
