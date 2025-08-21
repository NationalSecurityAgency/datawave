package datawave.webservice.query.limit;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Configuration bean for {@link QueryLimitProviderConfiguration}. In practice, this should be a singleton.
 */
@Component("queryLimitProviderConfig")
public class QueryLimitProviderConfiguration {
    
    private int defaultUserQueryLimit;
    private int defaultSystemQueryLimit;
    
    private List<UserConfiguration> userConfigs;
    private List<SystemConfiguration> systemConfigs;
    private List<QueryLogicGroupConfiguration> queryLogicGroupConfigs;
    
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
    
    public List<UserConfiguration> getUserConfigs() {
        return userConfigs;
    }
    
    public void setUserConfigs(List<UserConfiguration> userConfigs) {
        this.userConfigs = userConfigs;
    }
    
    public List<SystemConfiguration> getSystemConfigs() {
        return systemConfigs;
    }
    
    public void setSystemConfigs(List<SystemConfiguration> systemConfigs) {
        this.systemConfigs = systemConfigs;
    }
    
    public List<QueryLogicGroupConfiguration> getQueryLogicGroupConfigs() {
        return queryLogicGroupConfigs;
    }
    
    public void setQueryLogicGroupConfigs(List<QueryLogicGroupConfiguration> queryLogicGroupConfigs) {
        this.queryLogicGroupConfigs = queryLogicGroupConfigs;
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryLimitProviderConfiguration that = (QueryLimitProviderConfiguration) o;
        return defaultUserQueryLimit == that.defaultUserQueryLimit && defaultSystemQueryLimit == that.defaultSystemQueryLimit && Objects.equals(userConfigs,
                        that.userConfigs) && Objects.equals(systemConfigs, that.systemConfigs) && Objects.equals(queryLogicGroupConfigs,
                        that.queryLogicGroupConfigs);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(defaultUserQueryLimit, defaultSystemQueryLimit, userConfigs, systemConfigs, queryLogicGroupConfigs);
    }
    
    @Override
    public String toString() {
        return new StringJoiner(", ", QueryLimitProviderConfiguration.class.getSimpleName() + "[", "]").add("defaultUserQueryLimit=" + defaultUserQueryLimit)
                        .add("defaultSystemQueryLimit=" + defaultSystemQueryLimit).add("userConfigs=" + userConfigs).add("systemConfigs=" + systemConfigs)
                        .add("queryLogicGroupConfigs=" + queryLogicGroupConfigs).toString();
    }
}
