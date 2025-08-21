package datawave.webservice.query.limit;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Represents a custom query limit configuration that can be configured for a single user.
 */
public class UserConfiguration {
    
    // The user DN.
    private String userDn;
    
    // The user's concurrent query limit. This applies to the total number of queries the user may run across all systems.
    private Integer queryLimit;
    
    // Map of query logic group names to the user's concurrent query limit for the group.
    private Map<String, Integer> queryLogicGroupLimits;
    
    public String getUserDn() {
        return userDn;
    }
    
    public void setUserDn(String userDn) {
        this.userDn = userDn;
    }
    
    public Integer getQueryLimit() {
        return queryLimit;
    }
    
    public void setQueryLimit(Integer queryLimit) {
        this.queryLimit = queryLimit;
    }
    
    public Map<String,Integer> getQueryLogicGroupLimits() {
        return queryLogicGroupLimits;
    }
    
    public void setQueryLogicGroupLimits(Map<String,Integer> queryLogicGroupLimits) {
        this.queryLogicGroupLimits = queryLogicGroupLimits;
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UserConfiguration that = (UserConfiguration) o;
        return Objects.equals(userDn, that.userDn) && Objects.equals(queryLimit, that.queryLimit) && Objects.equals(queryLogicGroupLimits,
                        that.queryLogicGroupLimits);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(userDn, queryLimit, queryLogicGroupLimits);
    }
    
    @Override
    public String toString() {
        return new StringJoiner(", ", UserConfiguration.class.getSimpleName() + "[", "]").add("userDn='" + userDn + "'").add("queryLimit=" + queryLimit)
                        .add("queryLogicGroupLimits=" + queryLogicGroupLimits).toString();
    }
}
