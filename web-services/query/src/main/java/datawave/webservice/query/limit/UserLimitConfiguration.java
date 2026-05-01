package datawave.webservice.query.limit;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a custom query limit configuration that can be configured for a single user.
 */
public class UserLimitConfiguration {

    // The user DN.
    @JsonProperty
    private String userDn;

    // The user's concurrent query limit. This applies to the total number of queries the user may run across all systems.
    @JsonProperty
    private Integer queryLimit;

    // Map of query logic group names to the user's concurrent query limit for the group. The names may be regex patterns.
    @JsonProperty
    private Map<String,Integer> queryLogicGroupLimits;

    public UserLimitConfiguration() {
        this(null, null, null);
    }

    public UserLimitConfiguration(String userDn, Integer queryLimit, Map<String,Integer> queryLogicGroupLimits) {
        this.userDn = userDn;
        this.queryLimit = queryLimit;
        this.queryLogicGroupLimits = queryLogicGroupLimits == null ? Map.of() : Map.copyOf(queryLogicGroupLimits);
    }

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
        this.queryLogicGroupLimits = queryLogicGroupLimits == null ? Map.of() : queryLogicGroupLimits;
    }

    /**
     * Return whether this {@link UserLimitConfiguration} is considered equal to the given object. This {@code equals(Object)} implementation allows this
     * instance to be equal to an object that is a subclass of {@link UserLimitConfiguration}, such as {@link ImmutableUserLimitConfiguration}.
     *
     * @param o
     *            the object to compare
     * @return true if the object is equal to this {@link UserLimitConfiguration}, or false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        // Allow this instance to be considered equal to subclasses.
        if (!(o instanceof UserLimitConfiguration)) {
            return false;
        }
        UserLimitConfiguration that = (UserLimitConfiguration) o;
        return Objects.equals(userDn, that.userDn) && Objects.equals(queryLimit, that.queryLimit)
                        && Objects.equals(queryLogicGroupLimits, that.queryLogicGroupLimits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userDn, queryLimit, queryLogicGroupLimits);
    }

    @Override
    public String toString() {
        return toString(UserLimitConfiguration.class);
    }

    /**
     * Return a String representation of this {@link UserLimitConfiguration} referencing the given class as the instance of this {@link UserLimitConfiguration}.
     *
     * @param clazz
     *            the class
     * @return the string representation
     */
    protected String toString(Class<? extends UserLimitConfiguration> clazz) {
        return new StringJoiner(", ", clazz.getSimpleName() + "[", "]").add("userDn='" + userDn + "'").add("queryLimit=" + queryLimit)
                        .add("queryLogicGroupLimits=" + queryLogicGroupLimits).toString();
    }
}
