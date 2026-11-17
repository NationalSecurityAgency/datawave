package datawave.webservice.query.limit;

import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.StringJoiner;

public class SystemLimits {

    private final String systemPattern;

    private final int queryLimit;

    private final boolean countsAgainstUserLimit;

    private final GroupLimitCache groupLimitOverrides;

    public SystemLimits(String systemPattern, int queryLimit, boolean countsAgainstUserLimit, SortedSet<QueryLogicGroupLimit> groupLimitOverrides,
                    long maxCacheSize) {
        this.systemPattern = systemPattern;
        this.queryLimit = queryLimit;
        this.countsAgainstUserLimit = countsAgainstUserLimit;
        this.groupLimitOverrides = GroupLimitCache.of(groupLimitOverrides, maxCacheSize);
    }

    public String getSystemPattern() {
        return systemPattern;
    }

    public Integer getQueryLimit() {
        return queryLimit;
    }

    public boolean countsAgainstUserLimit() {
        return countsAgainstUserLimit;
    }

    public GroupLimitCache getGroupLimitOverrides() {
        return groupLimitOverrides;
    }

    public boolean overridesAnyGroupLimits() {
        return !groupLimitOverrides.isEmpty();
    }

    public Map<String,Integer> getBestGroupLimits(String queryLogic) {
        return groupLimitOverrides.getBestGroupLimits(queryLogic);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SystemLimits that = (SystemLimits) o;
        return countsAgainstUserLimit == that.countsAgainstUserLimit && Objects.equals(systemPattern, that.systemPattern)
                        && Objects.equals(queryLimit, that.queryLimit) && Objects.equals(groupLimitOverrides, that.groupLimitOverrides);
    }

    @Override
    public int hashCode() {
        return Objects.hash(systemPattern, queryLimit, countsAgainstUserLimit, groupLimitOverrides);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", SystemLimits.class.getSimpleName() + "[", "]").add("systemPattern='" + systemPattern + "'")
                        .add("queryLimit=" + queryLimit).add("countsAgainstUserLimit=" + countsAgainstUserLimit).add("groupLimitCache=" + groupLimitOverrides)
                        .toString();
    }
}
