package datawave.webservice.query.limit;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Maintains a set of query logic group limit overrides, as well as a small internal cache to improve efficiency when fetching query logic groups that match
 * against a particular query logic.
 */
public class GroupLimitCache {

    // The set of group limit overrides, sorted in best-match order.
    private final SortedSet<QueryLogicGroupLimit> groupLimits;

    // Internal cache for improved lookup efficiency.
    private final Cache<String,Map<String,Integer>> cache;

    public static GroupLimitCache of(SortedSet<QueryLogicGroupLimit> groupLimits, long maxCacheSize) {
        if (groupLimits != null && !groupLimits.isEmpty()) {
            return new GroupLimitCache(groupLimits, maxCacheSize);
        } else {
            return EMPTY_INSTANCE;
        }
    }

    private static final GroupLimitCache EMPTY_INSTANCE = new GroupLimitCache();

    public static GroupLimitCache emptyInstance() {
        return EMPTY_INSTANCE;
    }

    private GroupLimitCache(SortedSet<QueryLogicGroupLimit> groupsToLimits, long maxCacheSize) {
        this.cache = Caffeine.newBuilder().maximumSize(maxCacheSize).build();
        this.groupLimits = Collections.unmodifiableSortedSet(new TreeSet<>(groupsToLimits));
    }

    private GroupLimitCache() {
        this.cache = null;
        this.groupLimits = null;
    }

    /**
     * Return a map of groups that match against the query logic to their limits. The map will be in order of lowest value to highest.
     *
     * @param queryLogic
     *            the query logic
     * @return the map
     */
    public Map<String,Integer> getBestGroupLimits(String queryLogic) {
        if (!isEmpty()) {
            Map<String,Integer> limits = this.cache.getIfPresent(queryLogic);
            if (limits == null) {
                // Maintain insertion order. This will ensure a sorted order of lowest limit to highest when iterating over the entries.
                limits = new LinkedHashMap<>();
                for (QueryLogicGroupLimit limit : this.groupLimits) {
                    Matcher matcher = limit.getMatcher();
                    if (!limits.isEmpty() && matcher.getType() == Matcher.Type.ALL) {
                        break;
                    }

                    if (matcher.matches(queryLogic)) {
                        limits.put(limit.getGroupName(), limit.getQueryLimit());
                        if (matcher.getType() == Matcher.Type.EXACT || matcher.getType() == Matcher.Type.ALL) {
                            break;
                        }
                    }
                }
                limits = Collections.unmodifiableMap(limits);
                this.cache.put(queryLogic, limits);
            }
            return limits;
        } else {
            return Map.of();
        }
    }

    /**
     * Return whether this {@link GroupLimitCache} contains any overrides.
     *
     * @return true if this {@link GroupLimitCache} does not override any limits, or false otherwise
     */
    public boolean isEmpty() {
        return this.groupLimits == null;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupLimitCache that = (GroupLimitCache) o;
        return Objects.equals(groupLimits, that.groupLimits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupLimits);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", GroupLimitCache.class.getSimpleName() + "[", "]").add("groupLimits=" + groupLimits).toString();
    }
}
