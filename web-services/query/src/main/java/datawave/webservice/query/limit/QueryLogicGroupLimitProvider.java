package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

/**
 * This class is responsible for identifying and providing limits that should be enforced for query logic groups.
 */
public class QueryLogicGroupLimitProvider {

    private static final Logger log = Logger.getLogger(QueryLogicGroupLimitProvider.class);

    private final long maxCacheSize;

    private Map<String,QueryLogicGroupLimit> groupsToLimits = Map.of();

    private GroupLimitCache groupLimitCache;

    public QueryLogicGroupLimitProvider(long maxCacheSize, Collection<QueryLogicGroupLimitConfiguration> configs) {
        this.maxCacheSize = maxCacheSize;
        if (configs != null && !configs.isEmpty()) {
            populateLimits(configs);
        } else {
            this.groupLimitCache = GroupLimitCache.emptyInstance();
            this.groupsToLimits = Map.of();
        }
    }

    /**
     * Populate the limits to enforce for query logic groups.
     *
     * @param configs
     *            the configs to populate the limits from
     */
    private void populateLimits(Collection<QueryLogicGroupLimitConfiguration> configs) {
        Map<String,QueryLogicGroupLimit> groupLimits = new HashMap<>();
        // Create a matchable limit for each configuration.
        for (QueryLogicGroupLimitConfiguration config : configs) {

            // Identify the best matching strategy to use when matching query logics.
            String queryLogicPattern = config.getQueryLogicPattern();
            Matcher matcher = Matcher.getMatcher(queryLogicPattern, maxCacheSize);

            // Add a new matchable limit. The sorted set will be sorted in the following priority:
            // 1. First by matching type: EXACT, then PARTIAL, then ALL
            // 2. Then by query limit, from lowest to highest.
            // 3. Then by group name.
            groupLimits.put(config.getGroupName(), new QueryLogicGroupLimit(config.getGroupName(), matcher, config.getQueryLimit()));
        }

        SortedSet<QueryLogicGroupLimit> limits = new TreeSet<>(groupLimits.values());
        this.groupLimitCache = GroupLimitCache.of(limits, maxCacheSize);
        // @formatter:off
        this.groupsToLimits = Collections.unmodifiableMap(getMapSortedByValue(groupLimits));
        // @formatter:on
    }

    /**
     * Return a set of {@link QueryLogicGroupLimit} that represent a set of overridden group limits.
     *
     * @param groupOverrides
     *            a map of groups to their overridden limits
     * @param includeNonOverriddenGroups
     *            if true, include any default group limits that were not overridden in the returned set
     * @return the constructed overrides
     */
    public SortedSet<QueryLogicGroupLimit> createOverrides(Map<String,Integer> groupOverrides, boolean includeNonOverriddenGroups) {
        SortedSet<MatchableOverride> matchableOverrides = new TreeSet<>();
        groupOverrides.forEach((key, value) -> matchableOverrides.add(new MatchableOverride(Matcher.getMatcher(key, maxCacheSize), value)));

        SortedSet<QueryLogicGroupLimit> overrides = new TreeSet<>();
        for (String group : groupsToLimits.keySet()) {
            boolean overrideFound = false;
            for (MatchableOverride override : matchableOverrides) {
                if (override.matcher.matches(group)) {
                    overrides.add(new QueryLogicGroupLimit(group, groupsToLimits.get(group).getMatcher(), override.limit));
                    overrideFound = true;
                    break;
                }
            }
            if (!overrideFound && includeNonOverriddenGroups) {
                overrides.add(groupsToLimits.get(group));
            }
        }

        return overrides;
    }

    private Map<String,QueryLogicGroupLimit> getMapSortedByValue(Map<String,QueryLogicGroupLimit> map) {
        // @formatter:off
        return map.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        // @formatter:on
    }

    public Map<String,Integer> getBestGroupLimits(String queryLogic) {
        return groupLimitCache.getBestGroupLimits(queryLogic);
    }

    public Map<String,Matcher> getGroupMatchers(Set<String> groups) {
        Map<String,Matcher> map = new HashMap<>();
        groups.forEach(group -> map.put(group, groupsToLimits.get(group).getMatcher()));
        return map;
    }

    /**
     * Clean up this {@link QueryLogicGroupLimitProvider} and release its underlying resources.
     */
    public void cleanUp() {
        groupsToLimits = null;
        if (groupLimitCache != null) {
            try {
                groupLimitCache.cleanUp();
            } catch (Exception e) {
                log.warn("Failed to clear groupLimitCache", e);
            } finally {
                groupLimitCache = null;
            }
        }
    }

    /**
     * This class represents a sortable matchable group limit override.
     */
    private static class MatchableOverride implements Comparable<MatchableOverride> {
        private final Matcher matcher;
        private final int limit;

        public MatchableOverride(Matcher matcher, int limit) {
            this.matcher = matcher;
            this.limit = limit;
        }

        @Override
        public int compareTo(MatchableOverride o) {
            // First sort by the matcher type, sorting in order EXACT, PARTIAL, then ALL.
            int comparison = matcher.getType().compareTo(o.matcher.getType());

            // Then sort by the query limit from lowest to highest.
            if (comparison == 0) {
                comparison = Integer.compare(limit, o.limit);
            }

            return comparison;
        }
    }
}
