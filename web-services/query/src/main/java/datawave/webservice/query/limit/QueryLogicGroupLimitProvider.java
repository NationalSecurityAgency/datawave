package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * This class is responsible for identifying and providing limits that should be enforced for query logic groups.
 */
public class QueryLogicGroupLimitProvider {

    private static final Logger log = Logger.getLogger(QueryLogicGroupLimitProvider.class);

    private final long maxCacheSize;
    private Map<String,QueryLogicGroupLimit> groupsToLimits = Map.of();
    private GroupLimitCache groupLimitCache;

    private final ReadWriteLock groupCacheLock = new ReentrantReadWriteLock();
    private final Map<String,Set<String>> groupsToQueryLogics = new HashMap<>();
    private final QueryLogicsUpdateListener queryLogicsUpdateListener;

    public QueryLogicGroupLimitProvider(long maxCacheSize, Collection<QueryLogicGroupLimitConfiguration> configs) {
        this.maxCacheSize = maxCacheSize;
        if (configs != null && !configs.isEmpty()) {
            validateConfigs(configs);
            populateLimits(configs);
            this.queryLogicsUpdateListener = createQueryLogicsUpdateListener();
        } else {
            this.groupLimitCache = GroupLimitCache.emptyInstance();
            this.groupsToLimits = Map.of();
            this.queryLogicsUpdateListener = null;
        }
    }

    /**
     * Create and return a {@link QueryLogicsUpdateListener} that will update this provider with any new/removed query logics. These changes will be reflected
     * when we determine which query logic groups match a given query logic.
     *
     * @return the listener
     */
    private QueryLogicsUpdateListener createQueryLogicsUpdateListener() {
        return new QueryLogicsUpdateListener() {

            @Override
            public void forCreate(String queryLogic) {
                groupCacheLock.writeLock().lock();
                try {
                    for (Map.Entry<String,QueryLogicGroupLimit> entry : groupsToLimits.entrySet()) {
                        String group = entry.getKey();
                        QueryLogicGroupLimit limit = entry.getValue();
                        if (limit.getMatcher().matches(queryLogic)) {
                            Set<String> queryLogics = groupsToQueryLogics.computeIfAbsent(group, k -> new HashSet<>());
                            queryLogics.add(queryLogic);
                        }
                    }
                } catch (Exception e) {
                    log.error("Failed to add query logic " + queryLogic);
                    throw new RuntimeException("Failed to add query logic " + queryLogic, e);
                } finally {
                    groupCacheLock.writeLock().unlock();
                }
            }

            @Override
            public void forDelete(String queryLogic) {
                groupCacheLock.writeLock().lock();
                try {
                    for (Set<String> queryLogics : groupsToQueryLogics.values()) {
                        queryLogics.remove(queryLogic);
                    }
                } catch (Exception e) {
                    log.error("Failed to remove query logic " + queryLogic);
                    throw new RuntimeException("Failed to remove query logic " + queryLogic, e);
                } finally {
                    groupCacheLock.writeLock().unlock();
                }
            }
        };
    }

    /**
     * Validate the given configurations.
     *
     * @param configs
     *            the configurations to validate
     */
    private void validateConfigs(Collection<QueryLogicGroupLimitConfiguration> configs) {
        Set<String> groupNames = new HashSet<>();
        for (QueryLogicGroupLimitConfiguration config : configs) {

            // Verify that a group name was given.
            String groupName = config.getGroupName();
            if (StringUtils.isBlank(groupName)) {
                throw new IllegalArgumentException("Query logic group limit configuration given with blank group name");
            }

            // Verify that we have not seen a configuration with the group name before.
            if (groupNames.contains(groupName)) {
                throw new IllegalArgumentException("Multiple query logic group configurations given with group name '" + groupName + "'");
            } else {
                groupNames.add(groupName);
            }

            // Verify that the query limit is not negative.
            if (config.getQueryLimit() < 0) {
                throw new IllegalArgumentException("Negative limit given for query logic group '" + groupName + "'");
            }

            // Verify that a query logic pattern was given.
            String queryLogicPattern = config.getQueryLogicPattern();
            if (StringUtils.isBlank(queryLogicPattern)) {
                throw new IllegalArgumentException("Blank query logic pattern given for query logic group '" + groupName + "'");
            }

            // Verify that the pattern compiles if it is not simply a * as is occasionally used as a wildcard in configurations.
            try {
                if (!queryLogicPattern.equals(QueryLimiterUtils.SIMPLE_WILDCARD)) {
                    Pattern.compile(queryLogicPattern);
                }
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException("Invalid regex in query logic pattern '" + queryLogicPattern + "' for query logic group '" + groupName + "'",
                                e);
            }
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

    /**
     * Return a {@link LinkedHashMap} containing the mappings of the given map, sorted by value.
     *
     * @param map
     *            the map to sort
     * @return the sorted map
     */
    private Map<String,QueryLogicGroupLimit> getMapSortedByValue(Map<String,QueryLogicGroupLimit> map) {
        // @formatter:off
        return map.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        // @formatter:on
    }

    /**
     * Return a map of groups to limits where each group contains the given query logic, and the limit reflects the lowest limit configured for the group.
     *
     * @param queryLogic
     *            the query logic
     * @return the map of groups to limits
     */
    public Map<String,Integer> getBestGroupLimits(String queryLogic) {
        return groupLimitCache.getBestGroupLimits(queryLogic);
    }

    public Map<String,Matcher> getGroupMatchers(Set<String> groups) {
        Map<String,Matcher> map = new HashMap<>();
        for (String group : groups) {
            QueryLogicGroupLimit queryLogicGroupLimit = groupsToLimits.get(group);
            if (queryLogicGroupLimit != null) {
                map.put(group, queryLogicGroupLimit.getMatcher());
            }
        }
        return map;
    }

    public Collection<String> getQueryLogicsForGroup(String group) {
        this.groupCacheLock.readLock().lock();
        try {
            return Set.copyOf(groupsToQueryLogics.getOrDefault(group, Set.of()));
        } finally {
            this.groupCacheLock.readLock().unlock();
        }
    }

    /**
     * Update the initial set of query logics for this {@link QueryLogicGroupLimitProvider}, and register a listener for any query logic additions/removals.
     *
     * @param cache
     *            the cache
     */
    public void initializeFrom(QueryLogicCache cache) {
        if (cache != null && this.queryLogicsUpdateListener != null) {
            cache.addListener(this.queryLogicsUpdateListener);
            updateQueryLogics(cache.getQueryLogics());
        }
    }

    /**
     * Stop listening for updates from the given {@link QueryLogicCache}.
     *
     * @param cache
     *            the cache
     */
    public void stopListeningTo(QueryLogicCache cache) {
        if (cache != null && this.queryLogicsUpdateListener != null) {
            cache.removeListener(this.queryLogicsUpdateListener);
        }
    }

    /**
     * Update the set of query logics used to pre-identify matches between groups and query logics that match against the groups' patterns.
     *
     * @param queryLogics
     *            the query logics
     */
    private void updateQueryLogics(Set<String> queryLogics) {
        this.groupCacheLock.writeLock().lock();
        try {
            this.groupsToQueryLogics.clear();
            for (Map.Entry<String,QueryLogicGroupLimit> entry : this.groupsToLimits.entrySet()) {
                String group = entry.getKey();
                QueryLogicGroupLimit limit = entry.getValue();
                Set<String> matching = new HashSet<>(limit.getMatcher().getMatches(queryLogics));
                this.groupsToQueryLogics.put(group, matching);
            }
        } catch (Exception e) {
            log.error("Failed to update query logics");
            throw new RuntimeException("Failed to update query logics", e);
        } finally {
            this.groupCacheLock.writeLock().unlock();
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

            // Finally, compare by equality of the matcher to avoid collapsing entries with different matchers.
            if (comparison == 0) {
                return matcher.equals(o.matcher) ? 0 : 1;
            }

            return comparison;
        }
    }
}
