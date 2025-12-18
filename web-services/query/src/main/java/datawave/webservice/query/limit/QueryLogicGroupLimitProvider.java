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

    public QueryLogicGroupLimitProvider(long maxCacheSize, Collection<QueryLogicGroupLimitConfiguration> configs) {
        this.maxCacheSize = maxCacheSize;
        if (configs != null && !configs.isEmpty()) {
            validateConfigs(configs);
            populateLimits(configs);
        } else {
            this.groupLimitCache = GroupLimitCache.emptyInstance();
            this.groupsToLimits = Map.of();
        }

        if (log.isDebugEnabled()) {
            log.debug("Initalized with groupsToLimits: " + this.groupsToLimits);
        }
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
                if (!queryLogicPattern.equals(QueryLimitConstants.ASTERISK)) {
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

    private Map<String,QueryLogicGroupLimit> getMapSortedByValue(Map<String,QueryLogicGroupLimit> map) {
        // @formatter:off
        return map.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        // @formatter:on
    }

    public Map<String,Integer> getGroupLimits(String queryLogic) {
        return groupLimitCache.getGroupLimits(queryLogic);
    }

    public Map<String,Matcher> getGroupMatchers(Set<String> groups) {
        Map<String,Matcher> map = new HashMap<>();
        groups.forEach(group -> map.put(group, groupsToLimits.get(group).getMatcher()));
        return map;
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
