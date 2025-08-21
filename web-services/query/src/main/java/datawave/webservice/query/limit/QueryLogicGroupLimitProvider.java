package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * This class is responsible for extracting the limits from {@link QueryLogicGroupLimitConfiguration} instances and then identifying the query limits to enforce
 * for individual query logics, overridden or otherwise.
 */
public class QueryLogicGroupLimitProvider {

    private static final Logger log = Logger.getLogger(QueryLogicGroupLimitProvider.class);

    // Cache of query logic keys to either their best configured limits or empty optionals when they have no matching limit.
    private final Cache<String,Optional<QueryLogicGroupQueryLimit>> limitCache = Caffeine.newBuilder().build();

    // Cache of (userDn|systemName) + query logic keys to their best overriding limit, or empty optionals when they have no match.
    private final Cache<String,Optional<QueryLogicGroupQueryLimit>> overiddenLimitCache = Caffeine.newBuilder().build();

    // Map of group names to their corresponding query logic matcher.
    private Map<String,Matcher> groupMatchers;
    // The set of query limits in sorted order of best match to worst.
    private SortedSet<GroupMatchableLimit> configuredLimits;

    QueryLogicGroupLimitProvider(Collection<QueryLogicGroupLimitConfiguration> configs) {
        if (configs != null && !configs.isEmpty()) {
            validateConfigs(configs);
            populateLimits(configs);
        } else {
            configuredLimits = Collections.emptySortedSet();
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
        Map<String,Matcher> groupMatchers = new HashMap<>();
        SortedSet<GroupMatchableLimit> groupLimits = new TreeSet<>();
        // Create a matchable limit for each configuration.
        for (QueryLogicGroupLimitConfiguration config : configs) {

            // Identify the best matching strategy to use when matching query logics.
            String queryLogicPattern = config.getQueryLogicPattern();
            Matcher matcher = Matcher.getMatcher(queryLogicPattern);

            // Add a new matchable limit. The sorted set will be sorted in the following priority:
            // 1. First by matching type: EXACT, then PARTIAL, then ALL
            // 2. Then by query limit, from lowest to highest.
            // 3. Then by group name.
            groupLimits.add(new GroupMatchableLimit(config.getGroupName(), matcher, config.getQueryLimit()));
            groupMatchers.put(config.getGroupName(), matcher);
        }

        this.configuredLimits = Collections.unmodifiableSortedSet(groupLimits);
        this.groupMatchers = Map.copyOf(groupMatchers);
    }

    /**
     * Return the best matching {@link QueryLogicGroupQueryLimit} to use when enforcing limits for the given query logic. If no match was found, an empty
     * {@link Optional} will be returned
     *
     * @param queryLogic
     *            the query logic
     * @return the {@link QueryLogicGroupQueryLimit} if found
     */
    public Optional<QueryLogicGroupQueryLimit> getLimit(String queryLogic) {
        // Fetch the best match from the cache, if present.
        Optional<QueryLogicGroupQueryLimit> limit = limitCache.getIfPresent(queryLogic);
        // Otherwise, identify the best match and update the cache.
        if (limit == null) {
            // Look for a query logic group limit originating from a configuration that matches the query logic. The first matching limit will be the best
            // match due to how the limits are sorted, which is:
            // 1. First by matching type: EXACT, then PARTIAL, then ALL
            // 2. Then by query limit, from lowest to highest.
            // 3. Then by group name.
            for (GroupMatchableLimit matchableLimit : configuredLimits) {
                if (matchableLimit.matcher.matches(queryLogic)) {
                    limit = Optional.of(QueryLogicGroupQueryLimit.fromConfig(matchableLimit.groupName, matchableLimit.queryLimit));
                    break;
                }
            }

            // If no matching limit was found, then cache an empty optional.
            if (limit == null) {
                limit = Optional.empty();
            }

            // Cache it.
            if (log.isTraceEnabled()) {
                log.trace("Caching query logic group limit for '" + queryLogic + "': " + limit);
            }
            limitCache.put(queryLogic, limit);
        }
        return limit;
    }

    /**
     * Return the best matching overridden limit to use when enforcing limits for the given query logic. If no match was for the query logic against any of the
     * groups present in the map of overridden limits, an empty optional will be returned.
     *
     * @param sourceId
     *            either a user dn or a system name
     * @param queryLogic
     *            the query logic
     * @param overriddenLimits
     *            a map of query logic group names to their corresponding overriding limit
     * @return the {@link QueryLogicGroupQueryLimit} if found
     */
    public Optional<QueryLogicGroupQueryLimit> getOverriddenLimit(String sourceId, String queryLogic, SortedSet<MatchableLimit> overriddenLimits) {
        // The cache key consists of the source id and the query logic.
        String key = sourceId + queryLogic;
        Optional<QueryLogicGroupQueryLimit> optional = overiddenLimitCache.getIfPresent(key);
        // If we do not already have the best match cached, attempt to find it.
        if (optional == null) {
            // Obtain the names of all query logic groups that the query logic falls within.
            // @formatter:off
            Set<String> groupNames = groupMatchers.entrySet().stream()
                            .filter(entry -> entry.getValue().matches(queryLogic))
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toSet());
            // @formatter:on

            // If the query logic does not fall into any groups, cache and return an empty optional.
            if (groupNames.isEmpty()) {
                optional = Optional.empty();
            } else {
                // Otherwise, attempt to find the best overridden limit for a matching group name, if present. The first match will be the best match
                // (most direct match to a group name, and lowest limit).
                MatchableLimit overriddenLimit = null;
                for (MatchableLimit limit : overriddenLimits) {
                    if (limit.getMatcher().matchesAnyOf(groupNames)) {
                        overriddenLimit = limit;
                        break;
                    }
                }

                if (overriddenLimit != null) {
                    QueryLogicGroupQueryLimit limit = QueryLogicGroupQueryLimit.fromConfig(overriddenLimit.getPattern(), overriddenLimit.getQueryLimit());
                    optional = Optional.of(limit);
                } else {
                    optional = Optional.empty();
                }
            }

            // Cache it.
            if (log.isTraceEnabled()) {
                log.trace("Caching overridden query logic group limit for '" + key + "': " + optional);
            }
            overiddenLimitCache.put(key, optional);
        }
        return optional;
    }

    /**
     * This class represents a sortable query logic group and its limit.
     */
    private static class GroupMatchableLimit implements Comparable<GroupMatchableLimit> {

        private final String groupName;
        private final Matcher matcher;
        private final int queryLimit;

        public GroupMatchableLimit(String groupName, Matcher matcher, int queryLimit) {
            this.groupName = groupName;
            this.matcher = matcher;
            this.queryLimit = queryLimit;
        }

        @Override
        public int compareTo(GroupMatchableLimit o) {
            // First sort by the matcher type, sorting in order EXACT, PARTIAL, then ALL.
            int comparison = matcher.getType().compareTo(o.matcher.getType());

            // Then sort by the query limit from lowest to highest.
            if (comparison == 0) {
                comparison = Integer.compare(queryLimit, o.queryLimit);
            }

            // Then sort by the group name. In practice, this should always be unique.
            if (comparison == 0) {
                comparison = groupName.compareTo(o.groupName);
            }
            return comparison;
        }
    }
}
