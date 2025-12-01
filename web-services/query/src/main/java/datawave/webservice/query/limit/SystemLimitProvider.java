package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * This class is responsible for extracting the limits from {@link SystemLimitConfiguration} instances and then identifying the query limits to enforce for
 * individual systems, overridden or otherwise.
 */
public class SystemLimitProvider {

    private static final Logger log = Logger.getLogger(SystemLimitProvider.class);

    private final int defaultSystemQueryLimit;

    // Cache of systems to their best limit.
    private final Cache<String,SystemQueryLimit> limitCache = Caffeine.newBuilder().build();
    // Cache of systems to whether queries submitted on them should count against user query limits.
    private final Cache<String,Boolean> countsAgainstUserLimitCache = Caffeine.newBuilder().build();
    // The set of query limits in sorted order of best match to worst.
    private SortedSet<SystemMatchableLimit> configuredLimits;

    SystemLimitProvider(int defaultSystemQueryLimit, Collection<SystemLimitConfiguration> configs) {
        this.defaultSystemQueryLimit = defaultSystemQueryLimit;
        if (configs != null && !configs.isEmpty()) {
            validateConfigs(configs);
            populateLimits(configs);
        } else {
            this.configuredLimits = Collections.emptySortedSet();
        }
    }

    /**
     * Validate the given configurations.
     *
     * @param configs
     *            the configurations to validate
     */
    private void validateConfigs(Collection<SystemLimitConfiguration> configs) {
        Set<String> systemPatterns = new HashSet<>();
        Map<String,String> matcherPatterns = new HashMap<>();
        for (SystemLimitConfiguration config : configs) {
            // Verify that a system pattern was given.
            String systemPattern = config.getSystemPattern();
            if (StringUtils.isBlank(systemPattern)) {
                throw new IllegalArgumentException("System query limit configuration specified with blank system pattern");
            }

            // Verify that the pattern compiles if it is not simply a * as is occasionally used as a wildcard in configurations.
            try {
                if (!systemPattern.equals(QueryLimitConstants.ASTERISK)) {
                    Pattern.compile(systemPattern);
                }
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException("Invalid regex in system pattern '" + systemPattern + "'", e);
            }

            // Verify that we have not seen a configuration with the system pattern before.
            if (systemPatterns.contains(systemPattern)) {
                throw new IllegalArgumentException("Multiple query limit configurations specified with system pattern '" + systemPattern + "'");
            } else {
                systemPatterns.add(systemPattern);
            }

            // Fetch the matcher that would be used for the system pattern.
            Matcher matcher = Matcher.getMatcher(systemPattern);

            // Verify that we do not have an exact-matching pattern that is equivalent to a previously seen exact-matching pattern, such as 'SYSTEM-01' vs.
            // 'SYSTEM\\-01'.
            if (matcher instanceof StringMatcher) {
                String matcherPattern = ((StringMatcher) matcher).getValue();
                String equivalentSystemPattern = matcherPatterns.get(matcherPattern);
                if (equivalentSystemPattern != null) {
                    throw new IllegalArgumentException(
                                    "System pattern '" + systemPattern + "' will resolve to an exact match that is equivalent to system pattern '"
                                                    + equivalentSystemPattern + "' from another system configuration.");
                } else {
                    matcherPatterns.put(matcherPattern, systemPattern);
                }
            }

            // Verify that we do not have a negative query limit.
            if (config.getQueryLimit() != null && config.getQueryLimit() < 0) {
                throw new IllegalArgumentException("Negative query limit specified for system pattern '" + systemPattern + "'");
            }

            // Safeguard against allowing a configuration to potentially set whether queries on a system counts against user limits to false for all
            // systems. Only allow this to be done for exact system names, or non-wildcard-only patterns.
            if (QueryLimitConstants.wildcardOnlyPattern.matcher(systemPattern).matches() && !config.getCountsAgainstsUserLimit()) {
                throw new IllegalArgumentException("System pattern '" + systemPattern
                                + "' is wildcard-only and may not be used to override whether queries count against user limits to false");
            }

            // Verify that no invalid group name patterns were provided.
            Map<String,Integer> groupLimits = config.getQueryLogicGroupLimits();
            if (groupLimits != null) {
                for (Map.Entry<String,Integer> entry : groupLimits.entrySet()) {
                    String groupPattern = entry.getKey();
                    if (StringUtils.isBlank(groupPattern)) {
                        throw new IllegalArgumentException(
                                        "User group query limit configuration given with blank group pattern for system pattern '" + systemPattern + "'");
                    }
                    if (!groupPattern.equals(QueryLimitConstants.ASTERISK)) {
                        try {
                            Pattern.compile(groupPattern);
                        } catch (PatternSyntaxException e) {
                            throw new IllegalArgumentException(
                                            "Invalid query logic group name pattern: " + groupPattern + " given for system pattern " + systemPattern, e);
                        }
                    }
                    Integer limit = entry.getValue();
                    if (limit < 0) {
                        throw new IllegalArgumentException("Negative query logic group limit given for system pattern '" + systemPattern + "': " + limit);
                    }
                }
            }
        }
    }

    /**
     * Populate the limits to enforce for systems.
     *
     * @param configs
     *            the configs to populate the limits from
     */
    private void populateLimits(Collection<SystemLimitConfiguration> configs) {
        SortedSet<SystemMatchableLimit> configuredLimits = new TreeSet<>();
        for (SystemLimitConfiguration systemConfig : configs) {

            // Identify the best matching strategy to use for matching system names.
            String systemPattern = systemConfig.getSystemPattern();
            Matcher matcher = Matcher.getMatcher(systemPattern);

            // If the query limit given for the system was null or less than zero, use the default system query limit.
            Integer queryLimit = systemConfig.getQueryLimit();
            if (queryLimit == null || queryLimit < 0) {
                queryLimit = defaultSystemQueryLimit;
            }

            // If countsAgainstUserLimit is null, use the default.
            Boolean countsAgainstUserLimit = systemConfig.getCountsAgainstsUserLimit();
            if (countsAgainstUserLimit == null) {
                countsAgainstUserLimit = true;
            }

            // Create a matchable set for the query logic group limits.
            SortedSet<MatchableLimit> groupLimits = new TreeSet<>();
            for (Map.Entry<String,Integer> entry : systemConfig.getQueryLogicGroupLimits().entrySet()) {
                groupLimits.add(new MatchableLimit(entry.getKey(), entry.getValue()));
            }

            configuredLimits.add(new SystemMatchableLimit(matcher, systemPattern, queryLimit, countsAgainstUserLimit, groupLimits));
        }
        this.configuredLimits = Collections.unmodifiableSortedSet(configuredLimits);
    }

    /**
     * Return the best matching {@link SystemQueryLimit} to use for the given system. If no match was found to a configured limit, then default limits will be
     * returned.
     *
     * @param system
     *            the system name
     * @return the {@link SystemQueryLimit}
     */
    public SystemQueryLimit getLimit(String system) {
        // Check if we have the best match already cached.
        SystemQueryLimit limit = limitCache.getIfPresent(system);
        if (limit == null) {
            // If not, attempt to find one from configured limits. The first matching limit will be the best one due to how the limits are sorted, which is:
            // 1. First by matching type: EXACT, then PARTIAL, then ALL
            // 2. Then by query limit, from lowest to highest.
            // 3. Then by whether queries should apply to the user query limit, from true to false.
            // 4. Then by system pattern.
            for (SystemMatchableLimit matchableLimit : configuredLimits) {
                if (matchableLimit.matcher.matches(system)) {
                    limit = SystemQueryLimit.fromConfig(matchableLimit.systemPattern, matchableLimit.queryLimit, matchableLimit.countsAgainstUserLimit,
                                    matchableLimit.queryLogicGroupLimits);
                    break;
                }
            }

            // If a matching configured limit was not found, construct one from the default limits.
            if (limit == null) {
                limit = SystemQueryLimit.fromDefaults(system, defaultSystemQueryLimit);
            }

            // Cache it.
            if (log.isTraceEnabled()) {
                log.trace("Caching system query limit for '" + system + "': " + limit);
            }
            limitCache.put(system, limit);
        }
        return limit;
    }

    /**
     * Return whether queries count against the user query limit when submitted on the given system.
     *
     * @param system
     *            the system name
     * @return true if queries count against the user query limit, or false otherwise
     */
    public boolean countsAgainstUserLimit(String system) {
        // Check if we already have an evaluation for this cached.
        Boolean countsAgainstUserLimit = countsAgainstUserLimitCache.getIfPresent(system);
        if (countsAgainstUserLimit == null) {
            // If not, attempt to determine whether query limits apply based on the configured limits.
            for (SystemMatchableLimit matchableLimit : configuredLimits) {
                // Wildcard system patterns are not allowed to override countsAgainstUserLimit to false. If we encounter match type ALL, all remaining
                // limits have wildcard system patterns and can be skipped.
                if (matchableLimit.matcher.getType() == Matcher.Type.ALL) {
                    break;
                }

                // If we found a match, update the counts against user limit. If countsAgainstUserLimit is true or this is an exact match, we do not need
                // to evaluate any other matches.
                if (matchableLimit.matcher.matches(system)) {
                    countsAgainstUserLimit = matchableLimit.countsAgainstUserLimit;
                    if (countsAgainstUserLimit || matchableLimit.matcher.getType() == Matcher.Type.EXACT) {
                        break;
                    }
                }
            }

            // If we did not find any configured limit for the system, use the default value.
            if (countsAgainstUserLimit == null) {
                countsAgainstUserLimit = true;
            }

            // Cache it.
            if (log.isTraceEnabled()) {
                log.trace("Caching countsAgainstUserLimitCache for '" + system + "': " + countsAgainstUserLimitCache);
            }
            countsAgainstUserLimitCache.put(system, countsAgainstUserLimit);
        }
        return countsAgainstUserLimit;
    }

    /**
     * This class represents a sortable system pattern and its limit configuration.
     */
    private class SystemMatchableLimit implements Comparable<SystemMatchableLimit> {
        private final Matcher matcher;
        private final String systemPattern;
        private final int queryLimit;
        private final boolean countsAgainstUserLimit;
        private final SortedSet<MatchableLimit> queryLogicGroupLimits;

        public SystemMatchableLimit(Matcher matcher, String systemPattern, int queryLimit, boolean countsAgainstUserLimit,
                        SortedSet<MatchableLimit> queryLogicGroupLimits) {
            this.matcher = matcher;
            this.systemPattern = systemPattern;
            this.queryLimit = queryLimit;
            this.countsAgainstUserLimit = countsAgainstUserLimit;
            this.queryLogicGroupLimits = queryLogicGroupLimits == null ? Collections.emptySortedSet()
                            : Collections.unmodifiableSortedSet(queryLogicGroupLimits);
        }

        @Override
        public int compareTo(SystemMatchableLimit o) {
            // First sort by the type, sorting in order EXACT, PARTIAL, then ALL.
            int comparison = matcher.getType().compareTo(o.matcher.getType());

            // Then sort by the query limit from lowest to highest.
            if (comparison == 0) {
                comparison = Integer.compare(queryLimit, o.queryLimit);
            }

            // Then sort by whether queries count against the user limit, true to false.
            if (comparison == 0) {
                comparison = Boolean.compare(o.countsAgainstUserLimit, countsAgainstUserLimit);
            }

            // Then sort by the system pattern. In practice, this should always be unique.
            if (comparison == 0) {
                comparison = systemPattern.compareTo(o.systemPattern);
            }

            return comparison;
        }
    }
}
