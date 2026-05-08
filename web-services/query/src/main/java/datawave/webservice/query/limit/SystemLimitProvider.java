package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * This class is responsible for identifying and providing limits that should be enforced for systems.
 */
public class SystemLimitProvider {

    private static final Logger log = Logger.getLogger(SystemLimitProvider.class);

    private final int defaultSystemQueryLimit;

    private final long maxCacheSize;

    private Cache<String,Optional<SystemLimits>> systemLimitCache;

    private SortedSet<SortableSystemLimit> sortedSystemLimits;

    SystemLimitProvider(int defaultSystemQueryLimit, long maxCacheSize, Collection<SystemLimitConfiguration> configs,
                    QueryLogicGroupLimitProvider groupLimitProvider) {
        // If the default system query limit is less than zero, set it to the no-limit value.
        if (defaultSystemQueryLimit < 0) {
            this.defaultSystemQueryLimit = QueryLimitConstants.NO_LIMIT;
        } else {
            this.defaultSystemQueryLimit = defaultSystemQueryLimit;
        }

        this.maxCacheSize = maxCacheSize;
        if (configs != null && !configs.isEmpty()) {
            populateLimits(configs, groupLimitProvider);
            this.systemLimitCache = Caffeine.newBuilder().maximumSize(100).build();
        } else {
            this.sortedSystemLimits = Collections.emptySortedSet();
            this.systemLimitCache = null;
        }
    }

    /**
     * Populate the limits to enforce for systems.
     *
     * @param configs
     *            the configs to populate the limits from
     */
    private void populateLimits(Collection<SystemLimitConfiguration> configs, QueryLogicGroupLimitProvider groupLimitProvider) {
        SortedSet<SortableSystemLimit> systemLimits = new TreeSet<>();
        for (SystemLimitConfiguration config : configs) {

            // Identify the best matching strategy to use for matching system names.
            String systemPattern = config.getSystemPattern();
            Matcher matcher = Matcher.getMatcher(systemPattern, maxCacheSize);

            // If the query limit given for the system was null or less than zero, use the default system query limit.
            Integer customQueryLimit = config.getQueryLimit();
            Map<String,Integer> customGroupLimits = config.getQueryLogicGroupLimits();
            Boolean customCountsAgainstUserLimit = config.getCountsAgainstUserLimit();

            if (customQueryLimit == null && customGroupLimits == null && (customCountsAgainstUserLimit == null || customCountsAgainstUserLimit)) {
                if (log.isDebugEnabled()) {
                    log.debug("Custom limits provided for systems matching '" + systemPattern + "' do not override any defaults, skipping.");
                }
                continue;
            }

            // If the custom query limit is null, use the default system query limit.
            if (customQueryLimit == null) {
                customQueryLimit = defaultSystemQueryLimit;
            } else if (customQueryLimit < 0) {
                // Otherwise, if the custom query limit is less than 0, set it to the no-limit value.
                customQueryLimit = QueryLimitConstants.NO_LIMIT;
            }

            // If the custom counts against user limits is null, default to true.
            if (customCountsAgainstUserLimit == null) {
                customCountsAgainstUserLimit = Boolean.TRUE;
            }

            // If any custom group limits were given, construct the map of group limits to use.
            SortedSet<QueryLogicGroupLimit> groupLimitOverrides = null;
            if (customGroupLimits != null && !customGroupLimits.isEmpty()) {
                groupLimitOverrides = groupLimitProvider.createOverrides(config.getQueryLogicGroupLimits(), false);
            }

            systemLimits.add(new SortableSystemLimit(matcher, systemPattern, customQueryLimit, customCountsAgainstUserLimit, groupLimitOverrides));
        }
        this.sortedSystemLimits = Collections.unmodifiableSortedSet(systemLimits);
    }

    /**
     * Return the default maximum number of queries that may be running on any particular system.
     *
     * @return the default system query limit
     */
    public int getDefaultSystemQueryLimit() {
        return defaultSystemQueryLimit;
    }

    /**
     * Return the custom limits (if any found) to enforce for the given system.
     *
     * @param system
     *            the system
     * @return the optional system, possibly empty
     */
    public Optional<SystemLimits> getCustomLimits(String system) {
        if (systemLimitCache != null) {
            Optional<SystemLimits> optional = systemLimitCache.getIfPresent(system);
            if (optional == null) {
                // The candidates are sorted in best-match order. The first match we find is the one we should use for the system going forward.
                for (SortableSystemLimit candidate : sortedSystemLimits) {
                    if (candidate.matcher.matches(system)) {
                        SystemLimits systemLimits = new SystemLimits(candidate.systemPattern, candidate.queryLimit, candidate.countsAgainstUserLimit,
                                        candidate.groupLimitOverrides, maxCacheSize);
                        optional = Optional.of(systemLimits);
                        break;
                    }
                }
                // If no match was found, cache an empty optional.
                if (optional == null) {
                    optional = Optional.empty();
                }
                systemLimitCache.put(system, optional);
            }
            return optional;
        } else {
            return Optional.empty();
        }
    }

    /**
     * Return whether queries on the given system counts against the user limit.
     *
     * @param system
     *            the system
     * @return true if queries on the system count towards a user's query limit, or false otherwise
     */
    public boolean countsAgainstUserLimit(String system) {
        Optional<SystemLimits> customLimit = getCustomLimits(system);
        return customLimit.map(SystemLimits::countsAgainstUserLimit).orElse(true);
    }

    /**
     * Clean up this {@link SystemLimitProvider} and release its underlying resources.
     */
    public void cleanUp() {
        if (systemLimitCache != null) {
            try {
                systemLimitCache.invalidateAll();
            } catch (Exception e) {
                log.error("Failed to clear systemLimitCache", e);
            } finally {
                systemLimitCache = null;
            }
        }
        sortedSystemLimits = null;
    }

    /**
     * This class represents a sortable system pattern and its limit configuration.
     */
    private static class SortableSystemLimit implements Comparable<SortableSystemLimit> {

        private final Matcher matcher;
        private final String systemPattern;
        private final Integer queryLimit;
        private final Boolean countsAgainstUserLimit;
        private final SortedSet<QueryLogicGroupLimit> groupLimitOverrides;

        public SortableSystemLimit(Matcher matcher, String systemPattern, Integer queryLimit, Boolean countsAgainstUserLimit,
                        SortedSet<QueryLogicGroupLimit> groupLimitOverrides) {
            this.matcher = matcher;
            this.systemPattern = systemPattern;
            this.queryLimit = queryLimit;
            this.countsAgainstUserLimit = countsAgainstUserLimit;
            this.groupLimitOverrides = groupLimitOverrides;
        }

        @Override
        public int compareTo(SortableSystemLimit o) {
            // First sort by the type, sorting in order EXACT, PARTIAL, then ALL.
            int comparison = matcher.getType().compareTo(o.matcher.getType());

            // Then sort by the query limit from lowest to highest.
            if (comparison == 0) {
                comparison = Integer.compare(queryLimit, o.queryLimit);
            }

            // Finally, sort by whether there are any query logic limit overrides.
            if (comparison == 0) {
                comparison = Boolean.compare(groupLimitOverrides != null, o.groupLimitOverrides != null);
            }

            // Then sort by whether queries count against the user limit, true to false.
            if (comparison == 0) {
                comparison = Boolean.compare(o.countsAgainstUserLimit, countsAgainstUserLimit);
            }

            return comparison;
        }
    }
}
