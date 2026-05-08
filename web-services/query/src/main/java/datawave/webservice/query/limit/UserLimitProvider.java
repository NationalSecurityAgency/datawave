package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

import org.apache.log4j.Logger;

/**
 * This class is responsible for identifying and providing limits that should be enforced for users.
 */
public class UserLimitProvider {

    private static final Logger log = Logger.getLogger(UserLimitProvider.class);

    private final int defaultUserQueryLimit;

    private final long maxCacheSize;

    private Map<String,UserLimits> customLimits = new HashMap<>();

    UserLimitProvider(int defaultUserQueryLimit, long maxCacheSize, Collection<UserLimitConfiguration> configs,
                    QueryLogicGroupLimitProvider groupLimitProvider) {
        this.defaultUserQueryLimit = defaultUserQueryLimit;
        this.maxCacheSize = maxCacheSize;
        if (configs != null && !configs.isEmpty()) {
            populateLimits(configs, groupLimitProvider);
        } else {
            this.customLimits = Map.of();
        }
    }

    /**
     * Populate the limits to enforce for users.
     *
     * @param configs
     *            the configs to populate the limits from
     */
    private void populateLimits(Collection<UserLimitConfiguration> configs, QueryLogicGroupLimitProvider groupLimitProvider) {
        Map<String,UserLimits> configuredLimits = new HashMap<>();
        for (UserLimitConfiguration config : configs) {
            Integer customQueryLimit = config.getQueryLimit();
            Map<String,Integer> customGroupLimits = config.getQueryLogicGroupLimits();

            String userDn = config.getUserDn().trim().toLowerCase();

            if (customQueryLimit == null && (customGroupLimits == null || customGroupLimits.isEmpty())) {
                if (log.isDebugEnabled()) {
                    log.debug("Custom limits provided for user '" + userDn + "' do not override any defaults, skipping.");
                }
                continue;
            }

            // If the custom query limit is null or less than zero, use the default query limit.
            if (customQueryLimit == null || customQueryLimit < 0) {
                if (log.isDebugEnabled()) {
                    log.trace("Using default user query limit of " + defaultUserQueryLimit + " for user '" + userDn + "'");
                }
                customQueryLimit = defaultUserQueryLimit;
            }

            // If any custom group limits were given, construct the map of group limits to use.
            SortedSet<QueryLogicGroupLimit> groupLimitOverrides = null;
            if (customGroupLimits != null && !customGroupLimits.isEmpty()) {
                groupLimitOverrides = groupLimitProvider.createOverrides(config.getQueryLogicGroupLimits(), true);
            }

            configuredLimits.put(userDn, new UserLimits(userDn, customQueryLimit, groupLimitOverrides, maxCacheSize));
        }
        this.customLimits = Map.copyOf(configuredLimits);
    }

    public int getDefaultUserQueryLimit() {
        return defaultUserQueryLimit;
    }

    public boolean hasCustomLimits(String userDn) {
        return customLimits.containsKey(userDn);
    }

    public UserLimits getCustomLimits(String userDn) {
        return customLimits.get(userDn);
    }

    /**
     * Clean up this {@link UserLimitProvider} and release its underlying resources.
     */
    public void cleanUp() {
        customLimits = null;
    }
}
