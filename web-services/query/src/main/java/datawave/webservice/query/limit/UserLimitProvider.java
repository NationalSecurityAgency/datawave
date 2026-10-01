package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for identifying and providing limits that should be enforced for users.
 */
public class UserLimitProvider {

    private static final Logger log = LoggerFactory.getLogger(UserLimitProvider.class);

    private final int defaultUserQueryLimit;

    private final long maxCacheSize;

    private Map<String,UserLimits> customLimits = new HashMap<>();

    UserLimitProvider(int defaultUserQueryLimit, long maxCacheSize, Collection<UserLimitConfiguration> configs,
                    QueryLogicGroupLimitProvider groupLimitProvider) {
        this.defaultUserQueryLimit = defaultUserQueryLimit;
        this.maxCacheSize = maxCacheSize;
        if (configs != null && !configs.isEmpty()) {
            validateConfigs(configs);
            populateLimits(configs, groupLimitProvider);
        } else {
            this.customLimits = Map.of();
        }
    }

    /**
     * Validate the given configurations.
     *
     * @param configs
     *            the configurations to validate
     */
    private void validateConfigs(Collection<UserLimitConfiguration> configs) {
        Set<String> userDns = new HashSet<>();
        for (UserLimitConfiguration config : configs) {
            // Verify that a user dn was given.
            String userDn = config.getUserDn();
            if (StringUtils.isBlank(userDn)) {
                throw new IllegalArgumentException("User query limit configuration given with blank user DN");
            }

            // Verify we have not seen a configuration with the user dn before.
            if (userDns.contains(userDn)) {
                throw new IllegalArgumentException("Multiple query limit configurations specified for user '" + userDn + "'");
            } else {
                userDns.add(userDn);
            }

            // Verify that if the user query limit was overridden, it is not negative.
            if (config.getQueryLimit() != null && config.getQueryLimit() < 0) {
                throw new IllegalArgumentException("Negative user query limit given for user '" + userDn + "'");
            }

            // Verify that no invalid group name patterns were provided.
            Map<String,Integer> groupLimits = config.getQueryLogicGroupLimits();
            if (groupLimits != null) {
                for (Map.Entry<String,Integer> entry : groupLimits.entrySet()) {
                    String groupPattern = entry.getKey();
                    if (StringUtils.isBlank(groupPattern)) {
                        throw new IllegalArgumentException("User group query limit configuration given with blank group pattern for user '" + userDn + "'");
                    }
                    if (!groupPattern.equals(QueryLimiterUtils.ASTERISK)) {
                        try {
                            Pattern.compile(groupPattern);
                        } catch (PatternSyntaxException e) {
                            throw new IllegalArgumentException("Invalid query logic group name pattern: " + groupPattern + " given for user " + userDn, e);
                        }
                    }
                    Integer limit = entry.getValue();
                    if (limit < 0) {
                        throw new IllegalArgumentException("Negative query logic group limit given for user '" + userDn + "': " + limit);
                    }
                }
            }
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
                    log.debug("Custom limits provided for user '{}' do not override any defaults, skipping.", userDn);
                }
                continue;
            }

            // If the custom query limit is null or less than zero, use the default query limit.
            if (customQueryLimit == null || customQueryLimit < 0) {
                if (log.isDebugEnabled()) {
                    log.trace("Using default user query limit of {} for user '{}'", defaultUserQueryLimit, userDn);
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
}
