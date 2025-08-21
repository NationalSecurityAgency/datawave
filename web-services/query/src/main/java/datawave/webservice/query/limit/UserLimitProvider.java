package datawave.webservice.query.limit;

import java.util.Collection;
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
 * This class is responsible for extracting the limits from {@link UserLimitConfiguration} instances and then identifying the query limits to enforce for
 * individual users, overridden or otherwise.
 */
public class UserLimitProvider {

    private static final Logger log = Logger.getLogger(UserLimitProvider.class);

    private final int defaultUserQueryLimit;

    // Cache of user dns to their query limits.
    private final Cache<String,UserQueryLimit> cache = Caffeine.newBuilder().build();

    // Map of user dns to configured query limits.
    private Map<String,UserQueryLimit> configuredLimits;

    UserLimitProvider(int defaultUserQueryLimit, Collection<UserLimitConfiguration> configs) {
        this.defaultUserQueryLimit = defaultUserQueryLimit;
        if (configs != null && !configs.isEmpty()) {
            validateConfigs(configs);
            populateLimits(configs);
        } else {
            this.configuredLimits = Map.of();
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
                    if (!groupPattern.equals(QueryLimitConstants.ASTERISK)) {
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
    private void populateLimits(Collection<UserLimitConfiguration> configs) {
        Map<String,UserQueryLimit> configuredLimits = new HashMap<>();
        for (UserLimitConfiguration config : configs) {
            // If the query limit given for the user was null or less than zero, use the default user query limit.
            Integer queryLimit = config.getQueryLimit();
            if (queryLimit == null || queryLimit < 0) {
                queryLimit = defaultUserQueryLimit;
            }

            // Create a matchable set for the query logic group limits.
            SortedSet<MatchableLimit> groupLimits = new TreeSet<>();
            for (Map.Entry<String,Integer> entry : config.getQueryLogicGroupLimits().entrySet()) {
                groupLimits.add(new MatchableLimit(entry.getKey(), entry.getValue()));
            }

            configuredLimits.put(config.getUserDn(), UserQueryLimit.fromConfig(config.getUserDn(), queryLimit, groupLimits));
        }
        this.configuredLimits = Map.copyOf(configuredLimits);
    }

    /**
     * Return the best matching {@link UserQueryLimit} to use when enforcing limits for the given user. If no match was found to a configured limit, then
     * default limits will be returned.
     *
     * @param userDn
     *            the user DN
     * @return the {@link QueryLogicGroupQueryLimit} if found
     */
    public UserQueryLimit getLimit(String userDn) {
        // Fetch the limits from the cache if present.
        UserQueryLimit limit = cache.getIfPresent(userDn);
        if (limit == null) {
            // Otherwise attempt to get it from the map of configured limits.
            limit = configuredLimits.get(userDn);
            // If no match was found, use the default user query limit.
            if (limit == null) {
                limit = UserQueryLimit.fromDefaults(userDn, defaultUserQueryLimit);
            }

            // Put it in the cache.
            if (log.isTraceEnabled()) {
                log.trace("Caching user query limit for '" + userDn + "': " + limit);
            }
            cache.put(userDn, limit);
        }
        return limit;
    }
}
