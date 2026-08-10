package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang3.StringUtils;

public final class ValidationUtils {

    /**
     * Validate the given configuration
     *
     * @param config
     *            the configuration to validate
     */
    public static void validateQueryLimitConfig(QueryLimitConfiguration config) {
        if (config != null) {
            if (config.getDefaultUserQueryLimit() < 1) {
                throw new IllegalArgumentException("Default user query limit must be greater than 0");
            }
            if (config.getInternalCacheMaxSize() < 1) {
                throw new IllegalArgumentException("Internal cache max size must be greater than 0");
            }

            // No need to validate the default system query limit. Any negative value implies no limit for systems.

            List<QueryLogicGroupLimitConfiguration> queryLogicGroupConfigs = config.getQueryLogicGroupConfigs();
            if (queryLogicGroupConfigs != null && !queryLogicGroupConfigs.isEmpty()) {
                validateQueryLogicGroupConfigs(config.getQueryLogicGroupConfigs());
            }

            List<UserLimitConfiguration> userLimitConfigs = config.getUserConfigs();
            if (userLimitConfigs != null && !userLimitConfigs.isEmpty()) {
                validateUserLimitConfigs(userLimitConfigs);
            }

            List<SystemLimitConfiguration> systemLimitConfigs = config.getSystemConfigs();
            if (systemLimitConfigs != null && !systemLimitConfigs.isEmpty()) {
                validateSystemLimitConfigs(systemLimitConfigs, config.getInternalCacheMaxSize());
            }
        }
    }

    /**
     * Validate the given query logic group limit configurations.
     *
     * @param configs
     *            the configurations to validate
     */
    public static void validateQueryLogicGroupConfigs(Collection<QueryLogicGroupLimitConfiguration> configs) {
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
     * Validate the given user limit configurations.
     *
     * @param configs
     *            the configurations to validate
     */
    public static void validateUserLimitConfigs(Collection<UserLimitConfiguration> configs) {
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
     * Validate the given system limit configurations.
     *
     * @param configs
     *            the configurations to validate
     */
    public static void validateSystemLimitConfigs(Collection<SystemLimitConfiguration> configs, long maxCacheSize) {
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
            Matcher matcher = Matcher.getMatcher(systemPattern, maxCacheSize);

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

            // Safeguard against allowing a configuration to potentially set whether queries on a system counts against user limits to false for all
            // systems. Only allow this to be done for exact system names, or non-wildcard-only patterns.
            if (QueryLimitConstants.wildcardOnlyPattern.matcher(systemPattern).matches() && !config.getCountsAgainstUserLimit()) {
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

    private ValidationUtils() {
        throw new UnsupportedOperationException();
    }
}
